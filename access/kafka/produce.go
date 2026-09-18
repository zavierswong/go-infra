package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/zavierswong/go-infra/metrics"
)

// ProduceOption 定制单次发布。所有选项都是可选叠加的。
type ProduceOption func(*produceOptions)

type produceOptions struct {
	key       []byte
	headers   map[string][]byte
	partition int32 // 负数 = 不指定，交给分区器
	timestamp time.Time
}

func newProduceOptions() produceOptions {
	return produceOptions{partition: -1}
}

func (o *produceOptions) validate() error {
	if o.partition < -1 {
		return fmt.Errorf("%w: partition %d 非法（负数只允许 -1，即交给分区器）",
			ErrInvalidConfig, o.partition)
	}
	return nil
}

// WithKey 设置消息键。
//
// 键决定分区（默认按 hash）：**同一键的消息恒定进入同一分区，分区内有序**。
// 需要按业务实体保序（如同一订单的状态变更）时，必须用订单 ID 做键。
func WithKey(key []byte) ProduceOption {
	return func(o *produceOptions) { o.key = key }
}

// WithHeaders 附加自定义 headers，常用于链路追踪透传。
func WithHeaders(h map[string][]byte) ProduceOption {
	return func(o *produceOptions) {
		if o.headers == nil {
			o.headers = make(map[string][]byte, len(h))
		}
		for k, v := range h {
			o.headers[k] = v
		}
	}
}

// WithPartition 直接指定分区，跳过分区器。
// 只在明确知道自己在做什么时使用（如重放、补偿）；常规路径请用 WithKey。
func WithPartition(p int32) ProduceOption {
	return func(o *produceOptions) { o.partition = p }
}

// WithTimestamp 覆盖记录时间戳，默认取发送时刻。
func WithTimestamp(t time.Time) ProduceOption {
	return func(o *produceOptions) { o.timestamp = t }
}

// buildRecord 构造一条 kgo.Record。
func (c *Client) buildRecord(topic string, body []byte, o *produceOptions) *kgo.Record {
	rec := &kgo.Record{
		Topic:     topic,
		Key:       o.key,
		Value:     body,
		Partition: o.partition,
	}
	if o.timestamp.IsZero() {
		rec.Timestamp = time.Now()
	} else {
		rec.Timestamp = o.timestamp
	}
	if len(o.headers) > 0 {
		rec.Headers = make([]kgo.RecordHeader, 0, len(o.headers))
		for k, v := range o.headers {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: v})
		}
	}
	return rec
}

// produceDetail 描述一次投递的目标，用于事件归因。
func produceDetail(topic string, o *produceOptions, n int) string {
	if o.partition >= 0 {
		if n > 1 {
			return fmt.Sprintf("%s[partition=%d] x%d", topic, o.partition, n)
		}
		return fmt.Sprintf("%s[partition=%d]", topic, o.partition)
	}
	if n > 1 {
		return fmt.Sprintf("%s x%d", topic, n)
	}
	return topic
}

// Produce 同步投递一条消息，等 broker 确认（acks 生效）后才返回。
//
// 默认语义：幂等生产（重试不会重复）、批量攒发（后台自动合并小消息）、
// 送达总时长受 Config.DeliveryTimeout（默认 30s）约束。
//
// 返回值：
//   - nil：broker 已按 acks 语义确认收下；
//   - ErrProduce：投递失败，可能已送达也可能没有 —— errors.As 取出的
//     *kerr.Error 会说明具体原因（RecordTooLarge / UnknownTopicOrPartition 等）；
//   - ErrClosed / ErrInvalidConfig：客户端状态或参数问题。
//
// 注意 ctx 的作用范围：ctx 只约束"入队前"——记录一旦进入发送缓冲，
// ctx 取消不会中断投递（幂等生产的取消会造成重复，franz-go 默认禁止）。
// 需要放弃长时间未确认的记录时，靠 Config.DeliveryTimeout 而不是 ctx。
func (c *Client) Produce(ctx context.Context, topic string, body []byte, opts ...ProduceOption) error {
	o := newProduceOptions()
	for _, fn := range opts {
		fn(&o)
	}
	if err := o.validate(); err != nil {
		return err // 参数错误不计事件：它连一次投递尝试都不算
	}
	detail := produceDetail(topic, &o, 1)

	rec := c.buildRecord(topic, body, &o)
	errCh := make(chan error, 1)

	start := time.Now()
	c.cli.Produce(ctx, rec, func(_ *kgo.Record, err error) { errCh <- err })

	select {
	case err := <-errCh:
		err = c.wrapProduceErr(err)
		c.observe(metrics.OpPublish, start, err, detail)
		return err
	case <-ctx.Done():
		// 记录可能仍在缓冲里等待送达（ctx 只约束入队前），
		// 因此这里上报的是"调用方放弃了等待"，错误归类为 canceled/timeout。
		c.observe(metrics.OpPublish, start, ctx.Err(), detail)
		return ctx.Err()
	}
}

// ProduceAsync 异步投递一条消息：写入发送缓冲立即返回，确认走 promise 回调。
//
// 这是高吞吐路径 —— 调用方不等待网络往返，真正的并发度由 franz-go 的
// 缓冲 + 攒批 + 每 broker 并发请求撑起。背压机制：发送缓冲满时本调用会阻塞，
// 直到有空位（有界缓冲，防止内存无限堆积）。
//
// promise 在客户端的 IO 协程里被调用，**绝不能阻塞**；只需要结果通知时，
// 往带缓冲的 channel 里投一下就行。promise 的 err 语义与 Produce 的返回值一致。
//
// 投递是否成功完全以 promise 的 err 为准；本方法返回非 nil 时消息一定没进缓冲。
func (c *Client) ProduceAsync(
	ctx context.Context,
	topic string,
	body []byte,
	promise func(*kgo.Record, error),
	opts ...ProduceOption,
) error {
	o := newProduceOptions()
	for _, fn := range opts {
		fn(&o)
	}
	if err := o.validate(); err != nil {
		return err
	}
	detail := produceDetail(topic, &o, 1)

	rec := c.buildRecord(topic, body, &o)
	start := time.Now()

	c.cli.Produce(ctx, rec, func(r *kgo.Record, err error) {
		err = c.wrapProduceErr(err)
		c.observe(metrics.OpPublish, start, err, detail)
		if promise != nil {
			promise(r, err)
		}
	})
	return nil
}

// ProduceBatch 批量投递，等**全部**记录被确认后才返回。
//
// 实现上是"全部异步入队 + 一次性等待"：批内记录并行送达各分区，
// 总耗时接近最慢的一条而不是逐条之和 —— 这是它比循环调用 Produce 快的原因。
// 攒批与压缩由客户端自动完成，调用方不需要关心批的边界。
//
// 返回值是全部失败错误的聚合（errors.Join，成功则为 nil）；
// 整批只产生 1 个 publish 事件，Detail 形如 "orders x500"。
func (c *Client) ProduceBatch(ctx context.Context, topic string, bodies [][]byte, opts ...ProduceOption) error {
	o := newProduceOptions()
	for _, fn := range opts {
		fn(&o)
	}
	if err := o.validate(); err != nil {
		return err
	}
	if len(bodies) == 0 {
		return nil
	}
	detail := produceDetail(topic, &o, len(bodies))

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	start := time.Now()

	for _, body := range bodies {
		rec := c.buildRecord(topic, body, &o)
		wg.Add(1)
		c.cli.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err == nil {
				return
			}
			mu.Lock()
			errs = append(errs, c.wrapProduceErr(err))
			mu.Unlock()
		})
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-ctx.Done():
		// 与 Produce 同理：ctx 不中断已入队的记录，只结束本次等待。
		c.observe(metrics.OpPublish, start, ctx.Err(), detail)
		return ctx.Err()
	}

	err := errors.Join(errs...)
	c.observe(metrics.OpPublish, start, err, detail)
	return err
}

// wrapProduceErr 把 franz-go 的投递错误包上 ErrProduce 哨兵。
// 用 %w 链接原始错误，errors.Is / errors.As 都能继续穿透。
func (c *Client) wrapProduceErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, kgo.ErrClientClosed) {
		return fmt.Errorf("%w: %v", ErrClosed, err)
	}
	return fmt.Errorf("%w: %w", ErrProduce, err)
}
