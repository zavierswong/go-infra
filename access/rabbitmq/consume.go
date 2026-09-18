package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/zavierswong/go-infra/metrics"
)

// Handler 处理一条消息。
//
// 返回 nil → 本包自动 Ack；
// 返回 error → 本包按 NackPolicy 决定 requeue 还是送死信。
//
// 参数是完整的 amqp.Delivery，而不是旧版那样的裸 []byte：
// 消费方需要 MessageId 做幂等、需要 Headers 做路由/追踪、
// 需要 Redelivered 判断是否重复投递 —— 这些在旧版 API 里全部拿不到。
//
// ctx 会在消费者停止时被取消，耗时的处理应尊重它以便优雅退出。
type Handler func(ctx context.Context, d amqp.Delivery) error

// NackPolicy 决定处理失败时如何处置消息。
type NackPolicy struct {
	// Requeue 为 true 时把失败消息重新入队。默认 false。
	//
	// 为什么默认 false：requeue 会把消息放回队列头部，如果处理逻辑是确定性失败
	// （比如报文格式错误），就会形成"失败 → 重入队 → 立刻再失败"的死循环，
	// 把 CPU 和一个消费者协程彻底占满。默认改为交给死信队列，
	// 让失败消息可见、可人工处理。需要带间隔的重试请用 DLX + 延迟队列。
	Requeue bool

	// MaxRequeue 基于 broker 写入的 x-death 计数限制重入队次数：
	// 计数达到该值后强制 requeue=false，把消息推进死信。
	// 0 表示不按次数限制，完全由 Requeue 决定。
	MaxRequeue int64
}

// ConsumerConfig 描述一个消费者。
type ConsumerConfig struct {
	// Queue 要消费的队列名（必填）。
	Queue string

	// ConsumerTag 消费者标签。为空时本包自动生成"队列名-随机串"，
	// 这样停止时可以精确取消该消费者而不影响同队列的其他消费者。
	ConsumerTag string

	// Prefetch 是 QoS 预取条数，0 表示使用 Client 的 Config.Prefetch。
	// 每个消费者持有独占通道，所以这个值不会被别的消费者覆盖。
	Prefetch int

	// Concurrency 是并发处理协程数。0 或 1 表示串行处理，严格保序。
	// 大于 1 时同一队列的消息会被并发处理，不再保证顺序，
	// 此时务必保证处理逻辑幂等（发布侧是 at-least-once）。
	Concurrency int

	// Exclusive 声明独占消费：禁止同队列的其他消费者接入。
	Exclusive bool

	// NoLocal 为 true 时不接收本连接自己发布的消息。
	// 注意 RabbitMQ 未实现该标志，设为 true 会被服务端拒绝。
	NoLocal bool

	// Args 额外消费参数，如优先级消费 {"x-priority": 5}。
	Args amqp.Table

	// NackPolicy 处理失败时的处置策略。
	NackPolicy NackPolicy

	// DrainTimeout 是停止时等待在途处理结束的上限，0 表示默认 30s。
	// 超时后本包放弃等待并返回，避免一个卡住的 handler 让进程无法退出。
	DrainTimeout time.Duration

	// OnError 接收消费者自身的错误（注册失败、Ack/Nack 失败、handler panic、
	// 以及 handler 返回的错误）。为 nil 时只记日志。
	// 该回调会被多个协程并发调用，需自行保证线程安全，且不要阻塞。
	OnError func(err error)

	// Handler 消息处理函数（必填）。
	Handler Handler
}

// Consumer 是一个消费者实例。
//
// 每个 Consumer 自己持有独占通道并自带重建循环：
// 通道被 broker 关闭（队列被删、网络抖动、406/404 等）时，
// 只有这个消费者会重新注册，不会波及其他生产者与其他消费者 ——
// 这正是旧版"所有角色共享一条通道"时做不到的。
type Consumer struct {
	client *Client
	cfg    ConsumerConfig

	mu     sync.Mutex
	cancel context.CancelFunc

	// running 防重复 Run：第二次 Run 会覆盖 cancel，让第一个消费循环
	// 失去唯一的停止手段（只能等客户端整体关闭）。
	running bool
}

const (
	defaultDrainTimeout = 30 * time.Second
	consumeMinBackoff   = 500 * time.Millisecond
	consumeMaxBackoff   = 30 * time.Second
)

// NewConsumer 校验配置并创建一个消费者（此时还未开始消费）。
func (c *Client) NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Queue == "" {
		return nil, fmt.Errorf("%w: ConsumerConfig.Queue 不能为空", ErrInvalidConfig)
	}
	if cfg.Handler == nil {
		return nil, fmt.Errorf("%w: ConsumerConfig.Handler 不能为空", ErrInvalidConfig)
	}
	if cfg.Concurrency < 0 {
		return nil, fmt.Errorf("%w: ConsumerConfig.Concurrency 不能为负", ErrInvalidConfig)
	}
	if cfg.Prefetch == 0 {
		cfg.Prefetch = c.cfg.Prefetch
	}
	if cfg.ConsumerTag == "" {
		cfg.ConsumerTag = fmt.Sprintf("%s-%s", cfg.Queue, newMessageID())
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = defaultDrainTimeout
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}
	return &Consumer{client: c, cfg: cfg}, nil
}

// Consume 是"创建并运行消费者"的便捷写法（阻塞）。
//
// 需要独立控制生命周期（先启动、稍后停止）时请用 NewConsumer + Run。
func (c *Client) Consume(ctx context.Context, cfg ConsumerConfig) error {
	sub, err := c.NewConsumer(cfg)
	if err != nil {
		return err
	}
	return sub.Run(ctx)
}

// Run 开始消费并阻塞，直到 ctx 被取消、调用了 Close，或客户端被关闭。
//
// 通道级故障在内部自动恢复：重建通道 → 重新设置 Qos → 重新注册消费者，
// 期间调用方无需做任何事。因此"队列被误删后客户端永久瘫痪"这类问题不会发生。
//
// 每个实例只能调用一次：第二次调用会立即返回错误 —— 早先允许重复调用，
// 后一次 Run 会覆盖 cancel 字段，使第一个消费循环再也停不下来
// （要等到客户端整体关闭，其独占通道也随之泄漏）。
func (sub *Consumer) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sub.mu.Lock()
	if sub.running {
		sub.mu.Unlock()
		return fmt.Errorf("%w: Run 只能调用一次（queue=%s）", ErrConsume, sub.cfg.Queue)
	}
	sub.running = true
	sub.cancel = cancel
	sub.mu.Unlock()

	backoff := consumeMinBackoff
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if sub.client.Closed() {
			return nil
		}

		err := sub.consumeOnce(ctx)
		if err == nil || ctx.Err() != nil {
			return nil
		}

		sub.report(fmt.Errorf("消费者 %s 中断，%s 后重新注册: %w", sub.cfg.Queue, backoff, err))

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		if backoff < consumeMaxBackoff {
			backoff *= 2
			if backoff > consumeMaxBackoff {
				backoff = consumeMaxBackoff
			}
		}
	}
}

// Close 停止消费者。可重复调用。
func (sub *Consumer) Close() error {
	sub.mu.Lock()
	cancel := sub.cancel
	sub.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

// consumeOnce 在一条独占通道上完成"设置 Qos → 注册 → 收消息"的完整生命周期；
// 通道失效时返回错误，由 Run 决定重试。
func (sub *Consumer) consumeOnce(ctx context.Context) error {
	ch, err := sub.client.channel(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	if sub.cfg.Prefetch > 0 {
		if err := ch.Qos(sub.cfg.Prefetch, 0, false); err != nil {
			return fmt.Errorf("%w: 设置 Qos(prefetch=%d) 失败: %v", ErrConsume, sub.cfg.Prefetch, err)
		}
	}

	deliveries, err := ch.Consume(
		sub.cfg.Queue, sub.cfg.ConsumerTag,
		false, // autoAck 恒为 false：手动确认才能保证"处理完才丢消息"
		sub.cfg.Exclusive, sub.cfg.NoLocal, false, sub.cfg.Args,
	)
	if err != nil {
		return fmt.Errorf("%w: 注册消费者 %s 失败: %v", ErrConsume, sub.cfg.Queue, err)
	}

	sub.client.log.Infof(ctx, "消费者已就绪：queue=%s tag=%s prefetch=%d concurrency=%d",
		sub.cfg.Queue, sub.cfg.ConsumerTag, sub.cfg.Prefetch, sub.cfg.Concurrency)

	// sem 限制并发处理数；handlerWG 保证退出前在途消息都处理完。
	sem := make(chan struct{}, sub.cfg.Concurrency)
	var handlerWG sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			sub.waitHandlers(&handlerWG)
			return nil

		case d, ok := <-deliveries:
			if !ok {
				// 通道被 broker 关闭（队列被删、连接抖动、协议错误等）
				sub.waitHandlers(&handlerWG)
				if ctx.Err() != nil || sub.client.Closed() {
					return nil
				}
				return fmt.Errorf("%w: 投递通道已关闭（queue=%s）", ErrConsume, sub.cfg.Queue)
			}

			sem <- struct{}{}
			handlerWG.Add(1)
			go func(d amqp.Delivery) {
				defer func() {
					<-sem
					handlerWG.Done()
				}()
				sub.handle(ctx, d)
			}(d)
		}
	}
}

// waitHandlers 等待在途处理结束，超时则记录告警后放弃。
func (sub *Consumer) waitHandlers(wg *sync.WaitGroup) {
	if !waitGroupTimeout(wg, sub.cfg.DrainTimeout) {
		sub.client.log.Warnf(context.Background(),
			"消费者 %s 停止时仍有处理协程未结束（已等待 %s）", sub.cfg.Queue, sub.cfg.DrainTimeout)
	}
}

// handle 执行一次处理并按结果确认消息。
//
// 本方法会产出**两个**事件：
//   - consume：handler 的执行结果与耗时。它的错误率直接对应"业务处理失败率"；
//   - ack 或 nack：确认动作本身的结果与耗时。
//
// 之所以拆成两个而不是合成一个：nack 的两种含义（requeue 稍后重试 / 不 requeue
// 丢弃或进死信）必须能分开统计，否则"消息被丢弃"会被淹没在"重新入队"里 ——
// 而前者是真正需要立刻告警的。
func (sub *Consumer) handle(ctx context.Context, d amqp.Delivery) {
	detail := consumeDetail(sub.cfg.Queue, d)

	handleStart := time.Now()
	handlerErr := sub.safeHandle(ctx, d)
	sub.client.observe(metrics.OpConsume, handleStart, handlerErr, detail)

	if handlerErr == nil {
		ackStart := time.Now()
		ackErr := d.Ack(false)
		sub.client.observe(metrics.OpAck, ackStart, ackErr, detail)
		if ackErr != nil {
			sub.report(fmt.Errorf("queue=%s message_id=%s: Ack 失败: %w", sub.cfg.Queue, d.MessageId, ackErr))
		}
		return
	}

	requeue := sub.cfg.NackPolicy.Requeue
	attempts := deadLetterCount(d)
	if sub.cfg.NackPolicy.MaxRequeue > 0 && attempts >= sub.cfg.NackPolicy.MaxRequeue {
		requeue = false
	}

	// requeue 的决定与已死信次数必须进 Detail：它们决定了这条消息是
	// "稍后还会重试"还是"已经彻底丢了"，只看 nack 计数无法区分。
	nackDetail := fmt.Sprintf("%s requeue=%v x-death=%d", detail, requeue, attempts)

	nackStart := time.Now()
	nackErr := d.Nack(false, requeue)
	sub.client.observe(metrics.OpNack, nackStart, nackErr, nackDetail)

	if nackErr != nil {
		sub.report(fmt.Errorf("queue=%s message_id=%s: Nack 失败: %w（原处理错误: %v）",
			sub.cfg.Queue, d.MessageId, nackErr, handlerErr))
		return
	}
	sub.report(fmt.Errorf("queue=%s message_id=%s requeue=%v x-death=%d: 处理失败: %w",
		sub.cfg.Queue, d.MessageId, requeue, attempts, handlerErr))
}

// consumeDetail 描述一条被处理的消息，用于归因。
//
// 只放队列名、消息 ID 与重投标记，**不放消息体** ——
// 消息体是业务数据，而 Detail 会进日志（与 redis 包只取键名是同一个取舍）。
//
// Redelivered 值得单独标出来：它持续为 true 通常意味着有消息在反复重投，
// 是"毒消息"最直接的信号，比看 nack 计数更早。
func consumeDetail(queue string, d amqp.Delivery) string {
	redelivered := ""
	if d.Redelivered {
		redelivered = " redelivered=true"
	}
	return fmt.Sprintf("%s message_id=%s%s", queue, d.MessageId, redelivered)
}

// safeHandle 调用 handler 并把 panic 转成普通错误。
// 否则一个越界的下标就会让整个进程崩掉，且消息永远停在 unacked 状态。
func (sub *Consumer) safeHandle(ctx context.Context, d amqp.Delivery) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	return sub.cfg.Handler(ctx, d)
}

func (sub *Consumer) report(err error) {
	if err == nil {
		return
	}
	if sub.cfg.OnError != nil {
		sub.cfg.OnError(err)
		return
	}
	sub.client.log.Errorf(context.Background(), "%v", err)
}

// Pulled 是一条通过 Get 同步拉取到的消息。
//
// 它持有自己那条临时通道的所有权，因此调用方**必须**在
// 处理完消息后调用 Close 释放通道（推荐 defer p.Close()），
// 再在此之前调用 Ack 或 Nack —— 通道一旦释放，确认就无处可发了。
//
//	p, ok, err := cli.Get(ctx, "sms.send")
//	if err != nil { return err }
//	if !ok { return nil }        // 队列当前为空
//	defer p.Close()
//	if err := process(p.Delivery()); err != nil {
//	    return p.Nack(true)      // 重新入队
//	}
//	return p.Ack()
type Pulled struct {
	delivery amqp.Delivery
	channel  *amqp.Channel
	autoAck  bool
	// released 是原子量：Ack / Nack / Close 可能被并发调用（例如业务
	// goroutine 确认、收尾 goroutine 关闭通道），普通 bool 会构成数据竞争。
	released atomic.Bool

	// obs / instance 由 Get 注入，用于上报 ack / nack 事件。
	// Pulled 是低频路径（Get 天生低频），这里多带两个字段换取
	// "无论推还是拉，确认动作都能被观测到"的一致性。
	obs      metrics.Observer
	instance string
}

// Delivery 返回原始投递，可访问 Body 与全部元数据。
func (p *Pulled) Delivery() amqp.Delivery { return p.delivery }

// Body 是消息体的便捷访问器。
func (p *Pulled) Body() []byte { return p.delivery.Body }

// Ack 确认消息已被成功处理。autoAck 模式下是空操作。
func (p *Pulled) Ack() error {
	if p.autoAck || p.released.Load() {
		return nil
	}

	start := time.Now()
	err := p.delivery.Ack(false)
	p.observe(metrics.OpAck, start, err, "")
	return err
}

// Nack 拒绝消息，requeue 决定是否重新入队。autoAck 模式下是空操作。
//
// 注意 requeue=true 会把消息放回队头，确定性失败会形成死循环，
// 需要重试请改用死信队列配合延迟。
func (p *Pulled) Nack(requeue bool) error {
	if p.autoAck || p.released.Load() {
		return nil
	}

	start := time.Now()
	err := p.delivery.Nack(false, requeue)
	p.observe(metrics.OpNack, start, err, fmt.Sprintf("requeue=%v", requeue))
	return err
}

// observe 上报确认动作。instance 为空说明 Pulled 不是由 Get 创建的（不该发生），
// 此时仍然上报，避免静默丢数据。
func (p *Pulled) observe(op metrics.Op, start time.Time, err error, detail string) {
	if p.obs == nil {
		return
	}
	p.obs.ObserveOp(metrics.Event{
		Component: metrics.ComponentRabbitMQ,
		Instance:  p.instance,
		Op:        op,
		Duration:  time.Since(start),
		Err:       err,
		Reason:    classifyErr(err),
		Detail:    detail,
	})
}

// Close 释放本次拉取占用的通道。可重复调用，且可并发调用：
// Swap 保证真正关闭通道的只会有一个调用方。
func (p *Pulled) Close() error {
	if p.released.Swap(true) {
		return nil
	}
	return p.channel.Close()
}

// Get 同步拉取一条消息（BasicGet），队列为空时 ok 为 false。
//
// 与 Consume 的区别：Get 是"拉"模型，由调用方控制节奏，适合低频、批处理场景；
// Consume 是"推"模型，broker 主动投递，是常规选择。
//
// 实现上每次调用会临时开一条通道（Get 天生低频，这样最不容易出错）。
// 返回的 *Pulled 持有该通道，务必按 Pulled 的文档调用 Ack/Nack 与 Close。
func (c *Client) Get(ctx context.Context, queue string, autoAck bool) (*Pulled, bool, error) {
	ch, err := c.channel(ctx)
	if err != nil {
		return nil, false, err
	}

	d, ok, err := ch.Get(queue, autoAck)
	if err != nil {
		_ = ch.Close()
		return nil, false, fmt.Errorf("%w: 从队列 %s 拉取消息失败: %v", ErrConsume, queue, err)
	}
	if !ok {
		// 没拉到消息就不必占着通道
		_ = ch.Close()
		return nil, false, nil
	}
	return &Pulled{
		delivery: d,
		channel:  ch,
		autoAck:  autoAck,
		obs:      c.obs,
		instance: c.cfg.Name,
	}, true, nil
}

// QueueDepth 返回队列中待消费的消息数与消费者数，便于做积压监控。
func (c *Client) QueueDepth(ctx context.Context, queue string) (messages, consumers int, err error) {
	ch, err := c.channel(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = ch.Close() }()

	q, err := ch.QueueInspect(queue)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: 查询队列 %s 失败: %v", ErrTopology, queue, err)
	}
	return q.Messages, q.Consumers, nil
}

// deadLetterCount 汇总 broker 写入 x-death 头里的死信次数。
// 消息每被死信转发一次，broker 就会追加一条 x-death 记录。
func deadLetterCount(d amqp.Delivery) int64 {
	raw, ok := d.Headers["x-death"]
	if !ok {
		return 0
	}
	deaths, ok := raw.([]any)
	if !ok {
		return 0
	}
	var total int64
	for _, item := range deaths {
		entry, ok := item.(amqp.Table)
		if !ok {
			continue
		}
		if count, ok := entry["count"].(int64); ok {
			total += count
		}
	}
	return total
}

// errHandlerMissing 仅用于文档化可能的错误来源，避免 tests 里的空指针隐患。
var errHandlerMissing = errors.New("rabbitmq: Handler 未设置")
