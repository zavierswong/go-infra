// Package rabbitmq 封装 RabbitMQ（AMQP 0-9-1）的连接、拓扑、发布与消费。
//
// # 快速开始
//
//	cfg := rabbitmq.Config{
//	    Host: "127.0.0.1", Port: 5672, VHost: "app_vhost",
//	    Username: "app", Password: "123456",
//	    Exchanges: []rabbitmq.Exchange{
//	        {Name: "order", Kind: rabbitmq.ExchangeTopic, Durable: true},
//	    },
//	    Queues: []rabbitmq.Queue{
//	        {Name: "order.created", Durable: true},
//	    },
//	    Bindings: []rabbitmq.Binding{
//	        {Queue: "order.created", Exchange: "order", RoutingKey: "order.created.#"},
//	    },
//	}
//	cli, err := rabbitmq.Open(cfg)
//	if err != nil { return err }
//	defer cli.Close()
//
// # 能力范围
//
//   - 路由模式：direct / fanout / topic / headers 四种交换机全部支持；
//   - 数据可靠性：持久化交换机与队列、持久化消息、publisher confirm、
//     mandatory + basic.return 退回、消费者手动 Ack、死信队列（DLX）；
//   - 高级特性：延迟消息（DLX + 队列级 TTL）、优先级队列、quorum 队列、
//     lazy 队列、备选交换机、BasicGet 同步拉取、单活消费者参数、
//     消息 headers / MessageId / CorrelationId 全量透传；
//   - 故障恢复：连接级与**通道级**双重自愈，通道被 broker 关闭后自动重建并重放拓扑。
//
// # 与旧版 API 的关系
//
// 本文件的 RabbitMQ / Get / 旧签名 Publish 等仅为平滑迁移保留，
// 均已标记 Deprecated，且修正了原先会静默丢消息的语义。新代码请直接用 Open。
package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsumerHandler 是旧版的消息处理函数签名（只拿到裸 body）。
//
// Deprecated: 请改用 Handler，它能拿到消息的全部元数据。
type ConsumerHandler func(ctx context.Context, body []byte) error

// RabbitMQ 是旧版的客户端类型，现在只是 Client 的兼容包装。
//
// Deprecated: 请改用 Open 返回的 *Client —— 后者支持多实例、通道隔离与通道级自愈。
type RabbitMQ struct {
	*Client
}

var (
	legacyMu       sync.Mutex
	legacyInstance *RabbitMQ
)

// Get 返回进程内共享的 RabbitMQ 客户端（懒初始化）。
//
// Deprecated: 单例有三处硬伤 —— 多租户/多集群无法共存、并行测试互相污染、
// 首次传入的 cfg 会静默决定一切而后续调用的 cfg 被忽略。
//
// 与旧版的行为差异：初始化失败时**返回错误**，而不是在 sync.Once 里
// 无限重连把首个调用方永久阻塞。重试次数由 Config.DialAttempts 控制；
// 失败也不会被永久固化（下次调用会重新尝试）。
func Get(cfg Config) (*RabbitMQ, error) {
	legacyMu.Lock()
	defer legacyMu.Unlock()

	if legacyInstance != nil {
		return legacyInstance, nil
	}
	cli, err := Open(cfg)
	if err != nil {
		return nil, err
	}
	legacyInstance = &RabbitMQ{Client: cli}
	return legacyInstance, nil
}

// Close 关闭全局客户端并重置单例，使其可以再次初始化。
//
// Deprecated: 改用 (*Client).Close。
//
// 不重置全局变量的话，Close 之后 Get 会返回一个**已关闭**的客户端，
// 而后续调用又因为"实例已存在"直接复用它 —— 整个进程再也无法恢复。
func Close() error {
	legacyMu.Lock()
	inst := legacyInstance
	legacyInstance = nil
	legacyMu.Unlock()

	if inst == nil {
		return nil
	}
	// 内部用独立 ctx：Close 不应被调用方即将取消的请求 ctx 打断。
	return inst.Close(context.Background())
}

// Publish 是旧版的发布接口：Publish(queue, body, delay)。
//
// Deprecated: 改用 (*Client).PublishToQueue 或 PublishDelay。
//
// 语义变化（都是修复，不是破坏）：
//
//   - delay <= 0：走默认交换机投递到 queue，与旧版一致；
//     但现在会开启 publisher confirm 并等待 broker 确认，返回 nil 才代表真的收下了。
//   - delay > 0：旧版用 per-message TTL 假装延迟 —— 实测有消费者时消息 4ms 就到，
//     无消费者时消息在 TTL 到期后被直接丢弃，既不延迟又会丢消息。
//     新实现改走真实的延迟拓扑，因此要求配置里声明了 Config.Delay；
//     未声明时**返回错误**，而不是像旧版那样静默丢消息。
func (r *RabbitMQ) Publish(queue string, body []byte, delay int64) error {
	if delay > 0 {
		if r.Config().Delay == nil {
			return fmt.Errorf("%w: 旧版 Publish 的 delay 参数需要 Config.Delay 支持，"+
				"请声明 Config.Delay 并改用 PublishDelay", ErrInvalidConfig)
		}
		return r.PublishDelay(context.Background(), body,
			time.Duration(delay)*time.Millisecond, ToQueue(queue))
	}
	return r.PublishToQueue(context.Background(), queue, body)
}

// Channel 返回内部的发布通道。
//
// Deprecated: 把内部通道暴露出去，等于把客户端状态交给调用方随意破坏 ——
// 旧版正是因此出现"某个消费者注册失败时 ch.Close() 关掉了共享通道，
// 全体生产者一起瘫痪"。需要独占通道请用 NewConsumer。
func (r *RabbitMQ) Channel() (*amqp.Channel, error) {
	return r.ensure(context.Background())
}

// Consume 是旧版消费接口：返回裸的投递通道，由调用方自行 Ack/Nack。
//
// Deprecated: 该签名有两个无法在内部修补的问题 ——
// 一是拿不到消息元数据（MessageId/Headers/Redelivered），无法做幂等；
// 二是通道的归属无法交给本包管理，本包无法在通道被 broker 关闭后自动重建，
// 调用方会一直读一个永远关闭的通道。请改用 Consume / NewConsumer。
//
// 为不破坏现有调用方，这里仍然可用，但会为本次消费单独开一条通道
// （且该通道在调用方停止消费前无法被回收，这是该签名固有的代价）。
func (r *RabbitMQ) Consume(queue string, prefetch int) (<-chan amqp.Delivery, error) {
	ctx := context.Background()
	ch, err := r.channel(ctx)
	if err != nil {
		return nil, err
	}
	if prefetch > 0 {
		if err := ch.Qos(prefetch, 0, false); err != nil {
			_ = ch.Close()
			return nil, fmt.Errorf("%w: 设置 Qos 失败: %v", ErrConsume, err)
		}
	}
	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("%w: 注册消费者 %s 失败: %v", ErrConsume, queue, err)
	}
	return deliveries, nil
}

// StartConsumer 是旧版的并发消费入口。
//
// Deprecated: 改用 NewConsumer + Run，后者支持优雅停止、panic 兜底、
// 失败消息进入死信以及通道级故障自愈。
func (r *RabbitMQ) StartConsumer(ctx context.Context, name string, handler ConsumerHandler, concurrency int) {
	cfg := ConsumerConfig{
		Queue:       name,
		Concurrency: concurrency,
		Handler: func(ctx context.Context, d amqp.Delivery) error {
			return handler(ctx, d.Body)
		},
	}
	sub, err := r.NewConsumer(cfg)
	if err != nil {
		r.log.Errorf(ctx, "启动消费者 %s 失败: %v", name, err)
		return
	}
	go func() {
		if err := sub.Run(ctx); err != nil {
			r.log.Errorf(ctx, "消费者 %s 退出: %v", name, err)
		}
	}()
}

// Close 关闭连接。ctx 参数仅为兼容旧签名保留，不再使用。
//
// Deprecated: 改用 (*Client).Close。
func (r *RabbitMQ) Close(_ context.Context) error {
	return r.Client.Close()
}
