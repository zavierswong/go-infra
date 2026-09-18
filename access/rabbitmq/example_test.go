package rabbitmq_test

// 本文件是**可编译**的用法示例。
//
// 它们没有 `// Output:` 注释，因此 `go test` 只做**编译检查**、不会真正执行，
// 也就不需要 broker 在跑 —— 保证文档里的代码不会随 API 演进而失效。
//
// 需要真正跑通行为的示例请见 rabbitmq_test.go 里的集成用例（连真实 broker）。

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zavierswong/go-infra/access/rabbitmq"
)

// 连接、声明拓扑、发布、消费的最小完整流程。
func Example() {
	cli, err := rabbitmq.Open(rabbitmq.Config{
		Host:     "127.0.0.1",
		Port:     5672,
		VHost:    "app_vhost", // 不带前导 "/"
		Username: "app",
		Password: "123456",

		Exchanges: []rabbitmq.Exchange{
			{Name: "order", Kind: rabbitmq.ExchangeTopic, Durable: true},
		},
		Queues: []rabbitmq.Queue{
			{Name: "order.created", Durable: true},
		},
		Bindings: []rabbitmq.Binding{
			{Queue: "order.created", Exchange: "order", RoutingKey: "order.created.#"},
		},
	})
	if err != nil {
		log.Fatalf("连接 RabbitMQ 失败: %v", err)
	}
	defer func() { _ = cli.Close() }() // 可重复调用

	ctx := context.Background()

	// 发布：默认持久化 + publisher confirm；mandatory 让不可路由变成显式报错
	err = cli.Publish(ctx, []byte(`{"id":1}`),
		rabbitmq.ToExchange("order", "order.created.cn"),
		rabbitmq.WithMessageID("order-1"), // 消费方据此做幂等
		rabbitmq.WithMandatory(),
	)
	if err != nil {
		log.Fatalf("发布失败: %v", err)
	}

	// 消费：独占通道 + 自动 Ack/Nack + 通道级自愈
	sub, err := cli.NewConsumer(rabbitmq.ConsumerConfig{
		Queue:       "order.created",
		Prefetch:    10,
		Concurrency: 4,
		Handler: func(ctx context.Context, d amqp.Delivery) error {
			fmt.Printf("id=%s body=%s redelivered=%v\n",
				d.MessageId, d.Body, d.Redelivered)
			return nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = sub.Close() }()

	_ = sub.Run(ctx) // 阻塞直到 ctx 取消或 Close
}

// 点对点：不建交换机，直接投递到队列（走默认交换机）。
func ExampleToQueue() {
	cli, err := rabbitmq.Open(rabbitmq.Config{
		Host: "127.0.0.1", Port: 5672, VHost: "app_vhost",
		Username: "app", Password: "123456",
		Queues: []rabbitmq.Queue{{Name: "sms.send", Durable: true}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	_ = cli.PublishToQueue(context.Background(), "sms.send", []byte(`{"to":"138"}`))
}

// direct 交换机：routing key 精确匹配，一个队列一个业务类型。
func ExampleToExchange_direct() {
	cli, err := rabbitmq.Open(rabbitmq.Config{
		Host: "127.0.0.1", Port: 5672, VHost: "app_vhost",
		Username: "app", Password: "123456",
		Exchanges: []rabbitmq.Exchange{
			{Name: "sms", Kind: rabbitmq.ExchangeDirect, Durable: true},
		},
		Queues: []rabbitmq.Queue{
			{Name: "sms.send", Durable: true},
			{Name: "sms.report", Durable: true},
		},
		Bindings: []rabbitmq.Binding{
			{Queue: "sms.send", Exchange: "sms", RoutingKey: "send"},
			{Queue: "sms.report", Exchange: "sms", RoutingKey: "report"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	// routing key 必须完全等于 "send" 或 "report"
	_ = cli.Publish(context.Background(), []byte(`{}`),
		rabbitmq.ToExchange("sms", "send"))
}

// fanout 交换机：广播到所有绑定队列，routing key 必须为空。
func ExampleToExchange_fanout() {
	cli, err := rabbitmq.Open(rabbitmq.Config{
		Host: "127.0.0.1", Port: 5672, VHost: "app_vhost",
		Username: "app", Password: "123456",
		Exchanges: []rabbitmq.Exchange{
			{Name: "config.refresh", Kind: rabbitmq.ExchangeFanout, Durable: true},
		},
		Queues: []rabbitmq.Queue{
			{Name: "config.node.a", Durable: true},
			{Name: "config.node.b", Durable: true},
		},
		Bindings: []rabbitmq.Binding{
			// fanout 的 routing key 留空
			{Queue: "config.node.a", Exchange: "config.refresh"},
			{Queue: "config.node.b", Exchange: "config.refresh"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	_ = cli.Publish(context.Background(), []byte(`{}`),
		rabbitmq.ToExchange("config.refresh", ""))
}

// topic 交换机：routing key 以 "." 分段，"*" 匹配一段，"#" 匹配零到多段。
func ExampleToExchange_topic() {
	cli, err := rabbitmq.Open(rabbitmq.Config{
		Host: "127.0.0.1", Port: 5672, VHost: "app_vhost",
		Username: "app", Password: "123456",
		Exchanges: []rabbitmq.Exchange{
			{Name: "order", Kind: rabbitmq.ExchangeTopic, Durable: true},
		},
		Queues: []rabbitmq.Queue{
			{Name: "order.cn.pay", Durable: true},
			{Name: "order.all", Durable: true},
		},
		Bindings: []rabbitmq.Binding{
			{Queue: "order.cn.pay", Exchange: "order", RoutingKey: "order.*.cn.pay"},
			{Queue: "order.all", Exchange: "order", RoutingKey: "order.#"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	// 命中 order.cn.pay（`*` 恰好占 "created" 一段）与 order.all（`#` 覆盖其余）
	_ = cli.Publish(context.Background(), []byte(`{}`),
		rabbitmq.ToExchange("order", "order.created.cn.pay"))
}

// headers 交换机：按消息头匹配，routing key 被忽略；
// Binding.Args 里的 "x-match" 必须是 "all" 或 "any"。
func ExampleToExchange_headers() {
	cli, err := rabbitmq.Open(rabbitmq.Config{
		Host: "127.0.0.1", Port: 5672, VHost: "app_vhost",
		Username: "app", Password: "123456",
		Exchanges: []rabbitmq.Exchange{
			{Name: "notify", Kind: rabbitmq.ExchangeHeaders, Durable: true},
		},
		Queues: []rabbitmq.Queue{
			{Name: "notify.sms", Durable: true},
			{Name: "notify.email", Durable: true},
		},
		Bindings: []rabbitmq.Binding{
			{
				Queue: "notify.sms", Exchange: "notify",
				Args: amqp.Table{"x-match": "all", "channel": "sms"},
			},
			{
				Queue: "notify.email", Exchange: "notify",
				Args: amqp.Table{"x-match": "any", "channel": "email", "priority": "high"},
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	// routing key 传空串即可，路由完全由 headers 决定
	_ = cli.Publish(context.Background(), []byte(`{}`),
		rabbitmq.ToExchange("notify", ""),
		rabbitmq.WithHeaders(amqp.Table{"channel": "sms", "priority": "high"}))
}

// 可靠发布：mandatory 让「不可路由」从静默丢弃变成显式错误。
//
// 不开 mandatory 时，投递到不存在的队列会被 broker 直接丢弃，而 Publish 返回 nil。
func ExampleClient_Publish_mandatory() {
	cli, err := rabbitmq.Open(rabbitmq.Config{
		Host: "127.0.0.1", Port: 5672, VHost: "app_vhost",
		Username: "app", Password: "123456",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	err = cli.Publish(context.Background(), []byte(`{}`),
		rabbitmq.ToQueue("这个队列不存在"),
		rabbitmq.WithMandatory(),
	)
	if err != nil {
		// errors.Is(err, rabbitmq.ErrUnroutable) 为 true
		log.Printf("消息不可路由: %v", err)
	}
}

// 延迟消息：需要先在 Config.Delay 里声明档位（基于 DLX + 队列级 TTL，不依赖插件）。
func ExampleClient_PublishDelay() {
	cli, err := rabbitmq.Open(rabbitmq.Config{
		Host: "127.0.0.1", Port: 5672, VHost: "app_vhost",
		Username: "app", Password: "123456",
		Queues: []rabbitmq.Queue{
			{Name: "sms.send", Durable: true},
		},
		Delay: &rabbitmq.DelayConfig{
			Prefix: "sms", // → 交换机 "sms.delay"，队列 "sms.delay.5000"
			Delays: []time.Duration{
				5 * time.Second,
				30 * time.Second,
				5 * time.Minute,
			},
			DeadLetter: rabbitmq.DeadLetter{
				// Exchange 留空即默认交换机，RoutingKey 是目标队列名
				RoutingKey: "sms.send",
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	ctx := context.Background()

	// 两种等价写法
	_ = cli.PublishDelay(ctx, []byte(`{"to":"138"}`), 30*time.Second,
		rabbitmq.ToQueue("sms.send"))

	_ = cli.Publish(ctx, []byte(`{"to":"139"}`),
		rabbitmq.ToQueue("sms.send"),
		rabbitmq.WithDelay(30*time.Second))
}

// 消费者：并发度、失败处置策略、错误回调、优雅停止。
func ExampleClient_NewConsumer() {
	cli, err := rabbitmq.Open(rabbitmq.Config{
		Host: "127.0.0.1", Port: 5672, VHost: "app_vhost",
		Username: "app", Password: "123456",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	sub, err := cli.NewConsumer(rabbitmq.ConsumerConfig{
		Queue:    "order.created",
		Prefetch: 10,

		// 0 或 1 表示串行处理、严格保序；>1 时并发且不再保序（handler 必须幂等）
		Concurrency: 4,

		// 失败处置：默认 requeue=false，交给死信队列，避免确定性失败形成死循环
		NackPolicy: rabbitmq.NackPolicy{
			Requeue:    false,
			MaxRequeue: 3, // 基于 broker 的 x-death 计数限制重入队次数
		},

		// 会被多个协程并发调用，需线程安全且不能阻塞
		OnError: func(err error) {
			log.Printf("消费者错误: %v", err)
		},

		// 停止时等待在途处理结束的上限，0 表示默认 30s
		DrainTimeout: 30 * time.Second,

		Handler: func(ctx context.Context, d amqp.Delivery) error {
			// 返回 nil → 自动 Ack；返回 error → 按 NackPolicy 处置
			return nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// 通道级故障（404/406）由 Run 内部自动恢复：重建通道 → 重设 Qos → 重新注册
	go func() {
		if err := sub.Run(context.Background()); err != nil {
			log.Printf("消费者退出: %v", err)
		}
	}()

	// 优雅停止：取消消费者 → 等在途消息处理完（最多 DrainTimeout）
	defer func() { _ = sub.Close() }()
}

// 同步拉取：适合低频、批处理。返回的 Pulled 持有临时通道，
// 必须「先 Ack/Nack，再 Close」——通道一旦释放，确认就无处可发。
func ExampleClient_Get() {
	cli, err := rabbitmq.Open(rabbitmq.Config{
		Host: "127.0.0.1", Port: 5672, VHost: "app_vhost",
		Username: "app", Password: "123456",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	p, ok, err := cli.Get(context.Background(), "sms.send", false) // autoAck=false
	if err != nil {
		log.Fatal(err)
	}
	if !ok {
		return // 队列当前为空
	}
	defer func() { _ = p.Close() }()

	if len(p.Body()) == 0 {
		_ = p.Nack(true) // 重新入队
		return
	}
	_ = p.Ack()
}

// 注册异步退回回调：与 Publish 的同步判定互补，不会漏判。
func ExampleClient_SetReturnHandler() {
	cli, err := rabbitmq.Open(rabbitmq.Config{
		Host: "127.0.0.1", Port: 5672, VHost: "app_vhost",
		Username: "app", Password: "123456",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	cli.SetReturnHandler(func(r amqp.Return) {
		// 在库的派发协程里执行，不能阻塞太久
		log.Printf("消息被退回: rk=%s reply=%s(%d)",
			r.RoutingKey, r.ReplyText, r.ReplyCode)
	})

	cli.SetReturnHandler(nil) // 取消注册
}

// 死信队列：任何队列都可以把「处理失败 / 过期 / 超长被挤出」的消息转投到指定位置。
func ExampleDLXArgs() {
	args := rabbitmq.DLXArgs(rabbitmq.DeadLetter{
		Exchange:   "sms.dlx",  // 为空即默认交换机
		RoutingKey: "sms.dead", // 为空即沿用原 routing key
	})

	_ = rabbitmq.Queue{
		Name:    "sms.work",
		Durable: true,
		Args:    args,
	}
}

// 队列级扩展参数：quorum 队列（Raft 复制，数据安全性优于已废弃的 classic mirror）。
func ExampleQuorumArgs() {
	_ = rabbitmq.Queue{
		Name:    "order.critical",
		Durable: true, // quorum 队列必须 Durable 且非 Exclusive
		Args:    rabbitmq.QuorumArgs(),
	}
}

// 合并多种队列参数：amqp.Table 就是 map[string]any。
func ExampleQueue_args() {
	args := rabbitmq.MaxLengthArgs(100000)
	for k, v := range rabbitmq.DLXArgs(rabbitmq.DeadLetter{RoutingKey: "order.overflow"}) {
		args[k] = v
	}

	_ = rabbitmq.Queue{Name: "order.bulk", Durable: true, Args: args}
}

// 延迟拓扑的展开结果：DelayConfig 会被 normalize 并入 Config 的三组声明。
func ExampleDelayConfig_Topology() {
	d := rabbitmq.DelayConfig{
		Prefix: "sms",
		Delays: []time.Duration{5 * time.Second},
	}
	fmt.Println(d.ExchangeName())                          // sms.delay
	fmt.Println(d.QueueName(5 * time.Second))              // sms.delay.5000
	fmt.Println(rabbitmq.DelayRoutingKey(5 * time.Second)) // 5000

	exchanges, queues, bindings := d.Topology()
	fmt.Println(len(exchanges), len(queues), len(bindings))
}

// 健康探活与优雅关闭。
func ExampleClient_HealthCheck() {
	cli, err := rabbitmq.Open(rabbitmq.Config{
		Host: "127.0.0.1", Port: 5672, VHost: "app_vhost",
		Username: "app", Password: "123456",
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := cli.HealthCheck(ctx); err != nil {
		log.Printf("RabbitMQ 不可用: %v", err)
	}

	// 积压监控：待消费消息数 + 消费者数
	messages, consumers, err := cli.QueueDepth(ctx, "order.created")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("积压=%d 消费者=%d", messages, consumers)

	// Close 可重复调用，第二次起直接返回 nil；不会死锁
	_ = cli.Close()
	_ = cli.Close()
}
