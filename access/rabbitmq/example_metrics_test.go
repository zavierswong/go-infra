package rabbitmq_test

// 本文件是**可编译**的 metrics 用法示例。
//
// 与 example_test.go 一样不带 `// Output:`，`go test` 只做编译检查、不执行，
// 因此不需要 broker 在跑 —— 文档代码不会随 API 演进而失效。

import (
	"context"
	"log"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/zavierswong/go-infra/access/rabbitmq"
	"github.com/zavierswong/go-infra/metrics"
)

// 连接与通道状态：把"连接健康"和"发布通道可用"分开看。
//
// 这两件事必须分开，否则故障定位会绕远路：
// 连接还在但发布通道被 broker 关掉（改队列参数、资源锁）时，
// Publish 会立刻失败，而连接级探活却一切正常。
func ExampleClient_Status() {
	c, err := rabbitmq.Open(rabbitmq.Config{
		Host:     "127.0.0.1",
		Port:     5672,
		VHost:    "app_vhost",
		Username: "app",
		Password: "123456",

		// Name 是本实例的标识，与 Exchange.Name / Queue.Name 无关。
		Name: "order-mq",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	st := c.Status()
	log.Printf("instance=%s host=%s vhost=%s", st.Instance, st.Host, st.VHost)

	if st.Closed {
		log.Println("客户端已关闭")
	}
	if !st.Connected {
		// 连接断了：重连计数（Op = reconnect）应当同步开始上涨。
		log.Println("连接不可用")
	}
	if !st.PublishChannelReady {
		// 连接健康但发布通道不可用 → 下一次 Publish 会触发通道重建
		// （Op = channel_rebuild），这条曲线能提前暴露"通道在反复重建"。
		log.Println("发布通道不可用，下一次发布将触发重建")
	}

	// 队列积压：Ready 是"待投递"，Unacked 是"已投递但未确认"。
	// Unacked 长期不降通常意味着消费者卡死或没在 ack。
	ready, unacked, err := c.QueueDepth(context.Background(), "order.created")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("ready=%d unacked=%d", ready, unacked)
}

// 接入事件流：一条普通消息会产生**两条** Event。
//
// 之所以要心里有数：Observer 必须是非阻塞的，缓冲区要按
// "消息数 × 2" 来估。把消费与确认拆成两条事件，
// 才能分别回答"处理不动"和"确认不了"两个不同的问题。
func ExampleClient_observer() {
	obs := metrics.ObserverFunc(func(e metrics.Event) {
		if !e.IsError() {
			return
		}
		log.Printf("op=%s instance=%s 耗时=%s reason=%s err=%v detail=%s",
			e.Op, e.Instance, e.Duration, e.Reason, e.Err, e.Detail)
	})

	c, err := rabbitmq.Open(rabbitmq.Config{
		Host:     "127.0.0.1",
		Port:     5672,
		VHost:    "app_vhost",
		Username: "app",
		Password: "123456",
		Name:     "order-mq",
		Observer: obs,

		// 开确认后才能区分"已落 broker"与"发出去了但没人收"。
		Confirm: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	// 不可路由的消息会被 broker 退回（Op = return，Reason = rejected）。
	// mandatory 与 confirm 同时打开才拿得到这个信号。
	c.SetReturnHandler(func(r amqp091.Return) {
		log.Printf("消息不可路由: exchange=%s routingKey=%s code=%d %s",
			r.Exchange, r.RoutingKey, r.ReplyCode, r.ReplyText)
	})

	ctx := context.Background()

	// 发布 → Op = "publish"
	if err := c.PublishToQueue(ctx, "order.created", []byte(`{"id":1}`),
		rabbitmq.WithMandatory()); err != nil {
		log.Printf("发布失败: %v", err)
	}

	// 延迟发布 → Op = "publish"，Detail 里会带上 " (延迟 30s)"
	if err := c.PublishDelay(ctx, []byte(`{"id":2}`), 30*time.Second,
		rabbitmq.ToQueue("order.created")); err != nil {
		log.Printf("延迟发布失败: %v", err)
	}

	// 消费侧的事件流（具体消费写法见 ExampleClient_NewConsumer）：
	//   consume → handle 的耗时与结果；
	//   ack     → 处理成功并确认；
	//   nack    → 处理失败。Detail 里带 requeue 与 x-death 次数，
	//             **x-death 持续增长是"毒消息在反复重投"最早、最直接的信号**。
	// 把 consume 与 ack/nack 拆成两条事件，才能分别回答
	// "处理不动"（consume 变慢）和"确认不了"（ack 报错）这两个不同的问题。
}
