package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// 本文件是基于**真实 broker** 的集成测试，覆盖四种路由模式、数据可靠性与高级特性。
//
// 默认连接本机开发环境的 RabbitMQ（vhost app_vhost），可用环境变量覆盖：
//
//	TEST_RABBITMQ_HOST / TEST_RABBITMQ_PORT / TEST_RABBITMQ_VHOST
//	TEST_RABBITMQ_USER / TEST_RABBITMQ_PASS
//
// 跳过策略：`go test -short` 会跳过全部集成用例；broker 连不上时也会跳过，
// 但跳过信息里会写明地址与如何覆盖，避免出现"什么都没跑却是绿的"。
// CI 上请用 `go test -tags=integration` 或至少不要加 -short，让用例真正执行。

// testRunID 让每次运行使用独立的拓扑名，避免并行运行或残留数据互相干扰。
var testRunID = newMessageID()

const testPrefix = "gointra.test."

func name(parts ...string) string {
	return testPrefix + testRunID + "." + strings.Join(parts, ".")
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// testConfig 返回指向测试 broker 的基础配置。
func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Host:         envOr("TEST_RABBITMQ_HOST", "127.0.0.1"),
		Port:         envInt("TEST_RABBITMQ_PORT", 5672),
		VHost:        envOr("TEST_RABBITMQ_VHOST", "app_vhost"),
		Username:     envOr("TEST_RABBITMQ_USER", "app"),
		Password:     envOr("TEST_RABBITMQ_PASS", "123456"),
		DialAttempts: 1, // 测试里失败要立刻暴露，不重试
		Confirm:      true,
		ReturnWindow: 300 * time.Millisecond,
		Prefetch:     1,
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// openClient 建立连接。
//
// 只有在 **broker 不可达** 时才跳过用例；拓扑声明之类的错误必须让用例失败 ——
// 否则会把真实缺陷（比如队列参数被 broker 拒绝）伪装成绿色，
// 那就是我们一直在避免的"假绿"。
func openClient(t *testing.T, cfg Config) *Client {
	t.Helper()
	if testing.Short() {
		t.Skip("集成测试需要真实 RabbitMQ，-short 模式下跳过")
	}

	if err := probeReachable(cfg); err != nil {
		t.Skipf("RabbitMQ 不可达（%s:%d vhost=%s）：%v\n"+
			"请启动 broker，或用 TEST_RABBITMQ_HOST/PORT/VHOST/USER/PASS 覆盖",
			cfg.Host, cfg.Port, cfg.VHost, err)
	}

	cli, err := Open(cfg)
	if err != nil {
		t.Fatalf("broker 可达但初始化失败（这是真实问题，不是环境问题）: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// probeReachable 只验证 TCP+握手+认证是否成功，不声明任何拓扑。
func probeReachable(cfg Config) error {
	cfg = cfg.normalize()
	dialCfg, err := cfg.dialConfig()
	if err != nil {
		return err
	}
	conn, err := amqp.DialConfig(cfg.amqpURI(), dialCfg)
	if err != nil {
		return err
	}
	return conn.Close()
}

// cleanup 在用例结束后删除本次创建的队列与交换机，避免污染开发环境的 broker。
func cleanup(t *testing.T, cli *Client, queues, exchanges []string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ch, err := cli.channel(ctx)
		if err != nil {
			return
		}
		defer func() { _ = ch.Close() }()
		for _, q := range queues {
			_, _ = ch.QueueDelete(q, false, false, false)
		}
		for _, e := range exchanges {
			_ = ch.ExchangeDelete(e, false, false)
		}
	})
}

// collect 启动一个消费者，把收到的投递送进返回的通道。
func collect(t *testing.T, cli *Client, queue string, concurrency int) (<-chan amqp.Delivery, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan amqp.Delivery, 16)
	sub, err := cli.NewConsumer(ConsumerConfig{
		Queue:       queue,
		Concurrency: concurrency,
		Handler: func(_ context.Context, d amqp.Delivery) error {
			out <- d
			return nil
		},
	})
	if err != nil {
		cancel()
		t.Fatalf("创建消费者失败: %v", err)
	}
	go func() { _ = sub.Run(ctx) }()
	t.Cleanup(cancel)
	return out, cancel
}

// expectNothing 断言在给定窗口内没有消息到达。
func expectNothing(t *testing.T, ch <-chan amqp.Delivery, window time.Duration, what string) {
	t.Helper()
	select {
	case d := <-ch:
		t.Fatalf("%s：不应收到消息，却收到 %q", what, d.Body)
	case <-time.After(window):
	}
}

// ---------------------------------------------------------------------------
// 四种路由模式
// ---------------------------------------------------------------------------

// TestDirectRouting direct 交换机：routing key 精确匹配。
func TestDirectRouting(t *testing.T) {
	q := name("direct.q")
	ex := name("direct.ex")
	cfg := testConfig(t)
	cfg.Exchanges = []Exchange{{Name: ex, Kind: ExchangeDirect}}
	cfg.Queues = []Queue{{Name: q}}
	cfg.Bindings = []Binding{{Queue: q, Exchange: ex, RoutingKey: "order.created"}}

	cli := openClient(t, cfg)
	cleanup(t, cli, []string{q}, []string{ex})
	got, _ := collect(t, cli, q, 1)

	// 与绑定一致 → 应该收到
	if err := cli.Publish(t.Context(), []byte(`{"id":1}`),
		ToExchange(ex, "order.created"),
		WithMessageID("direct-1"),
		WithHeaders(amqp.Table{"trace_id": "t-1"}),
	); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	d := recv(t, got, 5*time.Second, "direct 匹配发布")
	if string(d.Body) != `{"id":1}` {
		t.Errorf("消息体不符: %q", d.Body)
	}
	if d.MessageId != "direct-1" {
		t.Errorf("MessageId 未透传: %q", d.MessageId)
	}
	if trace, _ := d.Headers["trace_id"].(string); trace != "t-1" {
		t.Errorf("Headers 未透传: %v", d.Headers)
	}
	if d.DeliveryMode != amqp.Persistent {
		t.Errorf("默认应为持久化消息，实际 DeliveryMode=%d", d.DeliveryMode)
	}
	if d.ContentType != "application/json" {
		t.Errorf("ContentType 期望 application/json，实际 %q", d.ContentType)
	}

	// routing key 不匹配 → broker 会丢弃；配合 mandatory 才能拿到报错
	err := cli.Publish(t.Context(), []byte("x"), ToExchange(ex, "order.paid"), WithMandatory())
	if !errors.Is(err, ErrUnroutable) {
		t.Errorf("routing key 不匹配时开了 mandatory 应返回 ErrUnroutable，实际: %v", err)
	}
}

// TestFanoutBroadcast fanout 交换机：一条消息广播到所有绑定队列。
func TestFanoutBroadcast(t *testing.T) {
	q1, q2 := name("fanout.q1"), name("fanout.q2")
	ex := name("fanout.ex")
	cfg := testConfig(t)
	cfg.Exchanges = []Exchange{{Name: ex, Kind: ExchangeFanout}}
	cfg.Queues = []Queue{{Name: q1}, {Name: q2}}
	// fanout 绑定的 routing key 必须为空
	cfg.Bindings = []Binding{
		{Queue: q1, Exchange: ex},
		{Queue: q2, Exchange: ex},
	}

	cli := openClient(t, cfg)
	cleanup(t, cli, []string{q1, q2}, []string{ex})
	got1, _ := collect(t, cli, q1, 1)
	got2, _ := collect(t, cli, q2, 1)

	// routing key 对 fanout 无意义，随便填也能到达两个队列
	if err := cli.Publish(t.Context(), []byte("broadcast"), ToExchange(ex, "any.key")); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	for i, ch := range []<-chan amqp.Delivery{got1, got2} {
		d := recv(t, ch, 5*time.Second, fmt.Sprintf("fanout 队列 %d", i+1))
		if string(d.Body) != "broadcast" {
			t.Errorf("队列 %d 收到 %q，期望 broadcast", i+1, d.Body)
		}
	}
}

// TestTopicRouting topic 交换机：支持 "*"（一段）与 "#"（多段）通配。
func TestTopicRouting(t *testing.T) {
	qStar, qHash, qExact := name("topic.star"), name("topic.hash"), name("topic.exact")
	ex := name("topic.ex")
	cfg := testConfig(t)
	cfg.Exchanges = []Exchange{{Name: ex, Kind: ExchangeTopic}}
	cfg.Queues = []Queue{{Name: qStar}, {Name: qHash}, {Name: qExact}}
	cfg.Bindings = []Binding{
		{Queue: qStar, Exchange: ex, RoutingKey: "order.created.*"}, // 恰好三段
		{Queue: qHash, Exchange: ex, RoutingKey: "order.#"},         // 两段及以上
		{Queue: qExact, Exchange: ex, RoutingKey: "order.paid"},     // 精确
	}

	cli := openClient(t, cfg)
	cleanup(t, cli, []string{qStar, qHash, qExact}, []string{ex})
	star, _ := collect(t, cli, qStar, 1)
	hash, _ := collect(t, cli, qHash, 1)
	exact, _ := collect(t, cli, qExact, 1)

	// "order.created.cn" 应命中 order.created.* 与 order.#
	if err := cli.Publish(t.Context(), []byte("v1"), ToExchange(ex, "order.created.cn")); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if d := recv(t, star, 5*time.Second, "order.created.*"); string(d.Body) != "v1" {
		t.Errorf("star 队列收到 %q", d.Body)
	}
	if d := recv(t, hash, 5*time.Second, "order.#"); string(d.Body) != "v1" {
		t.Errorf("hash 队列收到 %q", d.Body)
	}
	expectNothing(t, exact, 300*time.Millisecond, "order.paid 队列不该收到 order.created.cn")

	// "order.created.cn.extra" 是四段，order.created.* 不该命中
	if err := cli.Publish(t.Context(), []byte("v2"), ToExchange(ex, "order.created.cn.extra")); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if d := recv(t, hash, 5*time.Second, "order.# 收四段"); string(d.Body) != "v2" {
		t.Errorf("hash 队列收到 %q", d.Body)
	}
	expectNothing(t, star, 300*time.Millisecond, "* 只匹配一段")
}

// TestHeadersRouting headers 交换机：按消息头匹配，routing key 被忽略。
func TestHeadersRouting(t *testing.T) {
	qAll, qAny := name("headers.all"), name("headers.any")
	ex := name("headers.ex")
	cfg := testConfig(t)
	cfg.Exchanges = []Exchange{{Name: ex, Kind: ExchangeHeaders}}
	cfg.Queues = []Queue{{Name: qAll}, {Name: qAny}}
	cfg.Bindings = []Binding{
		{
			Queue: qAll, Exchange: ex,
			Args: amqp.Table{"x-match": "all", "type": "sms", "level": "high"},
		},
		{
			Queue: qAny, Exchange: ex,
			Args: amqp.Table{"x-match": "any", "type": "sms", "level": "high"},
		},
	}

	cli := openClient(t, cfg)
	cleanup(t, cli, []string{qAll, qAny}, []string{ex})
	all, _ := collect(t, cli, qAll, 1)
	any, _ := collect(t, cli, qAny, 1)

	// 两个头都命中 → all 与 any 都收到
	if err := cli.Publish(t.Context(), []byte("both"),
		ToExchange(ex, "ignored"),
		WithHeaders(amqp.Table{"type": "sms", "level": "high"}),
	); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if d := recv(t, all, 5*time.Second, "x-match=all 双头命中"); string(d.Body) != "both" {
		t.Errorf("all 队列收到 %q", d.Body)
	}
	if d := recv(t, any, 5*time.Second, "x-match=any 双头命中"); string(d.Body) != "both" {
		t.Errorf("any 队列收到 %q", d.Body)
	}

	// 只命中一个头 → all 不收到，any 收到
	if err := cli.Publish(t.Context(), []byte("one"),
		ToExchange(ex, "ignored"),
		WithHeaders(amqp.Table{"type": "sms"}),
	); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if d := recv(t, any, 5*time.Second, "x-match=any 单头命中"); string(d.Body) != "one" {
		t.Errorf("any 队列收到 %q", d.Body)
	}
	expectNothing(t, all, 300*time.Millisecond, "x-match=all 单头时应不匹配")
}

// ---------------------------------------------------------------------------
// 数据可靠性
// ---------------------------------------------------------------------------

// TestMandatoryVersusSilentDrop 对比"静默丢弃"与"显式报错"两种语义。
//
// 这是旧版最隐蔽的坑：Publish 到一个不存在的队列返回 nil，
// 调用方以为发成功了，消息其实被 broker 直接丢掉。
func TestMandatoryVersusSilentDrop(t *testing.T) {
	cli := openClient(t, testConfig(t))
	ghost := name("ghost.queue")

	// 默认（未开 mandatory）：broker 静默丢弃，Publish 返回 nil
	if err := cli.Publish(t.Context(), []byte("x"), ToQueue(ghost)); err != nil {
		t.Fatalf("未开 mandatory 时应返回 nil（消息被静默丢弃），实际: %v", err)
	}

	// 开 mandatory：拿到明确的 ErrUnroutable
	err := cli.Publish(t.Context(), []byte("x"), ToQueue(ghost), WithMandatory())
	if !errors.Is(err, ErrUnroutable) {
		t.Fatalf("开了 mandatory 应返回 ErrUnroutable，实际: %v", err)
	}
	if !strings.Contains(err.Error(), "NO_ROUTE") {
		t.Logf("退回原因: %v", err)
	}
}

// TestPublishConfirmBrokerAccepted confirm 模式：broker 确认后 Publish 才返回 nil，
// 且能确认消息真的进入了队列。
func TestPublishConfirmBrokerAccepted(t *testing.T) {
	q := name("confirm.q")
	cfg := testConfig(t)
	cfg.Queues = []Queue{{Name: q}}
	cli := openClient(t, cfg)
	cleanup(t, cli, []string{q}, nil)

	const total = 50
	for i := 0; i < total; i++ {
		if err := cli.PublishToQueue(t.Context(), q, []byte(fmt.Sprintf("m-%d", i))); err != nil {
			t.Fatalf("第 %d 条发布失败: %v", i, err)
		}
	}

	msgs, _, err := cli.QueueDepth(t.Context(), q)
	if err != nil {
		t.Fatalf("查询队列深度失败: %v", err)
	}
	if msgs != total {
		t.Errorf("队列中应有 %d 条消息（confirm 已确认），实际 %d", total, msgs)
	}
}

// TestNackGoesToDeadLetter 消费失败时消息进入死信队列，并带 x-death 痕迹。
//
// 这是"失败消息不丢"的关键：旧版 handler 只拿到 []byte，无法记录失败原因，
// 也没有死信拓扑，消息被 nack 后就永久消失了。
func TestNackGoesToDeadLetter(t *testing.T) {
	workQ, dlq := name("dlx.work"), name("dlx.dead")
	cfg := testConfig(t)
	cfg.Queues = []Queue{
		{Name: workQ, Args: DLXArgs(DeadLetter{RoutingKey: dlq})},
		{Name: dlq},
	}
	cli := openClient(t, cfg)
	cleanup(t, cli, []string{workQ, dlq}, nil)

	dead := make(chan amqp.Delivery, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := cli.NewConsumer(ConsumerConfig{
		Queue: workQ,
		Handler: func(_ context.Context, d amqp.Delivery) error {
			return fmt.Errorf("模拟业务失败: %s", d.Body)
		},
		OnError: func(error) {}, // 预期内的失败，不用打日志
	})
	if err != nil {
		t.Fatalf("创建消费者失败: %v", err)
	}
	go func() { _ = sub.Run(ctx) }()

	dlSub, err := cli.NewConsumer(ConsumerConfig{
		Queue: dlq,
		Handler: func(_ context.Context, d amqp.Delivery) error {
			dead <- d
			return nil
		},
	})
	if err != nil {
		t.Fatalf("创建死信消费者失败: %v", err)
	}
	go func() { _ = dlSub.Run(ctx) }()

	if err := cli.PublishToQueue(t.Context(), workQ, []byte("会失败的消息"), WithMessageID("dlx-1")); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	select {
	case d := <-dead:
		if string(d.Body) != "会失败的消息" {
			t.Errorf("死信内容不符: %q", d.Body)
		}
		if d.MessageId != "dlx-1" {
			t.Errorf("死信应保留 MessageId，实际 %q", d.MessageId)
		}
		if _, ok := d.Headers["x-death"]; !ok {
			t.Errorf("死信消息应带 x-death 头，实际 headers: %v", d.Headers)
		} else if n := deadLetterCount(d); n == 0 {
			t.Errorf("x-death 计数应大于 0，实际 %d", n)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("消息未在 8s 内进入死信队列")
	}
}

// TestDelayMessage 延迟消息：延迟期间不可见，到期后才进入业务队列。
//
// 旧版用 per-message TTL 假装延迟，实测有消费者时 4ms 就到、
// 无消费者时到期被丢弃。这里验证真实延迟语义。
func TestDelayMessage(t *testing.T) {
	targetQ := name("delay.target")
	targetEx := name("delay.target.ex")
	delayPrefix := name("delay")

	const delay = 1 * time.Second
	cfg := testConfig(t)
	cfg.Exchanges = []Exchange{{Name: targetEx, Kind: ExchangeDirect}}
	cfg.Queues = []Queue{{Name: targetQ}}
	cfg.Bindings = []Binding{{Queue: targetQ, Exchange: targetEx, RoutingKey: "target"}}
	cfg.Delay = &DelayConfig{
		Prefix:     delayPrefix,
		Delays:     []time.Duration{delay},
		DeadLetter: DeadLetter{Exchange: targetEx, RoutingKey: "target"},
	}

	cli := openClient(t, cfg)
	cleanup(t,
		cli,
		[]string{targetQ, cfg.Delay.QueueName(delay)},
		[]string{targetEx, cfg.Delay.ExchangeName()},
	)
	got, _ := collect(t, cli, targetQ, 1)

	start := time.Now()
	if err := cli.PublishDelay(t.Context(), []byte("延迟 1 秒"), delay, ToQueue(targetQ)); err != nil {
		t.Fatalf("投递延迟消息失败: %v", err)
	}

	// 延迟期内必须收不到
	expectNothing(t, got, delay/2, "延迟消息在延迟期内")

	// 到期后应该收到
	d := recv(t, got, 6*time.Second, "延迟消息到期")
	elapsed := time.Since(start)
	if elapsed < delay-200*time.Millisecond {
		t.Errorf("消息只过了 %s 就到了，延迟未生效（期望 >= %s）", elapsed, delay)
	}
	if string(d.Body) != "延迟 1 秒" {
		t.Errorf("消息体不符: %q", d.Body)
	}
	t.Logf("延迟消息在 %s 后到达（预期 %s）", elapsed.Round(10*time.Millisecond), delay)
}

// TestDelayWithoutConfigRejected 未声明延迟拓扑时必须报错，而不是静默丢消息。
func TestDelayWithoutConfigRejected(t *testing.T) {
	cli := openClient(t, testConfig(t))
	err := cli.PublishDelay(t.Context(), []byte("x"), time.Second, ToQueue(name("whatever")))
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("未配置延迟拓扑时应返回 ErrInvalidConfig，实际: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 故障自愈
// ---------------------------------------------------------------------------

// TestChannelFailureSelfHeal 是本套测试最重要的回归用例。
//
// 旧版只监听 connection 的 NotifyClose，但 404/406 这类错误只会让 **channel**
// 被 broker 关闭，connection 依然健康 —— 于是重连永不触发，
// 客户端此后每次 Publish/Consume 都失败，等于永久瘫痪，只能重启进程。
//
// 这里直接制造一次 channel 级故障，然后验证后续发布仍能自愈成功。
func TestChannelFailureSelfHeal(t *testing.T) {
	q := name("heal.q")
	cfg := testConfig(t)
	cfg.Queues = []Queue{{Name: q}}
	cli := openClient(t, cfg)
	cleanup(t, cli, []string{q}, nil)

	ctx := t.Context()

	// 先确认正常可用
	if err := cli.PublishToQueue(ctx, q, []byte("before")); err != nil {
		t.Fatalf("故障前发布失败: %v", err)
	}

	// 制造 channel 级故障：用与已声明队列不一致的属性重复声明同名队列
	// → broker 回 406 PRECONDITION_FAILED 并关闭整条 channel，但连接不受影响。
	ch, err := cli.ensure(ctx)
	if err != nil {
		t.Fatalf("获取发布通道失败: %v", err)
	}
	if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err == nil {
		t.Fatal("属性不一致的重复声明本应触发 406")
	}

	deadline := time.Now().Add(2 * time.Second)
	for !ch.IsClosed() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !ch.IsClosed() {
		t.Fatal("通道应已被 broker 关闭")
	}
	t.Log("已制造 channel 级故障：发布通道被 broker 关闭，连接仍然健康")

	// 关键断言：旧版到这里就永久瘫痪了
	if err := cli.PublishToQueue(ctx, q, []byte("after")); err != nil {
		t.Fatalf("通道故障后自愈失败（旧版缺陷复现）: %v", err)
	}

	msgs, _, err := cli.QueueDepth(ctx, q)
	if err != nil {
		t.Fatalf("查询队列深度失败: %v", err)
	}
	if msgs != 2 {
		t.Errorf("队列中应有故障前/后各 1 条消息，实际 %d 条", msgs)
	}
}

// TestConsumerSurvivesMissingQueue 消费者注册到不存在的队列时，
// 应自行重试而不是把整个客户端拖垮。
func TestConsumerSurvivesMissingQueue(t *testing.T) {
	q := name("survive.q")
	cfg := testConfig(t)
	cfg.Queues = []Queue{{Name: q}}
	cli := openClient(t, cfg)
	cleanup(t, cli, []string{q}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var errCount atomic.Int64
	// 消费一个不存在的队列 → 404，通道被关闭
	sub, err := cli.NewConsumer(ConsumerConfig{
		Queue:        name("不存在的队列"),
		OnError:      func(error) { errCount.Add(1) },
		Handler:      func(context.Context, amqp.Delivery) error { return nil },
		DrainTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("创建消费者失败: %v", err)
	}
	go func() { _ = sub.Run(ctx) }()

	time.Sleep(time.Second)
	if errCount.Load() == 0 {
		t.Fatal("消费者应报告注册失败")
	}

	// 消费者在死循环重试的同时，生产侧必须完全不受影响
	if err := cli.PublishToQueue(ctx, q, []byte("生产者无恙")); err != nil {
		t.Fatalf("消费者故障不应影响生产：%v", err)
	}
	if _, _, err := cli.QueueDepth(ctx, q); err != nil {
		t.Fatalf("查询队列深度失败: %v", err)
	}

	cancel()
	done := make(chan struct{})
	go func() { _ = sub.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("消费者未能及时停止")
	}
}

// TestCloseIdempotentAndNoDeadlock 复用旧版死锁缺陷的回归用例。
//
// 旧实现：Close() 在持锁状态下 `return r.channel.Close()`，
// defer 的 Unlock 不执行 → 后续所有 Channel() 抢 RLock 永久阻塞。
func TestCloseIdempotentAndNoDeadlock(t *testing.T) {
	q := name("close.q")
	cfg := testConfig(t)
	cfg.Queues = []Queue{{Name: q}}
	cli := openClient(t, cfg)
	cleanup(t, cli, []string{q}, nil)
	// 注意：openClient 已注册了一次 Close，这里额外手动 Close，验证幂等

	if err := cli.PublishToQueue(t.Context(), q, []byte("x")); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("首次 Close 失败: %v", err)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("重复 Close 应返回 nil，实际: %v", err)
	}

	// 关闭后的调用必须立刻返回 ErrClosed，不能卡住（旧版会死锁在这里）
	done := make(chan error, 1)
	go func() {
		done <- cli.PublishToQueue(context.Background(), q, []byte("y"))
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Errorf("关闭后发布应返回 ErrClosed，实际: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("关闭后调用卡住 —— 死锁缺陷复现")
	}
}

// TestConcurrentPublishAndConsume 并发安全：多生产者多消费者共用同一个 Client。
func TestConcurrentPublishAndConsume(t *testing.T) {
	q := name("concurrent.q")
	cfg := testConfig(t)
	cfg.Prefetch = 20
	cfg.Queues = []Queue{{Name: q}}
	cli := openClient(t, cfg)
	cleanup(t, cli, []string{q}, nil)

	const (
		producers        = 4
		perProducer      = 25
		total            = producers * perProducer
		consumerParallel = 3
	)

	var (
		received atomic.Int64
		mu       sync.Mutex
		seen     = make(map[string]struct{}, total)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := cli.NewConsumer(ConsumerConfig{
		Queue:       q,
		Prefetch:    20,
		Concurrency: consumerParallel,
		Handler: func(_ context.Context, d amqp.Delivery) error {
			mu.Lock()
			seen[d.MessageId] = struct{}{}
			mu.Unlock()
			received.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("创建消费者失败: %v", err)
	}
	go func() { _ = sub.Run(ctx) }()

	var wg sync.WaitGroup
	errs := make(chan error, producers)
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				body := []byte(fmt.Sprintf("p%d-%d", p, i))
				if err := cli.PublishToQueue(ctx, q, body,
					WithMessageID(fmt.Sprintf("p%d-%d", p, i))); err != nil {
					errs <- err
					return
				}
			}
		}(p)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("并发发布失败: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for received.Load() < total && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	got := received.Load()
	mu.Lock()
	unique := len(seen)
	mu.Unlock()

	if got != total {
		t.Errorf("应收到 %d 条，实际 %d 条", total, got)
	}
	if unique != total {
		t.Errorf("应收到 %d 条不重复消息，实际 %d 条（出现重复投递）", total, unique)
	}
}

// TestGetSyncPull BasicGet 同步拉取。
func TestGetSyncPull(t *testing.T) {
	q := name("get.q")
	cfg := testConfig(t)
	cfg.Queues = []Queue{{Name: q}}
	cli := openClient(t, cfg)
	cleanup(t, cli, []string{q}, nil)

	// 空队列拉取：ok 应为 false，而不是报错
	if p, ok, err := cli.Get(t.Context(), q, false); err != nil || ok || p != nil {
		t.Fatalf("空队列拉取应返回 (nil, false, nil)，实际 p=%v ok=%v err=%v", p, ok, err)
	}

	if err := cli.PublishToQueue(t.Context(), q, []byte("pulled")); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	// 给 broker 一点时间把消息落到队列
	var (
		p   *Pulled
		ok  bool
		err error
	)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		p, ok, err = cli.Get(t.Context(), q, false)
		if err != nil {
			t.Fatalf("拉取失败: %v", err)
		}
		if ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ok || p == nil {
		t.Fatal("5s 内未拉到消息")
	}
	defer func() { _ = p.Close() }()

	if string(p.Body()) != "pulled" {
		t.Errorf("消息体不符: %q", p.Body())
	}
	// 关键：Ack 必须在 Close 之前，否则通道已释放（旧写法正是在这里翻车）
	if err := p.Ack(); err != nil {
		t.Fatalf("Ack 失败: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("释放通道失败: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("重复 Close 应返回 nil，实际: %v", err)
	}
}

// TestConsumerGracefulStop 取消 ctx 后消费者应在合理时间内退出。
func TestConsumerGracefulStop(t *testing.T) {
	q := name("graceful.q")
	cfg := testConfig(t)
	cfg.Queues = []Queue{{Name: q}}
	cli := openClient(t, cfg)
	cleanup(t, cli, []string{q}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	sub, err := cli.NewConsumer(ConsumerConfig{
		Queue:        q,
		Concurrency:  2,
		DrainTimeout: 2 * time.Second,
		Handler: func(context.Context, amqp.Delivery) error {
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("创建消费者失败: %v", err)
	}

	runDone := make(chan struct{})
	go func() {
		_ = sub.Run(ctx)
		close(runDone)
	}()
	// 等消费者注册完成
	time.Sleep(500 * time.Millisecond)

	cancel()
	select {
	case <-runDone:
	case <-time.After(6 * time.Second):
		t.Fatal("取消 ctx 后消费者未能在 6s 内退出")
	}
}

// recv 从通道接收一条投递，超时即失败。
func recv(t *testing.T, ch <-chan amqp.Delivery, timeout time.Duration, what string) amqp.Delivery {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(timeout):
		t.Fatalf("%s：%s 内未收到消息", what, timeout)
		return amqp.Delivery{}
	}
}
