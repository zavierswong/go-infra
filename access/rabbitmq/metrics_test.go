package rabbitmq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/zavierswong/go-infra/metrics"
)

// ---------------------------------------------------------------------------
// 事件采集器（与 mysql / redis 两个包的写法保持一致）
// ---------------------------------------------------------------------------

type eventRecorder struct {
	mu     sync.Mutex
	events []metrics.Event
}

func (r *eventRecorder) ObserveOp(e metrics.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *eventRecorder) snapshot() []metrics.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]metrics.Event(nil), r.events...)
}

func (r *eventRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = nil
}

func (r *eventRecorder) waitForOp(t *testing.T, op metrics.Op, n int, d time.Duration) []metrics.Event {
	t.Helper()

	deadline := time.Now().Add(d)
	for {
		got := r.snapshot()
		count := 0
		for _, e := range got {
			if e.Op == op {
				count++
			}
		}
		if count >= n || time.Now().After(deadline) {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func pickOp(events []metrics.Event, op metrics.Op) (metrics.Event, bool) {
	for _, e := range events {
		if e.Op == op {
			return e, true
		}
	}
	return metrics.Event{}, false
}

// metricsConfig 返回带 Observer 的测试配置。
func metricsConfig(t *testing.T, rec *eventRecorder, queues ...string) Config {
	t.Helper()

	cfg := testConfig(t)
	cfg.Name = "mq"
	cfg.Observer = rec
	for _, q := range queues {
		cfg.Queues = append(cfg.Queues, Queue{Name: q})
	}
	return cfg
}

// ---------------------------------------------------------------------------
// 纯逻辑：没有 broker 也能跑
// ---------------------------------------------------------------------------

func TestStatusBeforeOpen(t *testing.T) {
	t.Parallel()

	c := &Client{cfg: Config{Name: "mq", Host: "127.0.0.1", VHost: "app_vhost"}}
	st := c.Status()

	if st.Instance != "mq" || st.Host != "127.0.0.1" || st.VHost != "app_vhost" {
		t.Fatalf("标识字段未透传: %+v", st)
	}
	if st.Closed || st.Connected || st.PublishChannelReady {
		t.Fatalf("未连接时三个状态都应为 false: %+v", st)
	}
}

func TestClassifyErrRabbitMQ(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want metrics.Reason
	}{
		{"nil", nil, metrics.ReasonNone},
		{"已关闭", ErrClosed, metrics.ReasonClosed},
		{"无法连接", ErrNotConnected, metrics.ReasonConnect},
		{"发布失败（链路问题）", ErrPublish, metrics.ReasonConnect},
		{"配置非法", ErrInvalidConfig, metrics.ReasonInvalid},
		{"拓扑声明失败", ErrTopology, metrics.ReasonInvalid},
		{"broker 未确认", ErrNotConfirmed, metrics.ReasonRejected},
		{"不可路由", ErrUnroutable, metrics.ReasonRejected},

		// 超时必须优先于 ErrNotConfirmed 判定：
		// 确认超时的错误链里同时含 ErrNotConfirmed 与 DeadlineExceeded，
		// 顺序反了会把"broker 太慢"误报成"消息被拒收"。
		{"确认超时（同时含 ErrNotConfirmed）",
			errors.Join(ErrNotConfirmed, context.DeadlineExceeded), metrics.ReasonTimeout},

		{"AMQP 404 队列不存在", &amqp.Error{Code: 404, Reason: "NOT_FOUND"}, metrics.ReasonNotFound},
		{"AMQP 406 属性不一致", &amqp.Error{Code: 406, Reason: "PRECONDITION_FAILED"}, metrics.ReasonInvalid},
		{"AMQP 403 权限不足", &amqp.Error{Code: 403, Reason: "ACCESS_REFUSED"}, metrics.ReasonInvalid},
		{"AMQP 405 队列被独占", &amqp.Error{Code: 405, Reason: "RESOURCE_LOCKED"}, metrics.ReasonConflict},
		{"AMQP 320 被强制关闭", &amqp.Error{Code: 320, Reason: "CONNECTION_FORCED"}, metrics.ReasonConnect},
		{"AMQP 504 通道错误", &amqp.Error{Code: 504, Reason: "CHANNEL_ERROR"}, metrics.ReasonConnect},
		{"包装后的 AMQP 错误", &amqp.Error{Code: 404}, metrics.ReasonNotFound},

		{"普通错误", errors.New("boom"), metrics.ReasonUnknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := classifyErr(tc.err); got != tc.want {
				t.Fatalf("classifyErr(%v) = %q，期望 %q", tc.err, got, tc.want)
			}
		})
	}
}

func TestPublishDetail(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opts []PublishOption
		want string
	}{
		{"投递到队列", []PublishOption{ToQueue("sms.send")}, "sms.send"},
		{"投递到交换机的某个 routing key",
			[]PublishOption{ToExchange("orders", "created")}, "orders / created"},
		{"延迟消息带上延迟时长",
			[]PublishOption{ToQueue("sms.send"), WithDelay(30 * time.Second)}, "sms.send (延迟 30s)"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			o := newPublishOptions()
			for _, fn := range tc.opts {
				fn(&o)
			}
			if got := publishDetail(&o); got != tc.want {
				t.Fatalf("publishDetail() = %q，期望 %q", got, tc.want)
			}
		})
	}
}

func TestConsumeDetail(t *testing.T) {
	t.Parallel()

	d := amqp.Delivery{MessageId: "m-1"}
	if got := consumeDetail("q1", d); got != "q1 message_id=m-1" {
		t.Errorf("基本形态错误: %q", got)
	}

	d.Redelivered = true
	got := consumeDetail("q1", d)
	if !strings.Contains(got, "redelivered=true") {
		t.Errorf("重投标记必须体现出来（毒消息的早期信号）: %q", got)
	}
}

// ---------------------------------------------------------------------------
// 集成：需要真实 broker
// ---------------------------------------------------------------------------

// TestMetricsNoReconnectEventOnFirstConnect 守护一个容易搞错的语义。
//
// 首次建连与断线重连走的是同一条 dial 路径。如果不在 Client 里记状态，
// 启动时的第一次连接就会被上报成一次 reconnect ——
// 于是"重连次数"这个指标从进程启动的第一秒起就是错的，
// 而且它会掩盖真实的重连（因为基线不再是 0）。
func TestMetricsNoReconnectEventOnFirstConnect(t *testing.T) {
	rec := &eventRecorder{}
	q := name("metrics.first.q")

	// rec 必须在 Open 之前就位，否则测不到建连阶段的事件。
	cli := openClient(t, metricsConfig(t, rec, q))
	cleanup(t, cli, []string{q}, nil)

	for _, e := range rec.snapshot() {
		if e.Op == metrics.OpReconnect {
			t.Fatalf("首次建连不应上报 reconnect: %+v", e)
		}
	}
}

func TestMetricsStatusAfterOpen(t *testing.T) {
	rec := &eventRecorder{}
	q := name("metrics.status.q")

	cli := openClient(t, metricsConfig(t, rec, q))
	cleanup(t, cli, []string{q}, nil)

	st := cli.Status()
	if st.Instance != "mq" {
		t.Errorf("Instance 应为 mq，实际 %q", st.Instance)
	}
	if st.Closed {
		t.Error("刚创建时 Closed 应为 false")
	}
	if !st.Connected {
		t.Error("刚创建时 Connected 应为 true")
	}
	if !st.PublishChannelReady {
		t.Error("刚创建时发布通道应可用")
	}

	if err := cli.Close(); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}
	if st := cli.Status(); !st.Closed || st.Connected || st.PublishChannelReady {
		t.Errorf("关闭后三个状态都应反映出来: %+v", st)
	}
}

func TestMetricsPublishEvent(t *testing.T) {
	rec := &eventRecorder{}
	q := name("metrics.pub.q")

	cli := openClient(t, metricsConfig(t, rec, q))
	cleanup(t, cli, []string{q}, nil)
	ctx := t.Context()

	rec.reset()

	if err := cli.PublishToQueue(ctx, q, []byte("hello")); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	events := rec.waitForOp(t, metrics.OpPublish, 1, 2*time.Second)
	e, ok := pickOp(events, metrics.OpPublish)
	if !ok {
		t.Fatalf("未收到 publish 事件，实际 %d 条", len(events))
	}

	if e.Component != metrics.ComponentRabbitMQ {
		t.Errorf("Component = %q", e.Component)
	}
	if e.Instance != "mq" {
		t.Errorf("Instance 应为 mq，实际 %q", e.Instance)
	}
	if e.IsError() {
		t.Errorf("成功发布不应是错误: %v", e.Err)
	}
	if e.Duration <= 0 {
		t.Errorf("Duration 应大于 0，实际 %v", e.Duration)
	}
	if e.Detail != q {
		t.Errorf("Detail 应为目标队列名 %q，实际 %q", q, e.Detail)
	}
}

// TestMetricsUnroutableProducesRejectedAndReturn 覆盖"消息没进任何队列"。
//
// 这是 MQ 上最需要告警的情形，而默认配置下 broker 会直接丢弃且 Publish 返回 nil。
// 开启 mandatory 后它会变成两个事件：
//   - publish 带 ErrUnroutable → reason=rejected
//   - return 事件（来自 drain 协程，不受同步窗口限制，是更可靠的信号）
func TestMetricsUnroutableProducesRejectedAndReturn(t *testing.T) {
	rec := &eventRecorder{}
	q := name("metrics.unroutable.q")

	// 注意：**不声明**这个队列，且不在 Config.Queues 里。
	// 直接投递到不存在的队列 → 默认交换机路由不到任何队列。
	cli := openClient(t, metricsConfig(t, rec))
	ctx := t.Context()

	rec.reset()

	err := cli.PublishToQueue(ctx, q, []byte("lost"), WithMandatory())
	if err == nil {
		t.Fatal("mandatory 投递到不存在的队列应当报错")
	}
	if !errors.Is(err, ErrUnroutable) {
		t.Fatalf("错误应为 ErrUnroutable，实际 %v", err)
	}

	events := rec.waitForOp(t, metrics.OpReturn, 1, 2*time.Second)

	pub, ok := pickOp(events, metrics.OpPublish)
	if !ok {
		t.Fatalf("未收到 publish 事件，实际 %d 条", len(events))
	}
	if pub.Reason != metrics.ReasonRejected {
		t.Errorf("不可路由的 publish 应归为 rejected，实际 %q", pub.Reason)
	}
	if !pub.IsError() {
		t.Error("不可路由必须计入错误率")
	}

	ret, ok := pickOp(events, metrics.OpReturn)
	if !ok {
		t.Fatal("未收到 return 事件（它是不可路由最可靠的信号）")
	}
	if ret.Reason != metrics.ReasonRejected {
		t.Errorf("return 事件应归为 rejected，实际 %q", ret.Reason)
	}
	if !strings.Contains(ret.Detail, q) {
		t.Errorf("return 的 Detail 应含 routing key，实际 %q", ret.Detail)
	}
}

func TestMetricsConsumeAndAckEvents(t *testing.T) {
	rec := &eventRecorder{}
	q := name("metrics.consume.q")

	cli := openClient(t, metricsConfig(t, rec, q))
	cleanup(t, cli, []string{q}, nil)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	handled := make(chan struct{}, 1)
	sub, err := cli.NewConsumer(ConsumerConfig{
		Queue: q,
		Handler: func(context.Context, amqp.Delivery) error {
			select {
			case handled <- struct{}{}:
			default:
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("创建消费者失败: %v", err)
	}
	go func() { _ = sub.Run(ctx) }()
	defer func() { _ = sub.Close() }()

	rec.reset()

	// 消费者可能还没注册完成，但消息会留在队列里，注册后照样会被收到。
	if err := cli.PublishToQueue(ctx, q, []byte("payload")); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler 未被调用")
	}

	events := rec.waitForOp(t, metrics.OpAck, 1, 5*time.Second)

	consume, ok := pickOp(events, metrics.OpConsume)
	if !ok {
		t.Fatalf("未收到 consume 事件，实际 %d 条", len(events))
	}
	if consume.Failed() {
		t.Errorf("handler 返回 nil，consume 事件不应带错误: %v", consume.Err)
	}
	if consume.Duration <= 0 {
		t.Errorf("consume 的 Duration 应大于 0，实际 %v", consume.Duration)
	}
	if !strings.Contains(consume.Detail, q) {
		t.Errorf("consume 的 Detail 应含队列名，实际 %q", consume.Detail)
	}

	ack, ok := pickOp(events, metrics.OpAck)
	if !ok {
		t.Fatal("未收到 ack 事件")
	}
	if ack.Failed() {
		t.Errorf("Ack 成功时不应带错误: %v", ack.Err)
	}
}

func TestMetricsHandlerErrorProducesNackEvent(t *testing.T) {
	rec := &eventRecorder{}
	q := name("metrics.nack.q")

	cli := openClient(t, metricsConfig(t, rec, q))
	cleanup(t, cli, []string{q}, nil)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	handlerErr := errors.New("业务处理失败")
	handled := make(chan struct{}, 1)

	sub, err := cli.NewConsumer(ConsumerConfig{
		Queue: q,
		Handler: func(context.Context, amqp.Delivery) error {
			select {
			case handled <- struct{}{}:
			default:
			}
			return handlerErr
		},
		// 明确不重新入队：确定性失败如果 requeue，会形成死循环。
		NackPolicy: NackPolicy{Requeue: false},
		// 吞掉日志噪音，同时断言错误确实被上报给了 OnError。
		OnError: func(error) {},
	})
	if err != nil {
		t.Fatalf("创建消费者失败: %v", err)
	}
	go func() { _ = sub.Run(ctx) }()
	defer func() { _ = sub.Close() }()

	rec.reset()

	if err := cli.PublishToQueue(ctx, q, []byte("poison")); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler 未被调用")
	}

	events := rec.waitForOp(t, metrics.OpNack, 1, 5*time.Second)

	consume, ok := pickOp(events, metrics.OpConsume)
	if !ok {
		t.Fatalf("未收到 consume 事件，实际 %d 条", len(events))
	}
	if !consume.IsError() {
		t.Error("handler 返回错误时 consume 事件必须计入错误率")
	}
	if !errors.Is(consume.Err, handlerErr) {
		t.Errorf("consume 事件的 Err 应保留 handler 的原始错误，实际 %v", consume.Err)
	}

	nack, ok := pickOp(events, metrics.OpNack)
	if !ok {
		t.Fatal("未收到 nack 事件")
	}
	if nack.Failed() {
		t.Errorf("Nack 调用本身成功，不应带错误: %v", nack.Err)
	}
	// requeue 的决定必须在 Detail 里 —— 它区分"稍后还会重试"与"这条消息已经丢了"。
	if !strings.Contains(nack.Detail, "requeue=false") {
		t.Errorf("nack 的 Detail 应记录 requeue 决定，实际 %q", nack.Detail)
	}
}

func TestMetricsChannelRebuildEvent(t *testing.T) {
	rec := &eventRecorder{}
	q := name("metrics.heal.q")

	cli := openClient(t, metricsConfig(t, rec, q))
	cleanup(t, cli, []string{q}, nil)
	ctx := t.Context()

	if err := cli.PublishToQueue(ctx, q, []byte("before")); err != nil {
		t.Fatalf("故障前发布失败: %v", err)
	}

	// 制造 channel 级故障：以不一致的属性重复声明同名队列，
	// broker 回 406 并关闭整条 channel，但连接仍然健康。
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

	rec.reset()

	// 这次发布会触发通道重建。
	if err := cli.PublishToQueue(ctx, q, []byte("after")); err != nil {
		t.Fatalf("通道故障后发布失败: %v", err)
	}

	events := rec.waitForOp(t, metrics.OpChannelRebuild, 1, 2*time.Second)
	e, ok := pickOp(events, metrics.OpChannelRebuild)
	if !ok {
		t.Fatalf("未收到 channel_rebuild 事件，实际 %d 条", len(events))
	}
	if e.Failed() {
		t.Errorf("自愈成功后的 channel_rebuild 不应带错误: %v", e.Err)
	}
	if e.Instance != "mq" {
		t.Errorf("Instance 应为 mq，实际 %q", e.Instance)
	}
	// 连接没断，所以不该出现 reconnect —— 这正是旧版最严重的盲区所在。
	if _, reconnected := pickOp(events, metrics.OpReconnect); reconnected {
		t.Error("连接仍然健康，不应上报 reconnect")
	}
}

func TestMetricsPulledAckEvent(t *testing.T) {
	rec := &eventRecorder{}
	q := name("metrics.pull.q")

	cli := openClient(t, metricsConfig(t, rec, q))
	cleanup(t, cli, []string{q}, nil)
	ctx := t.Context()

	if err := cli.PublishToQueue(ctx, q, []byte("pull-me")); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	rec.reset()

	p, ok, err := cli.Get(ctx, q, false)
	if err != nil {
		t.Fatalf("拉取失败: %v", err)
	}
	if !ok {
		t.Fatal("队列里应有刚发布的消息")
	}
	defer func() { _ = p.Close() }()

	if err := p.Ack(); err != nil {
		t.Fatalf("Ack 失败: %v", err)
	}

	events := rec.waitForOp(t, metrics.OpAck, 1, 2*time.Second)
	e, ok := pickOp(events, metrics.OpAck)
	if !ok {
		t.Fatalf("未收到 ack 事件，实际 %d 条", len(events))
	}
	if e.Instance != "mq" {
		t.Errorf("Instance 应为 mq，实际 %q", e.Instance)
	}
	if e.Failed() {
		t.Errorf("Ack 成功不应带错误: %v", e.Err)
	}
}

func TestMetricsNotConfiguredIsNoop(t *testing.T) {
	q := name("metrics.noop.q")

	cfg := testConfig(t)
	cfg.Name = "mq"
	cfg.Observer = nil
	cfg.Queues = []Queue{{Name: q}}

	cli := openClient(t, cfg)
	cleanup(t, cli, []string{q}, nil)
	ctx := t.Context()

	if err := cli.PublishToQueue(ctx, q, []byte("v")); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	msgs, _, err := cli.QueueDepth(ctx, q)
	if err != nil {
		t.Fatalf("查询队列深度失败: %v", err)
	}
	if msgs != 1 {
		t.Errorf("期望 1 条消息，实际 %d 条", msgs)
	}
}
