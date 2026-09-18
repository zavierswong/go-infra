package prometheus

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/zavierswong/go-infra/metrics"
)

const testNS = "test"

func newTestExporter() *Exporter {
	return New(
		WithNamespace(testNS),
		WithHealthInterval(time.Hour),
		WithHealthTimeout(time.Second),
	)
}

func mysqlEvent(op metrics.Op, reason metrics.Reason, err error) metrics.Event {
	return metrics.Event{
		Component: metrics.ComponentMySQL,
		Instance:  "order",
		Op:        op,
		Duration:  25 * time.Millisecond,
		Err:       err,
		Reason:    reason,
	}
}

// TestObserveOpHistogram 验证直方图收全部事件（成功与失败）。
func TestObserveOpHistogram(t *testing.T) {
	e := newTestExporter()

	e.ObserveOp(mysqlEvent(metrics.OpQuery, metrics.ReasonNone, nil))
	e.ObserveOp(mysqlEvent(metrics.OpUpdate, metrics.ReasonTimeout, context.DeadlineExceeded))

	name := testNS + "_operation_duration_seconds"
	if got := histogramCount(t, e.opDuration, name, "mysql", "order", "query"); got != 1 {
		t.Errorf("query 直方图 count = %v, want 1", got)
	}
	if got := histogramCount(t, e.opDuration, name, "mysql", "order", "update"); got != 1 {
		t.Errorf("update 直方图 count = %v, want 1", got)
	}
}

// TestObserveOpErrorClassification 验证错误按契约分流：
// not_found 绝不进 errors_total，其余失败只进 errors_total。
func TestObserveOpErrorClassification(t *testing.T) {
	e := newTestExporter()

	// not_found：只进 not_found_total。
	e.ObserveOp(mysqlEvent(metrics.OpQuery, metrics.ReasonNotFound, errors.New("empty")))
	// 普通错误：只进 errors_total。
	e.ObserveOp(mysqlEvent(metrics.OpQuery, metrics.ReasonTimeout, context.DeadlineExceeded))
	// 成功：两边都不进。
	e.ObserveOp(mysqlEvent(metrics.OpQuery, metrics.ReasonNone, nil))

	if got := testutil.ToFloat64(e.opErrors.WithLabelValues("mysql", "order", "query", "timeout")); got != 1 {
		t.Errorf("errors_total{reason=timeout} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(e.opErrors.WithLabelValues("mysql", "order", "query", "not_found")); got != 0 {
		t.Errorf("errors_total 不应包含 not_found 计数, got %v", got)
	}
	if got := testutil.ToFloat64(e.opNotFound.WithLabelValues("mysql", "order", "query")); got != 1 {
		t.Errorf("not_found_total = %v, want 1", got)
	}
}

// TestObserveOpInstancePlaceholder 验证空 Instance 被替换为占位符。
func TestObserveOpInstancePlaceholder(t *testing.T) {
	e := newTestExporter()

	ev := mysqlEvent(metrics.OpQuery, metrics.ReasonNone, nil)
	ev.Instance = ""
	e.ObserveOp(ev)

	name := testNS + "_operation_duration_seconds"
	if got := histogramCount(t, e.opDuration, name, "mysql", instancePlaceholder, "query"); got != 1 {
		t.Errorf("空 Instance 应落到 %q 标签, count = %v", instancePlaceholder, got)
	}
}

// TestObserveOpNilReceiver 零值 Exporter 不应 panic（Observer 可能被并发调用）。
func TestObserveOpNilReceiver(t *testing.T) {
	var e *Exporter
	e.ObserveOp(mysqlEvent(metrics.OpQuery, metrics.ReasonNone, nil))
}

// TestCollectPoolGauges 验证池水位 Gauge 的按需建序列与回落到 0。
func TestCollectPoolGauges(t *testing.T) {
	e := newTestExporter()

	cur := 0
	e.RegisterPool(func() metrics.PoolStats {
		return metrics.PoolStats{
			Component: metrics.ComponentRedis,
			Instance:  "cache",
			MaxOpen:   100,
			Open:      cur,
			InUse:     cur,
			// Pending 恒为 0：模拟 SQL 侧不提供排队深度的字段。
		}
	})

	collect(e)

	// 首次为 0：不建序列。
	if n := testutil.CollectAndCount(e.poolOpen); n != 0 {
		t.Fatalf("首次值为 0 时不应建序列, got %d series", n)
	}

	cur = 8
	collect(e)
	if got := testutil.ToFloat64(e.poolOpen.WithLabelValues("redis", "cache")); got != 8 {
		t.Errorf("pool_open = %v, want 8", got)
	}

	cur = 0
	collect(e)
	if got := testutil.ToFloat64(e.poolOpen.WithLabelValues("redis", "cache")); got != 0 {
		t.Errorf("回落到 0 后 pool_open = %v, want 0（序列已存在）", got)
	}

	// Pending 从未非零：整个周期内都不应有序列。
	if n := testutil.CollectAndCount(e.poolPending); n != 0 {
		t.Errorf("恒为 0 的字段不应建序列, got %d series", n)
	}
}

// TestCollectPoolCounters 验证累计量按相邻两次 Collect 的差值递增。
func TestCollectPoolCounters(t *testing.T) {
	e := newTestExporter()

	wait := int64(0)
	e.RegisterPool(func() metrics.PoolStats {
		return metrics.PoolStats{
			Component: metrics.ComponentPostgres,
			Instance:  "main",
			WaitCount: wait,
		}
	})

	collect(e) // 首次：wait == 0，不建序列
	if n := testutil.CollectAndCount(e.poolWaitCount); n != 0 {
		t.Fatalf("wait == 0 时不应建序列, got %d series", n)
	}

	wait = 5
	collect(e)
	if got := testutil.ToFloat64(e.poolWaitCount.WithLabelValues("postgres", "main")); got != 5 {
		t.Fatalf("首次非零应整体计入, got %v, want 5", got)
	}

	wait = 8
	collect(e)
	if got := testutil.ToFloat64(e.poolWaitCount.WithLabelValues("postgres", "main")); got != 8 {
		t.Errorf("差值递增后 = %v, want 8", got)
	}

	wait = 3 // 模拟组件重建导致计数回退：计数器不应回退
	collect(e)
	if got := testutil.ToFloat64(e.poolWaitCount.WithLabelValues("postgres", "main")); got != 8 {
		t.Errorf("计数器不应回退, got %v, want 8", got)
	}
}

// TestCollectPoolWaitDuration 验证等待时长按秒累计。
func TestCollectPoolWaitDuration(t *testing.T) {
	e := newTestExporter()

	wait := time.Duration(0)
	e.RegisterPool(func() metrics.PoolStats {
		return metrics.PoolStats{
			Component:    metrics.ComponentMySQL,
			Instance:     "order",
			WaitDuration: wait,
		}
	})

	collect(e)
	wait = 1500 * time.Millisecond
	collect(e)
	if got := testutil.ToFloat64(e.poolWaitSeconds.WithLabelValues("mysql", "order")); got != 1.5 {
		t.Errorf("pool_wait_duration_seconds_total = %v, want 1.5", got)
	}
}

// TestCollectPoolClosedReasons 验证 closed_total 的 reason 标签。
func TestCollectPoolClosedReasons(t *testing.T) {
	e := newTestExporter()

	e.RegisterPool(func() metrics.PoolStats {
		return metrics.PoolStats{
			Component:     metrics.ComponentMySQL,
			Instance:      "order",
			MaxIdleClosed: 2,
		}
	})

	collect(e)
	if got := testutil.ToFloat64(e.poolClosed.WithLabelValues("mysql", "order", "max_idle")); got != 2 {
		t.Errorf("pool_closed{reason=max_idle} = %v, want 2", got)
	}
	if n := testutil.CollectAndCount(e.poolClosed); n != 1 {
		t.Errorf("只应有 max_idle 一条序列, got %d", n)
	}
}

// TestCollectStatus 验证 MQ 状态快照的三个 Gauge（false 也要有值）。
func TestCollectStatus(t *testing.T) {
	e := newTestExporter()

	e.RegisterStatus(func() MQStatus {
		return MQStatus{
			Component:           metrics.ComponentRabbitMQ,
			Instance:            "notify",
			Connected:           true,
			PublishChannelReady: false,
		}
	})

	collect(e)
	if got := testutil.ToFloat64(e.statusClosed.WithLabelValues("rabbitmq", "notify")); got != 0 {
		t.Errorf("status_closed = %v, want 0", got)
	}
	if got := testutil.ToFloat64(e.statusConnect.WithLabelValues("rabbitmq", "notify")); got != 1 {
		t.Errorf("status_connected = %v, want 1", got)
	}
	if got := testutil.ToFloat64(e.statusPubReady.WithLabelValues("rabbitmq", "notify")); got != 0 {
		t.Errorf("status_publish_ready = %v, want 0（false 也是有效状态）", got)
	}
}

// TestRegisterHealth 验证注册后立即探活一次并写入 component_up。
func TestRegisterHealth(t *testing.T) {
	e := newTestExporter()
	defer e.Close()

	healthy := true
	done := make(chan struct{})
	e.RegisterHealth(metrics.ComponentMySQL, "order", func(ctx context.Context) error {
		if !healthy {
			return errors.New("down")
		}
		close(done)
		return nil
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("注册后应立即探活一次")
	}
	if got := testutil.ToFloat64(e.componentUp.WithLabelValues("mysql", "order")); got != 1 {
		t.Errorf("component_up = %v, want 1", got)
	}

	healthy = false
	e.runHealthOnce()
	if got := testutil.ToFloat64(e.componentUp.WithLabelValues("mysql", "order")); got != 0 {
		t.Errorf("探活失败后 component_up = %v, want 0", got)
	}
}

// TestRegisterHealthNil 忽略 nil 检查函数，且不启动探活协程。
func TestRegisterHealthNil(t *testing.T) {
	e := newTestExporter()
	defer e.Close()

	e.RegisterHealth(metrics.ComponentMySQL, "order", nil)

	e.mu.Lock()
	n := len(e.checks)
	started := e.healthStop != nil
	e.mu.Unlock()

	if n != 0 || started {
		t.Errorf("nil 检查函数不应被注册或启动, checks=%d started=%v", n, started)
	}
}

// TestHealthTimeout 探活超时应报 0 而不是挂起。
func TestHealthTimeout(t *testing.T) {
	e := New(WithNamespace(testNS), WithHealthTimeout(10*time.Millisecond))
	defer e.Close()

	e.RegisterHealth(metrics.ComponentMySQL, "slow", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	e.runHealthOnce()
	if got := testutil.ToFloat64(e.componentUp.WithLabelValues("mysql", "slow")); got != 0 {
		t.Errorf("超时的探活 component_up = %v, want 0", got)
	}
}

// TestNilSourceIgnored 注册 nil 来源应是空操作。
func TestNilSourceIgnored(t *testing.T) {
	e := newTestExporter()

	e.RegisterPool(nil)
	e.RegisterStatus(nil)

	collect(e) // 不应 panic
}

// TestPoolKeyIsolation 不同组件的同名 instance 互不串数。
func TestPoolKeyIsolation(t *testing.T) {
	e := newTestExporter()

	open := map[string]int{"mysql": 3, "rabbit": 0}
	e.RegisterPool(func() metrics.PoolStats {
		return metrics.PoolStats{Component: metrics.ComponentMySQL, Instance: "same", Open: open["mysql"]}
	})
	e.RegisterPool(func() metrics.PoolStats {
		return metrics.PoolStats{Component: metrics.ComponentRabbitMQ, Instance: "same", Open: open["rabbit"]}
	})

	open["rabbit"] = 7
	collect(e)

	if got := testutil.ToFloat64(e.poolOpen.WithLabelValues("mysql", "same")); got != 3 {
		t.Errorf("mysql pool_open = %v, want 3", got)
	}
	if got := testutil.ToFloat64(e.poolOpen.WithLabelValues("rabbitmq", "same")); got != 7 {
		t.Errorf("rabbitmq pool_open = %v, want 7", got)
	}
}

// ---- 测试辅助 ----

// collect 手动驱动一次 Collect，模拟一次 scrape。
func collect(e *Exporter) {
	ch := make(chan prom.Metric, 256)
	go func() {
		e.Collect(ch)
		close(ch)
	}()
	for range ch {
	}
}

// histogramCount 从临时 Registry 抓取直方图的 _count 值。
// testutil.ToFloat64 不支持 Histogram，只能自己展开 proto。
func histogramCount(t *testing.T, h *prom.HistogramVec, name string, labels ...string) float64 {
	t.Helper()

	reg := prom.NewPedanticRegistry()
	reg.MustRegister(h)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	want := map[string]string{"component": labels[0], "instance": labels[1], "op": labels[2]}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			if len(got) != len(want) {
				continue
			}
			equal := true
			for k, v := range want {
				if got[k] != v {
					equal = false
					break
				}
			}
			if equal {
				return float64(m.GetHistogram().GetSampleCount())
			}
		}
	}
	return math.NaN()
}

// kafkaEvent 构造一条 kafka 侧事件，验证事件通道对组件无关。
func kafkaEvent(op metrics.Op, reason metrics.Reason, err error) metrics.Event {
	return metrics.Event{
		Component: metrics.ComponentKafka,
		Instance:  "events",
		Op:        op,
		Duration:  3 * time.Millisecond,
		Err:       err,
		Reason:    reason,
	}
}

// TestObserveOpKafkaEvents 验证 kafka 的 publish/consume/commit 事件
// 走通用 ObserveOp 通道：直方图按 op 分列，错误按 reason 计数，
// not_found（如主题不存在）不进 errors_total。
func TestObserveOpKafkaEvents(t *testing.T) {
	e := newTestExporter()

	e.ObserveOp(kafkaEvent(metrics.OpPublish, metrics.ReasonNone, nil))
	e.ObserveOp(kafkaEvent(metrics.OpConsume, metrics.ReasonNone, nil))
	e.ObserveOp(kafkaEvent(metrics.OpCommit, metrics.ReasonConnect, errors.New("coordinator down")))
	e.ObserveOp(kafkaEvent(metrics.OpPublish, metrics.ReasonNotFound, errors.New("unknown topic")))

	name := testNS + "_operation_duration_seconds"
	// 直方图收全部事件：publish 有两条（成功 + not_found）。
	wantCount := map[string]float64{"publish": 2, "consume": 1, "commit": 1}
	for op, want := range wantCount {
		if got := histogramCount(t, e.opDuration, name, "kafka", "events", op); got != want {
			t.Errorf("kafka %s 直方图 count = %v, want %v", op, got, want)
		}
	}
	if got := testutil.ToFloat64(e.opErrors.WithLabelValues("kafka", "events", "commit", "connect")); got != 1 {
		t.Errorf("errors_total{op=commit,reason=connect} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(e.opErrors.WithLabelValues("kafka", "events", "publish", "not_found")); got != 0 {
		t.Error("kafka 的 not_found 也不应进 errors_total")
	}
	if got := testutil.ToFloat64(e.opNotFound.WithLabelValues("kafka", "events", "publish")); got != 1 {
		t.Errorf("not_found_total = %v, want 1", got)
	}
}

// TestCollectKafkaStatus 验证 kafka 缓冲水位 Gauge：
// 首次为零不建序列、非零建序列、回落零保持序列报 0；
// Closed 落在共享 status_closed 上且始终写入。
func TestCollectKafkaStatus(t *testing.T) {
	e := newTestExporter()

	produce, closed := int64(0), false
	e.RegisterKafkaStatus(func() KafkaStatus {
		return KafkaStatus{
			Component:              metrics.ComponentKafka,
			Instance:               "events",
			Closed:                 closed,
			ProduceBufferedRecords: produce,
			ProduceBufferedBytes:   produce * 100,
			// Fetch 侧恒为 0：纯生产端，fetch 序列永远不该出现。
		}
	})

	collect(e)
	if n := testutil.CollectAndCount(e.kafkaProduceRecords); n != 0 {
		t.Fatalf("首次为 0 不应建序列, got %d series", n)
	}
	// Closed 是状态语义：首次就要有序列。
	if got := testutil.ToFloat64(e.statusClosed.WithLabelValues("kafka", "events")); got != 0 {
		t.Errorf("status_closed = %v, want 0", got)
	}

	produce = 42
	closed = true
	collect(e)
	if got := testutil.ToFloat64(e.kafkaProduceRecords.WithLabelValues("kafka", "events")); got != 42 {
		t.Errorf("kafka_buffered_produce_records = %v, want 42", got)
	}
	if got := testutil.ToFloat64(e.kafkaProduceBytes.WithLabelValues("kafka", "events")); got != 4200 {
		t.Errorf("kafka_buffered_produce_bytes = %v, want 4200", got)
	}
	if got := testutil.ToFloat64(e.statusClosed.WithLabelValues("kafka", "events")); got != 1 {
		t.Errorf("status_closed = %v, want 1", got)
	}

	produce = 0
	closed = false
	collect(e)
	// 序列已存在，要回落到 0 而不是消失。
	if got := testutil.ToFloat64(e.kafkaProduceRecords.WithLabelValues("kafka", "events")); got != 0 {
		t.Errorf("回落后的水位应为 0, got %v", got)
	}
	// fetch 侧从未非零，不应建序列。
	if n := testutil.CollectAndCount(e.kafkaFetchRecords); n != 0 {
		t.Errorf("fetch 侧恒为 0 不应建序列, got %d series", n)
	}
}

// TestCollectKafkaStatusPlaceholder 验证空 Instance 落到占位符。
func TestCollectKafkaStatusPlaceholder(t *testing.T) {
	e := newTestExporter()

	e.RegisterKafkaStatus(func() KafkaStatus {
		return KafkaStatus{
			Component:              metrics.ComponentKafka,
			ProduceBufferedRecords: 7,
		}
	})
	collect(e)
	if got := testutil.ToFloat64(e.kafkaProduceRecords.WithLabelValues("kafka", instancePlaceholder)); got != 7 {
		t.Errorf("空 Instance 应落到 %q 标签, got %v", instancePlaceholder, got)
	}
}

// TestRegisterKafkaStatusNil 验证 nil 来源被安全忽略。
func TestRegisterKafkaStatusNil(t *testing.T) {
	e := newTestExporter()
	e.RegisterKafkaStatus(nil)
	collect(e) // 不应 panic
}

// TestCollectConcurrentNoDoubleCount 并发 scrape 不会把同一增量重复累加。
//
// 「读 last → 存本拍」在 Exporter.mu 内原子完成，后到的 Collect 读到已被
// 更新的 last，差值为 0，因此增量是望远镜式累加的；本用例把这个不变量
// 固化下来（曾经被误判为"递增在锁外 → 重复计 2 倍"，实为误报）。
func TestCollectConcurrentNoDoubleCount(t *testing.T) {
	e := newTestExporter()

	var wait int64
	e.RegisterPool(func() metrics.PoolStats {
		return metrics.PoolStats{
			Component: metrics.ComponentPostgres,
			Instance:  "main",
			WaitCount: atomic.LoadInt64(&wait),
		}
	})

	atomic.StoreInt64(&wait, 10)
	collect(e) // 首次：整体计入 10
	if got := testutil.ToFloat64(e.poolWaitCount.WithLabelValues("postgres", "main")); got != 10 {
		t.Fatalf("首次计入 = %v, want 10", got)
	}

	// 并发 scrape 4 次：增量 5 只能被计入一次。
	atomic.StoreInt64(&wait, 15)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			collect(e)
		}()
	}
	wg.Wait()

	if got := testutil.ToFloat64(e.poolWaitCount.WithLabelValues("postgres", "main")); got != 15 {
		t.Fatalf("并发 scrape 后 = %v, want 15（旧实现会得到 15 + 5*(n-1)）", got)
	}
}

// TestCollectConcurrentRegistry 端到端回归：同一 Registry 上的并发 Gather
// 也必须得到一致结果（这正是"两个并发 /metrics 请求"的真实形态）。
func TestCollectConcurrentRegistry(t *testing.T) {
	e := newTestExporter()

	var wait int64
	e.RegisterPool(func() metrics.PoolStats {
		return metrics.PoolStats{
			Component: metrics.ComponentRedis,
			Instance:  "cache",
			WaitCount: atomic.LoadInt64(&wait),
		}
	})
	reg := prom.NewRegistry()
	if err := reg.Register(e); err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	atomic.StoreInt64(&wait, 100)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := reg.Gather(); err != nil {
				t.Errorf("Gather: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := testutil.ToFloat64(e.poolWaitCount.WithLabelValues("redis", "cache")); got != 100 {
		t.Fatalf("并发 Gather 后 = %v, want 100", got)
	}
}
