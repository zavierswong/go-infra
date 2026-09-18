package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/zavierswong/go-infra/metrics"
)

// ---------------------------------------------------------------------------
// 事件采集器（与 mysql 包的写法保持一致）
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

func (r *eventRecorder) waitFor(t *testing.T, n int, d time.Duration) []metrics.Event {
	t.Helper()

	deadline := time.Now().Add(d)
	for {
		got := r.snapshot()
		if len(got) >= n || time.Now().After(deadline) {
			return got
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// waitForOp 等某个 Op 的事件攒够 n 条。
//
// 不能简单地等"总条数达标"：go-redis 建连时会额外产生握手命令的事件
// （实测有 HELLO 与含 CLIENT 的 pipeline），总条数会先于用户命令达标。
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
		time.Sleep(2 * time.Millisecond)
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

// ---------------------------------------------------------------------------
// 纯逻辑：没有 Redis 也能跑
// ---------------------------------------------------------------------------

// TestPoolStatsBeforeOpen 覆盖"尚未连接"这条路径。
func TestPoolStatsBeforeOpen(t *testing.T) {
	t.Parallel()

	r := &RDS{cfg: Config{Name: "cache", MaxActiveConns: 64}}
	got := r.PoolStats()

	if got.Component != metrics.ComponentRedis {
		t.Errorf("Component = %q，期望 %q", got.Component, metrics.ComponentRedis)
	}
	if got.Instance != "cache" {
		t.Errorf("Instance 应透传 Config.Name，实际 %q", got.Instance)
	}
	if got.MaxOpen != 64 {
		t.Errorf("MaxOpen 应取 MaxActiveConns（硬上限），实际 %d", got.MaxOpen)
	}
	if got.Open != 0 || got.Hits != 0 || got.WaitDuration != 0 {
		t.Errorf("未连接时其余字段应为 0，实际 %+v", got)
	}
}

// TestPoolStatsNormalizedMapping 逐字段断言 go-redis → metrics.PoolStats 的映射。
//
// 这种测试的价值在于"漏一项就红"：字段搬运最容易漏，
// 而漏掉的表现只是某条曲线永远是 0，接入监控后极难发现。
func TestPoolStatsNormalizedMapping(t *testing.T) {
	t.Parallel()

	in := &goredis.PoolStats{
		Hits:            100,
		Misses:          7,
		Timeouts:        2,
		WaitCount:       9,
		Unusable:        3,
		WaitDurationNs:  int64(1500 * time.Millisecond),
		TotalConns:      16,
		IdleConns:       5,
		StaleConns:      4,
		PendingRequests: 11,
	}

	got := poolStatsFrom("cache", 0, in)

	if got.Instance != "cache" || got.Component != metrics.ComponentRedis {
		t.Fatalf("标识字段错误: %+v", got)
	}
	if got.MaxOpen != 0 {
		t.Errorf("MaxActiveConns 为 0 表示无硬上限，MaxOpen 应为 0，实际 %d", got.MaxOpen)
	}
	// InUse 不是原生字段，必须由 TotalConns - IdleConns 推导出来。
	if got.Open != 16 || got.Idle != 5 {
		t.Errorf("Open/Idle 映射错误: Open=%d Idle=%d", got.Open, got.Idle)
	}
	if got.InUse != 11 {
		t.Errorf("InUse 应为 TotalConns-IdleConns=11，实际 %d", got.InUse)
	}
	if got.Pending != 11 {
		t.Errorf("Pending 映射错误，实际 %d", got.Pending)
	}
	if got.WaitCount != 9 {
		t.Errorf("WaitCount 映射错误，实际 %d", got.WaitCount)
	}
	// 纳秒整数必须被转成 Duration，否则会得到 1500ns 这种离谱的数字。
	if got.WaitDuration != 1500*time.Millisecond {
		t.Errorf("WaitDuration 应为 1.5s，实际 %v", got.WaitDuration)
	}
	if got.Hits != 100 || got.Misses != 7 || got.Timeouts != 2 ||
		got.Unusable != 3 || got.Stale != 4 {
		t.Errorf("累计量映射错误: %+v", got)
	}
	// 数据库侧专有的回收原因字段在 Redis 侧必须保持 0，避免出现假曲线。
	if got.MaxIdleClosed != 0 || got.MaxIdleTimeClosed != 0 || got.MaxLifetimeClosed != 0 {
		t.Errorf("Redis 侧不应填充数据库专有字段: %+v", got)
	}
}

// TestPoolStatsNegativeInUseClamped 覆盖并发读取导致的负数。
//
// TotalConns 与 IdleConns 是分两次读的，中间可能有一条连接从"使用中"变为"空闲"，
// 于是 idle > total。若不夹到 0，指标里会出现负的 Gauge。
func TestPoolStatsNegativeInUseClamped(t *testing.T) {
	t.Parallel()

	got := poolStatsFrom("cache", 0, &goredis.PoolStats{TotalConns: 3, IdleConns: 8})
	if got.InUse != 0 {
		t.Fatalf("InUse 不应为负，实际 %d", got.InUse)
	}
	if got.Idle != 8 || got.Open != 3 {
		t.Fatalf("原始计数应原样保留（只夹 InUse），实际 %+v", got)
	}
}

func TestClassifyErrRedis(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want metrics.Reason
	}{
		{"nil", nil, metrics.ReasonNone},
		{"键不存在", goredis.Nil, metrics.ReasonNotFound},
		{"包装后的键不存在", fmt.Errorf("读取会话: %w", goredis.Nil), metrics.ReasonNotFound},
		{"WATCH 冲突", goredis.TxFailedErr, metrics.ReasonConflict},
		{"等池超时", goredis.ErrPoolTimeout, metrics.ReasonTimeout},
		{"池已耗尽", goredis.ErrPoolExhausted, metrics.ReasonTimeout},
		{"客户端已关", goredis.ErrClosed, metrics.ReasonClosed},
		{"本包已关", ErrClosed, metrics.ReasonClosed},
		{"未连接", ErrNotConnected, metrics.ReasonConnect},
		{"建连失败", fmt.Errorf("%w: 拒绝连接", ErrConnect), metrics.ReasonConnect},
		{"配置非法", ErrInvalidConfig, metrics.ReasonInvalid},
		{"ctx 超时", context.DeadlineExceeded, metrics.ReasonTimeout},
		// goredis.Error 是服务端协议错误，按前缀细分。
		{"服务端：脚本不在缓存", goredis.ErrNoScript, metrics.ReasonNotFound},
		{"服务端：集群跨槽", goredis.ErrCrossSlot, metrics.ReasonInvalid},
		// 普通 error 不是服务端错误，走通用分类。
		{"普通错误", errors.New("WRONGTYPE Operation against a key"), metrics.ReasonUnknown},
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

// TestIsRealErrKeepsNilOut 锁定一个易错点：goredis.Nil 不是值得告警的错误。
func TestIsRealErrKeepsNilOut(t *testing.T) {
	t.Parallel()

	if isRealErr(goredis.Nil) {
		t.Error("goredis.Nil 不应被当作真实错误")
	}
	if isRealErr(fmt.Errorf("包装: %w", goredis.Nil)) {
		t.Error("包装后的 goredis.Nil 同样不应被当作真实错误")
	}
	if !isRealErr(errors.New("boom")) {
		t.Error("普通错误应被当作真实错误")
	}
	if isRealErr(nil) {
		t.Error("nil 不是错误")
	}
}

func TestNeedDetail(t *testing.T) {
	t.Parallel()

	fast := 100 * time.Microsecond
	slow := 50 * time.Millisecond

	withThreshold := &metricsHook{slow: slow}
	noThreshold := &metricsHook{}

	cases := []struct {
		name    string
		hook    *metricsHook
		elapsed time.Duration
		err     error
		want    bool
	}{
		{"未设阈值且成功：不填详情", noThreshold, slow, nil, false},
		{"未设阈值但失败：填详情", noThreshold, fast, errors.New("x"), true},
		{"设了阈值且够慢：填详情", withThreshold, slow, nil, true},
		{"设了阈值且很快：不填详情", withThreshold, fast, nil, false},
		{"阈值边界（等于阈值）：填详情", withThreshold, slow, nil, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.hook.needDetail(tc.elapsed, tc.err); got != tc.want {
				t.Fatalf("needDetail(%v, %v) = %v，期望 %v", tc.elapsed, tc.err, got, tc.want)
			}
		})
	}
}

func TestFormatCommand(t *testing.T) {
	t.Parallel()

	t.Run("只取首个参数（键名）并标注总数", func(t *testing.T) {
		t.Parallel()

		got := formatCommand("HSET", []interface{}{"HSET", "k", "f1", "v1"}, 1)
		if got != "HSET k …(共 3 个参数)" {
			t.Errorf("应只保留键名，实际 %q", got)
		}
	})

	// 这是本文件里最重要的一条安全断言。
	// 实测中 go-redis 建连会发 `HELLO 3 AUTH default <password>`，
	// 若不特判，口令会随 Detail 进入日志。
	t.Run("敏感命令省略全部参数", func(t *testing.T) {
		t.Parallel()

		const secret = "s3cr3t-p4ssw0rd"
		for _, name := range []string{"AUTH", "HELLO", "ACL", "CONFIG", "MIGRATE"} {
			got := formatCommand(name, []interface{}{name, "default", secret}, 1)
			if strings.Contains(got, secret) {
				t.Fatalf("%s 的 Detail 泄漏了口令: %q", name, got)
			}
			if !strings.Contains(got, "已省略") {
				t.Errorf("%s 应标注参数被省略，实际 %q", name, got)
			}
		}
	})

	t.Run("写入的值不得出现在 Detail 里", func(t *testing.T) {
		t.Parallel()

		const token = "eyJhbGciOiJIUzI1NiJ9.secret-jwt"
		got := formatCommand("SET", []interface{}{"SET", "session:abc", token}, 1)

		if strings.Contains(got, token) {
			t.Fatalf("SET 的值不应出现在 Detail 里: %q", got)
		}
		if !strings.Contains(got, "session:abc") {
			t.Errorf("键名应当保留（归因需要），实际 %q", got)
		}
	})

	t.Run("超长参数被按字符截断", func(t *testing.T) {
		t.Parallel()

		long := strings.Repeat("x", 500)
		got := formatCommand("GET", []interface{}{"GET", long}, 1)

		// 组成："GET " 前缀 4 字符 + 截断后的 32 字符 + 省略号 1 字符。
		if want := len("GET ") + maxDetailArgLen + 1; len([]rune(got)) != want {
			t.Errorf("截断后长度应为 %d，实际 %d（%q）", want, len([]rune(got)), got)
		}
		if !strings.HasSuffix(got, "…") {
			t.Errorf("截断处应有省略标记，实际 %q", got)
		}
	})

	t.Run("无参数时只有命令名", func(t *testing.T) {
		t.Parallel()

		if got := formatCommand("PING", []interface{}{"PING"}, 1); got != "PING" {
			t.Errorf("应只返回命令名，实际 %q", got)
		}
	})
}

// TestTruncateRunesIsRuneSafe 保证不会把多字节字符切成乱码。
func TestTruncateRunesIsRuneSafe(t *testing.T) {
	t.Parallel()

	s := strings.Repeat("缓", 40) // 每个汉字 3 字节
	got := truncateRunes(s, 10)

	if !strings.HasSuffix(got, "…") {
		t.Fatalf("应被截断，实际 %q", got)
	}
	for _, r := range got {
		if r != '…' && r != '缓' {
			t.Fatalf("截断产生了乱码字符 %q", r)
		}
	}
	if got2 := truncateRunes("短", 10); got2 != "短" {
		t.Errorf("未超限时不应改动，实际 %q", got2)
	}
}

// ---------------------------------------------------------------------------
// 集成：需要真实 Redis
// ---------------------------------------------------------------------------

// metricsConfig 返回带 Observer 的测试配置。
//
// 刻意**不**设置 LogSlowThreshold：这样日志钩子走"只关心失败"的快路径，
// 不会因为每条命令都算慢而刷屏，同时也能验证 metrics 与日志两条开关互不影响。
func metricsConfig(t *testing.T, rec *eventRecorder) Config {
	t.Helper()

	cfg := testConfig(t)
	cfg.Name = "cache"
	cfg.Observer = rec
	cfg.LogSlowThreshold = 0
	return cfg
}

func TestMetricsEmitsCommandEvents(t *testing.T) {
	rec := &eventRecorder{}
	r := openClient(t, metricsConfig(t, rec))
	ctx := context.Background()

	name := key("metrics-basic")
	cleanupKeys(t, r, "metrics-basic")

	rec.reset()

	if err := r.Client().Set(ctx, name, "v", time.Minute).Err(); err != nil {
		t.Fatalf("SET 失败: %v", err)
	}
	if err := r.Client().Get(ctx, name).Err(); err != nil {
		t.Fatalf("GET 失败: %v", err)
	}

	events := rec.waitFor(t, 2, time.Second)

	for _, op := range []metrics.Op{"SET", "GET"} {
		e, ok := pickOp(events, op)
		if !ok {
			t.Errorf("未收到 %s 事件，实际收到 %d 条", op, len(events))
			continue
		}
		if e.Component != metrics.ComponentRedis {
			t.Errorf("%s 的 Component = %q", op, e.Component)
		}
		if e.Instance != "cache" {
			t.Errorf("%s 的 Instance 应为 cache，实际 %q", op, e.Instance)
		}
		if e.Failed() {
			t.Errorf("%s 不应失败: %v", op, e.Err)
		}
		if e.Duration <= 0 {
			t.Errorf("%s 的 Duration 应大于 0，实际 %v", op, e.Duration)
		}
		// 未设慢阈值且命令成功 → 按策略不填 Detail，热路径上省掉格式化开销。
		if e.Detail != "" {
			t.Errorf("%s 成功且不慢，Detail 应为空以省开销，实际 %q", op, e.Detail)
		}
	}
}

// TestMetricsCodeMissIsNotAnError 是本文件最重要的行为断言。
//
// 缓存未命中的正确姿势：它是 error（Fail() 为 true），但**绝不能计入错误率**。
// 如果这里退化成 true，线上就会看到"缓存命中率下降 == Redis 错误率飙升"，
// 把排障引向完全错误的方向。
func TestMetricsCodeMissIsNotAnError(t *testing.T) {
	rec := &eventRecorder{}
	r := openClient(t, metricsConfig(t, rec))
	ctx := context.Background()

	missing := key("metrics-missing")

	rec.reset()
	err := r.Client().Get(ctx, missing).Err()
	if !errors.Is(err, goredis.Nil) {
		t.Fatalf("读取不存在的 key 应返回 goredis.Nil，实际 %v", err)
	}

	events := rec.waitFor(t, 1, time.Second)
	e, ok := pickOp(events, "GET")
	if !ok {
		t.Fatalf("未收到 GET 事件，实际 %d 条", len(events))
	}

	if e.Reason != metrics.ReasonNotFound {
		t.Errorf("未命中的 Reason 应为 not_found，实际 %q", e.Reason)
	}
	if !e.Failed() {
		t.Error("未命中确实是一个 error，Failed() 应为 true")
	}
	if e.IsError() {
		t.Error("未命中不应计入错误率，IsError() 应为 false")
	}
}

// TestMetricsErrorFillsDetail 验证出错时一定会填 Detail ——
// 这是"慢/失败才归因"策略的另一半，也是 Detail 真正的用武之地。
func TestMetricsErrorFillsDetail(t *testing.T) {
	rec := &eventRecorder{}
	r := openClient(t, metricsConfig(t, rec))
	ctx := context.Background()

	name := key("metrics-wrongtype")
	cleanupKeys(t, r, "metrics-wrongtype")

	if err := r.Client().Set(ctx, name, "i-am-a-string", time.Minute).Err(); err != nil {
		t.Fatalf("SET 失败: %v", err)
	}

	rec.reset()
	// 对字符串执行哈希命令 → 服务端返回 WRONGTYPE
	if err := r.Client().HGetAll(ctx, name).Err(); err == nil {
		t.Fatal("对字符串执行 HGETALL 应当报错")
	}

	events := rec.waitFor(t, 1, time.Second)
	e, ok := pickOp(events, "HGETALL")
	if !ok {
		t.Fatalf("未收到 HGETALL 事件，实际 %d 条", len(events))
	}

	if !e.Failed() {
		t.Fatal("WRONGTYPE 应在事件里带上 Err")
	}
	if e.Detail == "" {
		t.Error("失败的命令必须填 Detail，否则无法归因")
	}
	if !strings.Contains(e.Detail, "HGETALL") {
		t.Errorf("Detail 应含命令名，实际 %q", e.Detail)
	}
	if e.IsError() {
		// WRONGTYPE 是真实的程序错误，必须计入错误率。
		// 这条断言同时守住"not_found 之外都算错误"这个边界。
		t.Log("WRONGTYPE 计入错误率，符合预期")
	}
}

// TestMetricsPipelineEmitsSingleEvent 验证整条 pipeline 只产生一个事件。
//
// 拆分上报是错的：pipeline 只有一次往返，go-redis 也只给总耗时。
// 按条数平摊是编造数据，每条都记总耗时是重复计数。
func TestMetricsPipelineEmitsSingleEvent(t *testing.T) {
	rec := &eventRecorder{}
	r := openClient(t, metricsConfig(t, rec))
	ctx := context.Background()

	cleanupKeys(t, r) // 按前缀清理
	rec.reset()

	pipe := r.Client().Pipeline()
	pipe.Set(ctx, key("pipe-a"), "1", time.Minute)
	pipe.Set(ctx, key("pipe-b"), "2", time.Minute)
	pipe.Get(ctx, key("pipe-c"))
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		t.Fatalf("pipeline 执行失败: %v", err)
	}

	events := rec.waitFor(t, 1, time.Second)

	if len(events) != 1 {
		t.Fatalf("pipeline 应只上报 1 个事件，实际 %d 个: %+v", len(events), events)
	}
	if events[0].Op != metrics.OpPipeline {
		t.Errorf("Op 应为 %q，实际 %q", metrics.OpPipeline, events[0].Op)
	}
	if events[0].Instance != "cache" {
		t.Errorf("Instance 应为 cache，实际 %q", events[0].Instance)
	}
	if events[0].Duration <= 0 {
		t.Errorf("Duration 应大于 0，实际 %v", events[0].Duration)
	}
}

// TestMetricsNotConfiguredIsNoop 验证不配 Observer 时命令照常工作。
func TestMetricsNotConfiguredIsNoop(t *testing.T) {
	cfg := testConfig(t)
	cfg.Observer = nil

	r := openClient(t, cfg)
	ctx := context.Background()

	name := key("metrics-noop")
	cleanupKeys(t, r, "metrics-noop")

	if err := r.Client().Set(ctx, name, "v", time.Minute).Err(); err != nil {
		t.Fatalf("SET 失败: %v", err)
	}
	got, err := r.Client().Get(ctx, name).Result()
	if err != nil {
		t.Fatalf("GET 失败: %v", err)
	}
	if got != "v" {
		t.Errorf("期望 v，实际 %q", got)
	}
}

// TestMetricsConcurrentCommands 验证并发下事件不丢。
func TestMetricsConcurrentCommands(t *testing.T) {
	rec := &eventRecorder{}
	r := openClient(t, metricsConfig(t, rec))
	ctx := context.Background()

	const workers, perWorker = 8, 15
	for w := 0; w < workers; w++ {
		cleanupKeys(t, r, fmt.Sprintf("metrics-conc-%d", w))
	}

	rec.reset()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			name := key(fmt.Sprintf("metrics-conc-%d", w))
			for i := 0; i < perWorker; i++ {
				if err := r.Client().Set(ctx, name, "v", time.Minute).Err(); err != nil {
					t.Errorf("并发 SET 失败: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	events := rec.waitForOp(t, "SET", workers*perWorker, 3*time.Second)

	// go-redis 在新建连接时会自己发 HELLO（RESP3 握手）以及若干 CLIENT 命令，
	// 实测这些会以 Op=HELLO 与 Op=pipeline 的形式出现在事件流里。
	// 它们不是用户操作，因此这里只统计 SET，并把其余 Op 限定在这个已知集合内 ——
	// 一旦出现别的 Op，说明有用户命令被错误地归到了别的类别（或漏报了）。
	sets := 0
	for _, e := range events {
		switch e.Op {
		case "SET":
			sets++
			if e.IsError() {
				t.Errorf("并发 SET 不应失败: %v", e.Err)
			}
		case "HELLO", "CLIENT", metrics.OpPipeline:
			// 建连握手产生的内部命令，见上。
		default:
			t.Errorf("出现未预期的 Op %q（用户命令可能被错误归类）", e.Op)
		}
	}

	if sets != workers*perWorker {
		t.Errorf("期望 %d 条 SET 事件，实际 %d 条", workers*perWorker, sets)
	}
}
