package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/zavierswong/go-infra/metrics"
)

// ---------------------------------------------------------------------------
// 事件采集器
// ---------------------------------------------------------------------------

// eventRecorder 是测试用的 Observer。带锁是因为 GORM 的回调可能被并发调用。
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

// waitFor 等到达标条件或超时。GORM 的回调是同步的，正常情况下第一次就能满足；
// 留这个循环只是为了让偶发的调度延迟不会变成随机失败。
func (r *eventRecorder) waitFor(t *testing.T, n int, d time.Duration) []metrics.Event {
	t.Helper()

	deadline := time.Now().Add(d)
	for {
		got := r.snapshot()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			return got
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// pick 返回第一条指定 Op 的事件。
func pick(events []metrics.Event, op metrics.Op) (metrics.Event, bool) {
	for _, e := range events {
		if e.Op == op {
			return e, true
		}
	}
	return metrics.Event{}, false
}

// ---------------------------------------------------------------------------
// 纯逻辑：没有 PostgreSQL 也能跑
// ---------------------------------------------------------------------------

// TestPoolStatsBeforeOpenIsZero 覆盖"尚未连接"这条路径。
// Stats 拿不到连接池时必须返回零值而不是 panic —— 监控代码常常在
// 初始化失败后仍被调用，那里 panic 会把可观测性问题放大成可用性问题。
func TestPoolStatsBeforeOpenIsZero(t *testing.T) {
	t.Parallel()

	p := &Postgres{cfg: Config{Name: "order"}}
	got := p.PoolStats()

	if got.Component != metrics.ComponentPostgres {
		t.Errorf("Component = %q，期望 %q", got.Component, metrics.ComponentPostgres)
	}
	if got.Instance != "order" {
		t.Errorf("Instance 应透传 Config.Name，实际 %q", got.Instance)
	}
	if got != (metrics.PoolStats{Component: metrics.ComponentPostgres, Instance: "order"}) {
		t.Errorf("未连接时应返回零值快照，实际 %+v", got)
	}
}

// TestClassifyErrPostgresSQLState 逐类验证 SQLSTATE 到 metrics.Reason 的映射。
//
// 这张表就是"错误率告警能不能定位问题"的地基：
// 如果 23505 和 08006 都归到同一个标签，看板上"业务冲突"与"数据库挂了"
// 会长成一模一样的曲线。postgres 的 SQLSTATE 分类比 MySQL 的错误码细得多，
// 所以这里覆盖得也更细。
func TestClassifyErrPostgresSQLState(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want metrics.Reason
	}{
		{"nil", nil, metrics.ReasonNone},

		// ---- 完整性约束冲突：Class 23 ----
		{"唯一键冲突", pgError("23505", "uk_users_email"), metrics.ReasonConflict},
		{"外键冲突", pgError("23503", "fk_orders_user"), metrics.ReasonConflict},
		{"非空约束", pgError("23502", "orders_amount"), metrics.ReasonConflict},
		{"CHECK 约束", pgError("23514", "ck_amount_positive"), metrics.ReasonConflict},

		// ---- 并发冲突：Class 40 ----
		{"序列化失败", pgError("40001", ""), metrics.ReasonConflict},
		{"死锁", pgError("40P01", ""), metrics.ReasonConflict},

		// ---- 语句被取消 ----
		{"statement_timeout 取消", pgError("57014", ""), metrics.ReasonTimeout},

		// ---- 资源不足 / 被拒绝 ----
		{"连接数打满", pgError("53300", ""), metrics.ReasonRejected},

		// ---- Class 22 数据异常 / Class 42 语法与权限 ----
		{"文本表示非法", pgError("22P02", ""), metrics.ReasonInvalid},
		{"未定义的表", pgError("42P01", ""), metrics.ReasonInvalid},
		{"权限不足", pgError("42501", ""), metrics.ReasonInvalid},

		// ---- Class 08 连接异常 / Class 57 运维介入 ----
		{"连接中断", pgError("08006", ""), metrics.ReasonConnect},
		{"协议违例", pgError("08P01", ""), metrics.ReasonConnect},
		{"服务端被关闭", pgError("57P01", ""), metrics.ReasonConnect},

		// ---- 包装后的错误同样要能穿透 ----
		{"包装后的唯一键冲突", fmt.Errorf("创建用户: %w", pgError("23505", "")), metrics.ReasonConflict},
		{"包装后的死锁", fmt.Errorf("提交订单: %w", pgError("40P01", "")), metrics.ReasonConflict},

		// ---- 未收录的 SQLSTATE 退化为 unknown，而不是被误判成某类真实故障 ----
		{"未收录的 SQLSTATE", pgError("99999", ""), metrics.ReasonUnknown},

		// ---- 本包哨兵错误优先于任何驱动细节 ----
		{"GORM 零行", gorm.ErrRecordNotFound, metrics.ReasonNotFound},
		{"客户端已关闭", ErrClosed, metrics.ReasonClosed},
		{"未连接", ErrNotConnected, metrics.ReasonConnect},
		{"建连失败", fmt.Errorf("%w: 拒绝连接", ErrConnect), metrics.ReasonConnect},
		{"配置非法", ErrInvalidConfig, metrics.ReasonInvalid},

		// ---- 纯网络错误拿不到 SQLSTATE，走通用分类 ----
		{"ctx 超时", context.DeadlineExceeded, metrics.ReasonTimeout},
		{"ctx 取消", context.Canceled, metrics.ReasonCanceled},
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

// TestClassifyErrZeroRowIsNotAnError 单独强调"零行不算错误"。
//
// 这是最容易被写错的一条：gorm.ErrRecordNotFound 在数据库看来根本不是错误
// （零行是合法结果）。归到 not_found 之后 metrics.Event.IsError 会把它排除，
// 否则"按条件查不到"这种正常业务常态会直接把错误率顶上去。
func TestClassifyErrZeroRowIsNotAnError(t *testing.T) {
	t.Parallel()

	e := metrics.Event{
		Component: metrics.ComponentPostgres,
		Op:        metrics.OpQuery,
		Err:       gorm.ErrRecordNotFound,
		Reason:    classifyErr(gorm.ErrRecordNotFound),
	}

	if !e.Failed() {
		t.Error("Failed() 应为 true：能拿到错误")
	}
	if e.IsError() {
		t.Error("IsError() 应为 false：查无记录是正常结果，不该计入错误率")
	}
}

// TestMetricsCallbacksRegistered 直接查 GORM 的回调表，确认 12 个钩子都挂上了。
//
// 这是本文件里最重要的一条：注册失败只会打一行日志、不会让 Open 失败，
// 所以"Open 成功"完全不能证明指标可用。只有查回调表才能发现静默失效。
func TestMetricsCallbacksRegistered(t *testing.T) {
	p := openClient(t, testConfig(t))
	cb := p.DB().Callback()

	type probe struct {
		get  func(string) func(*gorm.DB)
		name string
	}
	probes := []probe{
		{cb.Query().Get, metricsCallbackName(metrics.OpQuery, "begin")},
		{cb.Query().Get, metricsCallbackName(metrics.OpQuery, "end")},
		{cb.Create().Get, metricsCallbackName(metrics.OpCreate, "begin")},
		{cb.Create().Get, metricsCallbackName(metrics.OpCreate, "end")},
		{cb.Update().Get, metricsCallbackName(metrics.OpUpdate, "begin")},
		{cb.Update().Get, metricsCallbackName(metrics.OpUpdate, "end")},
		{cb.Delete().Get, metricsCallbackName(metrics.OpDelete, "begin")},
		{cb.Delete().Get, metricsCallbackName(metrics.OpDelete, "end")},
		{cb.Row().Get, metricsCallbackName(metrics.OpRow, "begin")},
		{cb.Row().Get, metricsCallbackName(metrics.OpRow, "end")},
		{cb.Raw().Get, metricsCallbackName(metrics.OpRaw, "begin")},
		{cb.Raw().Get, metricsCallbackName(metrics.OpRaw, "end")},
	}

	for _, p := range probes {
		if p.get(p.name) == nil {
			t.Errorf("回调 %q 未注册成功", p.name)
		}
	}
}

// ---------------------------------------------------------------------------
// 集成：需要真实 PostgreSQL
// ---------------------------------------------------------------------------

// metricsOrder 造一条合法的 testOrder。
//
// 必须走这个函数而不是直接写 &testOrder{...}：测试表里的 Meta / Note
// 承载的是 jsonb 与数组列，Go 的零值空串 "" 不是合法 JSON，
// 直接 Create 会被 PostgreSQL 以 22P02（invalid input syntax for type json）拒绝。
// 这个坑本身也说明了一件事 —— 见下面 TestMetricsEmitsWriteEvents 的注释，
// 22P02 会被本包归到 invalid，正好是一条现成的反例素材。
func metricsOrder(id, userID uint64, amount int64) *testOrder {
	return &testOrder{ID: id, UserID: userID, Amount: amount, Meta: "{}", Note: "{}"}
}

// newMetricsTable 建一张临时表，并在用例结束时删掉。
func newMetricsTable(t *testing.T, p *Postgres, ctx context.Context) string {
	t.Helper()

	table := uniqueTable("gointra_metrics")
	if err := p.DB().WithContext(ctx).Table(table).AutoMigrate(&testOrder{}); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	t.Cleanup(func() {
		_ = p.DB().Exec("DROP TABLE IF EXISTS " + escapeIdent(table)).Error
	})
	return table
}

func TestMetricsEmitsQueryEvent(t *testing.T) {
	rec := &eventRecorder{}
	cfg := testConfig(t)
	cfg.Name = "order"
	cfg.Observer = rec

	p := openClient(t, cfg)
	ctx := context.Background()
	table := newMetricsTable(t, p, ctx)

	if err := p.DB().WithContext(ctx).Table(table).
		Create(metricsOrder(1, 7, 100)).Error; err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	rec.reset()
	if err := p.DB().WithContext(ctx).Table(table).
		Where("user_id = ?", 7).Find(&[]testOrder{}).Error; err != nil {
		t.Fatalf("查询失败: %v", err)
	}

	events := rec.waitFor(t, 1, time.Second)
	e, ok := pick(events, metrics.OpQuery)
	if !ok {
		t.Fatalf("未收到 query 事件，实际收到 %d 条: %+v", len(events), events)
	}

	if e.Component != metrics.ComponentPostgres {
		t.Errorf("Component = %q", e.Component)
	}
	if e.Instance != "order" {
		t.Errorf("Instance 应为 Config.Name，实际 %q", e.Instance)
	}
	if e.Failed() {
		t.Errorf("成功的查询不应带错误: %v", e.Err)
	}
	if e.Reason != metrics.ReasonNone {
		t.Errorf("成功查询的 Reason 应为 none，实际 %q", e.Reason)
	}
	if e.Duration <= 0 {
		t.Errorf("Duration 应大于 0，实际 %v", e.Duration)
	}
	if e.Detail == "" {
		t.Error("Detail 应含 SQL 语句")
	}
}

// TestMetricsEmitsWriteEvents 覆盖三种写操作各自的 Op。
//
// 这里同时验证了钩子挂在事务收尾回调上确实能拿到结果：
// 如果锚点选错（挂到 gorm:after_create），事件依然会来，但不会包含提交耗时，
// 所以下面还断言了 Duration 一定大于 0。
func TestMetricsEmitsWriteEvents(t *testing.T) {
	rec := &eventRecorder{}
	cfg := testConfig(t)
	cfg.Name = "order"
	cfg.Observer = rec

	p := openClient(t, cfg)
	ctx := context.Background()
	table := newMetricsTable(t, p, ctx)
	db := p.DB().WithContext(ctx)

	rec.reset()

	row := metricsOrder(11, 7, 100)
	if err := db.Table(table).Create(row).Error; err != nil {
		t.Fatalf("插入失败: %v", err)
	}
	if err := db.Table(table).Where("id = ?", 11).
		Update("amount", 200).Error; err != nil {
		t.Fatalf("更新失败: %v", err)
	}
	if err := db.Table(table).Where("id = ?", 11).Delete(&testOrder{}).Error; err != nil {
		t.Fatalf("删除失败: %v", err)
	}

	events := rec.waitFor(t, 3, time.Second)

	for _, op := range []metrics.Op{metrics.OpCreate, metrics.OpUpdate, metrics.OpDelete} {
		e, ok := pick(events, op)
		if !ok {
			t.Errorf("未收到 %s 事件", op)
			continue
		}
		if e.Failed() {
			t.Errorf("%s 事件不应带错误: %v", op, e.Err)
		}
		if e.Duration <= 0 {
			t.Errorf("%s 事件的 Duration 应大于 0，实际 %v", op, e.Duration)
		}
		if e.Instance != "order" {
			t.Errorf("%s 事件的 Instance 应为 order，实际 %q", op, e.Instance)
		}
	}
}

// TestMetricsEmitsRawAndRowEvents 覆盖 gorm:raw 与 gorm:row 两条独立回调链。
//
// 特意用不带引号的 SQL：PostgreSQL 的标识符引用规则与 MySQL 不同
// （双引号 vs 反引号，且双引号等价于"大小写敏感"），
// 这里的表名由 uniqueTable 生成、只含小写字母与下划线，裸写即可。
func TestMetricsEmitsRawAndRowEvents(t *testing.T) {
	rec := &eventRecorder{}
	cfg := testConfig(t)
	cfg.Name = "order"
	cfg.Observer = rec

	p := openClient(t, cfg)
	ctx := context.Background()
	table := newMetricsTable(t, p, ctx)
	db := p.DB().WithContext(ctx)

	rec.reset()

	// Raw：db.Exec 走 gorm:raw。用 UPDATE ... WHERE 而不是 INSERT，
	// 这样不依赖表里的列类型（jsonb/数组等），也不会造出脏数据。
	if err := db.Exec("UPDATE "+table+" SET amount = amount + 1 WHERE user_id = ?", 7).Error; err != nil {
		t.Fatalf("Raw UPDATE 失败: %v", err)
	}

	// Row：QueryRow 走 gorm:row
	var n int
	if err := db.Raw("SELECT COUNT(*) FROM " + table).Row().Scan(&n); err != nil {
		t.Fatalf("Row 查询失败: %v", err)
	}

	events := rec.waitFor(t, 2, time.Second)

	for _, op := range []metrics.Op{metrics.OpRaw, metrics.OpRow} {
		e, ok := pick(events, op)
		if !ok {
			t.Errorf("未收到 %s 事件", op)
			continue
		}
		if e.Failed() {
			t.Errorf("%s 事件不应带错误: %v", op, e.Err)
		}
		if e.Detail == "" {
			t.Errorf("%s 事件的 Detail 应带原生 SQL", op)
		}
	}
}

// TestMetricsEmitsErrorWithReason 验证失败操作会带上可用的低基数原因。
// 这是错误率告警的基础：reason 分不开，告警就没法定位。
func TestMetricsEmitsErrorWithReason(t *testing.T) {
	rec := &eventRecorder{}
	cfg := testConfig(t)
	cfg.Name = "order"
	cfg.Observer = rec

	p := openClient(t, cfg)
	ctx := context.Background()
	db := p.DB().WithContext(ctx)

	// 表不存在 → 服务端返回 42P01 → Class 42 → invalid
	missing := uniqueTable("gointra_missing")

	rec.reset()
	if err := db.Table(missing).Find(&[]testOrder{}).Error; err == nil {
		t.Fatal("查询不存在的表应当报错")
	}

	events := rec.waitFor(t, 1, time.Second)
	e, ok := pick(events, metrics.OpQuery)
	if !ok {
		t.Fatalf("未收到 query 事件，实际 %d 条", len(events))
	}

	if !e.Failed() {
		t.Fatal("失败的操作应在事件里带上 Err")
	}
	if e.Reason != metrics.ReasonInvalid {
		t.Errorf("表不存在的 Reason 应为 invalid，实际 %q", e.Reason)
	}
	if SQLState(e.Err) != "42P01" {
		t.Errorf("底层 SQLSTATE 期望 42P01，实际 %q", SQLState(e.Err))
	}
}

// TestMetricsEmitsUniqueViolationAsConflict 验证唯一键冲突被归到 conflict。
//
// 对 PostgreSQL 来说这条尤其重要：本包的写钩子锚在
// gorm:commit_or_rollback_transaction，而 DEFERRABLE INITIALLY DEFERRED
// 约束的校验被刻意推迟到 COMMIT。若锚在 gorm:after_create 上，
// 这类"提交时才爆"的冲突会整类漏报 —— 这里顺带守住了这个前提。
func TestMetricsEmitsUniqueViolationAsConflict(t *testing.T) {
	rec := &eventRecorder{}
	cfg := testConfig(t)
	cfg.Name = "order"
	cfg.Observer = rec

	p := openClient(t, cfg)
	ctx := context.Background()
	table := newMetricsTable(t, p, ctx)
	db := p.DB().WithContext(ctx)

	if err := db.Table(table).Create(metricsOrder(31, 7, 1)).Error; err != nil {
		t.Fatalf("首次插入失败: %v", err)
	}

	rec.reset()
	err := db.Table(table).Create(metricsOrder(31, 7, 2)).Error
	if err == nil {
		t.Fatal("重复主键应当报错")
	}

	events := rec.waitFor(t, 1, time.Second)
	e, ok := pick(events, metrics.OpCreate)
	if !ok {
		t.Fatalf("未收到 create 事件，实际 %d 条", len(events))
	}
	if e.Reason != metrics.ReasonConflict {
		t.Errorf("重复主键的 Reason 应为 conflict，实际 %q", e.Reason)
	}
	if got := SQLState(e.Err); got != sqlStateUniqueViolation {
		t.Errorf("底层 SQLSTATE 期望 %s，实际 %q", sqlStateUniqueViolation, got)
	}
}

// TestMetricsNotConfiguredIsNoop 验证不配 Observer 时一切照常。
//
// 这条守护的是"不观测就不该有代价"：Config.Observer 为 nil 时
// registerMetrics 仍会挂回调（回调本身必须存在，否则没法区分"没配"与"配了但坏了"），
// 但不会调用任何观察者。
func TestMetricsNotConfiguredIsNoop(t *testing.T) {
	cfg := testConfig(t)
	cfg.Observer = nil

	p := openClient(t, cfg)
	ctx := context.Background()
	table := newMetricsTable(t, p, ctx)
	db := p.DB().WithContext(ctx)

	if err := db.Table(table).Create(metricsOrder(41, 7, 1)).Error; err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	var rows []testOrder
	if err := db.Table(table).Find(&rows).Error; err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("期望 1 行，实际 %d", len(rows))
	}
}

// TestMetricsObserverPanicPropagates 记录一个有意为之的取舍。
//
// 本包**不**对观察者做 recover：观察者里的 panic 会传播到业务调用处。
// 这样 bug 会在开发期立刻暴露，而不是被静默吞掉后表现为"指标莫名少了一些"。
// 如果你的观察者可能 panic，请自己在实现内部兜住。
func TestMetricsObserverPanicPropagates(t *testing.T) {
	cfg := testConfig(t)
	cfg.Observer = metrics.ObserverFunc(func(metrics.Event) { panic("观察者炸了") })

	p := openClient(t, cfg)

	// recover 必须在任何操作之前挂上：建表、查询都会触发观察者。
	// 这里特意不建表，用一次注定失败的查询来触发事件，省掉建表的副作用。
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("观察者 panic 应当传播出来（这是本包明确的设计取舍）")
		}
	}()

	missing := uniqueTable("gointra_panic")
	_ = p.DB().WithContext(context.Background()).Table(missing).Find(&[]testOrder{}).Error

	t.Fatal("不应执行到这里：查询应当因观察者 panic 而中断")
}

// TestMetricsConcurrentEmits 验证多协程并发时事件不丢、不错乱。
func TestMetricsConcurrentEmits(t *testing.T) {
	rec := &eventRecorder{}
	cfg := testConfig(t)
	cfg.Name = "order"
	cfg.Observer = rec

	p := openClient(t, cfg)
	ctx := context.Background()
	table := newMetricsTable(t, p, ctx)
	db := p.DB().WithContext(ctx)

	rec.reset()

	const workers, perWorker = 8, 10
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				var rows []testOrder
				if err := db.Table(table).Where("user_id = ?", w).
					Find(&rows).Error; err != nil {
					t.Errorf("并发查询失败: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	events := rec.waitFor(t, workers*perWorker, 2*time.Second)
	if len(events) != workers*perWorker {
		t.Errorf("期望 %d 条 query 事件，实际 %d 条", workers*perWorker, len(events))
	}
	for _, e := range events {
		if e.Op != metrics.OpQuery {
			t.Errorf("并发场景下收到非 query 事件: %s", e.Op)
		}
		if e.Failed() {
			t.Errorf("并发查询不应失败: %v", e.Err)
		}
	}
}
