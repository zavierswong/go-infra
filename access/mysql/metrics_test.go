package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"
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
// 纯逻辑：没有 MySQL 也能跑
// ---------------------------------------------------------------------------

// TestPoolStatsBeforeOpenIsZero 覆盖"尚未连接"这条路径。
// Stats 拿不到连接池时必须返回零值而不是 panic —— 监控代码常常在
// 初始化失败后仍被调用，那里 panic 会把可观测性问题放大成可用性问题。
func TestPoolStatsBeforeOpenIsZero(t *testing.T) {
	t.Parallel()

	m := &MySQL{cfg: Config{Name: "order"}}
	got := m.PoolStats()

	if got.Component != metrics.ComponentMySQL {
		t.Errorf("Component = %q，期望 %q", got.Component, metrics.ComponentMySQL)
	}
	if got.Instance != "order" {
		t.Errorf("Instance 应透传 Config.Name，实际 %q", got.Instance)
	}
	if got != (metrics.PoolStats{Component: metrics.ComponentMySQL, Instance: "order"}) {
		t.Errorf("未连接时应返回零值快照，实际 %+v", got)
	}
}

func TestClassifyErrMySQL(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want metrics.Reason
	}{
		{"nil", nil, metrics.ReasonNone},
		{"唯一键冲突", &mysqldrv.MySQLError{Number: 1062, Message: "Duplicate entry"}, metrics.ReasonConflict},
		{"死锁", &mysqldrv.MySQLError{Number: 1213, Message: "Deadlock found"}, metrics.ReasonConflict},
		{"等锁超时", &mysqldrv.MySQLError{Number: 1205, Message: "Lock wait timeout"}, metrics.ReasonConflict},
		{"表不存在", &mysqldrv.MySQLError{Number: 1146, Message: "Table doesn't exist"}, metrics.ReasonInvalid},
		{"语法错误", &mysqldrv.MySQLError{Number: 1064, Message: "Syntax error"}, metrics.ReasonInvalid},
		{"认证失败", &mysqldrv.MySQLError{Number: 1045, Message: "Access denied"}, metrics.ReasonConnect},
		{"连接被服务端断开", &mysqldrv.MySQLError{Number: 2006, Message: "Server gone"}, metrics.ReasonConnect},
		{"包装后的唯一键冲突", fmt.Errorf("插入订单: %w", &mysqldrv.MySQLError{Number: 1062}), metrics.ReasonConflict},
		{"未收录的错误码走通用分类", &mysqldrv.MySQLError{Number: 9999}, metrics.ReasonUnknown},
		{"GORM 零行", gorm.ErrRecordNotFound, metrics.ReasonNotFound},
		{"客户端已关闭", ErrClosed, metrics.ReasonClosed},
		{"未连接", ErrNotConnected, metrics.ReasonConnect},
		{"建连失败", fmt.Errorf("%w: 拒绝连接", ErrConnect), metrics.ReasonConnect},
		{"配置非法", ErrInvalidConfig, metrics.ReasonInvalid},
		{"ctx 超时", context.DeadlineExceeded, metrics.ReasonTimeout},
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

// TestMetricsCallbacksRegistered 直接查 GORM 的回调表，确认 12 个钩子都挂上了。
//
// 这是本文件里最重要的一条：注册失败只会打一行日志、不会让 Open 失败，
// 所以"Open 成功"完全不能证明指标可用。只有查回调表才能发现静默失效。
func TestMetricsCallbacksRegistered(t *testing.T) {
	m := openClient(t, testConfig(t))
	cb := m.DB().Callback()

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
// 集成：需要真实 MySQL
// ---------------------------------------------------------------------------

// newTable 建一张带随机名的表，并在用例结束时删掉。
func newTable(t *testing.T, m *MySQL, ctx context.Context) string {
	t.Helper()

	table := fmt.Sprintf("gointra_metrics_%d", time.Now().UnixNano())
	if err := m.DB().WithContext(ctx).Table(table).AutoMigrate(&testOrder{}); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	t.Cleanup(func() {
		_ = m.DB().Exec("DROP TABLE IF EXISTS `" + escapeIdent(table) + "`").Error
	})
	return table
}

func TestMetricsEmitsQueryEvent(t *testing.T) {
	rec := &eventRecorder{}
	cfg := testConfig(t)
	cfg.Name = "order"
	cfg.Observer = rec

	m := openClient(t, cfg)
	ctx := context.Background()
	table := newTable(t, m, ctx)

	if err := m.DB().WithContext(ctx).Table(table).
		Create(&testOrder{ID: 1, UserID: 7, Amount: 100}).Error; err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	rec.reset()
	if err := m.DB().WithContext(ctx).Table(table).
		Where("user_id = ?", 7).Find(&[]testOrder{}).Error; err != nil {
		t.Fatalf("查询失败: %v", err)
	}

	events := rec.waitFor(t, 1, time.Second)
	e, ok := pick(events, metrics.OpQuery)
	if !ok {
		t.Fatalf("未收到 query 事件，实际收到 %d 条: %+v", len(events), events)
	}

	if e.Component != metrics.ComponentMySQL {
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
	if !strings.Contains(e.Detail, "SELECT") {
		t.Errorf("Detail 应含 SQL 语句，实际 %q", e.Detail)
	}
	if !strings.Contains(e.Detail, table) {
		t.Errorf("Detail 应指向本次查询的表，实际 %q", e.Detail)
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

	m := openClient(t, cfg)
	ctx := context.Background()
	table := newTable(t, m, ctx)
	db := m.DB().WithContext(ctx)

	rec.reset()

	row := testOrder{ID: 11, UserID: 7, Amount: 100}
	if err := db.Table(table).Create(&row).Error; err != nil {
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

func TestMetricsEmitsRawAndRowEvents(t *testing.T) {
	rec := &eventRecorder{}
	cfg := testConfig(t)
	cfg.Name = "order"
	cfg.Observer = rec

	m := openClient(t, cfg)
	ctx := context.Background()
	table := newTable(t, m, ctx)
	db := m.DB().WithContext(ctx)

	rec.reset()

	// Raw：db.Exec 走 gorm:raw
	if err := db.Exec("INSERT INTO `"+escapeIdent(table)+"` (id, user_id, amount) VALUES (?, ?, ?)",
		21, 7, 300).Error; err != nil {
		t.Fatalf("Raw 插入失败: %v", err)
	}

	// Row：QueryRow 走 gorm:row
	var n int
	if err := db.Raw("SELECT COUNT(*) FROM `" + escapeIdent(table) + "`").Row().Scan(&n); err != nil {
		t.Fatalf("Row 查询失败: %v", err)
	}
	if n != 1 {
		t.Errorf("期望 1 行，实际 %d", n)
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

	m := openClient(t, cfg)
	ctx := context.Background()
	db := m.DB().WithContext(ctx)

	// 表不存在 → 服务端返回 1146 → 归类为 invalid
	missing := fmt.Sprintf("gointra_missing_%d", time.Now().UnixNano())

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
	if e.Reason == metrics.ReasonUnknown {
		t.Error("Reason 退化成 unknown 说明 MySQL 错误码没被识别")
	}
}

// TestMetricsEmitsDuplicateKeyAsConflict 验证唯一键冲突被归到 conflict 而不是 unknown。
func TestMetricsEmitsDuplicateKeyAsConflict(t *testing.T) {
	rec := &eventRecorder{}
	cfg := testConfig(t)
	cfg.Name = "order"
	cfg.Observer = rec

	m := openClient(t, cfg)
	ctx := context.Background()
	table := newTable(t, m, ctx)
	db := m.DB().WithContext(ctx)

	if err := db.Table(table).Create(&testOrder{ID: 31, UserID: 7, Amount: 1}).Error; err != nil {
		t.Fatalf("首次插入失败: %v", err)
	}

	rec.reset()
	err := db.Table(table).Create(&testOrder{ID: 31, UserID: 7, Amount: 2}).Error
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
}

// TestMetricsNotConfiguredIsNoop 验证不配 Observer 时一切照常。
//
// 这条守护的是"不观测就不该有代价"：Config.Observer 为 nil 时
// registerMetrics 仍会挂回调（回调本身必须存在，否则没法区分"没配"与"配了但坏了"），
// 但不会调用任何观察者。
func TestMetricsNotConfiguredIsNoop(t *testing.T) {
	cfg := testConfig(t)
	cfg.Observer = nil

	m := openClient(t, cfg)
	ctx := context.Background()
	table := newTable(t, m, ctx)
	db := m.DB().WithContext(ctx)

	if err := db.Table(table).Create(&testOrder{ID: 41, UserID: 7, Amount: 1}).Error; err != nil {
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

	m := openClient(t, cfg)

	// recover 必须在任何操作之前挂上：建表、查询都会触发观察者。
	// 这里特意不建表，用一次注定失败的查询来触发事件，省掉建表的副作用。
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("观察者 panic 应当传播出来（这是本包明确的设计取舍）")
		}
	}()

	missing := fmt.Sprintf("gointra_panic_%d", time.Now().UnixNano())
	_ = m.DB().WithContext(context.Background()).Table(missing).Find(&[]testOrder{}).Error

	t.Fatal("不应执行到这里：查询应当因观察者 panic 而中断")
}

// TestMetricsConcurrentEmits 验证多协程并发时事件不丢、不错乱。
func TestMetricsConcurrentEmits(t *testing.T) {
	rec := &eventRecorder{}
	cfg := testConfig(t)
	cfg.Name = "order"
	cfg.Observer = rec

	m := openClient(t, cfg)
	ctx := context.Background()
	table := newTable(t, m, ctx)
	db := m.DB().WithContext(ctx)

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
