package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

// 本文件是连接**真实 MySQL** 的集成测试，覆盖连接池配置是否真正生效、
// 并发访问、Close / Reconnect 等生命周期行为。
//
// 默认连接本机开发环境（127.0.0.1:3306，root/123456），可用环境变量覆盖：
//
//	TEST_MYSQL_DSN   完整 DSN
//	TEST_MYSQL_DB    要使用的库名（默认 gointra_test，会自动创建）
//
// 跳过策略：`go test -short` 会跳过全部集成用例；MySQL 不可达时也会跳过，
// 但跳过信息里会写明地址与如何覆盖，避免出现"什么都没跑却是绿的"。
// 注意与 rabbitmq 包的区别：一旦**连得上但配置/操作出错**，用例会直接失败 ——
// 那种情况属于真实缺陷，不该被当成环境问题。

const defaultTestDB = "gointra_test"

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// testDSN 返回测试用的完整 DSN（含库名）与库名。
//
// 优先用 TEST_MYSQL_DSN；否则按 127.0.0.1:3306 + root/123456 拼一个，
// 库名由 TEST_MYSQL_DB 决定（默认 gointra_test）。
func testDSN(t *testing.T) (dsn, dbName string) {
	t.Helper()

	if v := os.Getenv("TEST_MYSQL_DSN"); v != "" {
		cfg, err := mysqldrv.ParseDSN(v)
		if err != nil {
			t.Fatalf("TEST_MYSQL_DSN 无法解析: %v", err)
		}
		return v, cfg.DBName
	}

	dbName = envOr("TEST_MYSQL_DB", defaultTestDB)
	dsn = "root:123456@tcp(127.0.0.1:3306)/" + dbName +
		"?charset=utf8mb4&parseTime=True&loc=Local"
	return dsn, dbName
}

// serverDSN 把 DSN 里的库名去掉，用于连接服务器本身（建库）。
func serverDSN(t *testing.T, dsn string) string {
	t.Helper()
	cfg, err := mysqldrv.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("DSN 无法解析: %v", err)
	}
	cfg.DBName = ""
	cfg.Timeout = 3 * time.Second
	return cfg.FormatDSN()
}

// ensureTestDB 确保测试库存在，并返回可直接使用的 DSN。
//
// 使用专属库名（默认 gointra_test），不去碰业务库 ——
// 在别人的环境里执行测试不应该产生副作用。
// 只有"连不上"才 skip；建库失败属于真实问题，直接 fail。
func ensureTestDB(t *testing.T) string {
	t.Helper()

	dsn, dbName := testDSN(t)
	if dbName == "" {
		t.Skip("未指定测试库名：请设置 TEST_MYSQL_DB，或使用含库名的 TEST_MYSQL_DSN")
	}

	conn, err := sql.Open("mysql", serverDSN(t, dsn))
	if err != nil {
		t.Skipf("无法构造到 MySQL 的连接: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		t.Skipf("MySQL 不可达，跳过集成测试: %v\n"+
			"请启动 MySQL，或用 TEST_MYSQL_DSN / TEST_MYSQL_DB 覆盖", err)
	}

	// 库名可能来自环境变量，这里做了反引号转义。
	stmt := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` "+
		"CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci", escapeIdent(dbName))
	if _, err := conn.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("创建测试库 %s 失败（这是真实问题，不是环境问题）: %v", dbName, err)
	}
	return dsn
}

// escapeIdent 转义标识符中的反引号。
func escapeIdent(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r == '`' {
			out = append(out, '`')
		}
		out = append(out, r)
	}
	return string(out)
}

// testConfig 返回指向测试库的、适合跑测试的基础配置。
func testConfig(t *testing.T) Config {
	t.Helper()
	if testing.Short() {
		t.Skip("集成测试需要真实 MySQL，-short 模式下跳过")
	}

	return Config{
		Dsn:                 ensureTestDB(t),
		MaxOpenConns:        8,
		MaxIdleConns:        8,
		ConnMaxLifetime:     10 * time.Minute,
		ConnMaxIdleTime:     time.Minute,
		ConnectTimeout:      3 * time.Second,
		DialAttempts:        1, // 测试里失败要立刻暴露，不重试
		HealthCheckInterval: time.Hour,
		LogLevel:            "error", // 别让 SQL 日志淹没测试输出
	}
}

func openClient(t *testing.T, cfg Config) *MySQL {
	t.Helper()
	m, err := Open(cfg)
	if err != nil {
		t.Fatalf("MySQL 可达但初始化失败（这是真实问题，不是环境问题）: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// ---------------------------------------------------------------------------
// 连接与连接池
// ---------------------------------------------------------------------------

// TestOpenAndHealth 验证启动即真实探活（而不是 gorm 的懒连接），
// 以及 Healthy / Closed 在生命周期中的取值。
func TestOpenAndHealth(t *testing.T) {
	// 探活间隔设得很大，确保 Healthy 反映的是"刚建连成功"而不是监控协程跑过
	cfg := testConfig(t)
	cfg.HealthCheckInterval = time.Hour

	m := openClient(t, cfg)

	if m.Closed() {
		t.Error("刚创建的客户端不应是已关闭状态")
	}
	if !m.Healthy() {
		t.Error("刚成功建连时 Healthy() 应为 true")
	}
	if err := m.HealthCheck(context.Background()); err != nil {
		t.Errorf("HealthCheck 失败: %v", err)
	}
	if m.DB() == nil {
		t.Error("DB() 不应返回 nil")
	}
	if m.SQL() == nil {
		t.Error("SQL() 不应返回 nil")
	}

	// 关闭后 Healthy 必须回到 false，否则监控会给出误导性的健康结论
	if err := m.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if m.Healthy() {
		t.Error("Close 后 Healthy() 应为 false")
	}
}

// TestPoolSettingsActuallyApplied 是"连接池提高性能"这一需求的核心断言：
// 配置里的池参数必须真的落到 database/sql 上，而不是只存在于结构体里。
func TestPoolSettingsActuallyApplied(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxOpenConns = 6
	cfg.MaxIdleConns = 4
	cfg.ConnMaxLifetime = 7 * time.Minute
	cfg.ConnMaxIdleTime = 90 * time.Second

	m := openClient(t, cfg)

	got := m.Stats()
	if got.MaxOpenConnections != 6 {
		t.Errorf("MaxOpenConnections 期望 6，实际 %d（配置没落到连接池上）", got.MaxOpenConnections)
	}

	// 有效配置应已补齐默认值，且 MaxIdleConns 不超过 MaxOpenConns
	eff := m.Config()
	if eff.MaxOpenConns != 6 || eff.MaxIdleConns != 4 {
		t.Errorf("生效配置不符: MaxOpenConns=%d MaxIdleConns=%d", eff.MaxOpenConns, eff.MaxIdleConns)
	}
	if eff.ConnMaxLifetime != 7*time.Minute {
		t.Errorf("ConnMaxLifetime 期望 7m，实际 %s", eff.ConnMaxLifetime)
	}
	if eff.ConnMaxIdleTime != 90*time.Second {
		t.Errorf("ConnMaxIdleTime 期望 90s，实际 %s", eff.ConnMaxIdleTime)
	}
}

// TestDefaultPoolIsSane 不显式配置时也应得到一组合理的池参数，
// 而不是 database/sql 那种 MaxIdleConns=2 的默认值。
func TestDefaultPoolIsSane(t *testing.T) {
	if testing.Short() {
		t.Skip("集成测试需要真实 MySQL，-short 模式下跳过")
	}
	dsn := ensureTestDB(t)

	m := openClient(t, Config{
		Dsn:                 dsn,
		DialAttempts:        1,
		HealthCheckInterval: time.Hour,
		LogLevel:            "error",
	})

	eff := m.Config()
	if eff.MaxIdleConns <= 2 {
		t.Errorf("默认 MaxIdleConns 应远大于 database/sql 的 2，实际 %d", eff.MaxIdleConns)
	}
	if eff.MaxIdleConns > eff.MaxOpenConns {
		t.Errorf("MaxIdleConns(%d) 不应大于 MaxOpenConns(%d)", eff.MaxIdleConns, eff.MaxOpenConns)
	}
	if m.Stats().MaxOpenConnections != eff.MaxOpenConns {
		t.Errorf("默认 MaxOpenConns 未生效: 统计=%d 配置=%d",
			m.Stats().MaxOpenConnections, eff.MaxOpenConns)
	}
}

// TestPoolReuseAcrossQueries 验证连接会被复用（空闲连接数增长），
// 这是连接池"避免反复拨号"的直接体现。
func TestPoolReuseAcrossQueries(t *testing.T) {
	m := openClient(t, testConfig(t))

	ctx := context.Background()
	for i := 0; i < 20; i++ {
		var one int
		if err := m.DB().WithContext(ctx).Raw("SELECT 1").Scan(&one).Error; err != nil {
			t.Fatalf("第 %d 次查询失败: %v", i, err)
		}
	}

	// 让空闲连接归位（database/sql 在释放后异步归还空闲计数）
	deadline := time.Now().Add(2 * time.Second)
	var stats sql.DBStats
	for time.Now().Before(deadline) {
		stats = m.Stats()
		if stats.Idle > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if stats.Idle == 0 {
		t.Errorf("查询结束后应存在空闲连接供复用，实际 Idle=0（统计: %+v）", stats)
	}
}

// ---------------------------------------------------------------------------
// 功能
// ---------------------------------------------------------------------------

type testOrder struct {
	ID     uint64 `gorm:"primaryKey"`
	UserID uint64 `gorm:"index"`
	Amount int64
}

// TestCRUDWithGorm 走一遍建表 → 写 → 读 → 删，确认 GORM 集成正常。
func TestCRUDWithGorm(t *testing.T) {
	m := openClient(t, testConfig(t))
	ctx := context.Background()

	table := fmt.Sprintf("gointra_it_%d", time.Now().UnixNano())
	db := m.DB().WithContext(ctx)

	if err := db.Table(table).AutoMigrate(&testOrder{}); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	t.Cleanup(func() {
		_ = m.DB().Exec("DROP TABLE IF EXISTS `" + escapeIdent(table) + "`").Error
	})

	// 写
	for i := 1; i <= 5; i++ {
		row := testOrder{ID: uint64(i), UserID: 1001, Amount: int64(i * 100)}
		if err := db.Table(table).Create(&row).Error; err != nil {
			t.Fatalf("插入第 %d 行失败: %v", i, err)
		}
	}

	// 读
	var rows []testOrder
	if err := db.Table(table).Where("user_id = ?", 1001).Order("id").Find(&rows).Error; err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if len(rows) != 5 {
		t.Errorf("期望 5 行，实际 %d", len(rows))
	}

	// 事务回滚
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Table(table).
			Create(&testOrder{ID: 999, UserID: 1001, Amount: 1}).Error; err != nil {
			return err
		}
		return errors.New("故意回滚")
	})
	if err == nil || err.Error() != "故意回滚" {
		t.Fatalf("事务应返回「故意回滚」，实际: %v", err)
	}
	var n int64
	if err := db.Table(table).Where("id = ?", 999).Count(&n).Error; err != nil {
		t.Fatalf("统计失败: %v", err)
	}
	if n != 0 {
		t.Errorf("回滚后不应存在 id=999 的行，实际 %d 行", n)
	}
}

// ---------------------------------------------------------------------------
// 并发
// ---------------------------------------------------------------------------

// TestConcurrentQueries 在 -race 下并发使用客户端。
//
// 旧版在这里会直接暴露数据竞争：写入方持结构体 m.mu，读取方持包级 mu，
// 两把锁互不相干，等于没有互斥。
func TestConcurrentQueries(t *testing.T) {
	m := openClient(t, testConfig(t))
	ctx := context.Background()

	const workers, rounds = 16, 15
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				// 同时读写连接池状态，制造与 DB()/Stats() 的并发
				_ = m.Stats()
				_ = m.Healthy()
				_ = m.Closed()

				var got int
				if err := m.DB().WithContext(ctx).Raw("SELECT ?", w*rounds+i).Scan(&got).Error; err != nil {
					errCh <- fmt.Errorf("worker %d 第 %d 轮: %w", w, i, err)
					return
				}
				if got != w*rounds+i {
					errCh <- fmt.Errorf("worker %d 第 %d 轮返回值错误: %d", w, i, got)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// TestConcurrentHealthAndStats 并发调用读方法，专门针对旧版的双锁问题。
func TestConcurrentHealthAndStats(t *testing.T) {
	m := openClient(t, testConfig(t))
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = m.HealthCheck(ctx)
				_ = m.Stats()
				_ = m.DB()
				_ = m.SQL()
			}
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// 生命周期
// ---------------------------------------------------------------------------

// TestCloseIsIdempotentAndClean 验证 Close 幂等、不死锁，且关闭后状态自洽。
//
// 旧版在持锁状态下 return，defer 的 Unlock 不执行，
// 第二次 Close（或任何抢锁操作）会永久阻塞。
func TestCloseIsIdempotentAndClean(t *testing.T) {
	m := openClient(t, testConfig(t))

	done := make(chan error, 1)
	go func() {
		// 第三次调用必须也立刻返回
		_ = m.Close()
		_ = m.Close()
		done <- m.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("重复 Close 不应报错: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close 超过 5s 未返回：出现死锁（旧版的缺陷）")
	}

	if !m.Closed() {
		t.Error("Close 后 Closed() 应为 true")
	}
	// 关闭后各查询接口应有明确结果，而不是 panic
	if err := m.HealthCheck(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后 HealthCheck 应返回 ErrClosed，实际: %v", err)
	}
	if m.DB() != nil {
		t.Error("关闭后 DB() 应返回 nil")
	}
	if m.SQL() != nil {
		t.Error("关闭后 SQL() 应返回 nil")
	}
	if stats := m.Stats(); stats.OpenConnections != 0 {
		t.Errorf("关闭后不应有打开的连接，实际 %d", stats.OpenConnections)
	}
	// 再次 Close 仍然安全
	if err := m.Close(); err != nil {
		t.Errorf("再次 Close 应返回 nil，实际: %v", err)
	}
}

// TestReconnect 验证主动重建连接池可用，并且旧池被真正关闭（不泄漏）。
func TestReconnect(t *testing.T) {
	m := openClient(t, testConfig(t))
	ctx := context.Background()

	before := m.SQL()
	if before == nil {
		t.Fatal("SQL() 不应为 nil")
	}

	if err := m.Reconnect(); err != nil {
		t.Fatalf("Reconnect 失败: %v", err)
	}

	after := m.SQL()
	if after == nil {
		t.Fatal("Reconnect 后 SQL() 不应为 nil")
	}
	if before == after {
		t.Error("Reconnect 应换成一个新的连接池")
	}

	// 旧池必须已被关闭：对它执行 Ping 应报错
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := before.PingContext(pingCtx); err == nil {
		t.Error("旧连接池应已被关闭（否则就是旧版那种重连泄漏）")
	}

	// 新池应可正常使用
	var one int
	if err := m.DB().WithContext(ctx).Raw("SELECT 1").Scan(&one).Error; err != nil {
		t.Fatalf("重连后查询失败: %v", err)
	}
	if one != 1 {
		t.Errorf("重连后查询返回值错误: %d", one)
	}
}

// TestReconnectAfterCloseFails 关闭后重建应被明确拒绝，而不是产生一个半死状态。
func TestReconnectAfterCloseFails(t *testing.T) {
	m := openClient(t, testConfig(t))
	if err := m.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if err := m.Reconnect(); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后 Reconnect 应返回 ErrClosed，实际: %v", err)
	}
}

// TestHealthCheckRespectsContext ctx 取消时 HealthCheck 应尽快返回。
func TestHealthCheckRespectsContext(t *testing.T) {
	m := openClient(t, testConfig(t))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立刻取消

	if err := m.HealthCheck(ctx); err == nil {
		t.Error("ctx 已取消时 HealthCheck 应返回错误")
	}
}

// TestLegacyGetStillWorks 兼容层不应被破坏。
func TestLegacyGetStillWorks(t *testing.T) {
	cfg := testConfig(t)

	first, err := Get(cfg)
	if err != nil {
		t.Fatalf("Get 失败: %v", err)
	}
	second, err := Get(cfg)
	if err != nil {
		t.Fatalf("第二次 Get 失败: %v", err)
	}
	if first != second {
		t.Error("Get 应返回同一个单例")
	}

	// 废弃的别名应与新方法等价
	if first.GetDB() != first.DB() {
		t.Error("GetDB 应与 DB 等价")
	}
	if first.GetSql() != first.SQL() {
		t.Error("GetSql 应与 SQL 等价")
	}

	var one int
	if err := first.GetDB().Raw("SELECT 1").Scan(&one).Error; err != nil {
		t.Fatalf("兼容层查询失败: %v", err)
	}
}
