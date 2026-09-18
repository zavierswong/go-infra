package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// 本文件是连接**真实 PostgreSQL** 的集成测试，覆盖：
//   - 连接池配置是否真正落到 database/sql 上；
//   - PostgreSQL 专有的服务端会话参数是否真的下达到了服务端（不是只写在结构体里）；
//   - 并发访问、Close / Reconnect 等生命周期行为；
//   - RETURNING / JSONB / 数组 / ON CONFLICT 等 PostgreSQL 特性在 GORM 下的表现。
//
// 默认连接本机开发环境（127.0.0.1:5432，postgres/123456），可用环境变量覆盖：
//
//	TEST_POSTGRES_DSN   完整 DSN（URL 或 keyword/value 皆可）
//	TEST_POSTGRES_DB    要使用的库名（默认 gointra_test，会自动创建）
//
// 跳过策略：`go test -short` 会跳过全部集成用例；PostgreSQL 不可达时也会跳过，
// 但跳过信息里会写明地址与如何覆盖，避免出现"什么都没跑却是绿的"。
// 一旦**连得上但配置/操作出错**，用例会直接失败 —— 那是真实缺陷，不是环境问题。

const defaultTestDB = "gointra_test"

// testDSN 返回测试用的完整 DSN（含库名）与库名。
func testDSN(t *testing.T) (dsn, dbName string) {
	t.Helper()

	if v := os.Getenv("TEST_POSTGRES_DSN"); v != "" {
		info, err := parseConnInfo(v)
		if err != nil {
			t.Fatalf("TEST_POSTGRES_DSN 无法解析: %v", err)
		}
		return v, info.dbName()
	}

	dbName = envOr("TEST_POSTGRES_DB", defaultTestDB)
	dsn = "host=" + envOr("TEST_POSTGRES_HOST", "127.0.0.1") +
		" port=" + envOr("TEST_POSTGRES_PORT", "5432") +
		" user=" + envOr("TEST_POSTGRES_USER", "postgres") +
		" password=" + envOr("TEST_POSTGRES_PASSWORD", "123456") +
		" dbname=" + dbName + " sslmode=disable"
	return dsn, dbName
}

// withDBName 把 DSN 的库名换成另一个。用于连到 postgres 维护库去建测试库。
func withDBName(t *testing.T, dsn, dbName string) string {
	t.Helper()
	info, err := parseConnInfo(dsn)
	if err != nil {
		t.Fatalf("DSN 无法解析: %v", err)
	}
	info.set("dbname", dbName)
	return info.String()
}

// ensureTestDB 确保测试库存在，并返回可直接使用的 DSN。
//
// 使用专属库名（默认 gointra_test），不去碰业务库 ——
// 在别人的环境里执行测试不应该产生副作用。
// 只有"连不上"才 skip；建库失败属于真实问题，直接 fail。
func ensureTestDB(t *testing.T) string {
	t.Helper()

	dsn, dbName := testDSN(t)
	if strings.TrimSpace(dbName) == "" {
		t.Skip("未指定测试库名：请设置 TEST_POSTGRES_DB，或使用含库名的 TEST_POSTGRES_DSN")
	}

	// 先连到维护库 postgres（它一定存在），再按需建测试库
	bootstrap, err := Open(Config{
		Dsn:            withDBName(t, dsn, "postgres"),
		DialAttempts:   1,
		ConnectTimeout: 3 * time.Second,
		LogLevel:       "error",
	})
	if err != nil {
		t.Skipf("PostgreSQL 不可达，跳过集成测试: %v\n"+
			"请启动 PostgreSQL，或用 TEST_POSTGRES_DSN / TEST_POSTGRES_DB 覆盖", err)
	}
	defer func() { _ = bootstrap.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 注意用 SQL() 而不是 DB()：GORM 默认把写操作包在事务里，
	// 而 PostgreSQL 的 CREATE DATABASE 不允许在事务块内执行。
	var exists bool
	if err := bootstrap.SQL().QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists); err != nil {
		t.Fatalf("查询 pg_database 失败（这是真实问题，不是环境问题）: %v", err)
	}
	if !exists {
		stmt := `CREATE DATABASE "` + escapeIdent(dbName) + `"`
		if _, err := bootstrap.SQL().ExecContext(ctx, stmt); err != nil {
			t.Fatalf("创建测试库 %s 失败（这是真实问题，不是环境问题）: %v", dbName, err)
		}
	}
	return dsn
}

// escapeIdent 转义标识符里的双引号。
func escapeIdent(s string) string { return strings.ReplaceAll(s, `"`, `""`) }

// testConfig 返回指向测试库的、适合跑测试的基础配置。
func testConfig(t *testing.T) Config {
	t.Helper()
	if testing.Short() {
		t.Skip("集成测试需要真实 PostgreSQL，-short 模式下跳过")
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
		ApplicationName:     "go-infra-test",
		LogLevel:            "error", // 别让 SQL 日志淹没测试输出
	}
}

func openClient(t *testing.T, cfg Config) *Postgres {
	t.Helper()
	p, err := Open(cfg)
	if err != nil {
		t.Fatalf("PostgreSQL 可达但初始化失败（这是真实问题，不是环境问题）: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// uniqueTable 生成一个带随机后缀的表名，避免并行/重复执行时互相干扰。
func uniqueTable(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

// ---------------------------------------------------------------------------
// 连接与连接池
// ---------------------------------------------------------------------------

// TestOpenAndHealth 验证启动即真实探活，以及 Healthy / Closed 在生命周期中的取值。
func TestOpenAndHealth(t *testing.T) {
	cfg := testConfig(t)
	cfg.HealthCheckInterval = time.Hour // 确保 Healthy 反映的是"刚建连成功"

	p := openClient(t, cfg)

	if p.Closed() {
		t.Error("刚创建的客户端不应是已关闭状态")
	}
	if !p.Healthy() {
		t.Error("刚成功建连时 Healthy() 应为 true")
	}
	if err := p.HealthCheck(context.Background()); err != nil {
		t.Errorf("HealthCheck 失败: %v", err)
	}
	if p.DB() == nil {
		t.Error("DB() 不应返回 nil")
	}
	if p.SQL() == nil {
		t.Error("SQL() 不应返回 nil")
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if p.Healthy() {
		t.Error("Close 后 Healthy() 应为 false")
	}
}

// TestPoolSettingsActuallyApplied 是"连接池可调"这一需求的核心断言：
// 配置里的池参数必须真的落到 database/sql 上，而不是只存在于结构体里。
func TestPoolSettingsActuallyApplied(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxOpenConns = 6
	cfg.MaxIdleConns = 4
	cfg.ConnMaxLifetime = 7 * time.Minute
	cfg.ConnMaxIdleTime = 90 * time.Second

	p := openClient(t, cfg)

	if got := p.Stats().MaxOpenConnections; got != 6 {
		t.Errorf("MaxOpenConnections 期望 6，实际 %d（配置没落到连接池上）", got)
	}

	eff := p.Config()
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
		t.Skip("集成测试需要真实 PostgreSQL，-short 模式下跳过")
	}
	dsn := ensureTestDB(t)

	p := openClient(t, Config{
		Dsn:                 dsn,
		DialAttempts:        1,
		HealthCheckInterval: time.Hour,
		LogLevel:            "error",
	})

	eff := p.Config()
	if eff.MaxIdleConns <= 2 {
		t.Errorf("默认 MaxIdleConns 应远大于 database/sql 的 2，实际 %d", eff.MaxIdleConns)
	}
	if eff.MaxIdleConns > eff.MaxOpenConns {
		t.Errorf("MaxIdleConns(%d) 不应大于 MaxOpenConns(%d)", eff.MaxIdleConns, eff.MaxOpenConns)
	}
	if p.Stats().MaxOpenConnections != eff.MaxOpenConns {
		t.Errorf("默认 MaxOpenConns 未生效: 统计=%d 配置=%d",
			p.Stats().MaxOpenConnections, eff.MaxOpenConns)
	}
}

// TestPoolReuseAcrossQueries 验证连接会被复用（空闲连接数增长），
// 这是连接池"避免反复拨号"的直接体现 —— 对 PostgreSQL 尤其重要，
// 因为每条新连接都要 fork 一个后端进程。
func TestPoolReuseAcrossQueries(t *testing.T) {
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		var one int
		if err := p.DB().WithContext(ctx).Raw("SELECT 1").Scan(&one).Error; err != nil {
			t.Fatalf("第 %d 次查询失败: %v", i, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	var stats sql.DBStats
	for time.Now().Before(deadline) {
		stats = p.Stats()
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
// 服务端会话参数：配了必须真的生效
// ---------------------------------------------------------------------------

// TestServerSettingsActuallyApplied 是本包最有价值的一组断言：
// 每个覆盖项都要能被**服务端自己**看到，而不是只出现在 DSN 字符串里。
//
// 这一层特别容易出错（参数名拼错、被写进 DSN 却没被 pgx 识别、
// 值被转义导致解析失败），而表现又往往是"功能静默不生效"，
// 所以必须逐个向服务端求证。
func TestServerSettingsActuallyApplied(t *testing.T) {
	cfg := testConfig(t)
	cfg.ApplicationName = "go-infra-settings-probe"
	cfg.SearchPath = "app,public"
	cfg.TimeZone = "UTC" // 容器默认是 Asia/Shanghai，用 UTC 才可区分
	cfg.StatementTimeout = 200 * time.Millisecond
	cfg.IdleInTransactionTimeout = 300 * time.Millisecond
	cfg.Settings = map[string]string{"work_mem": "12MB"}

	p := openClient(t, cfg)
	ctx := context.Background()

	// application_name：直接读 pg_stat_activity，确认服务端认出了这个应用名
	var appName string
	if err := p.SQL().QueryRowContext(ctx,
		"SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()").
		Scan(&appName); err != nil {
		t.Fatalf("读取 pg_stat_activity 失败: %v", err)
	}
	if appName != cfg.ApplicationName {
		t.Errorf("application_name 期望 %q，实际 %q", cfg.ApplicationName, appName)
	}

	// 其余会话参数：用 SHOW 逐个核对（SHOW 输出的是服务端归一化后的写法）
	//
	// 注意 search_path 的期望值是**原样**的 "app,public" 而不是 "app, public"：
	// 它是通过启动包（startup packet）作为普通字符串下发的，服务端原样保存；
	// 而 `SET search_path=app,public` 是走 SQL 解析器，会被重新序列化成 ", " 分隔。
	// 两者语义相同（GUC 是字符串列表类型，分隔符与空白都会被解析），
	// search_path 是否真的生效另见 TestSearchPathTakesEffect。
	for _, tc := range []struct {
		param string
		want  string
	}{
		{"timezone", "UTC"},
		{"search_path", "app,public"},
		{"statement_timeout", "200ms"},
		{"idle_in_transaction_session_timeout", "300ms"},
		{"work_mem", "12MB"},
	} {
		var got string
		if err := p.SQL().QueryRowContext(ctx, "SHOW "+tc.param).Scan(&got); err != nil {
			t.Errorf("SHOW %s 失败: %v", tc.param, err)
			continue
		}
		if got != tc.want {
			t.Errorf("服务端 %s 期望 %q，实际 %q（配置没有真正下发到会话）", tc.param, tc.want, got)
		}
	}
}

// TestSearchPathTakesEffect 用语义而非字符串来验证 search_path：
// 建一个专属 schema，把它放在 search_path 首位，current_schema() 就应该指向它。
//
// 这比"SHOW search_path 的字符串对不对"更能说明问题 ——
// 配置写进去但服务端没按列表解析，是这类参数最隐蔽的失败方式。
func TestSearchPathTakesEffect(t *testing.T) {
	const schema = "gointra_it_schema"

	cfg := testConfig(t)
	cfg.SearchPath = schema + ",public"

	p := openClient(t, cfg)
	ctx := context.Background()

	if _, err := p.SQL().ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+schema); err != nil {
		t.Fatalf("创建 schema 失败: %v", err)
	}
	t.Cleanup(func() { _, _ = p.SQL().Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE") })

	var current string
	if err := p.SQL().QueryRowContext(ctx, "SELECT current_schema()").Scan(&current); err != nil {
		t.Fatalf("current_schema() 失败: %v", err)
	}
	if current != schema {
		t.Errorf("current_schema() 期望 %q，实际 %q（search_path 没有生效）", schema, current)
	}
}

// TestStatementTimeoutActuallyCancelsQuery 用**行为**证明 statement_timeout 生效：// 配置 200ms，跑一个 1s 的查询，服务端必须主动取消它（SQLSTATE 57014）。
func TestStatementTimeoutActuallyCancelsQuery(t *testing.T) {
	cfg := testConfig(t)
	cfg.StatementTimeout = 200 * time.Millisecond

	p := openClient(t, cfg)
	ctx := context.Background()

	start := time.Now()
	err := p.DB().WithContext(ctx).Exec("SELECT pg_sleep(5)").Error
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("pg_sleep(5) 应在 statement_timeout=200ms 下被取消，实际成功返回")
	}
	if elapsed > 3*time.Second {
		t.Errorf("应在 200ms 左右被取消，实际耗时 %s（配置未生效？）", elapsed)
	}
	if !IsQueryCanceled(err) {
		t.Errorf("期望 SQLSTATE 57014（query_canceled），实际 SQLSTATE=%q err=%v", SQLState(err), err)
	}
	// 必须是**服务端**取消，而不是客户端 ctx 超时 —— 两者的区别正是
	// "statement_timeout 保护数据库"与"ctx 只是放弃等待"的差别。
	if errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("应是服务端取消（57014），而非客户端 ctx 超时: %v", err)
	}
}

// TestIdleInTransactionTimeoutKillsSession 验证 idle_in_transaction_session_timeout
// 真的会在事务挂起时掐掉连接。
//
// 这是 PostgreSQL 最经典的线上事故：开了事务忘了提交/回滚，
// 连接一直挂在 idle in transaction，持有锁并挡住 vacuum 回收死元组。
func TestIdleInTransactionTimeoutKillsSession(t *testing.T) {
	cfg := testConfig(t)
	cfg.IdleInTransactionTimeout = 300 * time.Millisecond

	p := openClient(t, cfg)
	ctx := context.Background()

	// 用 SQL() 直接取一条连接，避免 GORM 的事务包装
	conn, err := p.SQL().Conn(ctx)
	if err != nil {
		t.Fatalf("获取连接失败: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN 失败: %v", err)
	}

	// 挂在 idle in transaction 上，超过配置的 300ms
	time.Sleep(700 * time.Millisecond)

	var one int
	err = conn.QueryRowContext(ctx, "SELECT 1").Scan(&one)
	if err == nil {
		t.Error("空闲事务超过 idle_in_transaction_session_timeout 后，连接应已被服务端掐断")
	}
}

// TestPreferSimpleProtocolDisablesStatementCache 验证 PreferSimpleProtocol
// 真的切换了协议（这是 PgBouncer 事务池模式下的必需开关）。
//
// 判据：扩展协议会把语句缓存在 pg_prepared_statements 里，简单协议不会。
func TestPreferSimpleProtocolDisablesStatementCache(t *testing.T) {
	for _, tc := range []struct {
		name         string
		simple       bool
		wantPrepared bool
		maxOpenConns int
	}{
		{name: "扩展协议（默认）", simple: false, wantPrepared: true, maxOpenConns: 1},
		{name: "简单协议", simple: true, wantPrepared: false, maxOpenConns: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.PreferSimpleProtocol = tc.simple
			cfg.MaxOpenConns = tc.maxOpenConns
			cfg.MaxIdleConns = tc.maxOpenConns

			p := openClient(t, cfg)
			ctx := context.Background()

			// 同一条语句执行多次：扩展协议下会被缓存为预备语句
			for i := 0; i < 3; i++ {
				var one int
				if err := p.DB().WithContext(ctx).Raw("SELECT 1").Scan(&one).Error; err != nil {
					t.Fatalf("第 %d 次查询失败: %v", i, err)
				}
			}

			var count int
			if err := p.SQL().QueryRowContext(ctx,
				"SELECT count(*) FROM pg_prepared_statements").Scan(&count); err != nil {
				t.Fatalf("读取 pg_prepared_statements 失败: %v", err)
			}

			gotPrepared := count > 0
			if gotPrepared != tc.wantPrepared {
				t.Errorf("PreferSimpleProtocol=%v 时期望 预备语句存在=%v，实际 count=%d",
					tc.simple, tc.wantPrepared, count)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 功能：GORM + PostgreSQL 特性
// ---------------------------------------------------------------------------

type testOrder struct {
	ID     uint64 `gorm:"primaryKey"`
	UserID uint64 `gorm:"index"`
	Amount int64
	Meta   string `gorm:"type:jsonb"`
	Note   string `gorm:"uniqueIndex:uk_gointra_note"`
}

// TestCRUDWithGorm 走一遍建表 → 写 → 读 → 更新 → 事务回滚，
// 并顺带验证 PostgreSQL 的 RETURNING 与 ON CONFLICT。
func TestCRUDWithGorm(t *testing.T) {
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	table := uniqueTable("gointra_it_order")
	db := p.DB().WithContext(ctx)

	if err := db.Table(table).AutoMigrate(&testOrder{}); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	t.Cleanup(func() {
		_, _ = p.SQL().Exec("DROP TABLE IF EXISTS \"" + escapeIdent(table) + "\"")
	})

	// 写：不指定主键，靠 PostgreSQL 的 RETURNING 回填 —— 这是与 MySQL
	// （LastInsertId）不同的一条链，PostgreSQL 无自增 ID 可用。
	for i := 1; i <= 5; i++ {
		row := testOrder{UserID: 1001, Amount: int64(i * 100), Meta: `{"src":"it"}`, Note: fmt.Sprintf("n%d", i)}
		if err := db.Table(table).Create(&row).Error; err != nil {
			t.Fatalf("插入第 %d 行失败: %v", i, err)
		}
		if row.ID == 0 {
			t.Fatalf("第 %d 行插入后主键未被 RETURNING 回填（ID=0）", i)
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
	if len(rows) > 0 && rows[0].Meta != `{"src": "it"}` && rows[0].Meta != `{"src":"it"}` {
		t.Errorf("jsonb 列读回的内容不符: %q", rows[0].Meta)
	}

	// ON CONFLICT DO NOTHING：PostgreSQL 独有的幂等写入，MySQL 要写 INSERT IGNORE
	dup := testOrder{UserID: 1001, Amount: 1, Meta: `{}`, Note: "n1"}
	res := db.Table(table).Clauses(clause.OnConflict{DoNothing: true}).Create(&dup)
	if res.Error != nil {
		t.Fatalf("ON CONFLICT DO NOTHING 失败: %v", res.Error)
	}
	if res.RowsAffected != 0 {
		t.Errorf("唯一键冲突时应 0 行受影响，实际 %d", res.RowsAffected)
	}

	// 事务回滚
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Table(table).
			Create(&testOrder{UserID: 1001, Amount: 1, Meta: `{}`, Note: "rollback"}).Error; err != nil {
			return err
		}
		return errors.New("故意回滚")
	})
	if err == nil || err.Error() != "故意回滚" {
		t.Fatalf("事务应返回「故意回滚」，实际: %v", err)
	}
	var n int64
	if err := db.Table(table).Where("note = ?", "rollback").Count(&n).Error; err != nil {
		t.Fatalf("统计失败: %v", err)
	}
	if n != 0 {
		t.Errorf("回滚后不应存在 note=rollback 的行，实际 %d 行", n)
	}
}

// TestUniqueViolationMapsToBusinessError 唯一键冲突要能被稳定识别，
// 并能取到约束名 —— 这是把数据库错误翻译成业务错误的标准做法。
func TestUniqueViolationMapsToBusinessError(t *testing.T) {
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	table := uniqueTable("gointra_it_uniq")
	db := p.DB().WithContext(ctx)

	if err := db.Table(table).AutoMigrate(&testOrder{}); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	t.Cleanup(func() {
		_, _ = p.SQL().Exec("DROP TABLE IF EXISTS \"" + escapeIdent(table) + "\"")
	})

	row := testOrder{UserID: 1, Amount: 1, Meta: `{}`, Note: "dup"}
	if err := db.Table(table).Create(&row).Error; err != nil {
		t.Fatalf("首次插入失败: %v", err)
	}

	again := testOrder{UserID: 2, Amount: 2, Meta: `{}`, Note: "dup"}
	err := db.Table(table).Create(&again).Error
	if err == nil {
		t.Fatal("重复的 Note 应触发唯一键冲突")
	}

	if got := SQLState(err); got != sqlStateUniqueViolation {
		t.Errorf("SQLSTATE 期望 %s，实际 %q", sqlStateUniqueViolation, got)
	}
	if !IsUniqueViolation(err) {
		t.Error("IsUniqueViolation 应为 true")
	}
	if !IsIntegrityViolation(err) {
		t.Error("IsIntegrityViolation 应为 true")
	}
	if IsRetryable(err) {
		t.Error("唯一键冲突不应被判为可重试（重试还是冲突）")
	}
	if got := ConstraintName(err); !strings.Contains(got, "uk_gointra_note") {
		t.Errorf("约束名期望包含 uk_gointra_note，实际 %q", got)
	}
	// 注意区分：外键/非空等判定不能误报
	if IsForeignKeyViolation(err) || IsDeadlock(err) || IsQueryCanceled(err) {
		t.Errorf("唯一键冲突被误判成其它错误类型: %v", err)
	}
}

// TestArrayAndJSONBTypes 覆盖 PostgreSQL 的数组与 JSONB ——
// 这两类类型在 MySQL 里没有对应物，是选型时的常见差异点，
// 也是**经 database/sql 使用时边界最明显**的地方。
//
// 实测结论（连同绕行方案一起写成断言，避免以后忘记）：
//
//   - 写：把 `[]string` 当参数传是可行的 —— PostgreSQL 从目标列推断出 text[]，
//     pgx 用原生数组编码器把它编成 `{a,b,c}`。
//   - 读：**不能**直接 Scan 进 `[]string`。pgx 的 database/sql 通道只把已知 OID 映射成
//     驱动值，text[] 会退化成字符串 `{a,b,c}`，database/sql 随即报
//     `unsupported Scan, storing driver.Value type string into type *[]string`。
//     绕行方案是让服务端帮忙转：`array_to_json(tags)` 之后按 JSON 解析。
//   - GORM 同理：它也是走 database/sql，且 `[]string` 字段在写入时会被展开成
//     record `($1,$2,$3)`，PostgreSQL 直接报
//     `column "tags" is of type text[] but expression is of type record`（42804）。
//
// 需要完整的数组能力时，应直接使用 pgx 的 pgxpool（绕过 database/sql 这一层）。
func TestArrayAndJSONBTypes(t *testing.T) {
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	table := uniqueTable("gointra_it_types")
	if _, err := p.SQL().ExecContext(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id bigserial PRIMARY KEY, tags text[], attrs jsonb)`,
		escapeIdent(table))); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	t.Cleanup(func() {
		_, _ = p.SQL().Exec("DROP TABLE IF EXISTS \"" + escapeIdent(table) + "\"")
	})

	// 写：[]string 由 pgx 原生编码成 text[]，string 编码成 jsonb
	var id uint64
	if err := p.SQL().QueryRowContext(ctx, fmt.Sprintf(
		`INSERT INTO %s (tags, attrs) VALUES ($1, $2) RETURNING id`, escapeIdent(table)),
		[]string{"a", "b", "c"}, `{"n": 1}`).Scan(&id); err != nil {
		t.Fatalf("插入数组/JSONB 失败: %v", err)
	}
	if id == 0 {
		t.Fatal("RETURNING 应回填主键")
	}

	// 读：直接 Scan 进 []string 会失败（见上面的说明），这里断言失败本身，
	// 把"驱动能力边界"固化成回归用例；再走 array_to_json 的正路读出来。
	var tagsDirect []string
	err := p.SQL().QueryRowContext(ctx, fmt.Sprintf(
		`SELECT tags FROM %s WHERE id = $1`, escapeIdent(table)), id).Scan(&tagsDirect)
	if err == nil {
		t.Log("注意：当前驱动已支持直接 Scan 数组，上文注释中的限制可以移除了")
	} else if !strings.Contains(err.Error(), "unsupported Scan") {
		t.Errorf("直接 Scan 数组应以 unsupported Scan 失败，实际: %v", err)
	}

	var (
		tagsJSON string
		attrs    string
	)
	if err := p.SQL().QueryRowContext(ctx, fmt.Sprintf(
		`SELECT array_to_json(tags)::text, attrs::text FROM %s WHERE id = $1`, escapeIdent(table)), id).
		Scan(&tagsJSON, &attrs); err != nil {
		t.Fatalf("读回失败: %v", err)
	}

	var tags []string
	if err := json.Unmarshal([]byte(tagsJSON), &tags); err != nil {
		t.Fatalf("解析 array_to_json 结果失败: %v（原始值: %s）", err, tagsJSON)
	}
	if len(tags) != 3 || tags[0] != "a" || tags[2] != "c" {
		t.Errorf("text[] 读回不符: %#v（原始值: %s）", tags, tagsJSON)
	}
	if !strings.Contains(attrs, `"n"`) {
		t.Errorf("jsonb 读回不符: %q", attrs)
	}

	// 数组包含查询（PostgreSQL 的 @> 操作符，可被 GIN 索引加速）
	var count int
	if err := p.SQL().QueryRowContext(ctx, fmt.Sprintf(
		`SELECT count(*) FROM %s WHERE tags @> $1`, escapeIdent(table)), []string{"b"}).
		Scan(&count); err != nil {
		t.Fatalf("数组包含查询失败: %v", err)
	}
	if count != 1 {
		t.Errorf("tags @> {b} 期望命中 1 行，实际 %d", count)
	}

	// jsonb 路径查询（->> 取出文本）
	var n string
	if err := p.SQL().QueryRowContext(ctx, fmt.Sprintf(
		`SELECT attrs->>'n' FROM %s WHERE id = $1`, escapeIdent(table)), id).Scan(&n); err != nil {
		t.Fatalf("jsonb 路径查询失败: %v", err)
	}
	if n != "1" {
		t.Errorf("attrs->>'n' 期望 \"1\"，实际 %q", n)
	}
}

// TestContextCancellation 客户端 ctx 取消要能及时返回。
func TestContextCancellation(t *testing.T) {
	p := openClient(t, testConfig(t))

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := p.DB().WithContext(ctx).Exec("SELECT pg_sleep(5)").Error
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("ctx 超时后查询应返回错误")
	}
	if elapsed > 3*time.Second {
		t.Errorf("ctx 取消应在 150ms 左右生效，实际耗时 %s", elapsed)
	}
}

// ---------------------------------------------------------------------------
// 并发
// ---------------------------------------------------------------------------

// TestConcurrentQueries 在 -race 下并发使用客户端。
func TestConcurrentQueries(t *testing.T) {
	p := openClient(t, testConfig(t))
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
				_ = p.Stats()
				_ = p.Healthy()
				_ = p.Closed()

				var got int
				// 占位符必须带类型标注：PostgreSQL 在无从推断时会把参数当成 text，
				// 而 pgx 无法把 int 编码成 text（报 cannot find encode plan）。
				if err := p.DB().WithContext(ctx).Raw("SELECT ?::int", w*rounds+i).Scan(&got).Error; err != nil {
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

// TestConcurrentHealthAndStats 并发调用读方法。
func TestConcurrentHealthAndStats(t *testing.T) {
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = p.HealthCheck(ctx)
				_ = p.Stats()
				_ = p.DB()
				_ = p.SQL()
				_ = p.DSN()
			}
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// 生命周期
// ---------------------------------------------------------------------------

// TestCloseIsIdempotentAndClean 验证 Close 幂等、不死锁，且关闭后状态自洽。
func TestCloseIsIdempotentAndClean(t *testing.T) {
	p := openClient(t, testConfig(t))

	done := make(chan error, 1)
	go func() {
		_ = p.Close()
		_ = p.Close()
		done <- p.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("重复 Close 不应报错: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close 超过 5s 未返回：出现死锁")
	}

	if !p.Closed() {
		t.Error("Close 后 Closed() 应为 true")
	}
	if err := p.HealthCheck(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后 HealthCheck 应返回 ErrClosed，实际: %v", err)
	}
	if p.DB() != nil {
		t.Error("关闭后 DB() 应返回 nil")
	}
	if p.SQL() != nil {
		t.Error("关闭后 SQL() 应返回 nil")
	}
	if stats := p.Stats(); stats.OpenConnections != 0 {
		t.Errorf("关闭后不应有打开的连接，实际 %d", stats.OpenConnections)
	}
	if err := p.Close(); err != nil {
		t.Errorf("再次 Close 应返回 nil，实际: %v", err)
	}
}

// TestReconnect 验证主动重建连接池可用，并且旧池被真正关闭（不泄漏）。
func TestReconnect(t *testing.T) {
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	before := p.SQL()
	if before == nil {
		t.Fatal("SQL() 不应为 nil")
	}

	if err := p.Reconnect(); err != nil {
		t.Fatalf("Reconnect 失败: %v", err)
	}

	after := p.SQL()
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
		t.Error("旧连接池应已被关闭（否则就是重连泄漏）")
	}

	var one int
	if err := p.DB().WithContext(ctx).Raw("SELECT 1").Scan(&one).Error; err != nil {
		t.Fatalf("重连后查询失败: %v", err)
	}
	if one != 1 {
		t.Errorf("重连后查询返回值错误: %d", one)
	}
}

// TestReconnectAfterCloseFails 关闭后重建应被明确拒绝。
func TestReconnectAfterCloseFails(t *testing.T) {
	p := openClient(t, testConfig(t))
	if err := p.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if err := p.Reconnect(); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后 Reconnect 应返回 ErrClosed，实际: %v", err)
	}
}

// TestHealthCheckRespectsContext ctx 取消时 HealthCheck 应尽快返回。
func TestHealthCheckRespectsContext(t *testing.T) {
	p := openClient(t, testConfig(t))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立刻取消

	if err := p.HealthCheck(ctx); err == nil {
		t.Error("ctx 已取消时 HealthCheck 应返回错误")
	}
}

// TestDSNMethodMasksPassword 对外暴露的 DSN 必须脱敏。
func TestDSNMethodMasksPassword(t *testing.T) {
	cfg := testConfig(t)
	p := openClient(t, cfg)

	got := p.DSN()
	if strings.Contains(got, "123456") {
		t.Errorf("DSN() 泄漏了密码: %s", got)
	}
	if !strings.Contains(got, maskedPassword) {
		t.Errorf("DSN() 应包含脱敏占位符 %q，实际: %s", maskedPassword, got)
	}
	if !strings.Contains(got, cfg.ApplicationName) {
		t.Errorf("DSN() 应能看到注入的会话参数，实际: %s", got)
	}
}
