package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// 本文件是**不需要 PostgreSQL** 的单元测试：配置校验、DSN 两种形式的解析/合并、
// 密码脱敏、SQLSTATE 分类，以及"连不上时是否快速失败"。
// 需要真实 PostgreSQL 的用例见 postgres_test.go。

const (
	validURLDSN = "postgres://postgres:123456@127.0.0.1:5432/demo?sslmode=disable"
	validKVDSN  = "host=127.0.0.1 port=5432 user=postgres password=123456 dbname=demo sslmode=disable"
)

// envOr 读环境变量，为空时用默认值。
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// closedAddr 返回一个**保证连不上**的地址：先监听再立刻关闭，
// 端口在测试期间不会被别人占用，连接会立刻得到 ECONNREFUSED。
func closedAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("无法分配临时端口: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("关闭临时监听失败: %v", err)
	}
	return addr
}

// effectiveDSN 走完整的 ready + dsn 流程，返回最终生效的连接串。
func effectiveDSN(t *testing.T, cfg Config) string {
	t.Helper()
	cfg, err := cfg.ready()
	if err != nil {
		t.Fatalf("配置应合法，实际: %v", err)
	}
	dsn, err := cfg.dsn()
	if err != nil {
		t.Fatalf("组装 DSN 失败: %v", err)
	}
	return dsn
}

// ---------------------------------------------------------------------------
// 配置默认值与校验
// ---------------------------------------------------------------------------

// TestConfigNormalize 校验默认值填充与互相矛盾取值的修正。
func TestConfigNormalize(t *testing.T) {
	c := Config{Dsn: validKVDSN}.normalize()

	if want := defaultMaxIdleConns; c.MaxIdleConns != want {
		t.Errorf("MaxIdleConns 期望 %d，实际 %d", want, c.MaxIdleConns)
	}
	if want := defaultMaxOpenConns(); c.MaxOpenConns != want {
		t.Errorf("MaxOpenConns 期望 %d，实际 %d", want, c.MaxOpenConns)
	}
	if want := defaultConnMaxLifetime; c.ConnMaxLifetime != want {
		t.Errorf("ConnMaxLifetime 期望 %s，实际 %s", want, c.ConnMaxLifetime)
	}
	if want := defaultConnMaxIdleTime; c.ConnMaxIdleTime != want {
		t.Errorf("ConnMaxIdleTime 期望 %s，实际 %s", want, c.ConnMaxIdleTime)
	}
	if want := defaultDialAttempts; c.DialAttempts != want {
		t.Errorf("DialAttempts 期望 %d，实际 %d", want, c.DialAttempts)
	}
	if want := defaultConnectTimeout; c.ConnectTimeout != want {
		t.Errorf("ConnectTimeout 期望 %s，实际 %s", want, c.ConnectTimeout)
	}

	// 空闲连接多于最大连接没有意义，normalize 应当对齐到最大连接。
	c2 := Config{Dsn: validKVDSN, MaxOpenConns: 5, MaxIdleConns: 50}.normalize()
	if c2.MaxIdleConns != 5 {
		t.Errorf("MaxIdleConns 应被截断到 MaxOpenConns=5，实际 %d", c2.MaxIdleConns)
	}

	// 退避上限小于起始值时应抬到起始值，否则退避会越等越短。
	c3 := Config{Dsn: validKVDSN, DialBackoff: 10 * time.Second, DialMaxBackoff: time.Second}.normalize()
	if c3.DialMaxBackoff != 10*time.Second {
		t.Errorf("DialMaxBackoff 应被抬到 DialBackoff=10s，实际 %s", c3.DialMaxBackoff)
	}

	// defaultMaxOpenConns 的下限保护。
	if got := defaultMaxOpenConns(); got < 8 {
		t.Errorf("defaultMaxOpenConns 应至少有 8，实际 %d（GOMAXPROCS=%d）", got, runtime.GOMAXPROCS(0))
	}
}

// TestConfigValidate 覆盖各类非法配置。
//
// 注意：validate 接收的是**原始**配置（未 normalize），
// 否则"取值为负"会在 normalize 阶段被替换成默认值而永远测不出来。
func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string // 期望错误信息里出现的关键词
	}{
		{"Dsn 为空", Config{}, "Dsn 不能为空"},
		{"Dsn 只有空白", Config{Dsn: "   "}, "Dsn 不能为空"},
		{"Dsn 语法错误", Config{Dsn: "这不是一个 DSN"}, "缺少 '='"},
		{"Dsn 参数缺等号", Config{Dsn: "host=127.0.0.1 dbname"}, "缺少 '='"},
		{"Dsn 引号未闭合", Config{Dsn: "host=127.0.0.1 dbname='demo"}, "收尾单引号"},
		{"URL 形式缺库名", Config{Dsn: "postgres://user@127.0.0.1:5432/"}, "未指定数据库名"},
		{"keyword 形式缺库名", Config{Dsn: "host=127.0.0.1 user=postgres"}, "未指定数据库名"},

		{"MaxOpenConns 为负", Config{Dsn: validKVDSN, MaxOpenConns: -1}, "MaxOpenConns 不能为负"},
		{"MaxIdleConns 为负", Config{Dsn: validKVDSN, MaxIdleConns: -1}, "MaxIdleConns 不能为负"},
		{"RebuildAfterFailures 为负",
			Config{Dsn: validKVDSN, RebuildAfterFailures: -1}, "RebuildAfterFailures 不能为负"},

		{"sslmode 非法", Config{Dsn: validKVDSN, SSLMode: "no-tls"}, "SSLMode"},
		{"target_session_attrs 非法",
			Config{Dsn: validKVDSN, TargetSessionAttrs: "master"}, "TargetSessionAttrs"},

		// 单位哨兵：把秒/毫秒整数直接写给 Duration 字段。
		{"ConnMaxLifetime 单位写错", Config{Dsn: validKVDSN, ConnMaxLifetime: 1800}, "ConnMaxLifetime"},
		{"ConnectTimeout 单位写错", Config{Dsn: validKVDSN, ConnectTimeout: 5000}, "ConnectTimeout"},
		{"StatementTimeout 单位写错", Config{Dsn: validKVDSN, StatementTimeout: 30000}, "StatementTimeout"},
		{"IdleInTransactionTimeout 单位写错",
			Config{Dsn: validKVDSN, IdleInTransactionTimeout: 60000}, "IdleInTransactionTimeout"},
		{"SlowThreshold 单位写错", Config{Dsn: validKVDSN, SlowThreshold: 200}, "SlowThreshold"},

		{"LogLevel 非法", Config{Dsn: validKVDSN, LogLevel: "verbose"}, "LogLevel"},

		// Settings 的 key 会被拼进 DSN，字符集限制是**安全边界**，不只是规范问题。
		{"Settings key 含空格", Config{Dsn: validKVDSN, Settings: map[string]string{"work mem": "1"}}, "Settings"},
		{"Settings key 含大写", Config{Dsn: validKVDSN, Settings: map[string]string{"Work_Mem": "1"}}, "Settings"},
		{"Settings key 以数字开头", Config{Dsn: validKVDSN, Settings: map[string]string{"1x": "1"}}, "Settings"},
		{"Settings key 为空", Config{Dsn: validKVDSN, Settings: map[string]string{"": "1"}}, "Settings"},
		{"Settings 注入连接串",
			Config{Dsn: validKVDSN, Settings: map[string]string{"host=x password=evil": "1"}}, "Settings"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.validate()
			if err == nil {
				t.Fatalf("期望报错，实际通过")
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("期望 ErrInvalidConfig，实际 %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("错误信息应包含 %q，实际: %v", tc.want, err)
			}
		})
	}

	// 合法配置不应报错。SlowThreshold 的 0（默认）与负值（关闭）都合法。
	for _, cfg := range []Config{
		{Dsn: validURLDSN},
		{Dsn: validKVDSN},
		{Dsn: validKVDSN, SlowThreshold: -1},
		{Dsn: validKVDSN, SSLMode: "verify-full", TargetSessionAttrs: "prefer-standby"},
		{Dsn: validKVDSN, StatementTimeout: 30 * time.Second, IdleInTransactionTimeout: 10 * time.Minute},
		{Dsn: validKVDSN, Settings: map[string]string{"work_mem": "16MB", "myext.setting": "1"}},
		{Dsn: validKVDSN, MaxOpenConns: 0, MaxIdleConns: 0}, // 0 表示用默认值
		{Dsn: validKVDSN, DialAttempts: -1},                 // 负值表示无限重试
		{Dsn: "host=127.0.0.1 dbname=demo user=u"},          // 不带密码也合法
		{Dsn: "host = 127.0.0.1  dbname = demo"},            // '=' 两侧允许空白（libpq 规则）
		{Dsn: "postgresql://u@127.0.0.1:5432/demo"},         // postgresql:// 前缀
	} {
		if _, err := cfg.ready(); err != nil {
			t.Errorf("配置 %+v 应合法，实际: %v", cfg, err)
		}
	}
}

// ---------------------------------------------------------------------------
// DSN 合并：只补缺、不覆盖，且保持书写形式
// ---------------------------------------------------------------------------

// overrideFixture 返回一份把所有覆盖项都填满的配置。
func overrideFixture(dsn string) Config {
	return Config{
		Dsn:                      dsn,
		ConnectTimeout:           5 * time.Second,
		SSLMode:                  "disable",
		ApplicationName:          "go-infra",
		SearchPath:               "app,public",
		TimeZone:                 "Asia/Shanghai",
		TargetSessionAttrs:       "read-write",
		StatementTimeout:         30 * time.Second,
		IdleInTransactionTimeout: 10 * time.Minute,
		Settings: map[string]string{
			"work_mem":                      "16MB",
			"default_transaction_isolation": "read committed",
		},
	}
}

// assertOverridesApplied 断言所有覆盖项都进了 DSN，并且能被重新解析出来。
//
// 双重校验：先看字符串（人眼可读的"确实写进去了"），
// 再重新解析一遍（语法正确、值也能取回），避免写出"看起来对但解析不了"的 DSN。
func assertOverridesApplied(t *testing.T, dsn string) {
	t.Helper()

	// 时区必须**原样**出现：gorm 的 postgres 驱动用正则从 DSN 里直接取字符串去
	// time.LoadLocation，一旦被编码成 Asia%2FShanghai，每次建连都会失败。
	for _, want := range []string{
		"connect_timeout=5",
		"application_name=go-infra",
		"timezone=Asia/Shanghai",
		"target_session_attrs=read-write",
		"statement_timeout=30000",
		"idle_in_transaction_session_timeout=600000",
		"work_mem=16MB",
	} {
		if !strings.Contains(dsn, want) {
			t.Errorf("DSN 应包含 %q，实际: %s", want, dsn)
		}
	}

	// 需要转义的值单独断言（URL 形式用 %xx，keyword 形式用单引号）
	if !strings.Contains(dsn, "app") || !strings.Contains(dsn, "public") {
		t.Errorf("DSN 里应包含 search_path 的取值，实际: %s", dsn)
	}

	info, err := parseConnInfo(dsn)
	if err != nil {
		t.Fatalf("生成的 DSN 应能被重新解析，实际: %v（DSN: %s）", err, dsn)
	}
	checks := map[string]string{
		"connect_timeout":                     "5",
		"application_name":                    "go-infra",
		"timezone":                            "Asia/Shanghai",
		"target_session_attrs":                "read-write",
		"statement_timeout":                   "30000",
		"idle_in_transaction_session_timeout": "600000",
		"work_mem":                            "16MB",
		"search_path":                         "app,public",
		"default_transaction_isolation":       "read committed",
		"sslmode":                             "disable",
		"dbname":                              "demo",
	}
	for k, want := range checks {
		got, ok := info.get(k)
		if !ok {
			t.Errorf("重新解析后缺少参数 %s（DSN: %s）", k, dsn)
			continue
		}
		if got != want {
			t.Errorf("参数 %s 期望 %q，实际 %q", k, want, got)
		}
	}
}

// gormTimeZoneMatcher 是从 gorm.io/driver/postgres v1.6.3 里**原样抄过来**的正则。
//
// 驱动用它从 DSN 字符串里抠出时区值交给人 time.LoadLocation，
// 抠不出来（或抠到被转义的值）就不会注册 timestamp 的 ScanLocation，
// 表现为读出来的时间"差了几个小时"，或者直接建连失败。
// 这段逻辑属于跨包耦合，一旦驱动改了正则，这个测试会先失败。
var gormTimeZoneMatcher = regexp.MustCompile("(time_zone|TimeZone|timezone)=(.*?)($|&| )")

// TestTimezoneSurvivesGormDriverRegex 保证注入的时区能被 gorm 驱动的正则原样抠出来。
//
// 这是踩过的坑：最初用 url.Values.Encode() 组装 query，把 `Asia/Shanghai`
// 编码成了 `Asia%2FShanghai`，LoadLocation 直接失败，
// 而报错信息里完全没有 DSN，排查成本极高。
func TestTimezoneSurvivesGormDriverRegex(t *testing.T) {
	for name, dsn := range map[string]string{
		"URL 形式":      effectiveDSN(t, Config{Dsn: validURLDSN, TimeZone: "Asia/Shanghai"}),
		"keyword 形式":  effectiveDSN(t, Config{Dsn: validKVDSN, TimeZone: "Asia/Shanghai"}),
		"URL 形式(UTC)": effectiveDSN(t, Config{Dsn: validURLDSN, TimeZone: "UTC"}),
	} {
		t.Run(name, func(t *testing.T) {
			m := gormTimeZoneMatcher.FindStringSubmatch(dsn)
			if len(m) < 3 {
				t.Fatalf("gorm 驱动的正则没能从 DSN 里抠出时区，ReadLocation 会拿不到值: %s", dsn)
			}
			got := m[2]
			if strings.Contains(got, "%") {
				t.Errorf("时区值被百分号编码，time.LoadLocation(%q) 会失败: %s", got, dsn)
			}
			if _, err := time.LoadLocation(got); err != nil {
				t.Errorf("time.LoadLocation(%q) 失败: %v（DSN: %s）", got, err, dsn)
			}
		})
	}
}

// TestDSNOverridesURLForm URL 形式：覆盖项被写进 query，且仍是 URL 形式。
func TestDSNOverridesURLForm(t *testing.T) {
	dsn := effectiveDSN(t, overrideFixture(validURLDSN))

	if !strings.HasPrefix(dsn, "postgres://") {
		t.Errorf("输入是 URL 形式，输出也应保持 URL 形式，实际: %s", dsn)
	}
	if strings.Count(dsn, "sslmode=") != 1 {
		t.Errorf("sslmode 不应出现多次，实际: %s", dsn)
	}
	assertOverridesApplied(t, dsn)
}

// TestDSNOverridesKeywordForm keyword/value 形式：覆盖项按 `key=value` 追加，顺序稳定。
func TestDSNOverridesKeywordForm(t *testing.T) {
	dsn := effectiveDSN(t, overrideFixture(validKVDSN))

	if !strings.HasPrefix(dsn, "host=127.0.0.1") {
		t.Errorf("输入是 keyword/value 形式，输出应保持该形式，实际: %s", dsn)
	}
	assertOverridesApplied(t, dsn)

	// 含空格的取值必须被引号包起来，否则解析出来会被截断
	if !strings.Contains(dsn, `default_transaction_isolation='read committed'`) {
		t.Errorf("含空格的取值应加单引号，实际: %s", dsn)
	}

	// Settings 是 map，但写入顺序必须稳定（按键排序），否则每次生成的 DSN 都不同
	for i := 0; i < 5; i++ {
		if again := effectiveDSN(t, overrideFixture(validKVDSN)); again != dsn {
			t.Fatalf("同样的配置应生成同样的 DSN:\n  第一次: %s\n  第%d次: %s", dsn, i+2, again)
		}
	}
}

// TestDSNDoesNotOverrideUserValues 是"只补缺、不覆盖"的核心断言。
//
// 用户在 DSN 里显式写好的参数优先级最高 —— 否则"配置中心里写的值被代码悄悄改掉"
// 会成为最难排查的一类线上问题。
func TestDSNDoesNotOverrideUserValues(t *testing.T) {
	explicitKV := "host=127.0.0.1 port=5432 user=postgres dbname=demo " +
		"sslmode=verify-full connect_timeout=9 statement_timeout=1000 application_name=explicit"
	cfg := Config{
		Dsn:              explicitKV,
		SSLMode:          "disable",
		ConnectTimeout:   time.Second,
		StatementTimeout: 30 * time.Second,
		ApplicationName:  "go-infra",
	}
	dsn := effectiveDSN(t, cfg)

	for _, want := range []string{"sslmode=verify-full", "connect_timeout=9", "statement_timeout=1000", "application_name=explicit"} {
		if !strings.Contains(dsn, want) {
			t.Errorf("DSN 里显式写好的 %q 应被保留，实际: %s", want, dsn)
		}
	}
	for _, unwanted := range []string{"sslmode=disable", "statement_timeout=30000", "application_name=go-infra"} {
		if strings.Contains(dsn, unwanted) {
			t.Errorf("Config 不应覆盖 DSN 里已显式指定的值，但仍出现了 %q: %s", unwanted, dsn)
		}
	}

	// URL 形式同理
	explicitURL := "postgres://u:p@127.0.0.1:5432/demo?sslmode=verify-full&timeout_placeholder=1"
	urlDSN := effectiveDSN(t, Config{Dsn: explicitURL, SSLMode: "disable", ApplicationName: "go-infra"})
	if !strings.Contains(urlDSN, "sslmode=verify-full") {
		t.Errorf("URL 形式的显式 sslmode 应被保留，实际: %s", urlDSN)
	}
	if strings.Contains(urlDSN, "sslmode=disable") {
		t.Errorf("Config.SSLMode 不应覆盖 URL 里已显式的 sslmode: %s", urlDSN)
	}
	if !strings.Contains(urlDSN, "application_name=go-infra") {
		t.Errorf("URL 形式下缺失的覆盖项应被补齐，实际: %s", urlDSN)
	}
}

// TestDSNKeyCaseNormalized 参数名统一小写。
//
// pgx 不认大小写的差异：认不出的 key 会被当成**服务端 GUC** 透传，
// `?SSLMode=disable` 的结局是 `FATAL: unrecognized configuration parameter "SSLMode"`，
// 错误信息完全看不出是大小写问题。
func TestDSNKeyCaseNormalized(t *testing.T) {
	dsn := effectiveDSN(t, Config{Dsn: "host=127.0.0.1 DBNAME=demo SSLMODE=disable User=postgres"})
	lower := strings.ToLower(dsn)
	for _, want := range []string{"dbname=demo", "sslmode=disable", "user=postgres"} {
		if !strings.Contains(lower, want) {
			t.Errorf("参数名应被小写化，缺少 %q: %s", want, dsn)
		}
	}
	if strings.Contains(dsn, "DBNAME") || strings.Contains(dsn, "SSLMODE") || strings.Contains(dsn, "User") {
		t.Errorf("参数名里不应残留大写写法: %s", dsn)
	}

	urlDSN := effectiveDSN(t, Config{Dsn: "postgres://u@127.0.0.1:5432/demo?SSLMode=disable"})
	if !strings.Contains(urlDSN, "sslmode=disable") {
		t.Errorf("URL 形式的参数名也应被小写化: %s", urlDSN)
	}
}

// TestStatementTimeoutUnit PostgreSQL 的时间类 GUC 只接受"一个数值 + 一个单位"，
// 复合单位（1m30s）会被服务端拒绝并导致**建连失败**，所以必须换算成毫秒整数。
func TestStatementTimeoutUnit(t *testing.T) {
	cases := []struct {
		dur  time.Duration
		want string
	}{
		{time.Second, "1000"},
		{30 * time.Second, "30000"},
		{90 * time.Second, "90000"}, // 90s 若写成 "1m30s" 会被 PG 拒绝
		{5 * time.Minute, "300000"},
		{1500 * time.Millisecond, "1500"},
	}
	for _, tc := range cases {
		got := pgMillis(tc.dur)
		if got != tc.want {
			t.Errorf("pgMillis(%s) 期望 %s，实际 %s", tc.dur, tc.want, got)
		}
	}

	dsn := effectiveDSN(t, Config{Dsn: validKVDSN, StatementTimeout: 90 * time.Second})
	if strings.Contains(dsn, "1m30s") {
		t.Errorf("DSN 里不能出现复合时长（PG 会报 invalid value）: %s", dsn)
	}
	if !strings.Contains(dsn, "statement_timeout=90000") {
		t.Errorf("statement_timeout 应为毫秒整数: %s", dsn)
	}
}

// TestDSNRejectsMalformed 各类 DSN 语法错误都要在解析期被抓到。
func TestDSNRejectsMalformed(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		want string
	}{
		{"缺等号", "host=127.0.0.1 dbname", "缺少 '='"},
		{"引号未闭合", "dbname='demo", "收尾单引号"},
		{"裸值以反斜杠结尾", `dbname=demo\`, "孤立的反斜杠"},
		{"引号值后跟孤立反斜杠", `dbname='demo'\`, "缺少 '='"},
		{"URL 转义非法", "postgres://u@h:5432/d?sslmode=%zz", "转义非法"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseConnInfo(tc.dsn)
			if err == nil {
				t.Fatalf("应报错，实际通过")
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("期望 ErrInvalidConfig，实际: %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("错误信息应包含 %q，实际: %v", tc.want, err)
			}
		})
	}
}

// TestKeywordValueQuoting 覆盖 libpq 词法规则的边界：转义、引号、'=' 两侧空白。
func TestKeywordValueQuoting(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		key  string
		want string
	}{
		{"单引号包裹空格", `host=h password='p a s s' dbname=d`, "password", "p a s s"},
		{"引号内转义单引号", `host=h password='p\'s' dbname=d`, "password", "p's"},
		{"引号内转义反斜杠", `host=h password='C:\\pg' dbname=d`, "password", `C:\pg`},
		{"裸值里的反斜杠转义", `host=h password=a\ b dbname=d`, "password", "a b"},
		{"空值写在末尾", `host=h dbname=d password=`, "password", ""},
		{"空值引号写法", `host=h dbname=d password=''`, "password", ""},
		{"等号两侧空白", "host = h  dbname = demo", "dbname", "demo"},
		// 裸值以空白结束，所以含空格的值必须加引号 —— 这是 libpq 的规则，
		// 我们照做即可，不要"聪明地"把它拼回去。
		{"引号包裹含空格的值", `host=h options='-c x=1' dbname=d`, "options", "-c x=1"},
		{"裸值在空白处截断", `host=h options=-c x=1 dbname=d`, "options", "-c"},
		// '=' 之后的空白会被跳过（libpq: "Skip whitespace after the equal sign"），
		// 因此"空值 + 空格 + 下一个参数"会被当成前者的取值。这条看似反直觉，
		// 但它就是驱动（pgx）的行为，我们的解析必须与之一致，否则生成的 DSN
		// 在交付给驱动时含义会变。
		{"空值后紧跟参数会被吞掉", "host=h dbname=d password= user=u", "password", "user=u"},
		{"连续空白分隔", "host=h\t\tdbname=d", "dbname", "d"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			info, err := parseConnInfo(tc.raw)
			if err != nil {
				t.Fatalf("解析失败: %v", err)
			}
			got, ok := info.get(tc.key)
			if !ok {
				t.Fatalf("缺少参数 %s", tc.key)
			}
			if got != tc.want {
				t.Errorf("%s 期望 %q，实际 %q", tc.key, tc.want, got)
			}

			// 往返：序列化后再解析，必须得到同一个值
			again, err := parseConnInfo(info.String())
			if err != nil {
				t.Fatalf("序列化后的 DSN 无法重新解析: %v（%s）", err, info.String())
			}
			if got2, _ := again.get(tc.key); got2 != tc.want {
				t.Errorf("往返后 %s 期望 %q，实际 %q（DSN: %s）", tc.key, tc.want, got2, info.String())
			}
		})
	}
}

// TestURLDSNRoundTrip URL 形式的往返稳定性（含转义与特殊字符的密码）。
func TestURLDSNRoundTrip(t *testing.T) {
	raw := "postgres://us%40er:p%40ss%2Fword@127.0.0.1:5432/de%20mo?sslmode=disable&options=-c%20x%3D1"
	info, err := parseConnInfo(raw)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if got, _ := info.get("dbname"); got != "de mo" {
		t.Errorf("库名期望 %q，实际 %q", "de mo", got)
	}
	if got, _ := info.get("options"); got != "-c x=1" {
		t.Errorf("options 期望 %q，实际 %q", "-c x=1", got)
	}

	out := info.String()
	again, err := parseConnInfo(out)
	if err != nil {
		t.Fatalf("序列化后的 DSN 无法重新解析: %v（%s）", err, out)
	}
	if got, _ := again.get("dbname"); got != "de mo" {
		t.Errorf("往返后库名期望 %q，实际 %q（DSN: %s）", "de mo", got, out)
	}
	if got, _ := again.get("options"); got != "-c x=1" {
		t.Errorf("往返后 options 期望 %q，实际 %q（DSN: %s）", "-c x=1", got, out)
	}
	if u := again.url.User; u == nil || u.Username() != "us@er" {
		t.Errorf("往返后用户名应保持 %q，实际 %v（DSN: %s）", "us@er", u, out)
	}
}

// TestSanitizeDSNStyles 两种形式的密码都要被抹掉。
func TestSanitizeDSNStyles(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "URL 形式",
			in:   "postgres://postgres:123456@127.0.0.1:5432/demo?sslmode=disable",
			want: "postgres://postgres:" + maskedPassword + "@127.0.0.1:5432/demo?sslmode=disable",
		},
		{
			name: "keyword/value 形式",
			in:   "host=127.0.0.1 user=postgres password=123456 dbname=demo",
			want: "host=127.0.0.1 user=postgres password=" + maskedPassword + " dbname=demo",
		},
		{
			name: "keyword/value 形式（带引号的密码）",
			in:   `host=127.0.0.1 user=u password='p a s s' dbname=demo`,
			want: "host=127.0.0.1 user=u password=" + maskedPassword + " dbname=demo",
		},
		{
			name: "无密码时不改动",
			in:   "host=127.0.0.1 user=postgres dbname=demo",
			want: "host=127.0.0.1 user=postgres dbname=demo",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeDSN(tc.in)
			if got != tc.want {
				t.Errorf("期望 %q，实际 %q", tc.want, got)
			}
			if strings.Contains(got, "123456") || strings.Contains(got, "p a s s") {
				t.Errorf("密码泄漏: %s", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 初始化失败路径
// ---------------------------------------------------------------------------

// TestOpenRejectsInvalidConfig 非法配置必须在触网之前就被拒绝。
func TestOpenRejectsInvalidConfig(t *testing.T) {
	for _, cfg := range []Config{
		{},                               // 空配置
		{Dsn: "garbage"},                 // 语法错误
		{Dsn: validKVDSN, LogLevel: "x"}, // 日志级别非法
		{Dsn: validKVDSN, SSLMode: "nope"},
	} {
		p, err := Open(cfg)
		if err == nil {
			_ = p.Close()
			t.Fatalf("配置 %+v 应被拒绝", cfg)
		}
		if !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("配置 %+v 期望 ErrInvalidConfig，实际: %v", cfg, err)
		}
		if p != nil {
			t.Errorf("失败时不应返回实例，实际 %#v", p)
		}
	}
}

// TestOpenFailsFastWhenUnreachable 数据库不可达时必须**快速返回错误**，
// 而不是一直阻塞在初始化里。
func TestOpenFailsFastWhenUnreachable(t *testing.T) {
	addr := closedAddr(t)
	cfg := Config{
		Dsn:            "host=" + strings.Split(addr, ":")[0] + " port=" + strings.Split(addr, ":")[1] + " user=u dbname=demo sslmode=disable",
		DialAttempts:   1, // 失败即返回
		ConnectTimeout: time.Second,
		DialBackoff:    10 * time.Millisecond,
	}

	start := time.Now()
	p, err := Open(cfg)
	elapsed := time.Since(start)

	if err == nil {
		_ = p.Close()
		t.Fatal("连不上 PostgreSQL 时 Open 应返回错误")
	}
	if !errors.Is(err, ErrConnect) {
		t.Errorf("期望 ErrConnect，实际: %v", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("Open 应在 5s 内返回错误，实际耗时 %s", elapsed)
	}
	// 失败路径不能泄漏半可用实例
	if p != nil {
		t.Errorf("失败时不应返回实例，实际 %#v", p)
	}
	// 错误信息里不能带密码
	if strings.Contains(err.Error(), "123456") {
		t.Errorf("错误信息不应泄漏密码: %v", err)
	}
}

// TestDialAttemptsBounded 校验有限重试：DialAttempts=N 时总耗时应受退避参数约束。
func TestDialAttemptsBounded(t *testing.T) {
	addr := closedAddr(t)
	host, port, _ := net.SplitHostPort(addr)

	cfg := Config{
		Dsn:            "host=" + host + " port=" + port + " user=u dbname=demo sslmode=disable",
		DialAttempts:   3,
		ConnectTimeout: 500 * time.Millisecond,
		DialBackoff:    20 * time.Millisecond,
		DialMaxBackoff: 40 * time.Millisecond,
	}

	start := time.Now()
	p, err := Open(cfg)
	elapsed := time.Since(start)

	if err == nil {
		_ = p.Close()
		t.Fatal("应返回错误")
	}
	// 3 次尝试 + 两次退避（20ms + 40ms）应远小于 5s。
	if elapsed > 5*time.Second {
		t.Errorf("有限重试应在 5s 内结束，实际 %s", elapsed)
	}
}

// TestCloseWithoutOpenIsSafe 初始化失败时不留下半可用状态。
func TestCloseWithoutOpenIsSafe(t *testing.T) {
	addr := closedAddr(t)
	host, port, _ := net.SplitHostPort(addr)

	p, err := Open(Config{
		Dsn:          "host=" + host + " port=" + port + " user=u dbname=demo sslmode=disable",
		DialAttempts: 1,
	})
	if err == nil {
		_ = p.Close()
		t.Fatal("应返回错误")
	}
	if p != nil {
		t.Errorf("失败时不应返回实例，实际 %#v", p)
	}
	if !errors.Is(err, ErrConnect) {
		t.Errorf("期望 ErrConnect，实际: %v", err)
	}
}

// TestOpenRespectsDialTimeout 建连超时应受 ConnectTimeout 约束，
// 而不是等操作系统的 TCP 超时（macOS 约 75s）。
//
// 用一个"只接受连接、从不回应握手"的监听器模拟半死不活的数据库。
func TestOpenRespectsDialTimeout(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("无法监听: %v", err)
	}
	defer func() { _ = l.Close() }()

	// 接受连接但永不回数据；连接建立后保持不关闭
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, aerr := l.Accept()
			if aerr != nil {
				return
			}
			defer func() { _ = conn.Close() }()
		}
	}()

	host, port, _ := net.SplitHostPort(l.Addr().String())
	start := time.Now()
	p, err := Open(Config{
		Dsn:            "host=" + host + " port=" + port + " user=u dbname=demo sslmode=disable",
		DialAttempts:   1,
		ConnectTimeout: time.Second,
	})
	elapsed := time.Since(start)

	if err == nil {
		_ = p.Close()
		t.Fatal("握手无响应时 Open 应返回错误")
	}
	if elapsed > 10*time.Second {
		t.Errorf("应受 ConnectTimeout 约束，实际耗时 %s", elapsed)
	}
	_ = l.Close()
	<-done
}

// ---------------------------------------------------------------------------
// 哨兵错误与 SQLSTATE 分类
// ---------------------------------------------------------------------------

// TestErrorsAreSentinel 确保哨兵错误可被 errors.Is 判定，且互不相等。
func TestErrorsAreSentinel(t *testing.T) {
	all := []error{ErrInvalidConfig, ErrConnect, ErrClosed, ErrNotConnected}
	for i, a := range all {
		if a == nil {
			t.Fatalf("第 %d 个哨兵错误为 nil", i)
		}
		for j, b := range all {
			if i != j && errors.Is(a, b) {
				t.Errorf("哨兵错误不应互相匹配: %v 与 %v", a, b)
			}
		}
	}
	if errors.Is(context.Canceled, ErrConnect) {
		t.Error("context.Canceled 不应匹配 ErrConnect")
	}
}

// pgError 构造一个带 SQLSTATE 的服务端错误，并模拟 GORM 的包装方式。
func pgError(code, constraint string) error {
	return fmt.Errorf("执行 SQL 失败: %w", &pgconn.PgError{
		Severity:       "ERROR",
		Code:           code,
		Message:        "模拟的服务端错误",
		ConstraintName: constraint,
	})
}

// TestSQLStateClassification 逐个核对 SQLSTATE 分类函数。
func TestSQLStateClassification(t *testing.T) {
	cases := []struct {
		name           string
		err            error
		state          string
		retryable      bool
		unique         bool
		foreignKey     bool
		serialization  bool
		deadlock       bool
		queryCanceled  bool
		integrityClass bool
	}{
		{name: "唯一键冲突", err: pgError("23505", "uk_users_email"),
			state: "23505", unique: true, integrityClass: true},
		{name: "外键冲突", err: pgError("23503", "fk_orders_user"),
			state: "23503", foreignKey: true, integrityClass: true},
		{name: "非空冲突", err: pgError("23502", ""), state: "23502", integrityClass: true},
		{name: "CHECK 冲突", err: pgError("23514", ""), state: "23514", integrityClass: true},
		{name: "序列化失败", err: pgError("40001", ""),
			state: "40001", retryable: true, serialization: true},
		{name: "死锁", err: pgError("40P01", ""),
			state: "40P01", retryable: true, deadlock: true},
		{name: "连接断开", err: pgError("08006", ""), state: "08006", retryable: true},
		{name: "连接数打满", err: pgError("53300", ""), state: "53300", retryable: true},
		{name: "库正在关闭", err: pgError("57P03", ""), state: "57P03", retryable: true},
		// statement_timeout 触发时用的就是 57014 —— 重试同样会超时，不该算可重试
		{name: "语句被取消", err: pgError("57014", ""), state: "57014", queryCanceled: true},
		// COMMIT 结果未知：重试可能重复写入，必须由业务侧幂等处理
		{name: "事务结果未知", err: pgError("08007", ""), state: "08007"},
		{name: "语法错误", err: pgError("42601", ""), state: "42601"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SQLState(tc.err); got != tc.state {
				t.Errorf("SQLState 期望 %q，实际 %q", tc.state, got)
			}
			if got := IsRetryable(tc.err); got != tc.retryable {
				t.Errorf("IsRetryable 期望 %v，实际 %v（%s）", tc.retryable, got, tc.state)
			}
			if got := IsUniqueViolation(tc.err); got != tc.unique {
				t.Errorf("IsUniqueViolation 期望 %v，实际 %v", tc.unique, got)
			}
			if got := IsForeignKeyViolation(tc.err); got != tc.foreignKey {
				t.Errorf("IsForeignKeyViolation 期望 %v，实际 %v", tc.foreignKey, got)
			}
			if got := IsSerializationFailure(tc.err); got != tc.serialization {
				t.Errorf("IsSerializationFailure 期望 %v，实际 %v", tc.serialization, got)
			}
			if got := IsDeadlock(tc.err); got != tc.deadlock {
				t.Errorf("IsDeadlock 期望 %v，实际 %v", tc.deadlock, got)
			}
			if got := IsQueryCanceled(tc.err); got != tc.queryCanceled {
				t.Errorf("IsQueryCanceled 期望 %v，实际 %v", tc.queryCanceled, got)
			}
			if got := IsIntegrityViolation(tc.err); got != tc.integrityClass {
				t.Errorf("IsIntegrityViolation 期望 %v，实际 %v", tc.integrityClass, got)
			}
		})
	}
}

// TestConstraintName 约束名要能从错误链里取出来 —— 它是把数据库约束映射成
// 业务错误的关键信息。
func TestConstraintName(t *testing.T) {
	err := pgError("23505", "uk_users_email")
	if got := ConstraintName(err); got != "uk_users_email" {
		t.Errorf("ConstraintName 期望 %q，实际 %q", "uk_users_email", got)
	}
	if got := ConstraintName(errors.New("普通错误")); got != "" {
		t.Errorf("非 PostgreSQL 错误应返回空串，实际 %q", got)
	}
}

// TestSQLStateOnNonPGError 非 PostgreSQL 错误不应被误判。
func TestSQLStateOnNonPGError(t *testing.T) {
	for _, err := range []error{
		nil,
		errors.New("普通错误"),
		context.DeadlineExceeded,
		context.Canceled,
		ErrClosed,
	} {
		if got := SQLState(err); got != "" {
			t.Errorf("SQLState(%v) 应为空，实际 %q", err, got)
		}
		if IsRetryable(err) {
			t.Errorf("IsRetryable(%v) 应为 false", err)
		}
		if IsIntegrityViolation(err) {
			t.Errorf("IsIntegrityViolation(%v) 应为 false", err)
		}
	}
}
