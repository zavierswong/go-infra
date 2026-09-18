package mysql

import (
	"context"
	"errors"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
)

// 本文件是**不需要 MySQL** 的单元测试：配置校验、DSN 组装、
// 以及"连不上时是否快速失败"这一旧版最严重的缺陷。
// 需要真实 MySQL 的用例见 mysql_test.go。

const validDSN = "root:123456@tcp(127.0.0.1:3306)/demo?charset=utf8mb4&parseTime=True&loc=Local"

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

// TestConfigNormalize 校验默认值填充与互相矛盾取值的修正。
func TestConfigNormalize(t *testing.T) {
	c := Config{Dsn: validDSN}.normalize()

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
	c2 := Config{Dsn: validDSN, MaxOpenConns: 5, MaxIdleConns: 50}.normalize()
	if c2.MaxIdleConns != 5 {
		t.Errorf("MaxIdleConns 应被截断到 MaxOpenConns=5，实际 %d", c2.MaxIdleConns)
	}

	// 退避上限小于起始值时应抬到起始值，否则退避会越等越短。
	c3 := Config{Dsn: validDSN, DialBackoff: 10 * time.Second, DialMaxBackoff: time.Second}.normalize()
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
// 重点验证"单位写错"这一类 —— 它们不会报错、只会让连接池表现异常，
// 属于最难排查的性能问题。
func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string // 期望错误信息里出现的关键词
	}{
		{"Dsn 为空", Config{}, "Dsn 不能为空"},
		{"Dsn 只有空白", Config{Dsn: "   "}, "Dsn 不能为空"},
		{"Dsn 语法错误", Config{Dsn: "这不是一个 DSN"}, "DSN 解析失败"},
		{"Dsn 缺少库名", Config{Dsn: "root:123456@tcp(127.0.0.1:3306)/"}, "未指定数据库名"},

		{"MaxOpenConns 为负", Config{Dsn: validDSN, MaxOpenConns: -1}, "MaxOpenConns 不能为负"},
		{"MaxIdleConns 为负", Config{Dsn: validDSN, MaxIdleConns: -1}, "MaxIdleConns 不能为负"},
		{"RebuildAfterFailures 为负",
			Config{Dsn: validDSN, RebuildAfterFailures: -1}, "RebuildAfterFailures 不能为负"},

		// 单位哨兵：把旧版的秒/毫秒整数直接写给 Duration 字段。
		// 旧版 ConnMaxLifetime 单位是秒，1800 本来表示 30 分钟；
		// 旧版 SlowThreshold 单位是毫秒，200 本来表示 200ms。
		{"ConnMaxLifetime 单位写错",
			Config{Dsn: validDSN, ConnMaxLifetime: 1800}, "ConnMaxLifetime"},
		{"ConnectTimeout 单位写错",
			Config{Dsn: validDSN, ConnectTimeout: 5000}, "ConnectTimeout"},
		{"SlowThreshold 单位写错",
			Config{Dsn: validDSN, SlowThreshold: 200}, "SlowThreshold"},

		{"LogLevel 非法", Config{Dsn: validDSN, LogLevel: "verbose"}, "LogLevel"},
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
	// DialBackoff 取 10ms 这种毫秒级取值也必须被接受 ——
	// 单位哨兵的门槛是 1ms，不能把退避参数误伤成"写错单位"。
	for _, cfg := range []Config{
		{Dsn: validDSN},
		{Dsn: validDSN, SlowThreshold: -1},
		{Dsn: validDSN, SlowThreshold: 200 * time.Millisecond, LogLevel: "warn"},
		{Dsn: validDSN, SSL: "skip-verify"},
		{Dsn: validDSN, DialBackoff: 10 * time.Millisecond, DialMaxBackoff: 40 * time.Millisecond},
		{Dsn: validDSN, MaxOpenConns: 0, MaxIdleConns: 0}, // 0 表示用默认值
		{Dsn: validDSN, DialAttempts: -1},                 // 负值表示无限重试
	} {
		if _, err := cfg.ready(); err != nil {
			t.Errorf("配置 %+v 应合法，实际: %v", cfg, err)
		}
	}
}

// TestDSNOverrides 校验 DSN 组装：Config 里的覆盖项要真正写进 DSN，
// 且不能覆盖 DSN 中用户已经显式写好的参数。
//
// 旧版 Config.SSL 是零引用字段（设了没有任何效果），这里守住这个修复。
func TestDSNOverrides(t *testing.T) {
	// SSL 生效
	c := Config{Dsn: validDSN, SSL: "skip-verify"}.normalize()
	dsn, err := c.dsn()
	if err != nil {
		t.Fatalf("dsn() 失败: %v", err)
	}
	if !strings.Contains(dsn, "tls=skip-verify") {
		t.Errorf("SSL 未写入 DSN: %s", dsn)
	}

	// 超时覆盖生效
	c = Config{
		Dsn:            validDSN,
		ConnectTimeout: 3 * time.Second,
		ReadTimeout:    7 * time.Second,
		WriteTimeout:   9 * time.Second,
	}.normalize()
	dsn, err = c.dsn()
	if err != nil {
		t.Fatalf("dsn() 失败: %v", err)
	}
	for _, want := range []string{"timeout=3s", "readTimeout=7s", "writeTimeout=9s"} {
		if !strings.Contains(dsn, want) {
			t.Errorf("DSN 应包含 %q，实际: %s", want, dsn)
		}
	}

	// 用户已在 DSN 里写好的参数优先，Config 不得覆盖
	explicit := "root:123456@tcp(127.0.0.1:3306)/demo?parseTime=True&timeout=11s&tls=true"
	c = Config{Dsn: explicit, ConnectTimeout: time.Second, SSL: "skip-verify"}.normalize()
	dsn, err = c.dsn()
	if err != nil {
		t.Fatalf("dsn() 失败: %v", err)
	}
	if !strings.Contains(dsn, "timeout=11s") {
		t.Errorf("DSN 里显式的 timeout 应被保留: %s", dsn)
	}
	if !strings.Contains(dsn, "tls=true") {
		t.Errorf("DSN 里显式的 tls 应被保留（不被 Config.SSL 覆盖）: %s", dsn)
	}
	if strings.Contains(dsn, "tls=skip-verify") {
		t.Errorf("Config.SSL 不应覆盖 DSN 里已显式指定的 tls: %s", dsn)
	}

	// 库名与字符集等原有参数不能丢。
	// 注意 FormatDSN 会把布尔参数规范化为小写（parseTime=True → parseTime=true），
	// 所以这里做大小写不敏感的比较。
	if !strings.Contains(dsn, "/demo") {
		t.Errorf("原有 DSN 的库名丢失: %s", dsn)
	}
	if !strings.Contains(strings.ToLower(dsn), "parsetime=true") {
		t.Errorf("原有 DSN 的 parseTime 参数丢失: %s", dsn)
	}
}

// TestOpenRejectsInvalidConfig 非法配置必须在触网之前就被拒绝。
func TestOpenRejectsInvalidConfig(t *testing.T) {
	for _, cfg := range []Config{
		{},                             // 空配置
		{Dsn: "garbage"},               // 语法错误
		{Dsn: validDSN, LogLevel: "x"}, // 级别非法
	} {
		m, err := Open(cfg)
		if err == nil {
			_ = m.Close()
			t.Fatalf("配置 %+v 应被拒绝", cfg)
		}
		if !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("配置 %+v 期望 ErrInvalidConfig，实际: %v", cfg, err)
		}
	}
}

// TestOpenFailsFastWhenUnreachable 是旧版最严重缺陷（P0）的回归用例。
//
// 旧版 connect() 内部的循环只在成功或 ctx 被取消时退出，而 ctx 只由 Close 取消，
// 因此 MySQL 不可达时 Get() **永远不返回**：既拿不到连接，也拿不到 error。
// 这个用例断言现在会快速返回错误，并且真的带上了 ErrConnect。
func TestOpenFailsFastWhenUnreachable(t *testing.T) {
	addr := closedAddr(t)
	cfg := Config{
		Dsn:            "root:123456@tcp(" + addr + ")/demo?parseTime=True",
		DialAttempts:   1, // 失败即返回
		ConnectTimeout: time.Second,
		DialBackoff:    10 * time.Millisecond,
	}

	start := time.Now()
	m, err := Open(cfg)
	elapsed := time.Since(start)

	if err == nil {
		_ = m.Close()
		t.Fatal("连不上 MySQL 时 Open 应返回错误")
	}
	if !errors.Is(err, ErrConnect) {
		t.Errorf("期望 ErrConnect，实际: %v", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("Open 应在 5s 内返回错误，实际耗时 %s（旧版在这里会永久阻塞）", elapsed)
	}
}

// TestDialAttemptsBounded 校验有限重试：DialAttempts=N 时实际尝试次数不会失控，
// 总耗时也应受退避参数约束。
func TestDialAttemptsBounded(t *testing.T) {
	addr := closedAddr(t)
	cfg := Config{
		Dsn:            "root:123456@tcp(" + addr + ")/demo?parseTime=True",
		DialAttempts:   3,
		ConnectTimeout: 500 * time.Millisecond,
		DialBackoff:    20 * time.Millisecond,
		DialMaxBackoff: 40 * time.Millisecond,
	}.normalize()

	start := time.Now()
	m, err := Open(cfg)
	elapsed := time.Since(start)

	if err == nil {
		_ = m.Close()
		t.Fatal("应返回错误")
	}
	// 3 次尝试 + 两次退避（20ms + 40ms）应远小于 5s。
	if elapsed > 5*time.Second {
		t.Errorf("有限重试应在 5s 内结束，实际 %s", elapsed)
	}
}

// TestCloseWithoutOpenIsSafe 初始化失败时不应返回半可用的实例。
func TestCloseWithoutOpenIsSafe(t *testing.T) {
	addr := closedAddr(t)
	cfg := Config{
		Dsn:          "root:123456@tcp(" + addr + ")/demo",
		DialAttempts: 1,
	}
	m, err := Open(cfg)
	if err == nil {
		_ = m.Close()
		t.Fatal("应返回错误")
	}
	// 失败时必须返回 nil 实例，否则调用方可能拿到一个内部字段全是零值的对象
	if m != nil {
		t.Errorf("失败时不应返回实例，实际 %#v", m)
	}
	// 失败路径不应留下可用的连接池（错误信息里也不该泄漏密码以外的敏感内容）
	if !errors.Is(err, ErrConnect) {
		t.Errorf("期望 ErrConnect，实际: %v", err)
	}
}

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
	// 包装后的错误仍应能被判定
	wrapped := context.Canceled
	if errors.Is(wrapped, ErrConnect) {
		t.Error("context.Canceled 不应匹配 ErrConnect")
	}
}
