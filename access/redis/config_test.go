package redis

import (
	"crypto/tls"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// 本文件是**不需要 Redis** 的单元测试：配置校验、默认值、
// 重试语义修正，以及"连不上时是否快速失败"。
// 需要真实 Redis 的用例见 redis_test.go。

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
	c := Config{Addr: "127.0.0.1:6379"}.normalize()

	if want := defaultPoolSize(); c.PoolSize != want {
		t.Errorf("PoolSize 期望 %d，实际 %d", want, c.PoolSize)
	}
	if want := defaultReadTimeout; c.ReadTimeout != want {
		t.Errorf("ReadTimeout 期望 %s，实际 %s", want, c.ReadTimeout)
	}
	if want := defaultConnMaxLifetime; c.ConnMaxLifetime != want {
		t.Errorf("ConnMaxLifetime 期望 %s，实际 %s", want, c.ConnMaxLifetime)
	}
	// PoolTimeout 默认是 ReadTimeout + 1s（与 go-redis 一致）
	if want := c.ReadTimeout + time.Second; c.PoolTimeout != want {
		t.Errorf("PoolTimeout 期望 %s，实际 %s", want, c.PoolTimeout)
	}
	// MaxRetries：0 必须变成 3，而不是保持 0
	if want := defaultMaxRetries; c.MaxRetries != want {
		t.Errorf("MaxRetries 期望 %d，实际 %d", want, c.MaxRetries)
	}
	// Jitter 默认取寿命的 1/10，避免所有连接同时过期
	if want := c.ConnMaxLifetime / 10; c.ConnMaxLifetimeJitter != want {
		t.Errorf("ConnMaxLifetimeJitter 期望 %s，实际 %s", want, c.ConnMaxLifetimeJitter)
	}
	// ctx 超时默认生效：DisableContextTimeout 的零值 false 正好就是期望的默认行为。
	// 这条断言顺带守住"命名与默认值必须一致"—— 若换成正向命名的 bool，
	// 零值就与文档声称的默认值相反（本包第一版正是这么错的）。
	if c.DisableContextTimeout {
		t.Error("DisableContextTimeout 应默认为 false（即默认尊重 ctx 超时）")
	}

	// 硬上限低于基准池大小时，基准应被拉低到上限
	c2 := Config{Addr: "a:6379", PoolSize: 100, MaxActiveConns: 20}.normalize()
	if c2.PoolSize != 20 {
		t.Errorf("PoolSize 应被拉低到 MaxActiveConns=20，实际 %d", c2.PoolSize)
	}

	// MinIdleConns 不应超过 PoolSize
	c3 := Config{Addr: "a:6379", PoolSize: 10, MinIdleConns: 50}.normalize()
	if c3.MinIdleConns != 10 {
		t.Errorf("MinIdleConns 应被截断到 PoolSize=10，实际 %d", c3.MinIdleConns)
	}

	// 退避上限小于起始值时应抬到起始值
	c4 := Config{Addr: "a:6379", MinRetryBackoff: 100 * time.Millisecond, MaxRetryBackoff: time.Millisecond}.normalize()
	if c4.MaxRetryBackoff != 100*time.Millisecond {
		t.Errorf("MaxRetryBackoff 应被抬到起始值，实际 %s", c4.MaxRetryBackoff)
	}
}

// TestMaxRetriesSemantics 是本包最重要的语义回归。
//
// go-redis 的 MaxRetries 语义：0 → 3 次，**-1 → 禁用重试**。
// 旧版写的是 `MaxRetries: -1`，注释却是"最大重试次数"，
// 实际效果是把自动重试关掉，连带 MinRetryBackoff / MaxRetryBackoff 一起空转。
func TestMaxRetriesSemantics(t *testing.T) {
	// 0（未设置）→ 补成默认 3 次
	if got := (Config{Addr: "a:6379"}).normalize().MaxRetries; got != 3 {
		t.Errorf("MaxRetries 未设置时应补成 3，实际 %d", got)
	}

	// -1 是"显式禁用重试"，必须原样保留，不能被 normalize 改掉
	got := (Config{Addr: "a:6379", MaxRetries: -1}).normalize().MaxRetries
	if got != -1 {
		t.Errorf("MaxRetries=-1 应被保留（表示禁用重试），实际 %d", got)
	}

	// 显式正数应原样保留
	if got := (Config{Addr: "a:6379", MaxRetries: 5}).normalize().MaxRetries; got != 5 {
		t.Errorf("MaxRetries=5 应被保留，实际 %d", got)
	}

	// 小于 -1 是非法的（go-redis 未定义该取值）
	err := (Config{Addr: "a:6379", MaxRetries: -2}).validate()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("MaxRetries=-2 应被拒绝，实际: %v", err)
	}
}

// TestOptionsMapping 逐字段断言 Config → goredis.Options 的映射。
//
// 这个测试的价值在于：漏传一个字段**不会报错**，只是配置静默失效。
// 把映射逻辑抽成 options() 之后，这种错误可以在不连 Redis 的情况下被抓住。
func TestOptionsMapping(t *testing.T) {
	cfg := Config{
		Addr:                  "redis.internal:6380",
		Username:              "app",
		Password:              "s3cret",
		DB:                    3,
		ClientName:            "order-svc",
		Protocol:              2,
		PoolSize:              64,
		MinIdleConns:          16,
		MaxIdleConns:          32,
		MaxActiveConns:        200,
		PoolTimeout:           2 * time.Second,
		PoolFIFO:              true,
		ConnMaxIdleTime:       10 * time.Minute,
		ConnMaxLifetime:       90 * time.Minute,
		ConnMaxLifetimeJitter: 9 * time.Minute,
		DialTimeout:           4 * time.Second,
		ReadTimeout:           2 * time.Second,
		WriteTimeout:          3 * time.Second,
		MaxRetries:            7,
		MinRetryBackoff:       11 * time.Millisecond,
		MaxRetryBackoff:       33 * time.Millisecond,
	}.normalize()

	opts, err := cfg.options()
	if err != nil {
		t.Fatalf("options() 失败: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"Addr", opts.Addr, cfg.Addr},
		{"Username", opts.Username, cfg.Username},
		{"Password", opts.Password, cfg.Password},
		{"DB", opts.DB, cfg.DB},
		{"ClientName", opts.ClientName, cfg.ClientName},
		{"Protocol", opts.Protocol, cfg.Protocol},
		{"PoolSize", opts.PoolSize, cfg.PoolSize},
		{"MinIdleConns", opts.MinIdleConns, cfg.MinIdleConns},
		{"MaxIdleConns", opts.MaxIdleConns, cfg.MaxIdleConns},
		{"MaxActiveConns", opts.MaxActiveConns, cfg.MaxActiveConns},
		{"PoolTimeout", opts.PoolTimeout, cfg.PoolTimeout},
		{"PoolFIFO", opts.PoolFIFO, cfg.PoolFIFO},
		{"ConnMaxIdleTime", opts.ConnMaxIdleTime, cfg.ConnMaxIdleTime},
		{"ConnMaxLifetime", opts.ConnMaxLifetime, cfg.ConnMaxLifetime},
		{"ConnMaxLifetimeJitter", opts.ConnMaxLifetimeJitter, cfg.ConnMaxLifetimeJitter},
		{"DialTimeout", opts.DialTimeout, cfg.DialTimeout},
		{"ReadTimeout", opts.ReadTimeout, cfg.ReadTimeout},
		{"WriteTimeout", opts.WriteTimeout, cfg.WriteTimeout},
		{"MaxRetries", opts.MaxRetries, cfg.MaxRetries},
		{"MinRetryBackoff", opts.MinRetryBackoff, cfg.MinRetryBackoff},
		{"MaxRetryBackoff", opts.MaxRetryBackoff, cfg.MaxRetryBackoff},
		{"TLSConfig（未开启 TLS）", opts.TLSConfig, (*tls.Config)(nil)},
	}

	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s 映射错误：期望 %v，实际 %v", c.name, c.want, c.got)
		}
	}

	// 默认（不设 DisableContextTimeout）必须开启 ctx 超时尊重
	if !opts.ContextTimeoutEnabled {
		t.Error("默认情况下 ContextTimeoutEnabled 应为 true")
	}

	// 显式 DisableContextTimeout 时才关闭
	off := Config{Addr: "a:6379", DisableContextTimeout: true}.normalize()
	optsOff, err := off.options()
	if err != nil {
		t.Fatalf("options() 失败: %v", err)
	}
	if optsOff.ContextTimeoutEnabled {
		t.Error("DisableContextTimeout=true 时 ContextTimeoutEnabled 应为 false")
	}
}

// TestConfigValidate 覆盖各类非法配置。
func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"Addr 为空", Config{}, "Addr 不能为空"},
		{"Addr 只有空白", Config{Addr: "  "}, "Addr 不能为空"},
		{"DB 为负", Config{Addr: "a:6379", DB: -1}, "DB 不能为负"},
		{"PoolSize 为负", Config{Addr: "a:6379", PoolSize: -1}, "PoolSize 不能为负"},
		{"MaxActiveConns 为负", Config{Addr: "a:6379", MaxActiveConns: -1}, "MaxActiveConns 不能为负"},
		{"MinIdleConns 为负", Config{Addr: "a:6379", MinIdleConns: -1}, "MinIdleConns 不能为负"},
		{"MaxIdleConns 为负", Config{Addr: "a:6379", MaxIdleConns: -1}, "MaxIdleConns 不能为负"},
		{"Protocol 非法", Config{Addr: "a:6379", Protocol: 4}, "Protocol"},
		{"MaxRetries 小于 -1", Config{Addr: "a:6379", MaxRetries: -2}, "MaxRetries"},

		// 单位哨兵：把毫秒整数直接写给 Duration 字段
		{"ReadTimeout 单位写错", Config{Addr: "a:6379", ReadTimeout: 3000}, "ReadTimeout"},
		{"DialTimeout 单位写错", Config{Addr: "a:6379", DialTimeout: 5000}, "DialTimeout"},
		{"PoolTimeout 单位写错", Config{Addr: "a:6379", PoolTimeout: 4000}, "PoolTimeout"},
		{"ConnMaxIdleTime 单位写错", Config{Addr: "a:6379", ConnMaxIdleTime: 300000}, "ConnMaxIdleTime"},

		// TLS 双向认证必须成对提供证书与私钥
		{"TLS 只给了证书",
			Config{Addr: "a:6379", TLS: TLSConfig{Enable: true, CertFile: "/tmp/c.pem"}}, "CertFile 与 KeyFile"},
		{"TLS 只给了私钥",
			Config{Addr: "a:6379", TLS: TLSConfig{Enable: true, KeyFile: "/tmp/k.pem"}}, "CertFile 与 KeyFile"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.validate()
			if err == nil {
				t.Fatal("期望报错，实际通过")
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("期望 ErrInvalidConfig，实际 %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("错误信息应包含 %q，实际: %v", tc.want, err)
			}
		})
	}

	// 合法配置不应报错。
	// 特别是：MaxRetries=-1（禁用重试）、ReadTimeout=-1（不超时）、
	// ConnMaxLifetime 负值（不过期）都是 go-redis 定义的合法语义，
	// 不能被单位哨兵误伤 —— 哨兵只检查**正的**极小值。
	for _, cfg := range []Config{
		{Addr: "a:6379"},
		{Addr: "a:6379", MaxRetries: -1},
		{Addr: "a:6379", ReadTimeout: -1},
		{Addr: "a:6379", ReadTimeout: -2},
		{Addr: "a:6379", ConnMaxLifetime: -1},
		{Addr: "a:6379", Protocol: 2},
		{Addr: "a:6379", Protocol: 3},
		{Addr: "a:6379", MinRetryBackoff: 8 * time.Millisecond},
		{Addr: "a:6379", ConnMaxLifetime: time.Hour, ConnMaxLifetimeJitter: time.Minute},
	} {
		if _, err := cfg.ready(); err != nil {
			t.Errorf("配置 %+v 应合法，实际: %v", cfg, err)
		}
	}
}

// TestTLSConfigBuild 校验 TLS 配置的组装与错误处理。
func TestTLSConfigBuild(t *testing.T) {
	// 未开启 TLS → nil，调用方不应拿到一个空的 tls.Config
	c := Config{Addr: "127.0.0.1:6379"}
	got, err := c.tlsConfig()
	if err != nil {
		t.Fatalf("未开启 TLS 不应报错: %v", err)
	}
	if got != nil {
		t.Errorf("未开启 TLS 应返回 nil，实际 %#v", got)
	}

	// 开启 TLS、不指定 ServerName → 从 Addr 推导出 host
	c = Config{Addr: "redis.internal:6379", TLS: TLSConfig{Enable: true}}
	got, err = c.tlsConfig()
	if err != nil {
		t.Fatalf("tlsConfig 失败: %v", err)
	}
	if got == nil {
		t.Fatal("开启 TLS 应返回非 nil 配置")
	}
	if got.ServerName != "redis.internal" {
		t.Errorf("ServerName 期望从 Addr 推导为 redis.internal，实际 %q", got.ServerName)
	}
	if got.MinVersion == 0 {
		t.Error("应设置 MinVersion，避免回落到过旧的 TLS 版本")
	}

	// 显式 ServerName 优先于 Addr 推导
	c = Config{Addr: "10.0.0.1:6379", TLS: TLSConfig{Enable: true, ServerName: "redis.example.com"}}
	got, err = c.tlsConfig()
	if err != nil {
		t.Fatalf("tlsConfig 失败: %v", err)
	}
	if got.ServerName != "redis.example.com" {
		t.Errorf("显式 ServerName 应优先，实际 %q", got.ServerName)
	}

	// CA 文件不存在 → 报错（而不是静默用系统信任链）
	c = Config{Addr: "a:6379", TLS: TLSConfig{Enable: true, CAFile: "/definitely/not/here.pem"}}
	if _, err := c.tlsConfig(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("读取不存在的 CA 文件应报 ErrInvalidConfig，实际: %v", err)
	}

	// CA 文件内容不是证书 → 报错
	bad := filepath.Join(t.TempDir(), "bad.pem")
	if err := os.WriteFile(bad, []byte("这不是证书"), 0o600); err != nil {
		t.Fatal(err)
	}
	c = Config{Addr: "a:6379", TLS: TLSConfig{Enable: true, CAFile: bad}}
	if _, err := c.tlsConfig(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("非法 CA 内容应报 ErrInvalidConfig，实际: %v", err)
	}
}

// TestIsRealErr 确保缓存未命中（goredis.Nil）不被当成错误。
//
// 这是很多 Redis 封装会犯的错：把 key 不存在的正常业务结果
// 记为错误，日志与告警被噪音淹没。
func TestIsRealErr(t *testing.T) {
	if isRealErr(nil) {
		t.Error("nil 不是错误")
	}
	if isRealErr(goredis.Nil) {
		t.Error("goredis.Nil（key 不存在）不应被视为错误")
	}
	// 包装后的 goredis.Nil 也应被识别
	if isRealErr(errors.Join(errors.New("其他"), goredis.Nil)) {
		t.Error("包装后的 goredis.Nil 不应被视为错误")
	}
	if !isRealErr(errors.New("连接被拒绝")) {
		t.Error("普通错误应被识别为真实错误")
	}
}

// TestOpenRejectsInvalidConfig 非法配置必须在触网之前被拒绝。
func TestOpenRejectsInvalidConfig(t *testing.T) {
	for _, cfg := range []Config{
		{},
		{Addr: "a:6379", PoolSize: -1},
		{Addr: "a:6379", MaxRetries: -5},
	} {
		r, err := Open(cfg)
		if err == nil {
			_ = r.Close()
			t.Fatalf("配置 %+v 应被拒绝", cfg)
		}
		if !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("配置 %+v 期望 ErrInvalidConfig，实际: %v", cfg, err)
		}
	}
}

// TestOpenFailsFastWhenUnreachable 断言连不上时快速返回错误，而不是挂住。
func TestOpenFailsFastWhenUnreachable(t *testing.T) {
	cfg := Config{
		Addr:            closedAddr(t),
		DialTimeout:     500 * time.Millisecond,
		ReadTimeout:     500 * time.Millisecond,
		MaxRetries:      0, // → 3 次重试
		ConnMaxIdleTime: time.Minute,
	}

	start := time.Now()
	r, err := Open(cfg)
	elapsed := time.Since(start)

	if err == nil {
		_ = r.Close()
		t.Fatal("连不上 Redis 时 Open 应返回错误")
	}
	if !errors.Is(err, ErrConnect) {
		t.Errorf("期望 ErrConnect，实际: %v", err)
	}
	if elapsed > 10*time.Second {
		t.Errorf("Open 应在 10s 内返回错误，实际耗时 %s", elapsed)
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
	// 注意：goredis.Nil 是本包之外的类型，不能被本包的哨兵错误匹配
	if errors.Is(goredis.Nil, ErrConnect) {
		t.Error("goredis.Nil 不应匹配本包哨兵错误")
	}
}
