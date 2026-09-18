package mongo

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/zavierswong/go-infra/logger"
	"github.com/zavierswong/go-infra/metrics"
)

// 本文件是**不需要 MongoDB** 的纯逻辑测试：配置校验、URI 解析与脱敏、
// 驱动选项装配、错误判定、以及"连不上时能否快速失败"。
//
// 真实数据库的集成用例在 mongo_test.go。

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

// validURI 是一个语法完全合法、但本文件的用例**不会真的去连**的连接串。
// 它的作用是把校验流程推进到"字段检查"那一步 —— parseURI 是 validate 的第一步，
// URI 不合法会先返回，后面的字段断言就全都测不到了。
const validURI = "mongo://127.0.0.1:27017/gointra_test"

func mustReady(t *testing.T, cfg Config) Config {
	t.Helper()
	got, err := cfg.ready()
	if err != nil {
		t.Fatalf("ready() 应成功，实际报错: %v", err)
	}
	return got
}

func boolPtr(v bool) *bool { return &v }

func derefU64(p *uint64) any {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func derefStr(p *string) any {
	if p == nil {
		return "<nil>"
	}
	return *p
}

// ---------------------------------------------------------------------------
// Config 默认值
// ---------------------------------------------------------------------------

func TestConfigDefaults(t *testing.T) {
	got := mustReady(t, Config{URI: validURI})

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"MaxPoolSize", got.MaxPoolSize, defaultPoolSize},
		{"MaxConnecting", got.MaxConnecting, defaultMaxConnecting},
		{"DialAttempts", got.DialAttempts, defaultDialAttempts},
		{"DialBackoff", got.DialBackoff, defaultDialBackoff},
		{"DialProbeTimeout", got.DialProbeTimeout, defaultDialProbeTimeout},
		{"DialMaxBackoff", got.DialMaxBackoff, defaultDialMaxBackoff},
		{"HealthCheckInterval", got.HealthCheckInterval, defaultHealthCheckInterval},
		{"SlowThreshold", got.SlowThreshold, defaultSlowThreshold},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s 默认值期望 %v，实际 %v", c.name, c.want, c.got)
		}
	}

	// 这几项**刻意不补默认值**：0 在驱动里是合法取值，含义与"未配置"不同。
	//   MaxConnIdleTime=0 → 不因空闲关闭连接
	//   MaxConnecting 之外的池参数 0 → 懒建连
	//   ～Timeout=0       → 不限制
	// 给它们补默认值会改变驱动行为，所以必须保持 0。
	zeros := []struct {
		name string
		got  int64
	}{
		{"MaxConnIdleTime", int64(got.MaxConnIdleTime)},
		{"MinPoolSize", int64(got.MinPoolSize)},
		{"ConnectTimeout", int64(got.ConnectTimeout)},
		{"ServerSelectionTimeout", int64(got.ServerSelectionTimeout)},
		{"OperationTimeout", int64(got.OperationTimeout)},
		{"HeartbeatInterval", int64(got.HeartbeatInterval)},
	}
	for _, c := range zeros {
		if c.got != 0 {
			t.Errorf("%s 期望保持 0（交由驱动默认值），实际 %v", c.name, c.got)
		}
	}

	if got.DialMaxBackoff < got.DialBackoff {
		t.Errorf("DialMaxBackoff(%v) 不应小于 DialBackoff(%v)",
			got.DialMaxBackoff, got.DialBackoff)
	}
}

func TestConfigNormalizeMaxBackoff(t *testing.T) {
	// 上限写得比起始值还小：抬到起始值而不是报错 ——
	// 报错会让"把退避整体调大"变成要同时改两个字段，属于无谓的负担。
	cfg := mustReady(t, Config{
		URI:            validURI,
		DialBackoff:    10 * time.Second,
		DialMaxBackoff: time.Second,
	})
	if cfg.DialMaxBackoff != cfg.DialBackoff {
		t.Errorf("DialMaxBackoff 应被抬到 DialBackoff=%v，实际 %v",
			cfg.DialBackoff, cfg.DialMaxBackoff)
	}
}

func TestConfigNormalizeReadPreference(t *testing.T) {
	// 同一个取值写在 URI 里能用、写在 Config 里也应当能用。
	// 驱动的 readpref.ModeFromString 只认全小写无分隔的 "primarypreferred"，
	// 而驱动自己的 Mode.String() 输出的是驼峰 "primaryPreferred"。
	// normalize 先归一化成后者，日志与指标里读起来更顺。
	for _, in := range []string{
		"primaryPreferred", "primary-preferred", "primary_preferred",
		"PRIMARY_PREFERRED", " primarypreferred ",
	} {
		cfg := mustReady(t, Config{URI: validURI, ReadPreference: in})
		if cfg.ReadPreference != "primaryPreferred" {
			t.Errorf("ReadPreference=%q 应归一化为 %q，实际 %q",
				in, "primaryPreferred", cfg.ReadPreference)
		}
	}
}

func TestConfigNormalizeLogLevel(t *testing.T) {
	cfg := mustReady(t, Config{URI: validURI, LogLevel: "  WARN  "})
	if cfg.LogLevel != logLevelWarn {
		t.Errorf("LogLevel 应归一化为 %q，实际 %q", logLevelWarn, cfg.LogLevel)
	}
	if got := cfg.effectiveLogLevel(); got != logLevelWarn {
		t.Errorf("effectiveLogLevel() 期望 %q，实际 %q", logLevelWarn, got)
	}

	// 空值按 info 生效，但**不改写** c.LogLevel ——
	// 保留"用户到底配了什么"这件事，排查配置时用得上。
	empty := mustReady(t, Config{URI: validURI})
	if empty.LogLevel != "" {
		t.Errorf("LogLevel 应保持空串，实际 %q", empty.LogLevel)
	}
	if got := empty.effectiveLogLevel(); got != logLevelInfo {
		t.Errorf("空 LogLevel 的 effectiveLogLevel() 期望 %q，实际 %q", logLevelInfo, got)
	}
}

// ---------------------------------------------------------------------------
// Config 校验
// ---------------------------------------------------------------------------

func TestConfigValidateRejects(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantSub string
	}{
		{"URI 为空", Config{}, "URI 不能为空"},
		{"URI 只有空白", Config{URI: "   "}, "URI 不能为空"},
		{"scheme 不支持", Config{URI: "mysql://h:3306/db"}, "不支持的 scheme"},
		{"MaxPoolSize 为负", Config{URI: validURI, MaxPoolSize: -1}, "MaxPoolSize 不能为负"},
		{"MinPoolSize 为负", Config{URI: validURI, MinPoolSize: -1}, "MinPoolSize 不能为负"},
		{"MaxConnecting 为负", Config{URI: validURI, MaxConnecting: -1}, "MaxConnecting 不能为负"},
		{"RebuildAfterFailures 为负", Config{URI: validURI, RebuildAfterFailures: -1}, "RebuildAfterFailures 不能为负"},

		// MinPoolSize 的默认上限保护。
		//
		// 这条不能用驱动的 Validate() 代替：那边只在**两个字段都被显式设置**时
		// 才比较 MinPoolSize ≤ MaxPoolSize。只配了 MinPoolSize=200 而 MaxPoolSize
		// 留空（生效的是驱动默认 100）时它不报错，于是
		// "要求至少 200 个连接、上限却只有 100"的矛盾配置会在运行时静默生效。
		{
			"MinPoolSize 超过默认 MaxPoolSize",
			Config{URI: validURI, MinPoolSize: defaultPoolSize + 1},
			"超过了驱动默认的 MaxPoolSize",
		},
		{
			"MinPoolSize 大于显式 MaxPoolSize",
			Config{URI: validURI, MaxPoolSize: 10, MinPoolSize: 20},
			"不能大于 MaxPoolSize",
		},

		// 单位哨兵：把整数直接写进 Duration 字段（5 → 5ns）。
		// 这类配置不会报错，只会让客户端表现诡异，所以必须大声失败。
		{"ConnectTimeout 疑似单位写错", Config{URI: validURI, ConnectTimeout: 5}, "疑似单位写错"},
		{"DialProbeTimeout 疑似单位写错", Config{URI: validURI, DialProbeTimeout: 5}, "疑似单位写错"},
		{"HealthCheckInterval 疑似单位写错", Config{URI: validURI, HealthCheckInterval: 5}, "疑似单位写错"},
		{"SlowThreshold 疑似单位写错", Config{URI: validURI, SlowThreshold: 5}, "SlowThreshold"},

		{"心跳低于驱动下限", Config{URI: validURI, HeartbeatInterval: 100 * time.Millisecond}, "小于驱动的硬下限"},

		{"ReadPreference 非法", Config{URI: validURI, ReadPreference: "fastest"}, "ReadPreference"},
		{"ReadConcern 非法", Config{URI: validURI, ReadConcern: "eventual"}, "ReadConcern"},
		{"WriteConcern 为负", Config{URI: validURI, WriteConcern: "-1"}, "不能为负"},
		{"WriteConcern 含空白", Config{URI: validURI, WriteConcern: "my tag"}, "不含空白"},
		{"WriteConcern 全空白", Config{URI: validURI, WriteConcern: "  "}, "WriteConcern"},
		{"LogLevel 非法", Config{URI: validURI, LogLevel: "verbose"}, "LogLevel"},

		// driver v2 已删除的参数：必须启动期报错，不能静默不生效。
		// 这类故障最坏 —— 配置看着正常、驱动不报错、你以存在的超时根本不存在。
		{
			"socketTimeoutMS 在 v2 已失效",
			Config{URI: "mongo://127.0.0.1:27017/db?socketTimeoutMS=5000"},
			"已失效",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.cfg.ready()
			if err == nil {
				t.Fatal("期望报错，实际通过")
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("错误应可用 errors.Is(err, ErrInvalidConfig) 判定，实际: %v", err)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("错误信息应包含 %q，实际: %v", tc.wantSub, err)
			}
		})
	}
}

func TestConfigValidateAccepts(t *testing.T) {
	// 一组"看着极端但合法"的配置。共同点是 **0 / 负值有明确含义**，
	// 不能因为"看着像没填"就被判非法或被替换成默认值。
	cfg := mustReady(t, Config{
		URI: validURI,
		// 0 表示不因空闲关闭连接 —— 驱动默认行为，不是漏填。
		MaxConnIdleTime: 0,
		// 负值表示不判定慢操作（warn 级别下只打失败）。
		SlowThreshold: -1,
		// 边界值：正好等于心跳下限，应当被接受。
		HeartbeatInterval: minHeartbeatInterval,
		WriteConcern:      "majority",
		ReadConcern:       "MAJORITY", // 大小写不敏感
		// MinPoolSize 正好等于 MaxPoolSize 是允许的（池不收缩）。
		MaxPoolSize: 8,
		MinPoolSize: 8,
	})

	if cfg.DialProbeTimeout != defaultDialProbeTimeout {
		t.Errorf("DialProbeTimeout=0 应补默认值 %v，实际 %v",
			defaultDialProbeTimeout, cfg.DialProbeTimeout)
	}
	if cfg.HeartbeatInterval != minHeartbeatInterval {
		t.Errorf("边界值 %v 不应被改写，实际 %v", minHeartbeatInterval, cfg.HeartbeatInterval)
	}
	if cfg.SlowThreshold != -1 {
		t.Errorf("SlowThreshold=-1 应原样保留，实际 %v", cfg.SlowThreshold)
	}
	if cfg.MaxPoolSize != 8 || cfg.MinPoolSize != 8 {
		t.Errorf("显式配置的池大小不应被改写，实际 max=%d min=%d",
			cfg.MaxPoolSize, cfg.MinPoolSize)
	}
}

// ---------------------------------------------------------------------------
// 驱动选项装配
// ---------------------------------------------------------------------------

func TestClientOptionsApplyConfig(t *testing.T) {
	cfg := mustReady(t, Config{
		URI:                 validURI,
		Name:                "order",
		AppName:             "order-api@test",
		MaxPoolSize:         17,
		MinPoolSize:         3,
		MaxConnecting:       4,
		MaxConnIdleTime:     2 * time.Minute,
		ConnectTimeout:      3 * time.Second,
		OperationTimeout:    7 * time.Second,
		HeartbeatInterval:   2 * time.Second,
		ReplicaSet:          "rs0",
		ReadPreference:      "secondaryPreferred",
		ReadConcern:         "majority",
		WriteConcern:        "majority",
		WriteConcernJournal: boolPtr(true),
	})

	opts, info, err := cfg.clientOptions()
	if err != nil {
		t.Fatalf("clientOptions() 报错: %v", err)
	}
	if info.dbName != "gointra_test" {
		t.Errorf("默认库名期望 %q，实际 %q", "gointra_test", info.dbName)
	}

	// 用解引用后的值逐个核对：nil 表示该项根本没被写进驱动选项，
	// 那意味着"配置写了但不生效"，比写错值更难发现。
	checkU64 := func(name string, got *uint64, want uint64) {
		t.Helper()
		if got == nil {
			t.Errorf("%s 未被写入驱动选项", name)
			return
		}
		if *got != want {
			t.Errorf("%s 期望 %d，实际 %d", name, want, *got)
		}
	}
	checkDur := func(name string, got *time.Duration, want time.Duration) {
		t.Helper()
		if got == nil {
			t.Errorf("%s 未被写入驱动选项", name)
			return
		}
		if *got != want {
			t.Errorf("%s 期望 %v，实际 %v", name, want, *got)
		}
	}
	checkStr := func(name string, got *string, want string) {
		t.Helper()
		if got == nil {
			t.Errorf("%s 未被写入驱动选项", name)
			return
		}
		if *got != want {
			t.Errorf("%s 期望 %q，实际 %q", name, want, *got)
		}
	}

	checkU64("MaxPoolSize", opts.MaxPoolSize, 17)
	checkU64("MinPoolSize", opts.MinPoolSize, 3)
	checkU64("MaxConnecting", opts.MaxConnecting, 4)
	checkDur("MaxConnIdleTime", opts.MaxConnIdleTime, 2*time.Minute)
	checkDur("ConnectTimeout", opts.ConnectTimeout, 3*time.Second)
	checkDur("Timeout(CSOT)", opts.Timeout, 7*time.Second)
	checkDur("HeartbeatInterval", opts.HeartbeatInterval, 2*time.Second)
	checkStr("AppName", opts.AppName, "order-api@test")
	checkStr("ReplicaSet", opts.ReplicaSet, "rs0")

	if opts.ReadPreference == nil || opts.ReadPreference.Mode() != readpref.SecondaryPreferredMode {
		t.Errorf("ReadPreference 期望 secondaryPreferred，实际 %v", opts.ReadPreference)
	}
	if opts.ReadConcern == nil || opts.ReadConcern.Level != "majority" {
		t.Errorf("ReadConcern 期望 majority，实际 %+v", opts.ReadConcern)
	}
	if opts.WriteConcern == nil {
		t.Fatal("WriteConcern 未被写入驱动选项")
	}
	if opts.WriteConcern.W != "majority" {
		t.Errorf("WriteConcern.W 期望 majority，实际 %#v", opts.WriteConcern.W)
	}
	if opts.WriteConcern.Journal == nil || !*opts.WriteConcern.Journal {
		t.Errorf("WriteConcern.Journal 期望 true，实际 %v", opts.WriteConcern.Journal)
	}
	if !strings.Contains(strings.Join(opts.Hosts, ","), "127.0.0.1:27017") {
		t.Errorf("Hosts 期望包含 127.0.0.1:27017，实际 %v", opts.Hosts)
	}

	// Name 只进指标标签，**不**混进 appName ——
	// 否则服务端看到的应用名会随部署形态变化，反而不利于检索。
	if opts.AppName != nil && *opts.AppName == "order" {
		t.Error("AppName 不应被 Config.Name 覆盖")
	}
}

func TestClientOptionsURIOverridesConfig(t *testing.T) {
	// 本包的核心约定：**URI 优先**。
	//
	// 实现方式是把 ApplyURI 放在所有 Set* **之后**调用，靠驱动
	// "后设置覆盖先设置"的语义拿到结果，而不是自己维护一张
	// "URI 里有没有这个参数"的表。好处是语义永远与驱动一致。
	cfg := mustReady(t, Config{
		URI:         validURI + "?maxPoolSize=7&appName=from-uri&retryWrites=false",
		MaxPoolSize: 50,
		AppName:     "from-config",
		RetryWrites: boolPtr(true), // Config 说 true，URI 说 false
	})

	opts, _, err := cfg.clientOptions()
	if err != nil {
		t.Fatalf("clientOptions() 报错: %v", err)
	}

	if opts.MaxPoolSize == nil || *opts.MaxPoolSize != 7 {
		t.Errorf("URI 的 maxPoolSize=7 应覆盖 Config 的 50，实际 %v", derefU64(opts.MaxPoolSize))
	}
	if opts.AppName == nil || *opts.AppName != "from-uri" {
		t.Errorf("URI 的 appName 应覆盖 Config，实际 %v", derefStr(opts.AppName))
	}
	if opts.RetryWrites == nil || *opts.RetryWrites {
		t.Errorf("URI 的 retryWrites=false 应覆盖 Config 的 true，实际 %v", opts.RetryWrites)
	}
}

func TestClientOptionsConfigAppliesWhenURISilent(t *testing.T) {
	// 反向验证：URI 没写该参数时 Config 必须生效。
	// 缺了这条，上面的"URI 优先"用例在一个"Config 被完全忽略"的实现下也会通过。
	cfg := mustReady(t, Config{
		URI:         validURI,
		MaxPoolSize: 23,
		AppName:     "from-config",
	})

	opts, _, err := cfg.clientOptions()
	if err != nil {
		t.Fatalf("clientOptions() 报错: %v", err)
	}
	if opts.MaxPoolSize == nil || *opts.MaxPoolSize != 23 {
		t.Errorf("URI 未指定 maxPoolSize 时应采用 Config 的 23，实际 %v", derefU64(opts.MaxPoolSize))
	}
	if opts.AppName == nil || *opts.AppName != "from-config" {
		t.Errorf("URI 未指定 appName 时应采用 Config 的值，实际 %v", derefStr(opts.AppName))
	}
}

func TestClientOptionsDriverLevelValidation(t *testing.T) {
	// 这两条是**驱动**的约束，本包刻意不重写（重写只会与驱动版本脱节），
	// 但必须确认它真的被调用了 —— validate() 只做本包自己的检查，
	// 驱动级校验发生在 clientOptions() 里。
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			"directConnection 与多主机互斥",
			Config{
				URI:              "mongo://h1:27017,h2:27017/db",
				DirectConnection: boolPtr(true),
			},
		},
		{
			"loadBalanced 与 replicaSet 互斥",
			Config{
				URI:          "mongo://h1:27017/db?replicaSet=rs0",
				LoadBalanced: boolPtr(true),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := mustReady(t, tc.cfg)
			_, _, err := cfg.clientOptions()
			if err == nil {
				t.Fatal("期望驱动级校验失败，实际通过")
			}
			// 配置写错属于配置问题，不是连接问题 —— 哨兵必须是 ErrInvalidConfig。
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("错误应可用 errors.Is(err, ErrInvalidConfig) 判定，实际: %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// URI 解析
// ---------------------------------------------------------------------------

func TestParseURI(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantSub string // 非空表示期望报错，且错误信息包含它
	}{
		{name: "标准形式", in: "mongo://user:pw@127.0.0.1:27017/shop"},
		{name: "多主机", in: "mongo://h1:27017,h2:27017,h3:27017/shop?replicaSet=rs0"},
		{name: "无端口（走默认 27017）", in: "mongo://h1/shop"},
		{name: "SRV 形式", in: "mongo+srv://user:pw@cluster0.example.com/shop?retryWrites=true"},
		{name: "IPv6 带方括号与端口", in: "mongo://[::1]:27017/shop"},
		{name: "无路径（库名留空）", in: "mongo://127.0.0.1:27017"},
		{name: "参数名大小写混写", in: "mongo://h:27017/db?maxPoolSize=50&AppName=x"},
		{name: "前后空白被去掉", in: "  mongo://127.0.0.1:27017/db  "},

		{name: "空串", in: "", wantSub: "URI 不能为空"},
		{name: "非法 scheme", in: "http://127.0.0.1:27017/db", wantSub: "不支持的 scheme"},
		{name: "缺少主机", in: "mongo:///db", wantSub: "未指定主机"},
		{name: "SRV 不能写端口", in: "mongo+srv://cluster0.example.com:27017/db", wantSub: "不能指定端口"},
		{name: "主机列表有空项", in: "mongo://h1:27017,,h2:27017/db", wantSub: "空项"},
		// 端口写成非数字时，标准库的 url.Parse 会先一步拒绝
		// （invalid port ":abc" after host），所以这里拿到的是解析错误。
		{name: "端口非数字（标准库先拦）", in: "mongo://h1:abc/db", wantSub: "URI 解析失败"},
		// 但**空端口**（`h1:`）标准库是放行的，必须由本包自己拦 ——
		// 否则它会被当成"主机名的一部分"传给驱动，报出一个很难懂的错误。
		{name: "端口为空", in: "mongo://h1:/db", wantSub: "不是数字"},
		{name: "端口超范围", in: "mongo://h1:70000/db", wantSub: "超出范围"},
		{name: "端口为 0", in: "mongo://h1:0/db", wantSub: "超出范围"},
		{name: "库名里含斜杠", in: "mongo://h1:27017/a/b", wantSub: "不能是路径"},
		{name: "库名含空格", in: "mongo://h1:27017/a b", wantSub: "含非法字符"},
		{name: "参数转义非法", in: "mongo://h1:27017/db?x=%zz", wantSub: "转义非法"},
		{name: "参数重复", in: "mongo://h1:27017/db?a=1&a=2", wantSub: "重复出现"},
		{name: "参数大小写冲突", in: "mongo://h1:27017/db?maxPoolSize=1&MAXPOOLSIZE=2", wantSub: "大小写冲突"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info, err := parseURI(tc.in)
			if tc.wantSub == "" {
				if err != nil {
					t.Fatalf("期望解析成功，实际报错: %v", err)
				}
				if info == nil {
					t.Fatal("解析成功时不应返回 nil")
				}
				return
			}
			if err == nil {
				t.Fatalf("期望报错（%s），实际通过", tc.wantSub)
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("错误应可用 errors.Is(err, ErrInvalidConfig) 判定，实际: %v", err)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("错误信息应包含 %q，实际: %v", tc.wantSub, err)
			}
		})
	}
}

func TestParseURIFields(t *testing.T) {
	info, err := parseURI("mongo://user:pw@h1:27017,h2:27018/shop?replicaSet=rs0&maxPoolSize=50")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}

	if info.scheme != schemeMongoDB {
		t.Errorf("scheme 期望 %q，实际 %q", schemeMongoDB, info.scheme)
	}
	if info.dbName != "shop" {
		t.Errorf("dbName 期望 shop，实际 %q", info.dbName)
	}
	if got := info.hostsString(); got != "h1:27017,h2:27018" {
		t.Errorf("hostsString 期望 h1:27017,h2:27018，实际 %q", got)
	}
	if len(info.hosts) != 2 {
		t.Fatalf("hosts 期望 2 项，实际 %d 项: %v", len(info.hosts), info.hosts)
	}

	// MongoDB 的 URI 参数名**大小写不敏感**：URI 里写 maxPoolSize，
	// 查询时用任何大小写都必须命中（否则"配置写了不生效"会非常难查）。
	for _, key := range []string{"maxPoolSize", "maxpoolsize", "MAXPOOLSIZE", "MaxPoolSize"} {
		if v, ok := info.get(key); !ok || v != "50" {
			t.Errorf("get(%s) 期望 50，实际 %q ok=%v", key, v, ok)
		}
	}
	if !info.has("replicaSet") {
		t.Error("has(replicaSet) 期望 true")
	}
	if info.has("appName") {
		t.Error("未配置的参数 has() 应返回 false")
	}
}

func TestSplitHostPort(t *testing.T) {
	// 这个函数刻意没用 net.SplitHostPort —— 它要求端口必须存在，
	// 且对 `[::1]` 这类裸地址会直接报错。MongoDB 的 URI 允许主机不带端口，
	// 所以"没有端口"这一路必须自己处理。
	tests := []struct {
		in       string
		wantHost string
		wantPort string
		wantHas  bool
	}{
		{"h1:27017", "h1", "27017", true},
		{"h1", "h1", "", false},
		{"[::1]:27017", "[::1]", "27017", true},
		{"[::1]", "[::1]", "", false},
		{"a.b.c:27017", "a.b.c", "27017", true},
	}
	for _, tc := range tests {
		host, port, has := splitHostPort(tc.in)
		if host != tc.wantHost || port != tc.wantPort || has != tc.wantHas {
			t.Errorf("splitHostPort(%q) = (%q, %q, %v)，期望 (%q, %q, %v)",
				tc.in, host, port, has, tc.wantHost, tc.wantPort, tc.wantHas)
		}
	}
}

// ---------------------------------------------------------------------------
// URI 脱敏
// ---------------------------------------------------------------------------

func TestURISanitized(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		want     string   // 非空表示要求逐字节相等
		mustHide []string // 这些子串不得出现在结果里
	}{
		{
			name:     "userinfo 里的密码",
			in:       "mongo://user:s3cret@h1:27017/shop",
			want:     "mongo://user:***@h1:27017/shop",
			mustHide: []string{"s3cret"},
		},
		{
			// 没有口令时**逐字节原样返回**：日志里看到的就是配置里那一行，
			// 可以直接拿去 diff。
			name: "无口令时原样返回",
			in:   "mongo://h1:27017/shop?replicaSet=rs0",
			want: "mongo://h1:27017/shop?replicaSet=rs0",
		},
		{
			// 这是本包比 utils.SanitizeDSN 多覆盖的一处：
			// URI 里的口令不止 userinfo，证书私钥口令同样是口令。
			// 只抹 userinfo 的脱敏函数会把私钥口令原样写进日志。
			name:     "证书私钥口令",
			in:       "mongo://h1:27017/shop?tlsCertificateKeyFilePassword=pkpass",
			mustHide: []string{"pkpass"},
		},
		{
			name:     "两处口令同时存在",
			in:       "mongo://user:pw@h1:27017/shop?sslClientCertificateKeyPassword=pk2",
			mustHide: []string{"pw@", "pk2"},
		},
		{
			name:     "SRV 形式",
			in:       "mongo+srv://user:pw@cluster0.example.com/shop?retryWrites=true",
			mustHide: []string{"pw@"},
		},
		{
			name:     "用户名里有需转义的字符",
			in:       "mongo://u%40corp:pw@h1:27017/shop",
			mustHide: []string{"pw@"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info, err := parseURI(tc.in)
			if err != nil {
				t.Fatalf("解析失败: %v", err)
			}
			got := info.sanitized()

			if tc.want != "" && got != tc.want {
				t.Errorf("sanitized() = %q，期望 %q", got, tc.want)
			}
			for _, secret := range tc.mustHide {
				if strings.Contains(got, secret) {
					t.Errorf("脱敏结果里不应出现 %q，实际: %s", secret, got)
				}
			}
			// 脱敏不能只图"安全"到看不出连的是哪儿 ——
			// 那样日志就失去了排查价值。
			if !strings.Contains(got, "://") {
				t.Errorf("脱敏结果应保留 scheme，实际: %s", got)
			}
			if !strings.Contains(got, "h1:27017") && !strings.Contains(got, "cluster0.example.com") {
				t.Errorf("脱敏结果应保留主机，实际: %s", got)
			}
		})
	}
}

func TestURISanitizedIsIdempotent(t *testing.T) {
	// 脱敏结果再解析再脱敏应当稳定 —— 它是纯函数，不该越抹越花。
	info, err := parseURI("mongo://user:pw@h1:27017/shop")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	once := info.sanitized()

	info2, err := parseURI(once)
	if err != nil {
		t.Fatalf("脱敏结果应仍是合法 URI，实际报错: %v（结果: %s）", err, once)
	}
	if twice := info2.sanitized(); twice != once {
		t.Errorf("脱敏应幂等：第一次 %q，第二次 %q", once, twice)
	}
}

// ---------------------------------------------------------------------------
// 读偏好、写关注、日志级别
// ---------------------------------------------------------------------------

func TestParseReadPreference(t *testing.T) {
	valid := map[string]readpref.Mode{
		"primary":            readpref.PrimaryMode,
		"primaryPreferred":   readpref.PrimaryPreferredMode,
		"primary_preferred":  readpref.PrimaryPreferredMode,
		"primary-preferred":  readpref.PrimaryPreferredMode,
		"PRIMARYPREFERRED":   readpref.PrimaryPreferredMode,
		"secondary":          readpref.SecondaryMode,
		"secondaryPreferred": readpref.SecondaryPreferredMode,
		"nearest":            readpref.NearestMode,
	}
	for in, want := range valid {
		got, err := parseReadPreference(in)
		if err != nil {
			t.Errorf("parseReadPreference(%q) 期望成功，实际报错: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("parseReadPreference(%q) = %v，期望 %v", in, got, want)
		}
	}

	for _, in := range []string{"", "fastest", "primary nearest", "secondary2"} {
		if _, err := parseReadPreference(in); err == nil {
			t.Errorf("parseReadPreference(%q) 期望报错，实际通过", in)
		} else if !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("parseReadPreference(%q) 的错误应可判定为 ErrInvalidConfig，实际: %v", in, err)
		}
	}
}

func TestParseWriteConcernW(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    any
		wantErr bool
	}{
		{name: "数字 1", in: "1", want: 1},
		{name: "数字 0（不等待确认）", in: "0", want: 0},
		{name: "带正号的数字", in: "+2", want: 2},
		{name: "majority 是保留 tag set", in: "majority", want: "majority"},
		{name: "自定义 tag set", in: "dc1", want: "dc1"},
		{name: "负数非法", in: "-1", wantErr: true},
		{name: "含空白非法", in: "dc 1", wantErr: true},
		// 小数不是合法 w，但 parseIntStrict 会放行给 tag set 分支；
		// 服务端会拒绝它。这里把当前行为钉住，改动时能立刻看到。
		{name: "小数落入 tag set 分支", in: "1.5", want: "1.5"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseWriteConcernW(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("期望报错，实际得到 %#v", got)
				}
				if !errors.Is(err, ErrInvalidConfig) {
					t.Errorf("错误应可判定为 ErrInvalidConfig，实际: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("期望成功，实际报错: %v", err)
			}
			if got != tc.want {
				t.Errorf("parseWriteConcernW(%q) = %#v，期望 %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestBuildWriteConcern(t *testing.T) {
	// 两者都未配置 → nil，让 URI 或服务端默认值生效。
	// 返回一个"非 nil 的空结构体"是错的：那会把服务端默认的写关注覆盖掉。
	if wc := buildWriteConcern("", nil); wc != nil {
		t.Errorf("w 与 journal 都未配置时应返回 nil，实际 %#v", wc)
	}
	if wc := buildWriteConcern("  ", nil); wc != nil {
		t.Errorf("w 为空白时应返回 nil，实际 %#v", wc)
	}

	// 只配置 journal：W 必须保持 nil（而不是 0）——
	// W=nil 表示"不覆盖服务端默认"，W=0 表示"不等待任何确认"，两者天差地别。
	wc := buildWriteConcern("", boolPtr(false))
	if wc == nil {
		t.Fatal("只配置 journal 时应返回非 nil")
	}
	if wc.Journal == nil || *wc.Journal {
		t.Errorf("Journal 期望 false，实际 %v", wc.Journal)
	}
	if wc.W != nil {
		t.Errorf("未配置 w 时 W 应为 nil（交由服务端默认值），实际 %#v", wc.W)
	}

	// 只配置 w。
	wc = buildWriteConcern("majority", nil)
	if wc == nil || wc.W != "majority" {
		t.Errorf("期望 W=majority，实际 %#v", wc)
	}
	if wc.Journal != nil {
		t.Errorf("未配置 journal 时应为 nil，实际 %v", wc.Journal)
	}
}

func TestValidateLogLevel(t *testing.T) {
	for _, ok := range []string{"", "silent", "error", "warn", "info", "WARN", " Info "} {
		if err := validateLogLevel(ok); err != nil {
			t.Errorf("validateLogLevel(%q) 期望通过，实际: %v", ok, err)
		}
	}
	for _, bad := range []string{"verbose", "debug", "trace", "1"} {
		if err := validateLogLevel(bad); err == nil {
			t.Errorf("validateLogLevel(%q) 期望报错，实际通过", bad)
		}
	}
}

func TestParseIntStrict(t *testing.T) {
	// 这个函数不直接用 strconv.Atoi：它要回答的是
	// "用户写的这串东西是不是我理解的数字"。于是 "1.5"、"1e3"、" 1 "
	// 都必须走到 tag set 分支，而不是被宽松地解析成 1。
	tests := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"0", 0, false},
		{"7", 7, false},
		{"+7", 7, false},
		{"-7", -7, false},
		{"123456", 123456, false},
		{"", 0, true},
		{"+", 0, true},
		{"-", 0, true},
		{"1.5", 0, true},
		{"1e3", 0, true},
		{" 1", 0, true},
		{"1 ", 0, true},
		{"abc", 0, true},
	}
	for _, tc := range tests {
		got, err := parseIntStrict(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseIntStrict(%q) 期望报错，实际得到 %d", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseIntStrict(%q) 期望成功，实际报错: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseIntStrict(%q) = %d，期望 %d", tc.in, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// 哨兵错误与 Open 的失败路径（不需要数据库）
// ---------------------------------------------------------------------------

func TestSentinelErrorsAreDistinct(t *testing.T) {
	// 四个哨兵必须互不命中，否则 errors.Is 会串味：
	// 例如把 ErrNotConnected 当成 ErrClosed，调用方的重连逻辑就会停止工作。
	sentinels := map[string]error{
		"ErrInvalidConfig": ErrInvalidConfig,
		"ErrConnect":       ErrConnect,
		"ErrClosed":        ErrClosed,
		"ErrNotConnected":  ErrNotConnected,
	}
	for nameA, a := range sentinels {
		if a == nil {
			t.Errorf("%s 不应为 nil", nameA)
			continue
		}
		if !errors.Is(a, a) {
			t.Errorf("%s 应能被 errors.Is 自身命中", nameA)
		}
		for nameB, b := range sentinels {
			if nameA != nameB && errors.Is(a, b) {
				t.Errorf("%s 不应命中 %s（哨兵串味）", nameA, nameB)
			}
		}
	}
}

func TestOpenRejectsInvalidConfig(t *testing.T) {
	// 配置非法时 Open 必须**不建连**就返回，且错误归入 ErrInvalidConfig。
	tests := []struct {
		name string
		cfg  Config
	}{
		{"空 URI", Config{}},
		{"scheme 不支持", Config{URI: "redis://h:6379"}},
		{"字段非法", Config{URI: validURI, MaxPoolSize: -5}},
		{"v2 已删参数", Config{URI: "mongo://h:27017/db?socketTimeoutMS=100"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, err := Open(tc.cfg)
			if err == nil {
				_ = client.Close()
				t.Fatal("期望 Open 失败，实际成功")
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("错误应可判定为 ErrInvalidConfig，实际: %v", err)
			}
			if client != nil {
				t.Error("失败时不应返回客户端")
			}
			// 配置错误被归成"连不上"会让运维去查网络，方向就错了。
			if errors.Is(err, ErrConnect) {
				t.Errorf("配置错误不应归入 ErrConnect，实际: %v", err)
			}
		})
	}
}

func TestOpenFailsFastOnUnreachableHost(t *testing.T) {
	// 这是本包相对驱动最重要的行为差异。
	//
	// mongo.Connect **不验证可达性**（驱动文档原话：does not validate that
	// the MongoDB deployment is reachable），而 ServerSelectionTimeout 默认 30s。
	// 若直接用驱动默认值做探活，"连不上"要挂 30 秒才返回 ——
	// 乘上启动重试次数就是几分钟的启动等待。
	//
	// 本包用 DialProbeTimeout 给每次尝试封顶，于是
	// 最坏耗时 = DialAttempts × DialProbeTimeout，可算可控。
	//
	// 这里连一个**确定没人监听**的端口：1 号端口属于特权端口，
	// 测试机上不会有进程监听它。
	start := time.Now()
	client, err := Open(Config{
		URI:              "mongo://127.0.0.1:1/gointra_test",
		DialAttempts:     1,
		DialProbeTimeout: 300 * time.Millisecond,
		DialBackoff:      10 * time.Millisecond,
		DialMaxBackoff:   20 * time.Millisecond,
	})
	elapsed := time.Since(start)

	if err == nil {
		_ = client.Close()
		t.Fatal("连不上时 Open 应当失败，实际成功")
	}
	if client != nil {
		t.Error("失败时不应返回客户端")
	}
	if !errors.Is(err, ErrConnect) {
		t.Errorf("连不上应归入 ErrConnect，实际: %v", err)
	}
	if errors.Is(err, ErrInvalidConfig) {
		t.Errorf("这是连接问题、不是配置问题，不应归入 ErrInvalidConfig: %v", err)
	}
	// 给足余量：允许握手与退避，但绝不能接近驱动默认的 30s。
	if elapsed > 10*time.Second {
		t.Errorf("Open 耗时 %v，说明 DialProbeTimeout 没有生效"+
			"（预期远小于驱动默认的 ServerSelectionTimeout=30s）", elapsed)
	}
}

func TestOpenDialAttemptsRetries(t *testing.T) {
	// 验证重试**真的发生了**：不比较绝对时间（CI 上不稳定），
	// 而是比较"1 次尝试"与"3 次尝试"的相对耗时 ——
	// 每次尝试都会消耗一次 DialProbeTimeout + 一次退避，
	// 所以 3 次的耗时必须显著更大。
	measure := func(attempts int) time.Duration {
		t.Helper()
		start := time.Now()
		client, err := Open(Config{
			URI:              "mongo://127.0.0.1:1/gointra_test",
			DialAttempts:     attempts,
			DialProbeTimeout: 150 * time.Millisecond,
			DialBackoff:      150 * time.Millisecond,
			DialMaxBackoff:   150 * time.Millisecond,
		})
		if err == nil {
			_ = client.Close()
			t.Fatalf("DialAttempts=%d 时应当失败", attempts)
		}
		return time.Since(start)
	}

	one := measure(1)
	three := measure(3)

	if three <= one {
		t.Errorf("DialAttempts=3 的耗时(%v)应大于 DialAttempts=1(%v)，"+
			"说明重试没有真正发生", three, one)
	}
	// 上界：3 × (150ms 探活 + 150ms 退避) 约 900ms，留足余量。
	if three > 15*time.Second {
		t.Errorf("DialAttempts=3 耗时 %v 过长，退避或探活超时未受控", three)
	}
}

func TestDialRespectsCancelledContext(t *testing.T) {
	// 无限重试（DialAttempts<0）不能变成"永远卡住"。
	// 直接调 dial 并传一个**已取消**的 ctx：它必须立刻返回，
	// 而不是先做完一轮尝试再说。
	cfg := mustReady(t, Config{
		URI:              "mongo://127.0.0.1:1/gointra_test",
		DialAttempts:     -1, // 无限重试
		DialProbeTimeout: 50 * time.Millisecond,
		DialBackoff:      10 * time.Millisecond,
		DialMaxBackoff:   10 * time.Millisecond,
	})
	opts, _, err := cfg.clientOptions()
	if err != nil {
		t.Fatalf("clientOptions() 报错: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立刻取消

	m := &MongoDB{cfg: cfg, log: logger.NewPlog("MongoDB-Test"), ctx: ctx}
	m.cancel = func() {}

	start := time.Now()
	err = m.dial(ctx, opts)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("ctx 已取消时应返回错误")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("期望 context.Canceled，实际: %v", err)
	}
	if elapsed > time.Second {
		t.Errorf("dial 耗时 %v —— ctx 已取消却仍在重试", elapsed)
	}
}

func TestCloseWithoutOpen(t *testing.T) {
	// 零值 *MongoDB 上的操作必须安全：让 `defer c.Close()` 这类写法
	// 在初始化失败、c 为 nil 或零值的路径上也不会炸。
	var m MongoDB
	if err := m.Close(); err != nil {
		t.Errorf("零值 Close 应返回 nil，实际: %v", err)
	}
	if !m.Closed() {
		t.Error("Close 之后 Closed() 应为 true")
	}
	if m.Client() != nil {
		t.Error("Close 之后 Client() 应为 nil")
	}
	if m.Database("x") != nil {
		t.Error("Close 之后 Database() 应为 nil")
	}
	if m.Healthy() {
		t.Error("Close 之后 Healthy() 应为 false")
	}
	if err := m.HealthCheck(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("Close 之后 HealthCheck 应返回 ErrClosed，实际: %v", err)
	}
	if err := m.Reconnect(); !errors.Is(err, ErrClosed) {
		t.Errorf("Close 之后 Reconnect 应返回 ErrClosed，实际: %v", err)
	}
	// 幂等：停机的 defer 链里可能被调到两次。
	if err := m.Close(); err != nil {
		t.Errorf("第二次 Close 应返回 nil，实际: %v", err)
	}
}

// ---------------------------------------------------------------------------
// metrics 接入的装配（不涉及真实数据库）
// ---------------------------------------------------------------------------

func TestObserverNotRequired(t *testing.T) {
	// 无 Observer 且 LogLevel=silent 时 commandMonitor 必须返回 nil。
	//
	// 这是本包"零观测开销"的开关：只要注册了 Started 回调，驱动就必须
	// 把命令文档复制并序列化成 bson.Raw（见驱动的 redactStartedInformationCmd），
	// 那是一次与命令大小成正比的分配 —— 在热路径上不能白花。
	cfg := mustReady(t, Config{URI: validURI, LogLevel: logLevelSilent})
	m := &MongoDB{cfg: cfg}
	if cm := m.commandMonitor(); cm != nil {
		t.Error("无 Observer 且 silent 时不应注册命令监听器")
	}

	// 任一条件成立就要挂监听器。
	cases := []struct {
		name string
		cfg  Config
	}{
		{
			name: "有 Observer",
			cfg: Config{
				URI:      validURI,
				LogLevel: logLevelSilent,
				Observer: metrics.ObserverFunc(func(metrics.Event) {}),
			},
		},
		{name: "LogLevel=warn", cfg: Config{URI: validURI, LogLevel: logLevelWarn}},
		{name: "LogLevel=error", cfg: Config{URI: validURI, LogLevel: logLevelError}},
		{name: "LogLevel 为空（按 info 生效）", cfg: Config{URI: validURI}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ready := mustReady(t, tc.cfg)
			got := (&MongoDB{cfg: ready}).commandMonitor()
			if got == nil {
				t.Fatal("应注册命令监听器")
			}
			// 监听器要带上实例名，否则多实例部署时指标会叠成一条曲线。
			if got.instance != ready.Name {
				t.Errorf("instance 期望 %q，实际 %q", ready.Name, got.instance)
			}
		})
	}
}

func TestPoolStatsShape(t *testing.T) {
	// 没有流量时快照必须是"干净的全零 + 正确的实例名与上限"，
	// 而不是 nil 或负值。
	cfg := mustReady(t, Config{URI: validURI, Name: "order", MaxPoolSize: 42})
	m := &MongoDB{cfg: cfg}

	got := m.PoolStats()
	if got.Component != metrics.ComponentMongoDB {
		t.Errorf("Component 期望 %q，实际 %q", metrics.ComponentMongoDB, got.Component)
	}
	if got.Instance != "order" {
		t.Errorf("Instance 期望 order，实际 %q", got.Instance)
	}
	if got.MaxOpen != 42 {
		t.Errorf("MaxOpen 期望 42（每台服务器的上限），实际 %d", got.MaxOpen)
	}
	if got.Open != 0 || got.InUse != 0 || got.Idle != 0 || got.Pending != 0 {
		t.Errorf("无流量时各水位应为 0，实际 %+v", got)
	}
}
