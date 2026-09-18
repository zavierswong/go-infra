package rabbitmq

import (
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestAmqpURI 覆盖旧版 DSN 拼接的三个真实缺陷。
//
// 旧实现是 fmt.Sprintf("amqp%s://%s:%s@%s:%d%s", ...)，实测：
//   - VHost 写成 "prod"（不带前导斜杠）→ "...:5672prod"，URL 直接解析失败；
//   - 密码含 "/" 或 ":"                 → URL 解析失败；
//   - 用户名/密码含特殊字符              → 只能靠运气，@ 会被当成最后一个分隔符。
func TestAmqpURI(t *testing.T) {
	cases := []struct {
		name       string
		cfg        Config
		wantVHost  string
		wantUser   string
		wantPass   string
		wantScheme string
	}{
		{
			name:       "默认 vhost",
			cfg:        Config{Host: "rmq.internal", Port: 5672, VHost: "/", Username: "guest", Password: "guest"},
			wantVHost:  "",
			wantUser:   "guest",
			wantPass:   "guest",
			wantScheme: "amqp",
		},
		{
			name:       "vhost 不带前导斜杠（旧版会拼坏端口）",
			cfg:        Config{Host: "rmq.internal", Port: 5672, VHost: "prod", Username: "app", Password: "p"},
			wantVHost:  "prod",
			wantUser:   "app",
			wantPass:   "p",
			wantScheme: "amqp",
		},
		{
			name:       "vhost 带前导斜杠",
			cfg:        Config{Host: "rmq.internal", Port: 5672, VHost: "/app_vhost", Username: "app", Password: "p"},
			wantVHost:  "app_vhost",
			wantUser:   "app",
			wantPass:   "p",
			wantScheme: "amqp",
		},
		{
			name:       "密码含 @ 与 / 与 :",
			cfg:        Config{Host: "rmq.internal", Port: 5672, VHost: "/", Username: "app", Password: `p@ss/w0:rd`},
			wantVHost:  "",
			wantUser:   "app",
			wantPass:   `p@ss/w0:rd`,
			wantScheme: "amqp",
		},
		{
			name:       "TLS 时 scheme 为 amqps",
			cfg:        Config{Host: "rmq.internal", Port: 5671, VHost: "/", Username: "app", Password: "p", Tls: TLSConfig{Enable: true}},
			wantVHost:  "",
			wantUser:   "app",
			wantPass:   "p",
			wantScheme: "amqps",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.cfg.normalize().amqpURI()

			u, err := url.Parse(raw)
			if err != nil {
				t.Fatalf("生成的 URI 无法解析: %v\nURI: %s", err, raw)
			}
			if u.Scheme != tc.wantScheme {
				t.Errorf("scheme 期望 %q，实际 %q", tc.wantScheme, u.Scheme)
			}
			if got := u.Port(); got != "5672" && tc.wantScheme == "amqp" {
				t.Errorf("端口被拼坏: 期望 5672，实际 %q（URI=%s）", got, raw)
			}
			if got := strings.TrimPrefix(u.Path, "/"); got != tc.wantVHost {
				t.Errorf("vhost 期望 %q，实际 %q", tc.wantVHost, got)
			}
			if got := u.User.Username(); got != tc.wantUser {
				t.Errorf("username 期望 %q，实际 %q", tc.wantUser, got)
			}
			if got, _ := u.User.Password(); got != tc.wantPass {
				t.Errorf("password 期望 %q，实际 %q", tc.wantPass, got)
			}
		})
	}
}

// TestConfigValidate 覆盖拓扑校验：错误的声明应当在启动时就被拒绝，
// 而不是等到运行时收到 broker 的 406。
func TestConfigValidate(t *testing.T) {
	base := func() Config {
		return Config{
			Host: "h", Port: 5672, Username: "u",
			Exchanges: []Exchange{{Name: "ex", Kind: ExchangeDirect}},
			Queues:    []Queue{{Name: "q"}},
			Bindings:  []Binding{{Queue: "q", Exchange: "ex", RoutingKey: "rk"}},
		}
	}

	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"正常配置", func(*Config) {}, ""},
		{"host 为空", func(c *Config) { c.Host = "" }, "host"},
		{"端口越界", func(c *Config) { c.Port = 70000 }, "port"},
		{"用户名含非法字符", func(c *Config) { c.Username = "a:b" }, "username"},
		{"交换机类型非法", func(c *Config) { c.Exchanges[0].Kind = "unknown" }, "kind"},
		{"交换机名为空", func(c *Config) { c.Exchanges[0].Name = "" }, "exchanges[0].name"},
		{"交换机重复声明", func(c *Config) { c.Exchanges = append(c.Exchanges, c.Exchanges[0]) }, "重复"},
		{"队列名为空", func(c *Config) { c.Queues[0].Name = "" }, "queues[0].name"},
		{"绑定引用未声明队列", func(c *Config) { c.Bindings[0].Queue = "ghost" }, "未声明的队列"},
		{"绑定引用未声明交换机", func(c *Config) { c.Bindings[0].Exchange = "ghost" }, "未声明的交换机"},
		{
			"fanout 绑定带 routing key",
			func(c *Config) {
				c.Exchanges[0].Kind = ExchangeFanout
				c.Bindings[0].RoutingKey = "should-be-empty"
			},
			"routing_key 必须为空",
		},
		{
			"headers 绑定缺 x-match",
			func(c *Config) { c.Exchanges[0].Kind = ExchangeHeaders },
			"x-match",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mutate(&cfg)
			err := cfg.normalize().validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("期望校验通过，实际报错: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("期望报错包含 %q，实际通过校验", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("错误信息应包含 %q，实际: %v", tc.wantErr, err)
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("应可用 errors.Is(err, ErrInvalidConfig) 判定，实际: %v", err)
			}
		})
	}
}

// TestConfigNormalize 覆盖默认值补齐与延迟拓扑展开。
func TestConfigNormalize(t *testing.T) {
	cfg := Config{Host: "h", Username: "u"}.normalize()

	if cfg.Port != defaultPort {
		t.Errorf("Port 默认值期望 %d，实际 %d", defaultPort, cfg.Port)
	}
	if cfg.VHost != "/" {
		t.Errorf("VHost 默认值期望 \"/\"，实际 %q", cfg.VHost)
	}
	if cfg.DialAttempts != defaultDialAttempts {
		t.Errorf("DialAttempts 默认值期望 %d，实际 %d", defaultDialAttempts, cfg.DialAttempts)
	}
	if cfg.Prefetch != defaultPrefetch {
		t.Errorf("Prefetch 默认值期望 %d，实际 %d", defaultPrefetch, cfg.Prefetch)
	}
	if cfg.Heartbeat != defaultHeartbeat {
		t.Errorf("Heartbeat 默认值期望 %s，实际 %s", defaultHeartbeat, cfg.Heartbeat)
	}

	delayCfg := Config{
		Host: "h", Username: "u",
		Exchanges: []Exchange{{Name: "order", Kind: ExchangeTopic}},
		Queues:    []Queue{{Name: "order.q"}},
		Bindings:  []Binding{{Queue: "order.q", Exchange: "order", RoutingKey: "order.#"}},
		Delay: &DelayConfig{
			Prefix:     "sms",
			Delays:     []time.Duration{5 * time.Second, 30 * time.Second},
			DeadLetter: DeadLetter{Exchange: "order", RoutingKey: "order.sms"},
		},
	}.normalize()

	// 延迟拓扑被并入，且总数符合预期：1 业务交换机 + 1 延迟交换机
	if len(delayCfg.Exchanges) != 2 {
		t.Fatalf("交换机数量期望 2，实际 %d: %+v", len(delayCfg.Exchanges), delayCfg.Exchanges)
	}
	if len(delayCfg.Queues) != 3 { // 业务队列 + 2 个延迟档位
		t.Fatalf("队列数量期望 3，实际 %d", len(delayCfg.Queues))
	}
	if len(delayCfg.Bindings) != 3 {
		t.Fatalf("绑定数量期望 3，实际 %d", len(delayCfg.Bindings))
	}

	// 延迟队列必须带 TTL 与 DLX，否则消息到期会直接丢失
	var found bool
	for _, q := range delayCfg.Queues {
		if q.Name != "sms.delay.5000" {
			continue
		}
		found = true
		if got := q.Args["x-message-ttl"]; got != int64(5000) {
			t.Errorf("延迟队列 x-message-ttl 期望 5000，实际 %v", got)
		}
		if got := q.Args["x-dead-letter-exchange"]; got != "order" {
			t.Errorf("延迟队列 x-dead-letter-exchange 期望 order，实际 %v", got)
		}
		if got := q.Args["x-dead-letter-routing-key"]; got != "order.sms" {
			t.Errorf("延迟队列 x-dead-letter-routing-key 期望 order.sms，实际 %v", got)
		}
	}
	if !found {
		t.Errorf("未展开出 sms.delay.5000 队列: %+v", delayCfg.Queues)
	}

	if err := delayCfg.validate(); err != nil {
		t.Fatalf("展开后的配置应当合法，实际: %v", err)
	}
}

// TestPublishOptionValidation 选项校验：必须指定投递目标。
func TestPublishOptionValidation(t *testing.T) {
	c := &Client{}
	// 未指定投递目标时应当在触碰网络之前就报错
	err := c.Publish(t.Context(), []byte("x"))
	if err == nil {
		t.Fatal("未指定投递目标时应报错")
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("应可用 errors.Is(err, ErrInvalidConfig) 判定，实际: %v", err)
	}

	for _, opt := range []PublishOption{ToQueue("q"), ToExchange("ex", "rk")} {
		o := newPublishOptions()
		opt(&o)
		if err := o.validate(); err != nil {
			t.Errorf("选项应当合法，实际报错: %v", err)
		}
	}
}

// TestDelayRoutingKey 延迟档位与 routing key 的映射。
func TestDelayRoutingKey(t *testing.T) {
	cases := map[time.Duration]string{
		0:               "0",
		time.Second:     "1000",
		5 * time.Second: "5000",
		2 * time.Minute: "120000",
	}
	for d, want := range cases {
		if got := DelayRoutingKey(d); got != want {
			t.Errorf("DelayRoutingKey(%s) 期望 %q，实际 %q", d, want, got)
		}
	}
}

// TestQueueArgsBuilders 队列参数构造器的键名必须与 RabbitMQ 约定一致，
// 拼错一个键意味着该能力静默失效。
func TestQueueArgsBuilders(t *testing.T) {
	dlx := DLXArgs(DeadLetter{Exchange: "dead", RoutingKey: "dead.q"})
	if dlx["x-dead-letter-exchange"] != "dead" || dlx["x-dead-letter-routing-key"] != "dead.q" {
		t.Errorf("DLXArgs 结果异常: %v", dlx)
	}

	// Exchange 为空时必须显式写成空串而不是省略：
	// broker 对"有 routing key 但没有 dlx"会直接拒绝声明（routing_key_but_no_dlx_defined）。
	defaultDlx := DLXArgs(DeadLetter{RoutingKey: "dead.q"})
	if v, ok := defaultDlx["x-dead-letter-exchange"]; !ok || v != "" {
		t.Errorf("DeadLetter.Exchange 为空时应显式写入空串，实际: %#v", defaultDlx)
	}
	if got := MessageTTLArgs(3 * time.Second)["x-message-ttl"]; got != int64(3000) {
		t.Errorf("MessageTTLArgs 期望 3000，实际 %v", got)
	}
	if got := QuorumArgs()["x-queue-type"]; got != "quorum" {
		t.Errorf("QuorumArgs 期望 quorum，实际 %v", got)
	}
	if got := PriorityArgs(9)["x-max-priority"]; got != int64(9) {
		t.Errorf("PriorityArgs 期望 9，实际 %v", got)
	}
	if got := LazyArgs()["x-queue-mode"]; got != "lazy" {
		t.Errorf("LazyArgs 期望 lazy，实际 %v", got)
	}
}

// TestConfigStructsHaveMapstructureTags 守住一个容易漏的配置缺陷：
// 导出字段一旦缺 mapstructure tag，用 viper/yaml 配置时该字段就**静默失效** ——
// 不报错、不生效，只是永远等于零值。
//
// 本包就踩过这个坑：DelayConfig 与 DeadLetter 曾漏掉全部 tag，
// 结果 YAML 里的 dead_letter / exchange_kind 根本映射不进去，
// 而 Delays 因为大小写不敏感匹配侥幸能用，更难被发现。
//
// 顺带约束 tag 一律小写下划线风格，与 Config/Exchange/Queue/Binding 保持一致。
func TestConfigStructsHaveMapstructureTags(t *testing.T) {
	// 需要递归检查的配置结构。
	roots := []reflect.Type{
		reflect.TypeOf(Config{}),
		reflect.TypeOf(Exchange{}),
		reflect.TypeOf(Queue{}),
		reflect.TypeOf(Binding{}),
		reflect.TypeOf(TLSConfig{}),
		reflect.TypeOf(DeadLetter{}),
		reflect.TypeOf(DelayConfig{}),
	}

	seen := map[reflect.Type]bool{}

	var check func(rt reflect.Type, path string)
	check = func(rt reflect.Type, path string) {
		if seen[rt] {
			return
		}
		seen[rt] = true

		tags := make(map[string]string, rt.NumField())
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			if !f.IsExported() {
				continue
			}

			where := path + "." + f.Name
			tag := f.Tag.Get("mapstructure")
			if tag == "" {
				t.Errorf("%s 缺少 mapstructure tag：用配置文件时该字段会静默失效", where)
				continue
			}
			// "-" 是 mapstructure 的显式忽略标记，语义与"拼错的 tag"正好相反：
			// 前者是不打算从配置读，后者是想读却读不到。不能一起判。
			if tag == "-" {
				continue
			}
			if tag != strings.ToLower(tag) || strings.ContainsAny(tag, "- ") {
				t.Errorf("%s 的 tag %q 不是小写下划线风格", where, tag)
			}
			if prev, dup := tags[tag]; dup {
				t.Errorf("%s 的 tag %q 与字段 %s 重复", where, tag, prev)
			}
			tags[tag] = f.Name

			// 嵌套的结构体配置继续往下查（跳过 amqp.Table 这类 map 类型）。
			ft := f.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct && ft != reflect.TypeOf(time.Time{}) {
				check(ft, where)
			}
		}
	}

	for _, rt := range roots {
		check(rt, rt.Name())
	}
}
