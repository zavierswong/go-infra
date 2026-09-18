package redis

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/zavierswong/go-infra/metrics"
)

// Config 是 Redis 连接与**连接池**配置。
//
// 所有 time.Duration 字段都接受 "300ms" / "30s" / "2m" 这类字符串
// （viper 的默认解码器已包含 StringToTimeDurationHookFunc）。
//
// 单位提醒：本包的时长字段一律是 time.Duration。若把旧的毫秒/秒整数直接写进来
// （如 read_timeout: 3000），validate 会直接报错拦下，不会静默变成不可用的配置。
type Config struct {
	// ------------------------------------------------------------------
	// 连接
	// ------------------------------------------------------------------

	// Addr 是 host:port，必填（如 "127.0.0.1:6379"）。
	Addr string `mapstructure:"addr"`

	// Username 是 Redis 6+ ACL 用户名。仅有 requirepass 时留空即可。
	Username string `mapstructure:"username"`

	// Password 是密码（对应 ACL 用户的口令）。无密码留空。
	Password string `mapstructure:"password"`

	// DB 是库编号。注意：**哨兵/集群模式下必须为 0**。
	DB int `mapstructure:"db"`

	// ClientName 会通过 CLIENT SETNAME 写给服务端，
	// 便于在 `CLIENT LIST` / `redis-cli --stat` 里分辨连接来自哪个服务。
	ClientName string `mapstructure:"client_name"`

	// Protocol 是 RESP 协议版本：0 表示使用默认（RESP3）。
	// 老版本服务端不支持 RESP3 时设为 2。
	Protocol int `mapstructure:"protocol"`

	// TLS 是可选的安全连接配置。
	TLS TLSConfig `mapstructure:"tls"`

	// ------------------------------------------------------------------
	// 标识与可观测性
	// ------------------------------------------------------------------

	// Name 是实例名，用于区分同一个进程里的多个 Redis 连接
	// （缓存、队列、分布式锁常常各用一套）。
	//
	// 与 ClientName 的区别：ClientName 是写给**服务端**看的（CLIENT LIST 里显示），
	// Name 是写给**监控**看的（metrics.Event.Instance）。两者可以不同：
	// 前者通常带进程/主机名，后者应当是稳定的业务名。
	Name string `mapstructure:"name"`

	// Observer 接收每个命令结束后的 metrics.Event。
	//
	// 为 nil 时不会注册 metrics 钩子，热路径上没有任何额外开销。
	// 详见 github.com/zavierswong/go-infra/metrics 与 README 的「metrics 暴露」章节。
	Observer metrics.Observer `mapstructure:"-"`

	// ------------------------------------------------------------------
	// 连接池：吞吐与延迟的主要抓手
	// ------------------------------------------------------------------

	// PoolSize 是连接池的**基准**连接数。
	//
	// ⚠️ 注意它不是硬上限：go-redis 在池不够用时仍会继续新建连接，
	// 想真正封顶请用 MaxActiveConns。
	// 0 表示默认 GOMAXPROCS × 10。
	PoolSize int `mapstructure:"pool_size"`

	// MinIdleConns 是**始终保持**的最小空闲连接数。
	//
	// 这是本包最推荐调整的性能参数：默认 0 意味着空闲连接会被回收，
	// 下一波流量到来时每个请求都要先花一次 TCP + 认证的时间建连，
	// 表现为 P99 毛刺。建连越慢的链路（跨可用区、带 TLS、有代理）收益越明显。
	// 建议设为 PoolSize 的 1/4 ~ 1/2，例如 20。
	MinIdleConns int `mapstructure:"min_idle_conns"`

	// MaxIdleConns 是空闲连接数的上限，超出部分会被关闭。
	// 0 表示不限制（由 PoolSize / MaxActiveConns 间接约束）。
	MaxIdleConns int `mapstructure:"max_idle_conns"`

	// MaxActiveConns 是连接池的**硬上限**（同时存在的连接总数）。
	//
	// 池满后新的取连接操作会阻塞，直到有连接归还或超过 PoolTimeout。
	// 0 表示不限制。多实例部署时建议显式设置，以保证
	// 「实例数 × MaxActiveConns < Redis maxclients」。
	MaxActiveConns int `mapstructure:"max_active_conns"`

	// PoolTimeout 是池满时等待可用连接的时长，超时返回错误。
	//
	// 比起无限排队，**快速失败**通常是更好的选择：让上游的限流/降级
	// 尽早介入，而不是把请求堆在连接池里等到全线超时。
	// 0 表示默认 ReadTimeout + 1s。
	PoolTimeout time.Duration `mapstructure:"pool_timeout"`

	// PoolFIFO 决定取连接的顺序：false 为 LIFO（默认，复用最近的连接，
	// 对 CPU 缓存更友好），true 为 FIFO（更快回收空闲连接，池子占用更小）。
	PoolFIFO bool `mapstructure:"pool_fifo"`

	// ConnMaxIdleTime 是连接的最大空闲时间，超过后被关闭。
	// 应小于服务端的 timeout 配置，避免复用到已被服务端关闭的连接。
	// 0 表示默认 30m，-1 表示关闭该检查。
	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time"`

	// ConnMaxLifetime 是连接的最大存活时间。
	//
	// go-redis 的默认值是 0，即**永不过期** —— 这在主从切换、代理重启之后
	// 会留下指向旧节点的连接。本包默认 1h，配合 Jitter 让连接错峰重建。
	// 0 表示默认 1h，负值表示不过期。
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`

	// ConnMaxLifetimeJitter 给 ConnMaxLifetime 加随机抖动（±该值），
	// 避免所有连接在同一时刻集体过期造成瞬时建连风暴。
	// 0 表示默认 ConnMaxLifetime 的 1/10。
	ConnMaxLifetimeJitter time.Duration `mapstructure:"conn_max_lifetime_jitter"`

	// ------------------------------------------------------------------
	// 超时
	// ------------------------------------------------------------------

	// DialTimeout 是建连超时，0 表示默认 5s。
	DialTimeout time.Duration `mapstructure:"dial_timeout"`

	// ReadTimeout / WriteTimeout 是单条命令的读写超时。
	// ReadTimeout 为 -1 表示不超时、-2 表示完全不用 SetReadDeadline。
	// 0 表示默认 3s。
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`

	// DisableContextTimeout 为 true 时，客户端**不尊重**调用方传入的 ctx 超时与截止时间，
	// 只按 ReadTimeout 判定 —— 这是 go-redis 的原始默认行为。
	//
	// 本包默认 false，也就是默认**尊重 ctx**（ContextTimeoutEnabled=true）。
	// 理由：Go 使用者普遍预期"ctx 取消或超时了就立刻返回"，否则下游早就超时了，
	// 调用方还在等 Redis，白白占着一个连接和一个协程。
	//
	// 这里刻意用了 Disable 前缀而不是 ContextTimeout：
	// bool 的零值是 false，命名为「正向」就意味着默认值只能是"关闭"，
	// 想在注释里写"默认开启"就必然出现注释与实现不符（本包第一版就踩了这个坑）。
	// 标准库的 DisableKeepAlives / InsecureSkipVerify 也是同样的命名思路。
	//
	// 若你的调用链习惯用一个带短 deadline 的 ctx 兼做非 Redis 的工作，
	// 并希望 Redis 操作不受它影响，把它设为 true 即可回到 go-redis 的原行为。
	DisableContextTimeout bool `mapstructure:"disable_context_timeout"`

	// ------------------------------------------------------------------
	// 重试
	// ------------------------------------------------------------------

	// MaxRetries 是单条命令的最大重试次数。
	//
	//	go-redis 语义：0 表示默认 3 次；**-1 表示禁用重试**。
	//
	// 旧版这里写的是 -1，注释却是"最大重试次数" —— 实际效果是把自动重试
	// 关掉了，网络抖动时命令直接失败返回业务，连带的 MinRetryBackoff /
	// MaxRetryBackoff 也全部空转。这是本包修正的核心问题之一。
	// 0 表示默认 3 次。
	MaxRetries int `mapstructure:"max_retries"`

	// MinRetryBackoff / MaxRetryBackoff 是重试退避区间，0 表示默认 8ms / 512ms，
	// -1 表示禁用退避。
	MinRetryBackoff time.Duration `mapstructure:"min_retry_backoff"`
	MaxRetryBackoff time.Duration `mapstructure:"max_retry_backoff"`

	// ------------------------------------------------------------------
	// 日志
	// ------------------------------------------------------------------

	// LogSlowThreshold 大于 0 时，耗时超过该阈值的命令会以 Warn 级别记录。
	// 0 表示只记录失败的命令。
	LogSlowThreshold time.Duration `mapstructure:"log_slow_threshold"`

	// LogCommands 为 true 时会记录**每一条**命令（Debug 级别），仅用于排查问题，
	// 高 QPS 下会显著增加日志量。
	LogCommands bool `mapstructure:"log_commands"`
}

// TLSConfig 是 Redis 的 TLS 配置。
type TLSConfig struct {
	Enable bool `mapstructure:"enable"`

	// InsecureSkipVerify 跳过证书校验，仅用于自签名证书的测试环境。
	InsecureSkipVerify bool `mapstructure:"insecure_skip_verify"`

	// ServerName 用于 SNI 与证书校验，留空则取 Addr 的 host 部分。
	ServerName string `mapstructure:"server_name"`

	// CAFile 是 PEM 格式的根证书；留空则使用系统信任链。
	CAFile string `mapstructure:"ca_file"`

	// CertFile / KeyFile 是可选的双向认证客户端证书。
	CertFile string `mapstructure:"cert_file"`
	KeyFile  string `mapstructure:"key_file"`
}

// 默认值。集中在这里，便于 README 与测试对齐。
const (
	defaultDialTimeout     = 5 * time.Second
	defaultReadTimeout     = 3 * time.Second
	defaultWriteTimeout    = 3 * time.Second
	defaultConnMaxIdleTime = 30 * time.Minute
	defaultConnMaxLifetime = time.Hour
	defaultMaxRetries      = 3
	defaultMinRetryBackoff = 8 * time.Millisecond
	defaultMaxRetryBackoff = 512 * time.Millisecond

	// minSaneDuration 是"单位哨兵"的下限：正值却小于 1ms 的时长几乎一定是单位写错。
	// redis 的退避只有毫秒级，所以这里的门槛比 mysql 更低。
	minSaneDuration = time.Millisecond
)

// defaultPoolSize 与 go-redis 保持一致：GOMAXPROCS × 10。
func defaultPoolSize() int { return runtime.GOMAXPROCS(0) * 10 }

// ready 校验原始配置并补齐默认值。这是初始化时唯一应该走的入口。
//
// 顺序很关键：**先校验，再 normalize**。反过来的话，负数会在 normalize 阶段
// 被当成"未设置"替换成默认值，非法输入就被静默吞掉了。
func (c Config) ready() (Config, error) {
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c.normalize(), nil
}

// normalize 补齐默认值并修正互相矛盾的取值。
func (c Config) normalize() Config {
	if c.Protocol < 0 {
		c.Protocol = 0
	}
	if c.PoolSize <= 0 {
		c.PoolSize = defaultPoolSize()
	}
	if c.MaxActiveConns > 0 && c.MaxActiveConns < c.PoolSize {
		// 硬上限低于基准池大小会让池子一直在上限附近排队，按基准值对齐更符合直觉。
		c.PoolSize = c.MaxActiveConns
	}
	if c.MinIdleConns < 0 {
		c.MinIdleConns = 0
	}
	if c.MinIdleConns > c.PoolSize {
		c.MinIdleConns = c.PoolSize
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = defaultDialTimeout
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	if c.PoolTimeout <= 0 {
		c.PoolTimeout = c.ReadTimeout + time.Second
	}
	if c.ConnMaxIdleTime == 0 {
		c.ConnMaxIdleTime = defaultConnMaxIdleTime
	}
	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = defaultConnMaxLifetime
	}
	if c.ConnMaxLifetime > 0 && c.ConnMaxLifetimeJitter == 0 {
		c.ConnMaxLifetimeJitter = c.ConnMaxLifetime / 10
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = defaultMaxRetries
	}
	if c.MinRetryBackoff <= 0 {
		c.MinRetryBackoff = defaultMinRetryBackoff
	}
	if c.MaxRetryBackoff <= 0 {
		c.MaxRetryBackoff = defaultMaxRetryBackoff
	}
	if c.MaxRetryBackoff < c.MinRetryBackoff {
		c.MaxRetryBackoff = c.MinRetryBackoff
	}
	return c
}

// validate 检查**原始**配置是否可用。
//
// 必须在 normalize 之前调用，否则"取值为负"会被 normalize 当成"未设置"吞掉。
func (c Config) validate() error {
	if strings.TrimSpace(c.Addr) == "" {
		return fmt.Errorf("%w: Addr 不能为空", ErrInvalidConfig)
	}
	if c.DB < 0 {
		return fmt.Errorf("%w: DB 不能为负", ErrInvalidConfig)
	}
	if c.PoolSize < 0 {
		return fmt.Errorf("%w: PoolSize 不能为负（0 表示使用默认值）", ErrInvalidConfig)
	}
	if c.MaxActiveConns < 0 {
		return fmt.Errorf("%w: MaxActiveConns 不能为负（0 表示不限制）", ErrInvalidConfig)
	}
	if c.MinIdleConns < 0 {
		return fmt.Errorf("%w: MinIdleConns 不能为负", ErrInvalidConfig)
	}
	if c.MaxIdleConns < 0 {
		return fmt.Errorf("%w: MaxIdleConns 不能为负（0 表示不限制）", ErrInvalidConfig)
	}
	if c.Protocol != 0 && c.Protocol != 2 && c.Protocol != 3 {
		return fmt.Errorf("%w: Protocol=%d 非法，只支持 0（默认 RESP3）/ 2 / 3", ErrInvalidConfig, c.Protocol)
	}

	// MaxRetries：-1 是 go-redis 定义的"禁用重试"。允许它，但要求显式了解。
	if c.MaxRetries < -1 {
		return fmt.Errorf("%w: MaxRetries=%d 非法，-1 表示禁用重试，0 表示默认 3 次",
			ErrInvalidConfig, c.MaxRetries)
	}

	// 单位哨兵：正值但小得离谱，几乎一定是把毫秒整数直接写进了 Duration 字段。
	// 负值是合法语义（ReadTimeout 的 -1/-2、ConnMaxLifetime 的负值），所以只看正的。
	durations := []struct {
		name string
		val  time.Duration
	}{
		{"DialTimeout", c.DialTimeout},
		{"ReadTimeout", c.ReadTimeout},
		{"WriteTimeout", c.WriteTimeout},
		{"PoolTimeout", c.PoolTimeout},
		{"ConnMaxIdleTime", c.ConnMaxIdleTime},
		{"ConnMaxLifetime", c.ConnMaxLifetime},
		{"MinRetryBackoff", c.MinRetryBackoff},
		{"MaxRetryBackoff", c.MaxRetryBackoff},
		{"LogSlowThreshold", c.LogSlowThreshold},
	}
	for _, d := range durations {
		if d.val > 0 && d.val < minSaneDuration {
			return fmt.Errorf(
				"%w: %s=%v 过小，疑似单位写错；请写成带单位的字符串，例如 %s: 3s",
				ErrInvalidConfig, d.name, d.val, d.name)
		}
	}

	if c.TLS.Enable {
		if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
			return fmt.Errorf("%w: TLS 双向认证需要同时提供 CertFile 与 KeyFile", ErrInvalidConfig)
		}
	}
	return nil
}

// tlsConfig 把配置转换成 crypto/tls 的配置；未开启 TLS 时返回 nil。
func (c Config) tlsConfig() (*tls.Config, error) {
	if !c.TLS.Enable {
		return nil, nil
	}

	out := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: c.TLS.InsecureSkipVerify,
		ServerName:         c.TLS.ServerName,
	}
	if out.ServerName == "" {
		if host, _, ok := strings.Cut(c.Addr, ":"); ok {
			out.ServerName = host
		}
	}

	if c.TLS.CAFile != "" {
		pem, err := os.ReadFile(c.TLS.CAFile)
		if err != nil {
			return nil, fmt.Errorf("%w: 读取 CA 证书失败: %v", ErrInvalidConfig, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%w: CA 证书 %s 中没有可用的证书", ErrInvalidConfig, c.TLS.CAFile)
		}
		out.RootCAs = pool
	}

	if c.TLS.CertFile != "" && c.TLS.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.TLS.CertFile, c.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("%w: 加载客户端证书失败: %v", ErrInvalidConfig, err)
		}
		out.Certificates = []tls.Certificate{cert}
	}
	return out, nil
}
