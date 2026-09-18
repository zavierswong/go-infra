package mysql

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"

	"github.com/zavierswong/go-infra/metrics"
)

// Config 是 MySQL 连接与**连接池**配置。
//
// 所有 time.Duration 字段都接受 "300ms" / "30s" / "2m" 这类字符串
// （viper 的默认解码器已包含 StringToTimeDurationHookFunc）。
// 直接使用 mapstructure 时需要自行 DecodeHook(mapstructure.StringToTimeDurationHookFunc())。
//
// 单位提醒：旧版把 ConnMaxLifetime / SlowThreshold 定义为 int（分别是秒 / 毫秒），
// 本版统一改为 time.Duration。若沿用旧写法（如 conn_max_lifetime: 3600），
// 会被解析成 3600ns —— validate 会直接报错拦下，不会静默变成"连接刚建好就过期"。
type Config struct {
	// Dsn 是 go-sql-driver/mysql 的 DSN：
	//
	//	user:password@tcp(host:port)/dbname?charset=utf8mb4&parseTime=True&loc=Local
	//
	// 初始化时会被解析校验，语法错误会立刻返回而不是等到第一次查询。
	Dsn string `mapstructure:"dsn"`

	// ------------------------------------------------------------------
	// 标识与可观测性
	// ------------------------------------------------------------------

	// Name 是实例名，用于区分同一个进程里的多个 MySQL 连接。
	//
	// 多库、读写分离、灰度双写这类场景下，如果不给实例起名，
	// metrics.Event.Instance 就是空字符串，两个库的指标会被合并成一条曲线，
	// 反而掩盖问题。建议用简短且稳定的业务名（"order"、"order_ro"）。
	Name string `mapstructure:"name"`

	// Observer 接收每个操作结束后的 metrics.Event（查询、事务、慢操作）。
	//
	// 传 nil 表示不观测，此时不会有任何额外开销：Open 不会注册 GORM callback。
	// 详见 github.com/zavierswong/go-infra/metrics 与 README 的「metrics 暴露」章节。
	Observer metrics.Observer `mapstructure:"-"`

	// ------------------------------------------------------------------
	// 连接池：这四项是吞吐与延迟的主要抓手
	// ------------------------------------------------------------------

	// MaxIdleConns 是连接池中保留的**空闲**连接数上限。
	//
	// 这是最容易被忽略、但对性能影响最大的一个参数：
	// database/sql 的默认值只有 2，意味着 QPS 一上来空闲连接立刻被抢光，
	// 之后每个请求都要重新拨号（TCP + 认证 + 权限校验，实测毫秒级）。
	// 设成与 MaxOpenConns 相等可以避免这种"拨号风暴"。
	// 0 表示默认 10。
	MaxIdleConns int `mapstructure:"max_idle_conns"`

	// MaxOpenConns 是同时打开的连接数上限（含空闲与使用中）。
	//
	// MySQL 的每个连接都对应一个服务端线程，开太多会拖垮数据库；
	// 建议按 CPU 核数 × 2~4 估算。0 表示默认 GOMAXPROCS × 4（下限 8）。
	MaxOpenConns int `mapstructure:"max_open_conns"`

	// ConnMaxLifetime 是连接的最长存活时间，到点后被回收重建。
	//
	// **必须小于 MySQL 的 wait_timeout**（默认 28800s），否则会拿到
	// "服务端已经关掉、客户端还以为有效"的坏连接。
	// 0 表示默认 1800s。生产建议 600s~1800s。
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`

	// ConnMaxIdleTime 是连接的**空闲**存活时间，超过就被关闭归还给系统。
	// 与 ConnMaxLifetime 的区别：这个只在连接闲置时计时，用于削掉低频时段的冗余连接。
	// 0 表示默认 300s。
	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time"`

	// ------------------------------------------------------------------
	// 建连行为
	// ------------------------------------------------------------------

	// ConnectTimeout 是单次建连（拨号 + 握手）超时，会写入 DSN 的 timeout 参数。
	// 0 表示默认 5s。
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`

	// DialAttempts 是初始化时的最大尝试次数（含第一次）。
	//
	// 旧版在 sync.Once 内无限重试，MySQL 不可达时首个调用方会**永久阻塞**，
	// 而且永远拿不到 error。这里改为显式可控：0 表示默认 3 次，1 表示失败即返回，
	// 负值表示无限重试直到 Close（谨慎使用）。
	DialAttempts int `mapstructure:"dial_attempts"`

	// DialBackoff / DialMaxBackoff 是重试退避的起始值与上限，0 表示默认 1s / 30s。
	DialBackoff    time.Duration `mapstructure:"dial_backoff"`
	DialMaxBackoff time.Duration `mapstructure:"dial_max_backoff"`

	// ------------------------------------------------------------------
	// 健康监控
	// ------------------------------------------------------------------

	// HealthCheckInterval 是后台探活间隔，0 表示默认 30s。
	HealthCheckInterval time.Duration `mapstructure:"health_check_interval"`

	// RebuildAfterFailures 表示连续探活失败多少次后主动重建连接池。
	//
	// 默认 0，即**不重建**。原因：sql.DB 本身就是连接池，服务端恢复后
	// 下一次查询会自动拨号，探活失败通常并不需要重建；
	// 而旧版在探活失败后无限重连，还会把旧连接池直接丢掉不关闭（泄漏）。
	// 只有在"连接池确实卡死"这类场景才需要打开它（如设为 3）。
	RebuildAfterFailures int `mapstructure:"rebuild_after_failures"`

	// ------------------------------------------------------------------
	// DSN 覆盖项：仅当 DSN 里没有对应参数时才生效
	// ------------------------------------------------------------------

	// SSL 对应 DSN 的 tls 参数：true / false / skip-verify / preferred，
	// 或已注册的自定义 TLS 配置名。
	//
	// 旧版这个字段零引用（设了没有任何效果），现在会真正写进 DSN。
	SSL string `mapstructure:"ssl"`

	// ReadTimeout / WriteTimeout 是单条语句的读写超时，0 表示用驱动默认（无超时）。
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`

	// ------------------------------------------------------------------
	// 日志
	// ------------------------------------------------------------------

	// LogLevel 是 GORM 的 SQL 日志级别：silent / error / warn / info，空值按 info。
	LogLevel string `mapstructure:"log_level"`

	// SlowThreshold 是慢 SQL 阈值（如 200ms），0 表示 logger 的默认 200ms，
	// 负值表示关闭慢 SQL 告警。
	SlowThreshold time.Duration `mapstructure:"slow_threshold"`
}

// 默认值。集中在这里，便于 README 与测试对齐。
const (
	defaultMaxIdleConns        = 10
	defaultConnMaxLifetime     = 30 * time.Minute
	defaultConnMaxIdleTime     = 5 * time.Minute
	defaultConnectTimeout      = 5 * time.Second
	defaultDialAttempts        = 3
	defaultDialBackoff         = time.Second
	defaultDialMaxBackoff      = 30 * time.Second
	defaultHealthCheckInterval = 30 * time.Second

	// minSaneDuration 是"单位哨兵"的下限：任何配置成正值却小于 1ms 的时长，
	// 几乎一定是把旧版的秒/毫秒整数直接写进了 Duration 字段
	// （例如 conn_max_lifetime: 1800 → 1800ns）。
	//
	// 门槛取 1ms 而不是 1s：退避参数（DialBackoff）合法取值本来就在毫秒级，
	// 门槛太高会把正确配置误判为错误。
	minSaneDuration = time.Millisecond
)

// defaultMaxOpenConns 按 GOMAXPROCS 推导默认的最大连接数。
//
// 用 GOMAXPROCS 而不是 NumCPU：新版 Go 的 GOMAXPROCS 会尊重 cgroup 限额，
// 容器里能拿到真实的可用核数，不会因为宿主机核多而开出过大的连接池。
func defaultMaxOpenConns() int {
	n := runtime.GOMAXPROCS(0) * 4
	if n < 8 {
		return 8
	}
	return n
}

// ready 校验原始配置并补齐默认值。这是初始化时唯一应该走的入口。
//
// 顺序很关键：**先校验，再 normalize**。反过来的话，negative 取值会在
// normalize 阶段被当成"未设置"替换成默认值，非法输入就被静默吞掉了。
func (c Config) ready() (Config, error) {
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c.normalize(), nil
}

// normalize 补齐默认值并修正互相矛盾的取值。
func (c Config) normalize() Config {
	if c.MaxOpenConns <= 0 {
		c.MaxOpenConns = defaultMaxOpenConns()
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = defaultMaxIdleConns
	}
	// 空闲连接多于最大连接没有意义；database/sql 内部也会截断，显式对齐便于观察。
	if c.MaxIdleConns > c.MaxOpenConns {
		c.MaxIdleConns = c.MaxOpenConns
	}

	if c.ConnMaxLifetime <= 0 {
		c.ConnMaxLifetime = defaultConnMaxLifetime
	}
	if c.ConnMaxIdleTime <= 0 {
		c.ConnMaxIdleTime = defaultConnMaxIdleTime
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = defaultConnectTimeout
	}
	if c.DialAttempts == 0 {
		c.DialAttempts = defaultDialAttempts
	}
	if c.DialBackoff <= 0 {
		c.DialBackoff = defaultDialBackoff
	}
	if c.DialMaxBackoff <= 0 {
		c.DialMaxBackoff = defaultDialMaxBackoff
	}
	if c.DialMaxBackoff < c.DialBackoff {
		c.DialMaxBackoff = c.DialBackoff
	}
	if c.HealthCheckInterval <= 0 {
		c.HealthCheckInterval = defaultHealthCheckInterval
	}
	return c
}

// validate 检查**原始**配置是否可用。
//
// 必须在 normalize 之前调用：normalize 会把 0（未设置）与负值一并替换为默认值，
// 若先 normalize，就再也发现不了"取值为负"这类非法输入。
//
// 除了必填项，还专门拦一类"单位写错"的配置：旧版用 int 表示秒/毫秒，
// 直接迁移到 time.Duration 会得到纳秒级取值。这类配置不会报错，
// 只会让连接池表现异常（例如 ConnMaxLifetime=1.8µs 意味着每条连接一建好就过期，
// 每次请求都要重新拨号），属于最难排查的性能问题，所以必须在这里大声失败。
func (c Config) validate() error {
	if strings.TrimSpace(c.Dsn) == "" {
		return fmt.Errorf("%w: Dsn 不能为空", ErrInvalidConfig)
	}
	if _, err := c.driverCfg(); err != nil {
		return err
	}
	if c.MaxOpenConns < 0 {
		return fmt.Errorf("%w: MaxOpenConns 不能为负（0 表示使用默认值）", ErrInvalidConfig)
	}
	if c.MaxIdleConns < 0 {
		return fmt.Errorf("%w: MaxIdleConns 不能为负（0 表示使用默认值）", ErrInvalidConfig)
	}
	if c.RebuildAfterFailures < 0 {
		return fmt.Errorf("%w: RebuildAfterFailures 不能为负", ErrInvalidConfig)
	}

	// 单位哨兵：>0 但小得离谱，几乎一定是把 "3000"（秒/毫秒）直接写进了 Duration 字段。
	durations := []struct {
		name string
		val  time.Duration
	}{
		{"ConnMaxLifetime", c.ConnMaxLifetime},
		{"ConnMaxIdleTime", c.ConnMaxIdleTime},
		{"ConnectTimeout", c.ConnectTimeout},
		{"DialBackoff", c.DialBackoff},
		{"DialMaxBackoff", c.DialMaxBackoff},
		{"HealthCheckInterval", c.HealthCheckInterval},
		{"ReadTimeout", c.ReadTimeout},
		{"WriteTimeout", c.WriteTimeout},
	}
	for _, d := range durations {
		if d.val > 0 && d.val < minSaneDuration {
			return fmt.Errorf(
				"%w: %s=%v 过小，疑似单位写错；请写成带单位的字符串，例如 %s: 30s",
				ErrInvalidConfig, d.name, d.val, d.name)
		}
	}
	// SlowThreshold 单独判断：0 与负值都合法（负值表示关闭慢 SQL 告警）。
	if c.SlowThreshold > 0 && c.SlowThreshold < time.Millisecond {
		return fmt.Errorf(
			"%w: SlowThreshold=%v 过小，疑似单位写错；请写成如 SlowThreshold: 200ms",
			ErrInvalidConfig, c.SlowThreshold)
	}

	switch strings.ToLower(strings.TrimSpace(c.LogLevel)) {
	case "", "silent", "error", "warn", "info":
	default:
		return fmt.Errorf("%w: LogLevel=%q 非法，可选 silent/error/warn/info",
			ErrInvalidConfig, c.LogLevel)
	}
	return nil
}

// driverCfg 解析 DSN，并把 Config 里"仅当 DSN 未指定时才生效"的覆盖项写进去。
//
// 旧版直接把 Dsn 字符串交给 gorm，语法错误要等到第一次查询才暴露；
// 这里在初始化阶段就解析一次，顺带让 SSL / 超时字段真正生效。
func (c Config) driverCfg() (*mysqldrv.Config, error) {
	cfg, err := mysqldrv.ParseDSN(strings.TrimSpace(c.Dsn))
	if err != nil {
		return nil, fmt.Errorf("%w: DSN 解析失败: %v", ErrInvalidConfig, err)
	}
	if cfg.DBName == "" {
		return nil, fmt.Errorf("%w: DSN 未指定数据库名", ErrInvalidConfig)
	}
	if c.SSL != "" && cfg.TLSConfig == "" {
		cfg.TLSConfig = c.SSL
	}
	if c.ConnectTimeout > 0 && cfg.Timeout == 0 {
		cfg.Timeout = c.ConnectTimeout
	}
	if c.ReadTimeout > 0 && cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = c.ReadTimeout
	}
	if c.WriteTimeout > 0 && cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = c.WriteTimeout
	}
	return cfg, nil
}

// dsn 返回最终生效的 DSN 字符串。
func (c Config) dsn() (string, error) {
	cfg, err := c.driverCfg()
	if err != nil {
		return "", err
	}
	return cfg.FormatDSN(), nil
}
