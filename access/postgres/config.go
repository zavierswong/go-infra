package postgres

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/zavierswong/go-infra/metrics"
)

// Config 是 PostgreSQL 连接与**连接池**配置。
//
// 所有 time.Duration 字段都接受 "300ms" / "30s" / "2m" 这类字符串
// （viper 的默认解码器已包含 StringToTimeDurationHookFunc）。
// 直接使用 mapstructure 时需要自行 DecodeHook(mapstructure.StringToTimeDurationHookFunc())。
//
// 字段分三层：
//  1. 连接池与建连行为 —— 与 mysql 包语义完全一致，迁移心智成本为零；
//  2. PostgreSQL 专有的**服务端会话参数**（SSLMode / ApplicationName / SearchPath /
//     TimeZone / StatementTimeout / IdleInTransactionTimeout / TargetSessionAttrs）——
//     这类参数只能在建立连接时下发，连上以后改不了池子里已有的连接；
//  3. 逃生通道 Settings —— 任意 GUC 透传，覆盖上面没列举到的参数。
type Config struct {
	// Dsn 是 PostgreSQL 连接串，**两种写法都支持**：
	//
	//	postgres://user:password@host:5432/dbname?sslmode=disable&TimeZone=Asia/Shanghai
	//	host=127.0.0.1 port=5432 user=postgres password=123456 dbname=demo sslmode=disable
	//
	// 初始化时会被解析校验（含库名是否存在、参数是否可读），语法错误立刻返回。
	Dsn string `mapstructure:"dsn"`

	// ------------------------------------------------------------------
	// 标识与可观测性
	// ------------------------------------------------------------------

	// Name 是本实例在指标/日志里的标识，例如 "order-db"、"report-db"。
	//
	// 与 ApplicationName 的分工务必分清：
	//   - ApplicationName 会随建连下发给服务端，出现在 pg_stat_activity.application_name，
	//     是**给 DBA 看的**；
	//   - Name 只在本进程内使用，作为 metrics.Event.Instance 与监控快照的标签，
	//     是**给监控看的**。
	//
	// 留空时监控侧只能看到空串，多个库的曲线会叠在一起无法区分，
	// 因此多实例部署时请务必填写。
	Name string `mapstructure:"name"`

	// Observer 接收访问事件（每条语句的耗时、错误、SQL 文本）。
	//
	// 为 nil 时 Open 不会注册任何 GORM 回调，热路径上零开销 ——
	// 不接监控的调用方不需要付出代价。
	// 类型即 metrics.Observer，与 mysql/redis/rabbitmq 三个包完全一致。
	Observer metrics.Observer `mapstructure:"-"`

	// ------------------------------------------------------------------
	// 连接池：这四项是吞吐与延迟的主要抓手
	// ------------------------------------------------------------------

	// MaxIdleConns 是连接池中保留的**空闲**连接数上限。
	//
	// database/sql 的默认值只有 2：QPS 一上来空闲连接立刻被抢光，
	// 之后每个请求都要重新拨号 —— 对 PostgreSQL 尤其昂贵，
	// 因为它要 fork 一个后端进程并做认证。设成与 MaxOpenConns 相等可消掉这次开销。
	// 0 表示默认 10。
	MaxIdleConns int `mapstructure:"max_idle_conns"`

	// MaxOpenConns 是同时打开的连接数上限（含空闲与使用中）。
	//
	// ⚠️ PostgreSQL 的默认 max_connections 只有 100，且每个连接是一个独立后端进程
	// （常驻内存约 5~10MB）。多实例部署时务必显式设置：
	// `实例数 × MaxOpenConns < max_connections - 预留`。
	// 超限时报 SQLSTATE 53300（too_many_connections），可用 IsRetryable 判定后重试。
	// 0 表示默认 GOMAXPROCS × 4（下限 8）。
	MaxOpenConns int `mapstructure:"max_open_conns"`

	// ConnMaxLifetime 是连接的最长存活时间，到点后被回收重建。
	//
	// PostgreSQL 服务端没有 MySQL 那样的 wait_timeout，但下面这些都会单方面断开连接：
	//   - `idle_session_timeout`（PG 14+，默认 0 即关闭）；
	//   - 云厂商 / PgBouncer / 负载均衡的**空闲连接超时**（常见 60s ~ 10min）。
	// 取值必须小于其中最严格的那个，否则会偶发 "unexpected EOF" / "connection reset"。
	// 0 表示默认 1800s。
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`

	// ConnMaxIdleTime 是连接的**空闲**存活时间，超过就被关闭归还给系统。
	// 低频时段回收连接，能显著降低服务端后端进程数。0 表示默认 300s。
	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time"`

	// ------------------------------------------------------------------
	// 建连行为
	// ------------------------------------------------------------------

	// ConnectTimeout 是单次建连（拨号 + TLS + 认证）超时。
	//
	// 它以秒为单位写入 DSN 的 connect_timeout（pgx 对连接池建连的硬超时），
	// 同时也会作为 Open 阶段真实探活的 ctx 超时。
	// 不足 1s 的取值会被向上取整到 1s —— 精确的亚秒级控制由 ctx 保证。
	// 0 表示默认 5s。
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`

	// DialAttempts 是初始化时的最大尝试次数（含第一次）。
	// 0 表示默认 3 次，1 表示失败即返回，负值表示无限重试直到 Close（谨慎使用）。
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
	// 默认 0，即**不重建**：sql.DB 本身就是连接池，服务端恢复后下一次查询会自动拨号。
	// 只有"连接池确实卡死"这类场景才需要打开它（如设为 3）。
	RebuildAfterFailures int `mapstructure:"rebuild_after_failures"`

	// ------------------------------------------------------------------
	// PostgreSQL 专有的服务端会话参数
	//
	// 下面每一项都是**覆盖项**：只在 DSN 里没有写同名参数时才写入，
	// 不会覆盖你显式写在 DSN 里的取值。
	// ------------------------------------------------------------------

	// SSLMode 对应 sslmode，取值：disable / allow / prefer / require / verify-ca / verify-full。
	//
	// 取值非法会在启动期报错（旧版 mysql 包的同类字段是零引用，写了没有任何效果）。
	// 注意 pgx 的默认值是 prefer —— 它在 TLS 失败时会**静默降级为明文**，
	// 生产环境请显式写 require 或 verify-full。
	SSLMode string `mapstructure:"ssl_mode"`

	// ApplicationName 对应 application_name，显示在 pg_stat_activity.application_name。
	//
	// 强烈建议设置：出问题时一眼就能看出是哪个服务在压库、谁在跑长事务，
	// 也能用 pg_stat_activity 按应用维度做连接数治理。
	ApplicationName string `mapstructure:"application_name"`

	// SearchPath 对应 search_path，即 schema 搜索路径，如 "app,public"。
	// 多租户按 schema 隔离时必设。
	SearchPath string `mapstructure:"search_path"`

	// TimeZone 对应 timezone（GUC 名 TimeZone），如 "Asia/Shanghai"。
	//
	// 它决定 `timestamptz` 的显示时区，也决定了 gorm 驱动注册 timestamp 编解码器时
	// 使用的 ScanLocation —— 不设置的话，容器里通常是 UTC，
	// 表现为写进去的 time.Time 读出来"差了 8 小时"。
	TimeZone string `mapstructure:"time_zone"`

	// StatementTimeout 对应 statement_timeout：**服务端**单条语句超时。
	//
	// 客户端 ctx 超时只是放弃等待，服务端那条语句仍在跑；statement_timeout 才会
	// 真正取消它。它是保护数据库不被慢查询拖垮的最后一道闸门，建议生产必设（如 30s）。
	// 触发时报 SQLSTATE 57014，可用 IsQueryCanceled 判定。
	//
	// 内部换算成**毫秒整数**再写入 DSN：PostgreSQL 的时间类 GUC 只接受
	// "一个数值 + 一个单位"，直接写 `1m30s` 这种复合单位会被服务端拒绝。
	// 0 表示不设置（沿用服务端默认，即不限制）。
	StatementTimeout time.Duration `mapstructure:"statement_timeout"`

	// IdleInTransactionTimeout 对应 idle_in_transaction_session_timeout。
	//
	// PostgreSQL 最经典的线上事故：业务开了事务却没提交/回滚（常见于忘了 defer rollback），
	// 连接一直挂在 idle in transaction，**持有锁并挡住 vacuum 回收死元组**，
	// 表体积膨胀、DDL 被阻塞。设成 5m ~ 10m 可以让服务端直接掐掉这种连接。
	//
	// 同样按毫秒整数写入。0 表示不设置。
	IdleInTransactionTimeout time.Duration `mapstructure:"idle_in_transaction_timeout"`

	// TargetSessionAttrs 对应 target_session_attrs，用于主从识别：
	// any（默认）/ read-write / read-only / primary / standby / prefer-standby。
	//
	// 配合多个 host（`host=a,b port=5432,5432`）即可在客户端做读写分离路由，
	// 不需要额外的代理层。取值非法会在启动期报错。
	TargetSessionAttrs string `mapstructure:"target_session_attrs"`

	// Settings 是任意服务端会话参数的透传通道，用于上面没列举到的 GUC：
	//
	//	Settings: map[string]string{
	//	    "lock_timeout":                  "3s",
	//	    "work_mem":                      "16MB",
	//	    "default_transaction_isolation": "read committed",
	//	}
	//
	// key 必须是服务端 GUC 名（小写字母/数字/下划线/点），会在启动期校验；
	// 与上面几个强类型字段一样遵循"只补缺不覆盖"。
	Settings map[string]string `mapstructure:"settings"`

	// PreferSimpleProtocol 让 pgx 走**简单查询协议**（不做语句级准备/缓存）。
	//
	// 什么时候必须打开：服务端前面挂了 PgBouncer 且是 `pool_mode=transaction`
	// （或其它会跨请求复用后端连接的连接池）。此时 pgx 缓存的预备语句会在
	// 换连接后失效，报 `prepared statement "stmtcache_xxx" does not exist`。
	// 代价是失去了二进制传输与语句复用的性能收益，直连时不要打开。
	PreferSimpleProtocol bool `mapstructure:"prefer_simple_protocol"`

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
	// 几乎一定是把秒/毫秒整数直接写进了 Duration 字段
	// （例如 conn_max_lifetime: 1800 → 1800ns）。
	//
	// 门槛取 1ms 而不是 1s：退避参数（DialBackoff）合法取值本来就在毫秒级，
	// 门槛太高会把正确配置误判为错误。
	minSaneDuration = time.Millisecond
)

// validSSLModes 是 libpq 定义的合法 sslmode 取值。
var validSSLModes = []string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}

// validTargetSessionAttrs 是 pgx 支持的 target_session_attrs 取值。
var validTargetSessionAttrs = []string{"any", "read-write", "read-only", "primary", "standby", "prefer-standby"}

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
// 顺序很关键：**先校验，再 normalize**。反过来的话，负值会在 normalize 阶段
// 被当成"未设置"替换成默认值，非法输入就被静默吞掉了。
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
func (c Config) validate() error {
	info, err := parseConnInfo(c.Dsn)
	if err != nil {
		return err
	}
	if strings.TrimSpace(info.dbName()) == "" {
		// 不写库名时 PostgreSQL 会退化成"用连接用户名当库名"，
		// 表现为一个莫名其妙的 FATAL: database "xxx" does not exist。
		// 这里直接要求写清楚，把问题挡在启动期。
		return fmt.Errorf("%w: DSN 未指定数据库名（URL 形式写在 path，keyword/value 形式用 dbname=）",
			ErrInvalidConfig)
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

	if err := validateEnum("SSLMode", c.SSLMode, validSSLModes); err != nil {
		return err
	}
	if err := validateEnum("TargetSessionAttrs", c.TargetSessionAttrs, validTargetSessionAttrs); err != nil {
		return err
	}

	if err := c.validateDurations(); err != nil {
		return err
	}
	if err := validateSettings(c.Settings); err != nil {
		return err
	}

	switch strings.ToLower(strings.TrimSpace(c.LogLevel)) {
	case "", "silent", "error", "warn", "info":
	default:
		return fmt.Errorf("%w: LogLevel=%q 非法，可选 silent/error/warn/info",
			ErrInvalidConfig, c.LogLevel)
	}
	return nil
}

// validateEnum 校验枚举取值，空值表示"不设置"。错误信息里带上合法取值，便于当场改对。
func validateEnum(name, value string, allowed []string) error {
	if value == "" {
		return nil
	}
	for _, a := range allowed {
		if value == a {
			return nil
		}
	}
	return fmt.Errorf("%w: %s=%q 非法，可选 %s",
		ErrInvalidConfig, name, value, strings.Join(allowed, " / "))
}

// validateDurations 拦住"单位写错"的配置。
//
// 这类配置不会报错，只会让连接池表现异常（例如 ConnMaxLifetime=1.8µs 意味着
// 每条连接一建好就过期，每次请求都要重新拨号），属于最难排查的性能问题。
func (c Config) validateDurations() error {
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
		{"StatementTimeout", c.StatementTimeout},
		{"IdleInTransactionTimeout", c.IdleInTransactionTimeout},
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
	return nil
}

// validateSettings 校验透传的 GUC 名与取值。
//
// key 的字符集限制不只是"规范"问题：Settings 最终会被拼进 DSN 字符串，
// 若允许空白、`=`、单引号，就等于给了配置项一个**注入连接串**的口子
// （例如 key 写成 `host=x password=evil`）。
// 这里把 key 限死为 GUC 允许的字符集，从根上堵住。
func validateSettings(settings map[string]string) error {
	for k := range settings {
		if !isValidGUCName(k) {
			return fmt.Errorf(
				"%w: Settings 的 key %q 非法；须为服务端参数名（小写字母开头，仅含小写字母、数字、下划线、点），如 work_mem",
				ErrInvalidConfig, k)
		}
		if strings.ContainsRune(settings[k], 0) {
			return fmt.Errorf("%w: Settings[%q] 的取值包含 NUL 字节", ErrInvalidConfig, k)
		}
	}
	return nil
}

// isValidGUCName 判断是否为合法的 GUC 参数名。
//
// 允许点号是为了"扩展自定义参数"（如 `myext.setting`）。
func isValidGUCName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c == '_':
		case c >= '0' && c <= '9', c == '.':
			// 数字与点号不能出现在开头
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// sortStrings 是插入排序：Settings 通常只有个位数个键，
// 为它引入 sort 包不值得，而且这里要保持零依赖的简单性。
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// ---------------------------------------------------------------------------
// DSN 组装
// ---------------------------------------------------------------------------

// dsn 返回最终生效的 DSN 字符串：把 Config 里的覆盖项合并进用户 DSN。
//
// 合并规则（顺序即优先级）：用户 DSN 里已写的一律保留；没写的按 Config 补齐。
func (c Config) dsn() (string, error) {
	info, err := parseConnInfo(c.Dsn)
	if err != nil {
		return "", err
	}
	c.applyOverrides(info)
	return info.String(), nil
}

// applyOverrides 把 Config 的覆盖项写进连接串。
func (c Config) applyOverrides(info *connInfo) {
	// connect_timeout 在 pgx 里只接受**整数秒**，亚秒取值向上取整到 1s；
	// 精确的超时由 Open 阶段带 ctx 的探活保证。
	if c.ConnectTimeout > 0 {
		seconds := int64(c.ConnectTimeout / time.Second)
		if c.ConnectTimeout%time.Second != 0 {
			seconds++
		}
		if seconds < 1 {
			seconds = 1
		}
		info.setIfAbsent("connect_timeout", strconv.FormatInt(seconds, 10))
	}

	if c.SSLMode != "" {
		info.setIfAbsent("sslmode", c.SSLMode)
	}
	if c.ApplicationName != "" {
		info.setIfAbsent("application_name", c.ApplicationName)
	}
	if c.SearchPath != "" {
		info.setIfAbsent("search_path", c.SearchPath)
	}
	if c.TimeZone != "" {
		// 用小写的 timezone：gorm 的 postgres 驱动会用正则
		// `(time_zone|TimeZone|timezone)=(.*?)($|&| )` 从 DSN 里抠出时区字符串，
		// 再交给 time.LoadLocation 注册 timestamp 的 ScanLocation。
		// 写成小写可以让它抠到的值与 pgx 传给服务端的 GUC 名完全一致。
		info.setIfAbsent("timezone", c.TimeZone)
	}
	if c.TargetSessionAttrs != "" {
		info.setIfAbsent("target_session_attrs", c.TargetSessionAttrs)
	}
	if c.StatementTimeout > 0 {
		info.setIfAbsent("statement_timeout", pgMillis(c.StatementTimeout))
	}
	if c.IdleInTransactionTimeout > 0 {
		info.setIfAbsent("idle_in_transaction_session_timeout", pgMillis(c.IdleInTransactionTimeout))
	}

	// Settings 按键排序后写入：map 的遍历顺序是随机的，不排序的话
	// 每次生成的 DSN 参数顺序都可能不同，配置 diff 与日志里就会充满噪音。
	for _, k := range sortedKeys(c.Settings) {
		info.setIfAbsent(k, c.Settings[k])
	}
}

// sortedKeys 返回排序后的键列表。
func sortedKeys(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sortStrings(keys)
	return keys
}

// pgMillis 把时长换算成 PostgreSQL 时间类参数需要的**毫秒整数**。
//
// 为什么不直接写 time.Duration.String()（如 "1m30s"）：PostgreSQL 的时间类 GUC
// 只解析"一个数值 + 一个单位"，复合单位会报
// `invalid value for parameter "statement_timeout": "1m30s"`，
// 而且这个错误发生在**建连阶段**，表现为"数据库连不上"，很难联想到是超时配置写错。
func pgMillis(d time.Duration) string {
	ms := d.Milliseconds()
	if ms < 0 {
		ms = 0
	}
	return strconv.FormatInt(ms, 10)
}
