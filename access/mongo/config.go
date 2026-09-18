package mongo

import (
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/zavierswong/go-infra/metrics"
)

// Config 是 MongoDB 客户端配置。
//
// 所有 time.Duration 字段都接受 "300ms" / "30s" / "2m" 这类字符串
// （viper 的默认解码器已包含 StringToTimeDurationHookFunc）。
// 直接使用 mapstructure 时需要自行 DecodeHook(mapstructure.StringToTimeDurationHookFunc())。
//
// # 与 URI 的优先关系
//
// 本包里 **URI 永远优先**：Config 里的"覆盖项"只在 URI 未显式指定该参数时才生效。
// 这不是本包自己定的规则，而是驱动的语义 —— 驱动文档明确写着
// "later option setter calls overwrite the values from previous option setter calls,
// including the ApplyURI method"，本包把 ApplyURI 放在最后调用，于是天然得到
// "URI 覆盖 Config" 的行为，不需要自己维护一张"URI 里有没有这个参数"的表。
//
// 好处很实际：URI 是运维手里临时改的那个旋钮（改连接串比重发配置快），
// 覆盖语义与直觉一致，不会出现"改了 URI 却不生效"的情况。
type Config struct {
	// URI 是 MongoDB 连接串：
	//
	//	mongo://user:pass@host1:27017,host2:27017/db?replicaSet=rs0&authSource=admin
	//	mongo+srv://user:pass@cluster0.example.com/db?retryWrites=true
	//
	// 初始化时会被解析校验（scheme、主机、端口、参数转义、库名），
	// 语法错误会立刻返回，而不是等到第一次查询。
	//
	// SRV 形式（mongo+srv://）**不能写端口** —— 端口只能来自 DNS 记录。
	URI string `mapstructure:"uri"`

	// ------------------------------------------------------------------
	// 标识与可观测性
	// ------------------------------------------------------------------

	// Name 是实例名，用于区分同一个进程里的多个 MongoDB 连接。
	//
	// 多集群、读写分离（主库 + 分析从库）这类场景下，如果不给实例起名，
	// metrics.Event.Instance 就是空字符串，两个集群的指标会被合并成一条曲线。
	// 建议用简短且稳定的业务名（"order"、"order_analytics"）。
	Name string `mapstructure:"name"`

	// Observer 接收每个命令结束后的 metrics.Event，以及连接池快照。
	//
	// 传 nil 表示不观测，此时不会有任何额外开销：Open 不会注册命令监听器。
	// 详见 github.com/zavierswong/go-infra/metrics 与 README 的「metrics 暴露」章节。
	Observer metrics.Observer `mapstructure:"-"`

	// ------------------------------------------------------------------
	// 连接池：这四项是吞吐与延迟的主要抓手
	// ------------------------------------------------------------------

	// MaxPoolSize 是**每台服务器**的连接数上限。
	//
	// ⚠️ 是"每台"而不是"总共"：副本集有 3 个节点时，一个客户端的连接上限是
	// 3 × MaxPoolSize。多实例部署要按 `实例数 × 节点数 × MaxPoolSize` 核对
	// 服务端的 connection 上限（mongod 默认 65536）。
	//
	// 0 表示使用驱动的默认值 100。
	//
	// ⚠️ 注意与 URI 里的 `maxPoolSize=0` 区分：**URI 里的 0 表示"不限制"**，
	// 而本字段的 0 表示"用默认值"。要不限制只能写在 URI 里。
	//
	// 注意 Open 之后 Config() 返回的是**生效值**：URI 带了 maxPoolSize 时，
	// 这里会被改写为 URI 的取值（否则 PoolStats.MaxOpen 会与实际不符）。
	MaxPoolSize int `mapstructure:"max_pool_size"`

	// MinPoolSize 是池中保持的**最小**连接数（预先建好，不等请求来才建）。
	//
	// 默认 0 = 懒建连（按需创建）。设成 >0 可以削掉"首次请求"的抖动，
	// 代价是每个实例都会常驻这些连接 —— 在"实例多 × 节点多"的部署里
	// 这是一笔不小的固定开销，通常不值得。
	//
	// 0 表示使用驱动的默认值 0。
	MinPoolSize int `mapstructure:"min_pool_size"`

	// MaxConnecting 是**同时**在建的连接数上限（建连并发度）。
	//
	// 它保护的是服务端而不是客户端：池空了以后如果几十个 goroutine
	// 同时发起建连，mongod 会被握手风暴打慢。驱动默认 2，与 mongosh 一致。
	// 只有在"建连 RTT 很高（跨机房）且明显是建连瓶颈"时才考虑调大。
	//
	// 0 表示使用驱动的默认值 2。
	MaxConnecting int `mapstructure:"max_connecting"`

	// MaxConnIdleTime 是连接的**空闲**存活时间，超过就被关闭归还给系统。
	//
	// ⚠️ 0 表示**不因空闲而关闭**（这是驱动默认值），也就是池一旦长到
	// MaxPoolSize 就会一直占着那些连接。低频服务或实例数很多时，
	// 建议设成 5m~10m，让波谷时段把连接还回去。
	MaxConnIdleTime time.Duration `mapstructure:"max_conn_idle_time"`

	// ------------------------------------------------------------------
	// 超时：这四项职责完全不同，最常见的踩坑点都在这
	// ------------------------------------------------------------------

	// ConnectTimeout 是**单次建连**（TCP + TLS + 握手 + 认证）的超时。
	//
	// ⚠️ 它**不是**单次操作的超时，操作超时看 SocketTimeout / OperationTimeout。
	// 0 表示使用驱动的默认值 30s。
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`

	// ServerSelectionTimeout 是**选不到可用节点**时等待多久才放弃。
	//
	// 这个参数决定了"MongoDB 挂了"多久之后业务才能拿到错误：
	// 驱动默认 30s，意味着主节点宕机后的请求会**挂起 30 秒**才返回失败，
	// 而不是立刻失败。对在线服务 30s 往往太长（调用方的超时早就到了）。
	// 建议设成比上游超时略小（如 3s~5s）。
	//
	// 0 表示使用驱动的默认值 30s。
	ServerSelectionTimeout time.Duration `mapstructure:"server_selection_timeout"`

	// OperationTimeout 是客户端侧的**全操作**超时（CSOT，Client-Side Operation Timeout），
	// 对应 URI 的 timeoutMS。
	//
	// 它用来给那些"忘了传 ctx 超时"的调用兜底。
	//
	// ⚠️ **这是本包最容易误用的一个配置。** 驱动的实现是：
	//
	//	if timeout == nil || IsTimeoutContext(parent) {
	//		return parent, cancel     // 什么都不做
	//	}
	//
	// 其中 IsTimeoutContext 判定的是"ctx 上**有没有** deadline"（不看长短）。
	// 也就是说：**只要调用方的 ctx 带了任意 deadline，本字段就完全失效**，
	// 由那个 deadline 单独说了算。举两个会踩到的例子：
	//
	//	// 例子一：HTTP 请求的 ctx 常有很长的 deadline
	//	ctx, _ := context.WithTimeout(r.Context(), 5*time.Minute)
	//	coll.Find(ctx, ...)   // OperationTimeout=5s 不生效，这个操作可以跑 5 分钟
	//
	//	// 例子二：只有完全不设 deadline 时，OperationTimeout 才起作用
	//	coll.Find(context.Background(), ...)   // 受 5s 限制 ✅
	//
	// 所以它是"兜底"而不是"上限"：想让 5s 一定生效，必须自己给每次操作套 ctx。
	//
	// 0 表示不限制（驱动默认）。
	OperationTimeout time.Duration `mapstructure:"operation_timeout"`

	// HeartbeatInterval 是后台探测各节点状态的间隔（SDAM 心跳）。
	//
	// 它决定了"主从切换后多久被发现"：默认 10s 意味着最坏情况下
	// 有近 10s 的窗口仍在往旧主节点发请求（那些请求会失败并被驱动重试）。
	// 调小可以更快收敛，代价是节点多时心跳流量线性增长。
	//
	// 驱动有 500ms 的硬下限，低于它会被拒绝。
	// 0 表示使用驱动的默认值 10s。
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`

	// ------------------------------------------------------------------
	// 建连行为（本包自己的重试策略，不是驱动参数）
	// ------------------------------------------------------------------

	// DialAttempts 是初始化时的最大尝试次数（含第一次）。
	//
	// 0 表示默认 3 次，1 表示失败即返回，负值表示无限重试直到 Close。
	//
	// ⚠️ 启动最坏耗时 ≈ DialAttempts × DialProbeTimeout。
	// 默认配置下最坏 3 × 5s = 15s。这个乘积是可控的，因为每次尝试的探活
	// 用的是本包自己的 DialProbeTimeout，**不受 ServerSelectionTimeout（默认 30s）影响**。
	DialAttempts int `mapstructure:"dial_attempts"`

	// DialProbeTimeout 是**每次建连尝试的探活上限**（本包自己的旋钮，不是驱动参数）。
	//
	// 为什么必须单独有它，而不是复用 ServerSelectionTimeout：
	// 后者默认 30s，是给**业务操作**用的旋钮。若拿它来卡启动期的探活，
	// DialAttempts=3 时最坏要挂 90 秒才启动失败 —— 停机发布时这是灾难。
	// 拆开之后，两件事各自有独立的、语义清晰的旋钮：
	//
	//	启动期探活   → DialProbeTimeout（默认 5s，属于"启动流程"）
	//	运行期选节点 → ServerSelectionTimeout（默认 30s，属于"业务请求"）
	//
	// 它同时作用于 Open 时的首次验证、后台健康检查（HealthCheck / monitor 的 Ping）、
	// 以及 Close / 重建时的 drain 等待。
	//
	// 0 表示默认 5s。
	DialProbeTimeout time.Duration `mapstructure:"dial_probe_timeout"`

	// DialBackoff / DialMaxBackoff 是重试退避的起始值与上限，0 表示默认 1s / 30s。
	DialBackoff    time.Duration `mapstructure:"dial_backoff"`
	DialMaxBackoff time.Duration `mapstructure:"dial_max_backoff"`

	// ------------------------------------------------------------------
	// 健康监控
	// ------------------------------------------------------------------

	// HealthCheckInterval 是后台探活间隔，0 表示默认 30s。
	HealthCheckInterval time.Duration `mapstructure:"health_check_interval"`

	// RebuildAfterFailures 表示连续探活失败多少次后主动重建客户端。
	//
	// 默认 0，即**不重建**。MongoDB 驱动本来就在后台维护拓扑并自动重连，
	// 服务端恢复后会自行可用，探活失败通常并不需要重建。
	// 只有在"客户端状态确实卡死"这类场景才需要打开它（如设为 3）。
	RebuildAfterFailures int `mapstructure:"rebuild_after_failures"`

	// ------------------------------------------------------------------
	// 覆盖项：仅当 URI 里没有对应参数时才生效
	// ------------------------------------------------------------------

	// AppName 对应 URI 的 appName，会随每次建连握手上报给服务端。
	//
	// 它是**排查问题时最有性价比的一项配置**：服务端的
	// `db.currentOp()` / mongod 日志 / `$currentOp` 都能看到它，
	// 于是"这条慢查询是哪个服务发的"一眼就有答案。
	// 建议一定要设，用服务名 + 环境（"order-api@prod"）。
	AppName string `mapstructure:"app_name"`

	// ReplicaSet 对应 URI 的 replicaSet，用于副本集名校验。
	//
	// 设置后驱动会**校验**实际连接的副本集名是否一致，能挡住"连错集群"
	// 这类事故（例如把测试环境的 URI 指向了生产集群）。生产环境建议显式设置。
	ReplicaSet string `mapstructure:"replica_set"`

	// ReadPreference 对应 URI 的 readPreference。
	//
	// 取值：primary / primaryPreferred / secondary / secondaryPreferred / nearest
	// （大小写与连字符不敏感，"primary-preferred" 也接受）。
	//
	// ⚠️ 默认 primary。读写分离到 secondary 时要清楚代价：
	// 从节点可能有复制延迟，读到的数据可能落后（**读己之写**会失效）。
	ReadPreference string `mapstructure:"read_preference"`

	// ReadConcern 对应 URI 的 readConcernLevel。
	//
	// 取值：local / available / majority / linearizable / snapshot。
	// 需要"读到的数据不会被回滚"时应至少用 majority。
	ReadConcern string `mapstructure:"read_concern"`

	// WriteConcern 对应 URI 的 w。
	//
	// 取值："majority"（多数派落盘）/ "1"（主节点确认）/ "0"（不等待确认）/
	// 自定义 tag set。数字与字符串都接受。
	//
	// ⚠️ "0" 表示**根本不等待服务端确认**，调用方拿不到任何写入结果，
	// 丢了也不知道。除了日志、埋点这类可丢数据，不要用它。
	WriteConcern string `mapstructure:"write_concern"`

	// WriteConcernJournal 对应 URI 的 journal：是否要求写入已落到磁盘日志。
	//
	// 用 *bool 而不是 bool，是为了区分"没配"与"配成 false" ——
	// 前者应当让 URI 或服务端默认值生效，后者是明确的"不要落盘"。
	WriteConcernJournal *bool `mapstructure:"write_concern_journal"`

	// RetryWrites / RetryReads 对应 URI 的 retryWrites / retryReads。
	//
	// 驱动默认都为 true，**建议保持打开**：它会自动重试那些
	// "服务端确认可安全重试"的失败（网络抖动、主从切换）。
	//
	// 用 *bool 的原因同上：nil 表示未配置。
	RetryWrites *bool `mapstructure:"retry_writes"`
	RetryReads  *bool `mapstructure:"retry_reads"`

	// DirectConnection 对应 URI 的 directConnection：只连给定主机，不做拓扑发现。
	//
	// 单节点部署（开发环境、某些云托管实例）需要它；
	// 副本集上用它会**绕过故障转移** —— 连的那个节点挂了就是彻底不可用。
	//
	// ⚠️ 与"多主机"和"SRV 形式"互斥，同时配置会被驱动拒绝（本包在启动期就会报错）。
	DirectConnection *bool `mapstructure:"direct_connection"`

	// LoadBalanced 对应 URI 的 loadBalanced，用于 Load Balancer 前置的部署
	// （Atlas Serverless、部分云托管形态）。
	//
	// ⚠️ 打开后**多主机与 replicaSet 都不允许**，而且必须配合 SRV 或 LB 地址。
	// 自建副本集不要打开它。
	LoadBalanced *bool `mapstructure:"load_balanced"`

	// ------------------------------------------------------------------
	// 日志
	// ------------------------------------------------------------------

	// LogLevel 控制命令日志：silent / error / warn / info，空值按 info。
	//
	//	silent  一条都不打（指标仍然照常上报）
	//	error   只打失败的命令
	//	warn   失败的命令 + 超过 SlowThreshold 的慢命令
	//	info   全部命令
	//
	// ⚠️ warn / info 级别下命令日志的量与业务 QPS 成正比，
	// 生产环境建议用 warn 或 error。
	LogLevel string `mapstructure:"log_level"`

	// SlowThreshold 是慢操作阈值（如 200ms），0 表示默认 200ms，
	// 负值表示不判定慢操作（即 warn 级别下只打失败）。
	SlowThreshold time.Duration `mapstructure:"slow_threshold"`
}

// 本包补齐的默认值。**只包含"驱动默认值不合适或需要显式可见"的项**，
// 其余（RetryWrites、MaxConnecting 等）保持驱动默认，不在这里重复声明。
const (
	defaultDialAttempts        = 3
	defaultDialProbeTimeout    = 5 * time.Second
	defaultDialBackoff         = time.Second
	defaultDialMaxBackoff      = 30 * time.Second
	defaultHealthCheckInterval = 30 * time.Second
	defaultSlowThreshold       = 200 * time.Millisecond

	// defaultPoolSize 是驱动默认的 MaxPoolSize，本包显式设置它。
	//
	// 值与驱动默认相同（行为不变），显式设置的意义是**可见**：
	// 它会被写进启动日志，也会成为 metrics.PoolStats.MaxOpen，
	// 于是"到底允许多少连接"不用去翻驱动源码。
	defaultPoolSize = 100
	// defaultMaxConnecting 同样是驱动默认值（2），显式设置以求可见。
	defaultMaxConnecting = 2

	// minSaneDuration 是"单位哨兵"的下限：任何配置成正值却小于 1ms 的时长，
	// 几乎一定是把"秒/毫秒"的整数直接写进了 Duration 字段
	// （例如 connect_timeout: 5 → 5ns）。
	minSaneDuration = time.Millisecond

	// minHeartbeatInterval 是驱动对心跳间隔的硬下限。
	// 放在这里是为了在 validate 阶段就给出可读的错误，
	// 而不是等驱动的 "heartbeatFrequencyMS must exceed the minimum heartbeat interval of 500ms"。
	minHeartbeatInterval = 500 * time.Millisecond
)

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

// normalize 补齐默认值。
func (c Config) normalize() Config {
	if c.MaxPoolSize == 0 {
		c.MaxPoolSize = defaultPoolSize
	}
	if c.MaxConnecting == 0 {
		c.MaxConnecting = defaultMaxConnecting
	}
	if c.DialAttempts == 0 {
		c.DialAttempts = defaultDialAttempts
	}
	if c.DialBackoff <= 0 {
		c.DialBackoff = defaultDialBackoff
	}
	if c.DialProbeTimeout <= 0 {
		c.DialProbeTimeout = defaultDialProbeTimeout
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
	if c.SlowThreshold == 0 {
		c.SlowThreshold = defaultSlowThreshold
	}

	// ReadPreference 统一成驱动的规范写法。
	//
	// 起因是大小写与分隔符的不一致："PrimaryPreferred" / "primary-preferred"
	// / "primary_preferred" 在 URI 里都是合法的，而驱动的 ModeFromString
	// 只认全小写无分隔的 "primarypreferred"。这里先归一化再交给驱动，
	// 免得同一个取值"写在 URI 里能用、写在 Config 里报错"。
	//
	// 归一化后的取值用驱动自己的 Mode.String()，即驼峰形式（primaryPreferred）
	// —— 它比 "primarypreferred" 好读，而且与驱动其它输出一致。
	if c.ReadPreference != "" {
		if mode, err := parseReadPreference(c.ReadPreference); err == nil {
			c.ReadPreference = mode.String()
		}
	}
	c.LogLevel = strings.ToLower(strings.TrimSpace(c.LogLevel))

	return c
}

// validate 检查**原始**配置是否可用。
//
// 三层校验，顺序不能反：
//  1. 结构性问题（URI 能不能解析、字段取值是否合理）—— 给出中文可读的错误；
//  2. 驱动级约束交给 options.ClientOptions.Validate()（minPoolSize ≤ maxPoolSize、
//     directConnection 与多主机互斥、heartbeat 下限、loadBalanced 组合约束等），
//     这些不值得重写一遍，重写只会与驱动版本脱节。
func (c Config) validate() error {
	// 第 1 层：结构化校验（含 URI 解析）。
	info, err := parseURI(c.URI)
	if err != nil {
		return err
	}
	if err := validateURICompat(info); err != nil {
		return err
	}

	if c.MaxPoolSize < 0 {
		return fmt.Errorf("%w: MaxPoolSize 不能为负（0 表示使用驱动默认值 %d）",
			ErrInvalidConfig, defaultPoolSize)
	}
	if c.MinPoolSize < 0 {
		return fmt.Errorf("%w: MinPoolSize 不能为负（0 表示使用驱动默认值）", ErrInvalidConfig)
	}
	if c.MaxConnecting < 0 {
		return fmt.Errorf("%w: MaxConnecting 不能为负（0 表示使用驱动默认值 %d）",
			ErrInvalidConfig, defaultMaxConnecting)
	}
	if c.RebuildAfterFailures < 0 {
		return fmt.Errorf("%w: RebuildAfterFailures 不能为负", ErrInvalidConfig)
	}

	// MinPoolSize 的默认上限保护。
	//
	// 这个检查不能用 options.Validate() 代替：那里只在**两个字段都被显式设置**时
	// 才比较 MinPoolSize ≤ MaxPoolSize。若只配了 MinPoolSize=200 而 MaxPoolSize
	// 留空（此时生效的是驱动默认 100），它就不会报错，
	// 而结果是"要求至少 200 个连接、上限却只有 100"的矛盾配置在运行时静默生效。
	if c.MaxPoolSize == 0 && c.MinPoolSize > defaultPoolSize {
		return fmt.Errorf(
			"%w: MinPoolSize=%d 超过了驱动默认的 MaxPoolSize=%d；"+
				"请显式设置 MaxPoolSize，或把 MinPoolSize 调到 %d 以内",
			ErrInvalidConfig, c.MinPoolSize, defaultPoolSize, defaultPoolSize)
	}
	if c.MaxPoolSize > 0 && c.MinPoolSize > c.MaxPoolSize {
		return fmt.Errorf("%w: MinPoolSize=%d 不能大于 MaxPoolSize=%d",
			ErrInvalidConfig, c.MinPoolSize, c.MaxPoolSize)
	}

	// 心跳下限：驱动也会拦，但它的错误信息里没有"该改成多少"。
	if c.HeartbeatInterval > 0 && c.HeartbeatInterval < minHeartbeatInterval {
		return fmt.Errorf(
			"%w: HeartbeatInterval=%v 小于驱动的硬下限 %v；"+
				"心跳过密会放大节点多时的探测流量",
			ErrInvalidConfig, c.HeartbeatInterval, minHeartbeatInterval)
	}

	if err := c.validateDurations(); err != nil {
		return err
	}

	if c.ReadPreference != "" {
		if _, err := parseReadPreference(c.ReadPreference); err != nil {
			return err
		}
	}
	if c.ReadConcern != "" {
		// 驱动对 readConcernLevel 只做透传，写错了要等服务端报错才暴露，
		// 所以在这里按规范取值白名单拦一道。
		switch strings.ToLower(c.ReadConcern) {
		case "local", "available", "majority", "linearizable", "snapshot":
		default:
			return fmt.Errorf(
				"%w: ReadConcern=%q 非法，可选 local/available/majority/linearizable/snapshot",
				ErrInvalidConfig, c.ReadConcern)
		}
	}
	if c.WriteConcern != "" {
		if err := validateWriteConcern(c.WriteConcern); err != nil {
			return err
		}
	}

	if err := validateLogLevel(c.LogLevel); err != nil {
		return err
	}

	// 第 2 层（驱动级校验）不在这里做。
	//
	// 原因是它有副作用：构造驱动选项时 ApplyURI 会**读取 TLS 相关的证书文件**，
	// 而构造好的选项对象正是建连要用的那一份。在这里顺带构造一次、建连时再构造一次，
	// 既有开销，也可能因为文件在两次调用之间被改动而得到不一致的结果。
	//
	// 所以驱动级校验统一放在 clientOptions() 里，由 Open 调用一次，
	// 错误同样以 ErrInvalidConfig 返回（URI/选项写错是配置问题，不是连接问题）。
	return nil
}

// validateDurations 是"单位哨兵"。
//
// 这类配置不会报错，只会让客户端表现异常：例如 ServerSelectionTimeout=3ns
// 意味着"选节点几乎不给时间"，任何一次轻微抖动都直接失败；
// ConnectTimeout=30ns 意味着永远连不上。都属于最难排查的性能问题，
// 所以必须在这里大声失败。
func (c Config) validateDurations() error {
	durations := []struct {
		name string
		val  time.Duration
	}{
		{"MaxConnIdleTime", c.MaxConnIdleTime},
		{"ConnectTimeout", c.ConnectTimeout},
		{"ServerSelectionTimeout", c.ServerSelectionTimeout},
		{"OperationTimeout", c.OperationTimeout},
		{"HeartbeatInterval", c.HeartbeatInterval},
		{"DialBackoff", c.DialBackoff},
		{"DialProbeTimeout", c.DialProbeTimeout},
		{"DialMaxBackoff", c.DialMaxBackoff},
		{"HealthCheckInterval", c.HealthCheckInterval},
	}
	for _, d := range durations {
		if d.val > 0 && d.val < minSaneDuration {
			return fmt.Errorf(
				"%w: %s=%v 过小，疑似单位写错；请写成带单位的字符串，例如 %s: 5s",
				ErrInvalidConfig, d.name, d.val, d.name)
		}
	}
	// SlowThreshold 单独判断：0 与负值都合法（负值表示不判定慢操作）。
	if c.SlowThreshold > 0 && c.SlowThreshold < minSaneDuration {
		return fmt.Errorf(
			"%w: SlowThreshold=%v 过小，疑似单位写错；请写成如 SlowThreshold: 200ms",
			ErrInvalidConfig, c.SlowThreshold)
	}
	return nil
}

// clientOptions 构造驱动选项，并做驱动级校验。**这是唯一构造选项的地方。**
//
// 调用顺序是本文件最需要留意的细节：
//
//  1. 先用 Config 里的值设置各项选项；
//  2. **最后** ApplyURI(uri)。
//
// 驱动文档明确写着后调用的 Set* 会覆盖先调用的（包括 ApplyURI），
// 所以第 2 步让 URI 覆盖同名项，就得到了"URI 优先"的语义，
// 不需要自己解析 URI 去判断某个参数有没有出现。
//
// 返回值里的 uriInfo 供调用方复用（拿默认库名、主机列表），避免重复解析。
//
// 注意它有副作用：ApplyURI 对含 TLS 参数的 URI 会**读取本地证书文件**。
// 因此它只应在初始化时调用一次（见 Config.validate 的说明）。
func (c Config) clientOptions() (*options.ClientOptions, *uriInfo, error) {
	info, err := parseURI(c.URI)
	if err != nil {
		return nil, nil, err
	}

	opts := options.Client()
	c.applyTo(opts)
	// ApplyURI 用归一化后的串：驱动只认 mongodb://（见 uriInfo.driverURI）。
	opts.ApplyURI(info.driverURI())

	if err := opts.Validate(); err != nil {
		return nil, nil, fmt.Errorf("%w: 客户端选项校验失败: %v", ErrInvalidConfig, err)
	}
	return opts, info, nil
}

// applyTo 把 Config 里显式设置的项写进驱动选项。
//
// 只写**非零值**：零值代表"没配"，此时应当让 URI 或驱动默认值生效。
// 唯一例外是 MaxPoolSize / MaxConnecting —— 它们在 normalize 里已被补齐成
// 驱动默认值，写进去不会改变行为，只让取值在日志与指标里可见。
func (c Config) applyTo(opts *options.ClientOptions) {
	// ---- 连接池 ----
	if c.MaxPoolSize > 0 {
		opts.SetMaxPoolSize(uint64(c.MaxPoolSize))
	}
	if c.MinPoolSize > 0 {
		opts.SetMinPoolSize(uint64(c.MinPoolSize))
	}
	if c.MaxConnecting > 0 {
		opts.SetMaxConnecting(uint64(c.MaxConnecting))
	}
	// MaxConnIdleTime=0 是合法取值（不因空闲关闭），所以只在 >0 时设置。
	if c.MaxConnIdleTime > 0 {
		opts.SetMaxConnIdleTime(c.MaxConnIdleTime)
	}

	// ---- 超时 ----
	// 这几个字段的 0 都表示"不限制/用驱动默认"，一律不设置。
	if c.ConnectTimeout > 0 {
		opts.SetConnectTimeout(c.ConnectTimeout)
	}
	if c.ServerSelectionTimeout > 0 {
		opts.SetServerSelectionTimeout(c.ServerSelectionTimeout)
	}
	if c.OperationTimeout > 0 {
		// SetTimeout 就是 CSOT（URI 里的 timeoutMS）。
		//
		// 它顶替了 v1 的 socketTimeoutMS 与各处 MaxTime —— 那两个在 v2 里
		// 已被删除，见 validateURICompat。
		opts.SetTimeout(c.OperationTimeout)
	}
	if c.HeartbeatInterval > 0 {
		opts.SetHeartbeatInterval(c.HeartbeatInterval)
	}

	// ---- 标识 ----
	if c.AppName != "" {
		// 实例名（Name）只进指标标签，**不**用它兜底 AppName：
		// 否则服务端看到的应用名会随部署形态变化，反而不好检索。
		opts.SetAppName(c.AppName)
	}

	// ---- 拓扑 ----
	if c.ReplicaSet != "" {
		opts.SetReplicaSet(c.ReplicaSet)
	}
	if c.DirectConnection != nil {
		opts.SetDirect(*c.DirectConnection)
	}
	if c.LoadBalanced != nil {
		opts.SetLoadBalanced(*c.LoadBalanced)
	}

	// ---- 读写关注 ----
	if c.ReadPreference != "" {
		// 到这里已经过 validate，不会失败。
		if mode, err := parseReadPreference(c.ReadPreference); err == nil {
			if rp, err := readpref.New(mode); err == nil {
				opts.SetReadPreference(rp)
			}
		}
	}
	if c.ReadConcern != "" {
		opts.SetReadConcern(&readconcern.ReadConcern{
			Level: strings.ToLower(c.ReadConcern),
		})
	}
	if wc := buildWriteConcern(c.WriteConcern, c.WriteConcernJournal); wc != nil {
		opts.SetWriteConcern(wc)
	}

	// ---- 重试 ----
	if c.RetryWrites != nil {
		opts.SetRetryWrites(*c.RetryWrites)
	}
	if c.RetryReads != nil {
		opts.SetRetryReads(*c.RetryReads)
	}
}

// effectiveMaxPoolSize 返回驱动**实际生效**的每节点连接上限。
//
// 为什么不能直接用 Config.MaxPoolSize：URI 里的 maxPoolSize 会覆盖它
// （见文件头"URI 永远优先"的说明），而 PoolStats.MaxOpen、启动日志、
// Status 都读 Config。不回写的话会出现"URI 写 7、指标报 100"的错位 ——
// 而"池吃满"的告警正是基于这个数。
//
// 注意 0 的语义差异：**URI 里的 0 表示不限制**（没有硬上限），
// 而 Config 里的 0 表示"用驱动默认值 100"（normalize 已把它补齐成 100，
// 所以走到这里时 Config 侧不会是 0）。返回值如实反映 URI 的意图。
func effectiveMaxPoolSize(opts *options.ClientOptions) int {
	if opts == nil || opts.MaxPoolSize == nil {
		return 0
	}
	return int(*opts.MaxPoolSize)
}

// parseReadPreference 解析读偏好，对大小写与分隔符宽容。
//
// 驱动的 readpref.ModeFromString 只认 "primarypreferred" 这种全小写无分隔的写法，
// 而 URI 规范与各语言驱动普遍用 "primaryPreferred" / "primary-preferred"。
// 这里先归一化再交给驱动，避免"同一个取值写在 URI 里能用、写在 Config 里报错"。
func parseReadPreference(s string) (readpref.Mode, error) {
	normalized := strings.NewReplacer("-", "", "_", "", " ", "").
		Replace(strings.ToLower(strings.TrimSpace(s)))

	mode, err := readpref.ModeFromString(normalized)
	if err != nil {
		return 0, fmt.Errorf(
			"%w: ReadPreference=%q 非法，可选 primary/primaryPreferred/secondary/"+
				"secondaryPreferred/nearest", ErrInvalidConfig, s)
	}
	return mode, nil
}

// validateWriteConcern 校验写关注取值。
//
// 驱动对 w 是 any 透传（int 或 string 都收），写错也只能等服务端报错，
// 所以这里按规范拦一道：数字，或 "majority"，或非空 tag set 字符串。
func validateWriteConcern(s string) error {
	v := strings.TrimSpace(s)
	if v == "" {
		return fmt.Errorf("%w: WriteConcern 不能是空白字符串（不配置请留空字段）",
			ErrInvalidConfig)
	}
	if _, err := parseWriteConcernW(v); err != nil {
		return err
	}
	return nil
}

// parseWriteConcernW 把 w 取值转成驱动要的类型（int 或 string）。
//
// 数字用 int 传，其余按 tag set 字符串传 —— 与 drivers 规范一致：
// w 只接受 int32 或 string，"majority" 是保留的 tag set。
func parseWriteConcernW(s string) (any, error) {
	if n, err := parseIntStrict(s); err == nil {
		if n < 0 {
			return nil, fmt.Errorf("%w: WriteConcern=%q 不能为负", ErrInvalidConfig, s)
		}
		return n, nil
	}
	// 非数字：当作 tag set 名。规范要求非空且不含空白。
	if strings.ContainsAny(s, " \t") {
		return nil, fmt.Errorf(
			"%w: WriteConcern=%q 非法 —— 只能是数字、%q 或 tag set 名（不含空白）",
			ErrInvalidConfig, s, "majority")
	}
	return s, nil
}

// buildWriteConcern 组装驱动的 writeconcern。
//
// w 与 journal 都未配置时返回 nil，让 URI 或服务端默认值生效。
func buildWriteConcern(w string, journal *bool) *writeconcern.WriteConcern {
	if strings.TrimSpace(w) == "" && journal == nil {
		return nil
	}
	wc := &writeconcern.WriteConcern{Journal: journal}
	if strings.TrimSpace(w) != "" {
		if v, err := parseWriteConcernW(strings.TrimSpace(w)); err == nil {
			wc.W = v
		}
	}
	return wc
}

// validateLogLevel 校验日志级别。
func validateLogLevel(level string) error {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "", "silent", "error", "warn", "info":
		return nil
	default:
		return fmt.Errorf("%w: LogLevel=%q 非法，可选 silent/error/warn/info",
			ErrInvalidConfig, level)
	}
}

// LogLevel 的归一化取值。用字符串常量而不是 iota，
// 是为了让"配置里的取值"和"日志里的取值"长得一模一样，排查时不用换算。
const (
	logLevelSilent = "silent"
	logLevelError  = "error"
	logLevelWarn   = "warn"
	logLevelInfo   = "info"
)

// effectiveLogLevel 返回生效的日志级别（空值按 info）。
func (c Config) effectiveLogLevel() string {
	if c.LogLevel == "" {
		return logLevelInfo
	}
	return strings.ToLower(strings.TrimSpace(c.LogLevel))
}

// validateURICompat 拦下"在 v2 里已经失效、但驱动仍会解析"的 URI 参数。
//
// 目前只发现一个，但它非常值得单独拦：**socketTimeoutMS**。
//
// driver v1 有这个参数（单次网络读写超时），v2 把它**删除**了
// （见驱动仓库的 docs/migration-2.0.md「MaxTime」一节：v2 要求用
// ClientOptions.Timeout 或 ctx deadline 来控制超时）。
// 但 v2 的连接串解析器仍然认识 sockettimeoutms 并把它存进 ConnString ——
// 只是 ApplyURI **不再把它应用到选项上**。
//
// 结果是最坏的一类故障：配置看起来完全正常、驱动也不报错、日志里什么异常都没有，
// 而你以为存在的"操作超时"根本不存在，慢查询会一直挂着。
// 所以这里选择**启动期直接报错**，而不是静默忽略。
func validateURICompat(info *uriInfo) error {
	if value, ok := info.get("sockettimeoutms"); ok {
		return fmt.Errorf(
			"%w: URI 参数 socketTimeoutMS=%s 在 mongo-driver v2 中**已失效**"+
				"（驱动仍会解析它，但不再应用到客户端选项上，属于静默不生效）；"+
				"请改用 Config.OperationTimeout（URI 的 timeoutMS）或给操作传入带 deadline 的 ctx",
			ErrInvalidConfig, value)
	}
	return nil
}

// parseIntStrict 解析十进制整数，拒绝空白、正负号以外的多余字符与空串。
//
// 不用 strconv.Atoi 直接判定，是因为它会接受 "+1" 与前后空白之外的写法差异
// 不适合用在"判断用户写的是不是数字"这个语义上；这里要求整个字符串都是数字
// （允许一个前导 +/-），于是 "1.5"、"1e3"、" 1 " 都会走到 tag set 分支。
func parseIntStrict(s string) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("空字符串不是数字")
	}
	start := 0
	if s[0] == '+' || s[0] == '-' {
		start = 1
	}
	if start == len(s) {
		return 0, fmt.Errorf("%q 只有符号位，不是数字", s)
	}
	n := 0
	for _, r := range s[start:] {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("%q 含非数字字符", s)
		}
		n = n*10 + int(r-'0')
	}
	if s[0] == '-' {
		n = -n
	}
	return n, nil
}
