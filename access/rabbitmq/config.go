package rabbitmq

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/zavierswong/go-infra/metrics"
)

// ExchangeKind 是 AMQP 0-9-1 定义的四种交换机类型，决定消息如何被路由。
type ExchangeKind string

const (
	// ExchangeDirect 按 routing key 精确匹配。点对点、按业务类型分流用它。
	ExchangeDirect ExchangeKind = "direct"

	// ExchangeFanout 忽略 routing key，广播到所有绑定队列。
	// 绑定时 routing key 必须为空，否则会与已有绑定冲突。
	ExchangeFanout ExchangeKind = "fanout"

	// ExchangeTopic 按 "." 分段匹配，"*" 匹配一段，"#" 匹配零到多段。
	ExchangeTopic ExchangeKind = "topic"

	// ExchangeHeaders 按消息 headers 匹配，routing key 被忽略。
	// 必须在 Binding.Args 里给出 "x-match"，取值 "all" 或 "any"。
	ExchangeHeaders ExchangeKind = "headers"
)

func (k ExchangeKind) valid() bool {
	switch k {
	case ExchangeDirect, ExchangeFanout, ExchangeTopic, ExchangeHeaders:
		return true
	}
	return false
}

// TLSConfig 是 amqp(s) 连接的安全配置。
type TLSConfig struct {
	Enable             bool   `mapstructure:"enable"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
	ServerName         string `mapstructure:"server_name"`
	// CAFile 指定 PEM 格式的根证书；为空则使用系统信任链。
	CAFile string `mapstructure:"ca_file"`
}

// Exchange 声明一个交换机。
type Exchange struct {
	Name       string       `mapstructure:"name"`
	Kind       ExchangeKind `mapstructure:"kind"` // 空值按 direct 处理
	Durable    bool         `mapstructure:"durable"`
	AutoDelete bool         `mapstructure:"auto_delete"`
	Internal   bool         `mapstructure:"internal"`
	// AlternateExchange 对应 "x-alternate-exchange"：消息在此交换机上路由不到时，
	// 由 broker 转投到该备选交换机 —— 比 mandatory 更可靠，因为不依赖生产者在线。
	AlternateExchange string     `mapstructure:"alternate_exchange"`
	Args              amqp.Table `mapstructure:"args"`
}

// Queue 声明一个队列。
//
// 相比旧版补上了 AutoDelete/Exclusive 的真实生效（旧版硬编码为 false 导致字段空转），
// 并去掉了 NoWait —— 跳过服务端确认会让声明失败无从感知，属于反模式。
type Queue struct {
	Name       string `mapstructure:"name"`
	Durable    bool   `mapstructure:"durable"`
	AutoDelete bool   `mapstructure:"auto_delete"`
	Exclusive  bool   `mapstructure:"exclusive"`
	// Args 承载队列级扩展能力，见 DLXArgs / QuorumArgs / PriorityArgs / LazyArgs。
	Args amqp.Table `mapstructure:"args"`
}

// Binding 把队列绑定到交换机。
type Binding struct {
	Queue      string `mapstructure:"queue"`
	Exchange   string `mapstructure:"exchange"`
	RoutingKey string `mapstructure:"routing_key"`
	// Args 仅 headers 交换机需要，须含 "x-match"。
	Args amqp.Table `mapstructure:"args"`
}

// DeadLetter 描述死信的去向（对应队列的 x-dead-letter-* 参数）。
type DeadLetter struct {
	// Exchange 为空表示投递到默认交换机，此时 RoutingKey 必须是一个已存在的队列名。
	Exchange   string `mapstructure:"exchange"`
	RoutingKey string `mapstructure:"routing_key"`
}

// Config 是连接与拓扑的完整配置。
type Config struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	VHost    string `mapstructure:"vhost"` // 建议不带前导 "/"（如 "app_vhost"）；空值等价于 "/"
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`

	Tls TLSConfig `mapstructure:"tls"`

	// ------------------------------------------------------------------
	// 标识与可观测性
	// ------------------------------------------------------------------

	// Name 是实例名，用于区分同一个进程里的多个 RabbitMQ 连接。
	//
	// 注意与 Exchange.Name / Queue.Name 的区别：那两个是 broker 上的实体名，
	// 这个只用于监控标签（metrics.Event.Instance）。为空时指标会缺少实例维度，
	// 多实例部署下几套 MQ 的曲线会被合并，反而掩盖问题。
	Name string `mapstructure:"name"`

	// Observer 接收消息流上的 metrics.Event：
	// publish / consume / ack / nack / return / reconnect / channel_rebuild。
	//
	// 一条正常消息会产生 2 个事件（consume + ack），所以观察者必须按
	// "非阻塞 + 有界缓冲"实现，否则会拖慢消费速率。
	// 详见 github.com/zavierswong/go-infra/metrics 与 README 的「metrics 暴露」章节。
	Observer metrics.Observer `mapstructure:"-"`

	// Heartbeat 心跳间隔，0 表示默认 10s。
	Heartbeat time.Duration `mapstructure:"heartbeat"`
	// ConnectTimeout 单次建连超时，0 表示默认 10s。
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`
	// DialAttempts 是首次建连的最大尝试次数（含第一次）。
	// 0 表示默认 4 次；负值表示无限重试直到客户端被关闭；设为 1 表示失败即返回。
	// 旧版在 sync.Once 内无限重试，会让首个调用方永久阻塞，这里改为显式可控。
	DialAttempts int `mapstructure:"dial_attempts"`
	// DialBackoff / DialMaxBackoff 重试退避的起始值与上限，0 表示默认 1s / 30s。
	DialBackoff    time.Duration `mapstructure:"dial_backoff"`
	DialMaxBackoff time.Duration `mapstructure:"dial_max_backoff"`

	// Confirm 是否为生产通道开启 publisher confirm。默认 true。
	// 关闭后 Publish 无法得知 broker 是否真的收下了消息，只应在极在意吞吐时关闭。
	Confirm bool `mapstructure:"confirm"`
	// ConfirmTimeout 等待 broker 确认的超时，0 表示默认 5s。
	ConfirmTimeout time.Duration `mapstructure:"confirm_timeout"`
	// ReturnWindow 是 mandatory 发布后等待 basic.return 的窗口：
	// 0 表示默认 100ms，负值表示不等待（只走异步回调）。
	//
	// 背景：broker 对不可路由的消息会先发 basic.return、再回 confirm ack，
	// 但本进程把 return 从库的缓冲通道派发给调用方需要一次调度，
	// 因此无法完全零窗口地同步判定。该窗口只在调用方显式开启 mandatory 时生效。
	ReturnWindow time.Duration `mapstructure:"return_window"`

	// Prefetch 是消费者的默认 QoS 预取条数，0 表示默认 1。
	Prefetch int `mapstructure:"prefetch"`

	Exchanges []Exchange `mapstructure:"exchanges"`
	Queues    []Queue    `mapstructure:"queues"`
	Bindings  []Binding  `mapstructure:"bindings"`

	// Delay 是可选的内建延迟拓扑。声明它之后，normalize 会把
	// 延迟交换机、各档位延迟队列与绑定自动并入上面三组声明，
	// 随后即可直接用 PublishDelay 投递延迟消息。
	Delay *DelayConfig `mapstructure:"delay"`
}

// 默认值集中在此，便于测试与文档对齐。
const (
	defaultPort           = 5672
	defaultHeartbeat      = 10 * time.Second
	defaultConnectTimeout = 10 * time.Second
	defaultDialAttempts   = 4
	defaultDialBackoff    = time.Second
	defaultDialMaxBackoff = 30 * time.Second
	defaultConfirmTimeout = 5 * time.Second
	defaultReturnWindow   = 100 * time.Millisecond
	defaultPrefetch       = 1
)

// normalize 补齐默认值、展开内建延迟拓扑。不报错，只做修正。
func (c Config) normalize() Config {
	if c.Port == 0 {
		c.Port = defaultPort
	}
	if c.VHost == "" {
		c.VHost = "/"
	}
	if c.Heartbeat <= 0 {
		c.Heartbeat = defaultHeartbeat
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
	if c.ConfirmTimeout <= 0 {
		c.ConfirmTimeout = defaultConfirmTimeout
	}
	if c.Prefetch <= 0 {
		c.Prefetch = defaultPrefetch
	}
	for i := range c.Exchanges {
		if c.Exchanges[i].Kind == "" {
			c.Exchanges[i].Kind = ExchangeDirect
		}
	}
	if c.Delay != nil {
		ex, q, b := c.Delay.Topology()
		c.Exchanges = append(c.Exchanges, ex...)
		c.Queues = append(c.Queues, q...)
		c.Bindings = append(c.Bindings, b...)
	}
	return c
}

// validate 校验配置合法性，错误信息直接指出字段名。
func (c Config) validate() error {
	if strings.TrimSpace(c.Host) == "" {
		return fmt.Errorf("%w: host 不能为空", ErrInvalidConfig)
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("%w: port %d 非法", ErrInvalidConfig, c.Port)
	}
	if strings.ContainsAny(c.Username, ":@/") {
		return fmt.Errorf("%w: username %q 含非法字符（: @ /）", ErrInvalidConfig, c.Username)
	}

	seenEx := make(map[string]struct{}, len(c.Exchanges))
	for i, ex := range c.Exchanges {
		if strings.TrimSpace(ex.Name) == "" {
			return fmt.Errorf("%w: exchanges[%d].name 不能为空（默认交换机无需声明）", ErrInvalidConfig, i)
		}
		if !ex.Kind.valid() {
			return fmt.Errorf("%w: exchanges[%d] %q 的 kind=%q 非法，可选 %s/%s/%s/%s",
				ErrInvalidConfig, i, ex.Name, ex.Kind,
				ExchangeDirect, ExchangeFanout, ExchangeTopic, ExchangeHeaders)
		}
		if _, dup := seenEx[ex.Name]; dup {
			return fmt.Errorf("%w: 交换机 %q 重复声明", ErrInvalidConfig, ex.Name)
		}
		seenEx[ex.Name] = struct{}{}
	}

	seenQ := make(map[string]struct{}, len(c.Queues))
	for i, q := range c.Queues {
		if strings.TrimSpace(q.Name) == "" {
			return fmt.Errorf("%w: queues[%d].name 不能为空", ErrInvalidConfig, i)
		}
		if _, dup := seenQ[q.Name]; dup {
			return fmt.Errorf("%w: 队列 %q 重复声明", ErrInvalidConfig, q.Name)
		}
		seenQ[q.Name] = struct{}{}
	}

	for i, b := range c.Bindings {
		if strings.TrimSpace(b.Queue) == "" {
			return fmt.Errorf("%w: bindings[%d].queue 不能为空", ErrInvalidConfig, i)
		}
		if strings.TrimSpace(b.Exchange) == "" {
			return fmt.Errorf("%w: bindings[%d].exchange 不能为空（默认交换机无需绑定）", ErrInvalidConfig, i)
		}
		if _, ok := seenQ[b.Queue]; !ok {
			return fmt.Errorf("%w: bindings[%d] 引用了未声明的队列 %q", ErrInvalidConfig, i, b.Queue)
		}
		ex, ok := c.exchangeByName(b.Exchange)
		if !ok {
			return fmt.Errorf("%w: bindings[%d] 引用了未声明的交换机 %q", ErrInvalidConfig, i, b.Exchange)
		}
		switch ex.Kind {
		case ExchangeFanout:
			if b.RoutingKey != "" {
				return fmt.Errorf("%w: bindings[%d] 绑定到 fanout 交换机 %q，routing_key 必须为空",
					ErrInvalidConfig, i, b.Exchange)
			}
		case ExchangeHeaders:
			if _, ok := b.Args["x-match"]; !ok {
				return fmt.Errorf("%w: bindings[%d] 绑定到 headers 交换机 %q，Args 必须含 \"x-match\"（all/any）",
					ErrInvalidConfig, i, b.Exchange)
			}
		}
	}
	return nil
}

func (c Config) exchangeByName(name string) (Exchange, bool) {
	for _, ex := range c.Exchanges {
		if ex.Name == name {
			return ex, true
		}
	}
	return Exchange{}, false
}

// uriPath 返回 vhost 在 AMQP URI 中的 path 部分。
//
// AMQP URI 里 vhost 是"路径"，默认 vhost 写作 "/"。
// 旧实现把 VHost 直接拼在端口后面，配置写成 "prod" 就会拼出 "...:5672prod"，
// 端口变成 "5672prod"，URL 直接解析失败。
func (c Config) uriPath() string {
	v := strings.TrimSpace(c.VHost)
	if v == "" || v == "/" {
		return "/"
	}
	return "/" + strings.TrimPrefix(v, "/")
}

// amqpURI 生成转义正确的 AMQP URI。
//
// 用户名与密码交给 url.UserPassword 转义，因此密码里出现 "@" "/" ":" 等字符
// 都不会破坏 URI（旧实现直接 Sprintf，含 "/" 或 ":" 的密码会解析失败）。
func (c Config) amqpURI() string {
	scheme := "amqp"
	if c.Tls.Enable {
		scheme = "amqps"
	}
	u := url.URL{
		Scheme: scheme,
		User:   url.UserPassword(c.Username, c.Password),
		Host:   net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		Path:   c.uriPath(),
	}
	return u.String()
}

// dialConfig 构造 amqp.Config，含心跳、连接超时与 TLS。
func (c Config) dialConfig() (amqp.Config, error) {
	dc := amqp.Config{
		Heartbeat: c.Heartbeat,
		Locale:    "en_US",
		Dial:      (&net.Dialer{Timeout: c.ConnectTimeout}).Dial,
	}
	if !c.Tls.Enable {
		return dc, nil
	}

	tlsCfg := &tls.Config{
		//nolint:gosec // 由调用方显式开启，用于自签证书场景
		InsecureSkipVerify: c.Tls.InsecureSkipVerify,
		ServerName:         c.Tls.ServerName,
	}
	if c.Tls.CAFile != "" {
		pem, err := os.ReadFile(c.Tls.CAFile)
		if err != nil {
			return dc, fmt.Errorf("%w: 读取 CA 证书失败: %v", ErrInvalidConfig, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return dc, fmt.Errorf("%w: CA 证书 %s 中没有可用的 PEM 证书", ErrInvalidConfig, c.Tls.CAFile)
		}
		tlsCfg.RootCAs = pool
	}
	dc.TLSClientConfig = tlsCfg
	return dc, nil
}

// ---------------------------------------------------------------------------
// 队列参数便捷构造
// ---------------------------------------------------------------------------

// DLXArgs 返回"死信转发"所需的队列参数。
//
//	Queue{Name: "sms.retry", Durable: true,
//	      Args: rabbitmq.DLXArgs(rabbitmq.DeadLetter{RoutingKey: "sms.send"})}
//
// 当消息被 nack(requeue=false)、被 reject、超过 x-message-ttl、
// 或队列超长被挤出时，broker 会把它转投到 dl.Exchange（为空即默认交换机）
// 并使用 dl.RoutingKey。这是"失败不丢消息"的关键配置。
//
// 注意 x-dead-letter-exchange 必须**始终显式写出**（默认交换机要写空串）。
// 实测：只给 x-dead-letter-routing-key 而不给 exchange，
// broker 会直接拒绝声明并报
// `PRECONDITION_FAILED ... routing_key_but_no_dlx_defined`。
func DLXArgs(dl DeadLetter) amqp.Table {
	return amqp.Table{
		"x-dead-letter-exchange":    dl.Exchange, // 空串即默认交换机，但必须写出
		"x-dead-letter-routing-key": dl.RoutingKey,
	}
}

// MessageTTLArgs 为队列设置统一的存活时间，超时消息走死信。
// 这是"延迟队列"的基础：消息在队列里待到过期，再由 DLX 转投到目标。
func MessageTTLArgs(ttl time.Duration) amqp.Table {
	return amqp.Table{"x-message-ttl": ttl.Milliseconds()}
}

// MaxLengthArgs 限制队列长度，超出时按 x-overflow 策略（默认 drop-head）处理。
func MaxLengthArgs(maxLength int64) amqp.Table {
	return amqp.Table{"x-max-length": maxLength}
}

// QuorumArgs 声明 quorum 队列（RabbitMQ 3.8+ 的 Raft 复制队列），
// 数据安全性优于已废弃的 classic mirror。需 Durable=true 且非 Exclusive。
func QuorumArgs() amqp.Table {
	return amqp.Table{"x-queue-type": "quorum"}
}

// PriorityArgs 声明优先级队列，max 取值 1..255（0 表示不启用）。
func PriorityArgs(maxPriority uint8) amqp.Table {
	return amqp.Table{"x-max-priority": int64(maxPriority)}
}

// LazyArgs 声明 lazy 队列：消息尽量不驻留内存，适合大吞吐、可接受更高延迟的场景。
func LazyArgs() amqp.Table {
	return amqp.Table{"x-queue-mode": "lazy"}
}

// ---------------------------------------------------------------------------
// 延迟消息拓扑
// ---------------------------------------------------------------------------

// DelayRoutingKey 把延迟时长映射为延迟交换机的 routing key（毫秒字符串）。
func DelayRoutingKey(d time.Duration) string {
	return strconv.FormatInt(d.Milliseconds(), 10)
}

// DelayConfig 描述一组"延迟投递"拓扑。
//
// 原理（RabbitMQ 官方推荐的 DLX 方案，不依赖任何插件）：
//
//	producer ──publish(rk="5000")──▶ <Prefix>.delay  (direct exchange)
//	                                       │ bind rk="5000"
//	                                       ▼
//	                            <Prefix>.delay.5000  (TTL=5000ms,
//	                                       │          DLX=DeadLetter.Exchange,
//	                                       │          DLX-rk=DeadLetter.RoutingKey)
//	                                       │ 消息在此滞留至多 5s
//	                                       ▼ 过期 → broker 转投
//	                            <DeadLetter.Exchange> ──▶ 目标队列
//
// 注意：档位必须是预先声明好的离散值。需要任意延迟请部署
// rabbitmq_delayed_message_exchange 插件，然后直接用普通 Publish。
type DelayConfig struct {
	// Prefix 用于生成拓扑名："sms" → 交换机 "sms.delay"、队列 "sms.delay.5000"。
	Prefix string `mapstructure:"prefix"`
	// ExchangeKind 延迟入口交换机类型，默认 direct（推荐，避免同档位被多个队列重复消费）。
	ExchangeKind ExchangeKind `mapstructure:"exchange_kind"`
	// Delays 需要支持的延迟档位，每个档位声明一个队列。
	Delays []time.Duration `mapstructure:"delays"`
	// DeadLetter 消息到期后的去向，通常是业务交换机 + 目标 routing key。
	DeadLetter DeadLetter `mapstructure:"dead_letter"`
}

// ExchangeName 返回延迟入口交换机名。
func (d DelayConfig) ExchangeName() string {
	return d.Prefix + ".delay"
}

// QueueName 返回某档延迟对应的队列名。
func (d DelayConfig) QueueName(delay time.Duration) string {
	return fmt.Sprintf("%s.%d", d.ExchangeName(), delay.Milliseconds())
}

// Topology 展开为可直接放进 Config 的三组声明。
func (d DelayConfig) Topology() (exchanges []Exchange, queues []Queue, bindings []Binding) {
	kind := d.ExchangeKind
	if kind == "" {
		kind = ExchangeDirect
	}

	exchanges = []Exchange{{Name: d.ExchangeName(), Kind: kind, Durable: true}}
	queues = make([]Queue, 0, len(d.Delays))
	bindings = make([]Binding, 0, len(d.Delays))

	for _, delay := range d.Delays {
		args := DLXArgs(d.DeadLetter)
		args["x-message-ttl"] = delay.Milliseconds()

		queues = append(queues, Queue{
			Name:    d.QueueName(delay),
			Durable: true,
			Args:    args,
		})
		bindings = append(bindings, Binding{
			Queue:      d.QueueName(delay),
			Exchange:   d.ExchangeName(),
			RoutingKey: DelayRoutingKey(delay),
		})
	}
	return exchanges, queues, bindings
}
