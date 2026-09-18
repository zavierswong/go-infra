package kafka

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/zavierswong/go-infra/metrics"
)

// compressionCodec 把配置里的压缩名映射成 franz-go 的编解码器。
func compressionCodec(name string) kgo.CompressionCodec {
	switch name {
	case "gzip":
		return kgo.GzipCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	case "none":
		return kgo.NoCompression()
	default: // "snappy"（默认值在 normalize 里补齐）
		return kgo.SnappyCompression()
	}
}

// TLSConfig 是 Kafka 连接的安全配置。
type TLSConfig struct {
	Enable             bool   `mapstructure:"enable"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
	ServerName         string `mapstructure:"server_name"`
	// CAFile 指定 PEM 格式的根证书；为空则使用系统信任链。
	CAFile string `mapstructure:"ca_file"`
}

// SASLConfig 是 SASL 认证配置。Mechanism 为空表示不认证。
type SASLConfig struct {
	// Mechanism 支持 "plain" / "scram-sha-256" / "scram-sha-512"。
	//
	// 注意 Kafka 的 SASL/PLAIN 在未开启 TLS 时密码是明文传输的，
	// 生产环境请搭配 TLSConfig.Enable 使用。
	Mechanism string `mapstructure:"mechanism"`
	Username  string `mapstructure:"username"`
	Password  string `mapstructure:"password"`
}

// TopicSpec 描述一个主题，供 Config.Topics 在 Open 时幂等创建。
type TopicSpec struct {
	// Topic 主题名（必填）。
	Topic string `mapstructure:"topic"`
	// Partitions 分区数。0 表示默认 3。
	//
	// 分区数是 Kafka 并行度的上限：单主题的吞吐天花板 ≈
	// min(生产端, 分区数 × 单分区吞吐, 消费组内消费者 × 分区)。
	// 后续可以增分区（只增不减），一开始就按目标吞吐规划最好。
	Partitions int32 `mapstructure:"partitions"`
	// ReplicationFactor 副本数。0 表示默认 1（单机开发够用），
	// 生产集群建议 3（配合 acks=all，允许一台副本宕机不丢数据）。
	ReplicationFactor int16 `mapstructure:"replication_factor"`
	// Configs 主题级配置，如 {"retention.ms": "86400000"}。
	Configs map[string]string `mapstructure:"configs"`
}

// Config 是 Kafka 客户端的完整配置。
type Config struct {
	// Brokers 是 seed broker 地址（host:port），至少一个。
	// 不需要把整个集群都列出来：客户端会通过 metadata 自动发现其余节点。
	// 端口省略时按 Kafka 惯例取 9092。
	Brokers []string `mapstructure:"brokers"`

	// ClientID 客户端标识，会出现在 broker 日志与请求日志里。
	// 0 表示默认 "go-infra-kafka"。
	ClientID string `mapstructure:"client_id"`

	// ------------------------------------------------------------------
	// 标识与可观测性
	// ------------------------------------------------------------------

	// Name 是实例名，用于区分同一个进程里的多个 Kafka 客户端。
	//
	// 注意与 ClientID 的区别：ClientID 是协议层的客户端标识（broker 可见），
	// 这个只用于监控标签（metrics.Event.Instance）。多实例部署必须填，
	// 否则几套集群的曲线会叠在一起。
	Name string `mapstructure:"name"`

	// Observer 接收消息流上的 metrics.Event：
	// publish（含批量）/ consume / commit / reconnect。
	//
	// 批量投递（ProduceBatch）整批只产生 1 个事件；
	// 一条正常消费消息会产生 1~2 个事件（consume + 可能的 commit）。
	// 观察者必须按"非阻塞 + 有界缓冲"实现，详见 metrics 包文档。
	Observer metrics.Observer `mapstructure:"-"`

	TLS  TLSConfig  `mapstructure:"tls"`
	SASL SASLConfig `mapstructure:"sasl"`

	// ------------------------------------------------------------------
	// 生产者
	// ------------------------------------------------------------------

	// Acks 是"多少副本确认才算成功"：
	//   - "all"（默认）：全部 ISR 副本落盘，最可靠；
	//   - "one"：仅 leader 确认，leader 宕机可能丢；
	//   - "none"：写进页缓存即返回，最快但会丢。
	Acks string `mapstructure:"acks"`

	// DisableIdempotence 关闭幂等生产。默认开启幂等：
	// broker 按 (producer, sequence) 去重，客户端重试不会产生重复消息，
	// 这是"重试安全"的前提。关闭的唯一理由是兼容极老的 broker（<0.11）。
	DisableIdempotence bool `mapstructure:"disable_idempotence"`

	// Compression 批压缩算法：snappy（默认）/ gzip / lz4 / zstd / none。
	// 压缩在批次级别进行，Linger 越大、批次越满，压缩收益越高。
	// zstd 压缩比最好但 CPU 稍高；snappy 是吞吐/CPU 的均衡默认值。
	Compression string `mapstructure:"compression"`

	// Linger 是"攒批等待"窗口，0 表示攒满一批或立即发送（默认）。
	//
	// 高 QPS 下微批化能显著降低请求数与提升压缩率，代价是增加最多
	// Linger 的延迟。典型取值 5~20ms。
	Linger time.Duration `mapstructure:"linger"`

	// BatchMaxBytes 单批发送上限。0 表示默认 1MB（Kafka 默认 message.max.bytes）。
	// 超过单批上限的大消息会独占一批发送。
	BatchMaxBytes int32 `mapstructure:"batch_max_bytes"`

	// MaxBufferedRecords 是未确认记录的缓冲上限。0 表示默认 10000。
	// 缓冲满时 Produce / ProduceAsync 会阻塞等待 —— 这是有意的背压：
	// 宁可让调用方慢下来，也不在内存里无限堆积。
	MaxBufferedRecords int `mapstructure:"max_buffered_records"`

	// DeliveryTimeout 是单条记录从入队到被确认（或判定失败）的总时长，
	// 覆盖全部重试。0 表示默认 30s；负值表示不限时（完全交给重试策略）。
	//
	// 为什么默认有限：broker 端容量不足（如副本数不够、分区不可用）时，
	// 记录会无限重试。不限时的默认值会让每个调用方挂起一个等待者，
	// 高并发下把 goroutine 堆成雪崩；有限超时把"集群侧异常"及时暴露成错误。
	DeliveryTimeout time.Duration `mapstructure:"delivery_timeout"`

	// ------------------------------------------------------------------
	// 连接
	// ------------------------------------------------------------------

	// DialTimeout 单次建连超时，0 表示默认 10s。
	DialTimeout time.Duration `mapstructure:"dial_timeout"`

	// Topics 是 Open 时幂等创建的主题（对应 rabbitmq 包的拓扑声明习惯）。
	// 任一创建失败会让 Open 返回错误。
	Topics []TopicSpec `mapstructure:"topics"`
}

// 默认值集中在此，便于测试与文档对齐。
const (
	defaultClientID      = "go-infra-kafka"
	defaultAcks          = "all"
	defaultCompression   = "snappy"
	defaultBatchMaxBytes = int32(1 << 20) // 1MB，与 Kafka broker 默认对齐
	defaultBuffered      = 10000
	defaultDeliverTO     = 30 * time.Second
	defaultDialTimeout   = 10 * time.Second

	defaultPartitions = int32(3)
	defaultRF         = int16(1)
)

// normalize 补齐默认值。不报错，只做修正。
func (c Config) normalize() Config {
	if c.ClientID == "" {
		c.ClientID = defaultClientID
	}
	if c.Acks == "" {
		c.Acks = defaultAcks
	}
	if c.Compression == "" {
		c.Compression = defaultCompression
	}
	if c.BatchMaxBytes <= 0 {
		c.BatchMaxBytes = defaultBatchMaxBytes
	}
	if c.MaxBufferedRecords <= 0 {
		c.MaxBufferedRecords = defaultBuffered
	}
	if c.DeliveryTimeout == 0 {
		c.DeliveryTimeout = defaultDeliverTO
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = defaultDialTimeout
	}
	for i := range c.Topics {
		if c.Topics[i].Partitions <= 0 {
			c.Topics[i].Partitions = defaultPartitions
		}
		if c.Topics[i].ReplicationFactor <= 0 {
			c.Topics[i].ReplicationFactor = defaultRF
		}
	}
	return c
}

// validate 校验配置合法性，错误信息直接指出字段名。
func (c Config) validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("%w: brokers 至少需要一个 seed 地址", ErrInvalidConfig)
	}
	for i, b := range c.Brokers {
		if strings.TrimSpace(b) == "" {
			return fmt.Errorf("%w: brokers[%d] 不能为空", ErrInvalidConfig, i)
		}
		// 只做结构检查，不实际拨号：Open 是懒连接的，首条消息或 Ping 才触网。
		if _, _, err := net.SplitHostPort(strings.TrimSpace(b)); err != nil {
			// 允许省略端口（按 9092 处理），但不允许 "host:port:extra" 这类畸形。
			if !strings.Contains(strings.TrimSpace(b), ":") {
				continue
			}
			return fmt.Errorf("%w: brokers[%d] %q 不是合法的 host:port", ErrInvalidConfig, i, b)
		}
	}

	switch c.Acks {
	case "all", "one", "none":
	default:
		return fmt.Errorf("%w: acks %q 非法，可选 all / one / none", ErrInvalidConfig, c.Acks)
	}

	switch c.Compression {
	case "snappy", "gzip", "lz4", "zstd", "none":
	default:
		return fmt.Errorf("%w: compression %q 非法，可选 snappy / gzip / lz4 / zstd / none",
			ErrInvalidConfig, c.Compression)
	}

	if c.DeliveryTimeout < 0 && c.DisableIdempotence {
		// 幂等关闭 + 不限时重试 = 消息可能重复且永远悬着，几乎一定是配置错误。
		return fmt.Errorf("%w: DeliveryTimeout 为负（不限时）时不应关闭幂等生产",
			ErrInvalidConfig)
	}

	if err := c.SASL.validate(); err != nil {
		return err
	}
	return nil
}

func (s SASLConfig) enabled() bool { return strings.TrimSpace(s.Mechanism) != "" }

// mechanism 把配置映射成 franz-go 的 SASL 机制。
func (s SASLConfig) mechanism() (sasl.Mechanism, error) {
	switch s.Mechanism {
	case "plain":
		return plain.Auth{User: s.Username, Pass: s.Password}.AsMechanism(), nil
	case "scram-sha-256":
		return scram.Auth{User: s.Username, Pass: s.Password}.AsSha256Mechanism(), nil
	case "scram-sha-512":
		return scram.Auth{User: s.Username, Pass: s.Password}.AsSha512Mechanism(), nil
	}
	return nil, fmt.Errorf("%w: sasl.mechanism %q 非法", ErrInvalidConfig, s.Mechanism)
}

func (s SASLConfig) validate() error {
	if !s.enabled() {
		return nil
	}
	switch s.Mechanism {
	case "plain", "scram-sha-256", "scram-sha-512":
	default:
		return fmt.Errorf("%w: sasl.mechanism %q 非法，可选 plain / scram-sha-256 / scram-sha-512",
			ErrInvalidConfig, s.Mechanism)
	}
	if strings.TrimSpace(s.Username) == "" {
		return fmt.Errorf("%w: sasl.username 不能为空（mechanism=%s）", ErrInvalidConfig, s.Mechanism)
	}
	return nil
}
