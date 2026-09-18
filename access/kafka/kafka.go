package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/zavierswong/go-infra/logger"
	"github.com/zavierswong/go-infra/metrics"
)

// Client 是一个 Kafka 客户端（生产者 + 主题管理）。
//
// 设计与本仓库其他 access 包一致：
//
//  1. **非单例**：Open 返回独立实例，多集群 / 多 vhost 场景互不干扰；
//  2. **懒连接**：Open 只做配置校验与主题声明，不阻塞建连 ——
//     首条消息、Ping 或消费启动才真正触网，进程启动不会因为
//     Kafka 还没就绪而卡死；
//  3. **消费独立成组**：Kafka 的消费组必须独占一个底层客户端
//     （franz-go 明确建议），所以 NewGroup 会另建连接，
//     与生产端互不影响，Close 时一并回收。
//
// Client 的所有导出方法都可并发调用。
type Client struct {
	cfg Config
	log *logger.Plog

	// obs 是预解析好的观察者：Config.Observer 为 nil 时是 NopObserver。
	obs metrics.Observer

	cli *kgo.Client
	adm *kadm.Client

	mu     sync.RWMutex
	closed bool
}

// Open 创建 Kafka 客户端，并按 Config.Topics 幂等创建主题。
//
// Open 不做真实建连（懒连接），配置错误在这里报，网络错误推迟到使用时。
// 返回的 Client 必须由调用方负责 Close。
func Open(cfg Config) (*Client, error) {
	cfg = cfg.normalize()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.DialTimeout(cfg.DialTimeout),
	}

	// ---- 可靠性：acks + 幂等 + 限时送达 ----
	switch cfg.Acks {
	case "all":
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case "one":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	case "none":
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	}
	if !cfg.DisableIdempotence {
		// franz-go 默认即幂等，这里显式写出是为了与 Config 文档对齐：
		// 看到"没有 DisableIdempotentWrite"就是"幂等开启"。
	} else {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	if cfg.DeliveryTimeout > 0 {
		opts = append(opts, kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout))
	}

	// ---- 吞吐：攒批 + 压缩 + 背压 ----
	if cfg.Linger > 0 {
		opts = append(opts, kgo.ProducerLinger(cfg.Linger))
	}
	opts = append(opts,
		kgo.ProducerBatchMaxBytes(cfg.BatchMaxBytes),
		kgo.MaxBufferedRecords(cfg.MaxBufferedRecords),
		kgo.ProducerBatchCompression(compressionCodec(cfg.Compression)),
	)

	// ---- TLS / SASL ----
	if cfg.TLS.Enable {
		tlsCfg, err := buildTLS(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	if cfg.SASL.enabled() {
		mech, err := cfg.SASL.mechanism()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mech))
	}

	c := &Client{
		cfg: cfg,
		log: logger.NewPlog("Kafka"),
		obs: metrics.OrNop(cfg.Observer),
	}

	// broker 建连失败必须可观测：懒连接的设计下，Open 成功不代表集群可达，
	// 如果连"连不上"都不进事件流，监控上就只剩超时困惑。
	opts = append(opts, kgo.WithHooks(hookConnectErrors(c)))

	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: 创建客户端失败: %v", ErrInvalidConfig, err)
	}
	c.cli = cli
	c.adm = kadm.NewClient(cli)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout+5*time.Second)
	defer cancel()
	if err := c.ensureTopics(ctx); err != nil {
		cli.Close()
		return nil, err
	}
	return c, nil
}

// Close 关闭客户端。可重复调用，第二次起直接返回 nil。
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	c.log.Infof(context.Background(), "开始关闭 Kafka 客户端...")
	c.adm.Close()
	c.cli.Close()
	c.log.Infof(context.Background(), "Kafka 客户端已关闭")
	return nil
}

// Closed 报告客户端是否已关闭。
func (c *Client) Closed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

// Config 返回生效后的配置（已补齐默认值）。
func (c *Client) Config() Config { return c.cfg }

// HealthCheck 验证能否从集群拿到 metadata，可用于探活接口。
// 与 Produce 不同，它不占生产缓冲，也不会留下未确认记录。
func (c *Client) HealthCheck(ctx context.Context) error {
	if c.Closed() {
		return ErrClosed
	}
	start := time.Now()
	err := c.cli.Ping(ctx)
	c.observe(metrics.OpPing, start, err, "")
	return err
}

// Status 是客户端的瞬时状态快照，适合映射成 Gauge。
//
// Kafka 没有传统意义上的"连接池"，这里给出的是 franz-go 内部的
// 生产缓冲水位 —— 缓冲持续增长（逼近 MaxBufferedRecords）就是
// broker 处理不过来或分区不可用的最早信号。
type Status struct {
	// Instance 是实例名，来自 Config.Name。
	Instance string
	// Closed 表示客户端是否已关闭。
	Closed bool
	// ClientID 是协议层客户端标识。
	ClientID string
	// Group 是消费组名。Client.Status() 恒为空 —— 生产端客户端不消费；
	// Group.Status() 填消费组名，此时水位字段只看 Fetch 侧。
	Group string
	// SeedBrokers 是配置里的 seed 地址。
	SeedBrokers []string

	// ProduceBufferedRecords 是已入队但尚未被 broker 确认的记录数。
	ProduceBufferedRecords int64
	// ProduceBufferedBytes 是生产缓冲中的字节量。
	ProduceBufferedBytes int64
	// FetchBufferedRecords 是已拉取、待 PollRecords 返回的记录数（消费组侧）。
	FetchBufferedRecords int64
	// FetchBufferedBytes 是待消费缓冲中的字节量。
	FetchBufferedBytes int64
}

// Status 返回当前状态快照。
func (c *Client) Status() Status {
	st := Status{
		Instance:    c.cfg.Name,
		ClientID:    c.cfg.ClientID,
		SeedBrokers: c.cfg.Brokers,
	}
	c.mu.RLock()
	st.Closed = c.closed
	c.mu.RUnlock()
	if st.Closed {
		return st
	}

	st.ProduceBufferedRecords = c.cli.BufferedProduceRecords()
	st.ProduceBufferedBytes = c.cli.BufferedProduceBytes()
	st.FetchBufferedRecords = c.cli.BufferedFetchRecords()
	st.FetchBufferedBytes = c.cli.BufferedFetchBytes()
	return st
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

// hookConnectErrors 返回一个只上报"broker 建连失败"的 hook。
//
// 成功建连不上报：正常重启、扩缩容都会产生大量成功连接，上报只会淹没
// 真正的信号；失败才值得进错误率曲线。
type connectErrHook struct{ c *Client }

func (h connectErrHook) OnBrokerConnect(meta kgo.BrokerMetadata, initDur time.Duration, _ net.Conn, err error) {
	if err == nil {
		return
	}
	h.c.observe(metrics.OpReconnect, time.Now().Add(-initDur), err,
		fmt.Sprintf("%s:%d", meta.Host, meta.Port))
}

func hookConnectErrors(c *Client) kgo.Hook { return connectErrHook{c} }

func buildTLS(cfg TLSConfig) (*tls.Config, error) {
	t := &tls.Config{
		//nolint:gosec // 由调用方显式开启，用于自签证书场景
		InsecureSkipVerify: cfg.InsecureSkipVerify,
		ServerName:         cfg.ServerName,
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("%w: 读取 CA 证书失败: %v", ErrInvalidConfig, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%w: CA 证书 %s 中没有可用的 PEM 证书", ErrInvalidConfig, cfg.CAFile)
		}
		t.RootCAs = pool
	}
	return t, nil
}

// ensureTopics 幂等创建 Config.Topics 里声明的主题。
func (c *Client) ensureTopics(ctx context.Context) error {
	if len(c.cfg.Topics) == 0 {
		return nil
	}

	var errs []error
	for _, spec := range c.cfg.Topics {
		if err := c.ensureOne(ctx, spec); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
