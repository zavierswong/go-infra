package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNormalize(t *testing.T) {
	cfg := Config{Brokers: []string{"broker1:9092"}}.normalize()

	if cfg.ClientID != defaultClientID {
		t.Errorf("ClientID = %q, 期望默认 %q", cfg.ClientID, defaultClientID)
	}
	if cfg.Acks != defaultAcks {
		t.Errorf("Acks = %q, 期望默认 %q", cfg.Acks, defaultAcks)
	}
	if cfg.Compression != defaultCompression {
		t.Errorf("Compression = %q, 期望默认 %q", cfg.Compression, defaultCompression)
	}
	if cfg.BatchMaxBytes != defaultBatchMaxBytes {
		t.Errorf("BatchMaxBytes = %d, 期望默认 %d", cfg.BatchMaxBytes, defaultBatchMaxBytes)
	}
	if cfg.MaxBufferedRecords != defaultBuffered {
		t.Errorf("MaxBufferedRecords = %d, 期望默认 %d", cfg.MaxBufferedRecords, defaultBuffered)
	}
	if cfg.DeliveryTimeout != defaultDeliverTO {
		t.Errorf("DeliveryTimeout = %s, 期望默认 %s", cfg.DeliveryTimeout, defaultDeliverTO)
	}
	if cfg.DialTimeout != defaultDialTimeout {
		t.Errorf("DialTimeout = %s, 期望默认 %s", cfg.DialTimeout, defaultDialTimeout)
	}
}

func TestNormalizeTopics(t *testing.T) {
	cfg := Config{
		Brokers: []string{"b:9092"},
		Topics:  []TopicSpec{{Topic: "t"}}, // 分区与副本为 0
	}.normalize()

	if cfg.Topics[0].Partitions != defaultPartitions {
		t.Errorf("Partitions = %d, 期望默认 %d", cfg.Topics[0].Partitions, defaultPartitions)
	}
	if cfg.Topics[0].ReplicationFactor != defaultRF {
		t.Errorf("ReplicationFactor = %d, 期望默认 %d", cfg.Topics[0].ReplicationFactor, defaultRF)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"缺 brokers", func(c *Config) { c.Brokers = nil }, true},
		{"空 broker 条目", func(c *Config) { c.Brokers = []string{" "} }, true},
		{"畸形地址 host:port:extra", func(c *Config) { c.Brokers = []string{"a:1:2"} }, true},
		{"省略端口合法", func(c *Config) { c.Brokers = []string{"localhost"} }, false},
		{"acks 非法", func(c *Config) { c.Acks = "two" }, true},
		{"acks=one 合法", func(c *Config) { c.Acks = "one" }, false},
		{"compression 非法", func(c *Config) { c.Compression = "brotli" }, true},
		{"compression=zstd 合法", func(c *Config) { c.Compression = "zstd" }, false},
		{"sasl 缺用户名", func(c *Config) {
			c.SASL = SASLConfig{Mechanism: "plain", Password: "p"}
		}, true},
		{"sasl 机制非法", func(c *Config) {
			c.SASL = SASLConfig{Mechanism: "kerberos", Username: "u", Password: "p"}
		}, true},
		{"sasl scram 合法", func(c *Config) {
			c.SASL = SASLConfig{Mechanism: "scram-sha-256", Username: "u", Password: "p"}
		}, false},
		{"关闭幂等 + 不限时重试", func(c *Config) {
			c.DisableIdempotence = true
			c.DeliveryTimeout = -1
		}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{Brokers: []string{"b:9092"}}
			tt.mutate(&cfg)
			cfg = cfg.normalize()
			err := cfg.validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestSASLEnabled(t *testing.T) {
	if (SASLConfig{}).enabled() {
		t.Error("空 SASL 不应视为启用")
	}
	if !(SASLConfig{Mechanism: "plain"}).enabled() {
		t.Error("Mechanism=plain 应视为启用")
	}
}

func TestCompressionCodec(t *testing.T) {
	// 映射关系必须与 Config 文档一致，出错会导致压缩静默降级为默认 snappy。
	// CompressionCodec 没有可比对的导出字段，用"与同名列的编解码器相等"来验证
	// （kgo.CompressionCodec 是只含未导出字段的值类型，== 逐字段比较）。
	cases := map[string]kgo.CompressionCodec{
		"gzip": kgo.GzipCompression(), "lz4": kgo.Lz4Compression(),
		"zstd": kgo.ZstdCompression(), "none": kgo.NoCompression(),
		"snappy": kgo.SnappyCompression(),
	}
	for in, want := range cases {
		if got := compressionCodec(in); got != want {
			t.Errorf("compressionCodec(%q) != %s", in, in)
		}
	}
	if compressionCodec("") != kgo.SnappyCompression() {
		t.Error("未知压缩名应回落 snappy")
	}
}

// Open 是懒连接的：配一个不存在的 broker 也能成功创建客户端，
// 这正是"进程启动不因 Kafka 未就绪而卡死"的保证。
func TestOpenLazyAndClose(t *testing.T) {
	c, err := Open(Config{
		Brokers: []string{"127.0.0.1:1"}, // 没有任何 Kafka 会监听这个端口
		Name:    "unit",
	})
	if err != nil {
		t.Fatalf("懒连接的 Open 不应失败: %v", err)
	}
	if c.Closed() {
		t.Fatal("新建客户端不应处于关闭状态")
	}

	st := c.Status()
	if st.Closed || st.Instance != "unit" {
		t.Errorf("Status = %+v", st)
	}

	// 已关闭后 HealthCheck 必须直接报 ErrClosed，而不是尝试触网。
	if err := c.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if !c.Closed() {
		t.Error("Close 后 Closed() 应为 true")
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close 应可重复调用: %v", err)
	}
	if err := c.HealthCheck(t.Context()); err == nil {
		t.Error("已关闭的客户端 HealthCheck 应报错")
	}
}

func TestGroupConfigDefaults(t *testing.T) {
	parent, err := Open(Config{Brokers: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	t.Cleanup(func() { _ = parent.Close() })
	g, err := parent.NewGroup(GroupConfig{
		Group:   "g",
		Topics:  []string{"t"},
		Handler: func(context.Context, *kgo.Record) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewGroup() = %v", err)
	}
	defer func() {
		g.Close()
		g.cli.Close()
	}()
	if g.cfg.Concurrency != 1 {
		t.Errorf("Concurrency 默认应为 1, got %d", g.cfg.Concurrency)
	}
	if g.cfg.MaxPollRecords != defaultMaxPollRecords {
		t.Errorf("MaxPollRecords 默认应为 %d, got %d", defaultMaxPollRecords, g.cfg.MaxPollRecords)
	}
	if g.cfg.DrainTimeout != defaultDrainTimeout {
		t.Errorf("DrainTimeout 默认应为 %s, got %s", defaultDrainTimeout, g.cfg.DrainTimeout)
	}
	if g.cfg.CommitInterval != defaultCommitInterval {
		t.Errorf("CommitInterval 默认应为 %s, got %s", defaultCommitInterval, g.cfg.CommitInterval)
	}
}

func TestNewGroupValidation(t *testing.T) {
	parent, err := Open(Config{Brokers: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	t.Cleanup(func() { _ = parent.Close() })
	handler := func(context.Context, *kgo.Record) error { return nil }

	if _, err := parent.NewGroup(GroupConfig{Topics: []string{"t"}, Handler: handler}); err == nil {
		t.Error("缺 Group 应报错")
	}
	if _, err := parent.NewGroup(GroupConfig{Group: "g", Handler: handler}); err == nil {
		t.Error("缺 Topics 应报错")
	}
	if _, err := parent.NewGroup(GroupConfig{Group: "g", Topics: []string{"t"}}); err == nil {
		t.Error("缺 Handler 应报错")
	}
	if _, err := parent.NewGroup(GroupConfig{
		Group: "g", Topics: []string{"t"}, Handler: handler, Concurrency: -1,
	}); err == nil {
		t.Error("Concurrency 为负应报错")
	}
}

func TestProduceOptionsValidation(t *testing.T) {
	o := newProduceOptions()
	WithPartition(-2)(&o)
	if err := o.validate(); err == nil {
		t.Error("partition < -1 应报错")
	}

	o2 := newProduceOptions()
	WithPartition(3)(&o2)
	WithKey([]byte("k"))(&o2)
	if err := o2.validate(); err != nil {
		t.Errorf("合法选项不应报错: %v", err)
	}
	if o2.partition != 3 || string(o2.key) != "k" {
		t.Errorf("选项未生效: %+v", o2)
	}
}

func TestProduceDetail(t *testing.T) {
	o := newProduceOptions()
	if got := produceDetail("orders", &o, 1); got != "orders" {
		t.Errorf("produceDetail = %q", got)
	}
	o.partition = 2
	if got := produceDetail("orders", &o, 1); got != "orders[partition=2]" {
		t.Errorf("produceDetail = %q", got)
	}
	if got := produceDetail("orders", &o, 500); got != "orders[partition=2] x500" {
		t.Errorf("produceDetail = %q", got)
	}
}
