package tracing

import (
	"fmt"
	"time"
)

// Config 是 tracing 初始化配置。
//
// 零值不是合法配置（ServiceName 必填），其余字段零值都会回落到
// 与 OpenTelemetry 默认值一致的约定值 —— 这与 access 各包的
// "validate → normalize 两阶段"约定相同：先检查取值合法性，
// 再补默认值，顺序反了负数会被当成"未设置"而漏检。
type Config struct {
	// ServiceName 服务名，必填。落到 OTLP Resource 的 service.name，
	// 是后端（Tempo/Jaeger/SkyWalking）按服务聚合链路的唯一依据。
	ServiceName string

	// ServiceVersion 服务版本，可选。落到 resource 的 service.version。
	ServiceVersion string

	// Environment 部署环境（dev/staging/prod...），可选。
	// 落到 resource 的 deployment.environment.name。
	Environment string

	// Endpoint OTLP 上报地址，形如 "collector:4317"。
	// 为空时使用 SDK 默认（localhost:4317）。
	Endpoint string

	// Protocol OTLP 传输协议："grpc"（默认）或 "http"。
	// 两者对应 Collector 的 4317（gRPC）/ 4318（HTTP）端口，不要混用。
	Protocol string

	// Insecure 是否用明文上报。Collector 未挂 TLS 时必须为 true，
	// 否则 gRPC/HTTP 握手直接失败。
	Insecure bool

	// Headers 附加到每条 OTLP 请求的 header，典型用途是网关鉴权
	// （如 {"authorization": "Bearer ..."}）。
	Headers map[string]string

	// SamplingRatio 采样比例，取值 (0,1]：
	//   0（零值）→ 1.0 全采样；
	//   负数 → 配置错误；
	//   >1 → 归一化为 1。
	// 采样器是 ParentBased(TraceIDRatioBased) —— 上游已采样的请求
	// 全链路跟随，只有根 span 由比例决定。
	// 注意 0 表示"未设置"而非"一个都不采"（真要全关就别调用 Init）。
	SamplingRatio float64

	// BatchTimeout 批量导出的最长等待间隔，默认 5s。
	BatchTimeout time.Duration

	// ExportTimeout 单次导出超时，默认 30s。
	ExportTimeout time.Duration

	// MaxQueueSize 导出队列长度，默认 2048。队列满时新 span 被丢弃
	// （计数进 SDK 的 dropped 计数器），不会阻塞业务。
	MaxQueueSize int

	// MaxExportBatchSize 单批导出条数，默认 512。
	MaxExportBatchSize int
}

const (
	protocolGRPC = "grpc"
	protocolHTTP = "http"

	defaultSamplingRatio  = 1.0
	defaultBatchTimeout   = 5 * time.Second
	defaultExportTimeout  = 30 * time.Second
	defaultMaxQueueSize   = 2048
	defaultMaxExportBatch = 512
)

// ready 按约定先 validate 再 normalize。
func (c Config) ready() (Config, error) {
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c.normalize(), nil
}

func (c Config) validate() error {
	if c.ServiceName == "" {
		return fmt.Errorf("ServiceName 必填")
	}
	switch c.Protocol {
	case "", protocolGRPC, protocolHTTP:
	default:
		return fmt.Errorf("Protocol 只支持 %q 或 %q，收到 %q", protocolGRPC, protocolHTTP, c.Protocol)
	}
	if c.SamplingRatio < 0 {
		return fmt.Errorf("SamplingRatio 不能为负（0 表示未设置，默认全采样）")
	}
	if c.BatchTimeout < 0 || c.ExportTimeout < 0 {
		return fmt.Errorf("BatchTimeout / ExportTimeout 不能为负")
	}
	if c.MaxQueueSize < 0 || c.MaxExportBatchSize < 0 {
		return fmt.Errorf("MaxQueueSize / MaxExportBatchSize 不能为负")
	}
	return nil
}

func (c Config) normalize() Config {
	if c.Protocol == "" {
		c.Protocol = protocolGRPC
	}
	if c.SamplingRatio == 0 {
		c.SamplingRatio = defaultSamplingRatio
	}
	if c.SamplingRatio > 1 {
		c.SamplingRatio = 1
	}
	if c.BatchTimeout == 0 {
		c.BatchTimeout = defaultBatchTimeout
	}
	if c.ExportTimeout == 0 {
		c.ExportTimeout = defaultExportTimeout
	}
	if c.MaxQueueSize == 0 {
		c.MaxQueueSize = defaultMaxQueueSize
	}
	if c.MaxExportBatchSize == 0 {
		c.MaxExportBatchSize = defaultMaxExportBatch
	}
	return c
}
