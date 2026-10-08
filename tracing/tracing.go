// Package tracing 为 go-infra 提供分布式链路追踪（OpenTelemetry）能力。
//
// # 分层铁律
//
// 与 prometheus/ 包一样，本包是 OpenTelemetry 的**唯一接入点**：
// access/*、logger、metrics 契约包都不 import otel，保持零观测依赖。
// 业务侧只需要 import 本包并调用 Init，之后：
//
//   - 全局 TracerProvider / Propagator 就位，任何持有 otel API 的
//     第三方库（gorm 插件、各官方 instrumentation）自动接入；
//   - 用 [NewLogHandler] 配合 logger.SetHandlerWrapper 注册后，所有日志
//     自动携带 trace_id / span_id，与 logger 包的手工 trace_id 机制互通；
//   - HTTP 进出口用 [HTTPMiddleware] 与 [NewRoundTripper]，
//     MQ/自定义协议用 [InjectHeaders] / [ExtractHeaders]（或
//     [InjectMap] / [ExtractMap]）手工搬运上下文。
//
// # 上报通道
//
// 通过 OTLP 导出到 Collector（gRPC 默认 :4317 / HTTP :4318），
// 再由 Collector 转发到 Tempo/Jaeger 等后端。进程退出时必须调用
// Init 返回的 shutdown（建议注册进 shutdown/ 的钩子），
// 否则批处理器里未满批的 span 会丢失。
package tracing

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	nooptp "go.opentelemetry.io/otel/trace/noop"
)

// ShutdownFunc 刷新并停止 TracerProvider。应在进程退出前调用一次，
// 建议注册进 shutdown/ 的钩子链。ctx 约束刷新等待时长。
type ShutdownFunc func(context.Context) error

// tracerName 本包自建 span（HTTP 中间件 / RoundTripper）的
// instrumentation scope，便于后端按来源区分。
const tracerName = "github.com/zavierswong/go-infra/tracing"

var (
	// initMu 串行化 Init：全局 provider 是进程级共享状态，并发替换
	// 会让除最后一次之外的 provider 永远失去 shutdown 句柄。
	initMu sync.Mutex
	// curProvider 记录当前生效的 provider，供重复 Init 时关掉旧的。
	curProvider atomic.Pointer[sdktrace.TracerProvider]
)

// Init 初始化全局链路追踪：构建 TracerProvider（OTLP 批量导出）与
// W3C TraceContext + Baggage 传播器，并设为 otel 全局。
//
// 重复调用是安全的：会先停掉上一次的 provider（刷出残留 span、
// 停掉批处理器协程、关闭 exporter），再替换。配置热重载因此可行，
// 但仍建议进程内只 Init 一次。
//
// ctx 只用于构建 exporter 的握手阶段；返回后即可取消 ctx。
func Init(ctx context.Context, cfg Config) (ShutdownFunc, error) {
	cfg, err := cfg.ready()
	if err != nil {
		return nil, fmt.Errorf("tracing 配置非法: %w", err)
	}

	// 全局 provider 是进程级共享状态：并发 Init 会互相覆盖，且只有
	// 最后一次的 shutdown 句柄能被拿到 —— 其余 provider 的批处理器
	// 协程与 exporter 连接永久泄漏。因此整个替换过程串行化。
	initMu.Lock()
	defer initMu.Unlock()

	exp, err := newExporter(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("创建 OTLP exporter 失败: %w", err)
	}
	// exporter 一旦建成就持有 gRPC/HTTP 连接（含后台协程），
	// 后续任何一步失败都必须关掉它，否则泄漏。
	ok := false
	defer func() {
		if !ok {
			_ = exp.Shutdown(context.Background())
		}
	}()

	res, err := resource.Merge(resource.Default(),
		resource.NewSchemaless(
			attribute.String("service.name", cfg.ServiceName),
			attribute.String("service.version", cfg.ServiceVersion),
			attribute.String("deployment.environment.name", cfg.Environment),
		))
	if err != nil {
		return nil, fmt.Errorf("构建 resource 失败: %w", err)
	}

	// ParentBased：上游已决定采样时全链路跟随，不做二次采样判定；
	// 根 span 才按比例掷骰子。直接用 TraceIDRatioBased 会把上游
	// 采样过的请求在本服务"重新掷骰"，链路会断。
	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SamplingRatio))

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(cfg.BatchTimeout),
			sdktrace.WithExportTimeout(cfg.ExportTimeout),
			sdktrace.WithMaxQueueSize(cfg.MaxQueueSize),
			sdktrace.WithMaxExportBatchSize(cfg.MaxExportBatchSize),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	// 覆盖前先停掉上一个 provider：它的批处理器协程、exporter 连接
	// 只有在 Shutdown 之后才会释放。
	if prev := curProvider.Swap(provider); prev != nil {
		_ = prev.Shutdown(context.Background())
	}

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	ok = true

	return func(ctx context.Context) error {
		// Shutdown 之后全局 provider 仍指向已停止的 SDK，后续 span 会被
		// 静默丢弃并往 stderr 刷错误。复位成 noop 让"追踪已关闭"这件事
		// 在行为上是安静且明确的。
		if curProvider.CompareAndSwap(provider, nil) {
			otel.SetTracerProvider(nooptp.NewTracerProvider())
		}

		err := provider.Shutdown(ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			// 刷新超时只代表丢弃了尾部未导出的 span，provider 已停止。
			return fmt.Errorf("tracing shutdown 超时（部分 span 未导出）: %w", err)
		}
		return err
	}, nil
}

func newExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	switch cfg.Protocol {
	case protocolHTTP:
		opts := []otlptracehttp.Option{
			otlptracehttp.WithEndpoint(cfg.Endpoint),
			otlptracehttp.WithHeaders(cfg.Headers),
			// 与 gRPC 分支对齐：不设超时的话，Collector 半死不活
			// （连得上但不返回）会让导出请求无限挂住，占满批处理器。
			otlptracehttp.WithTimeout(cfg.ExportTimeout),
		}
		if cfg.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		return otlptracehttp.New(ctx, opts...)
	default: // grpc
		opts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(cfg.Endpoint),
			otlptracegrpc.WithHeaders(cfg.Headers),
			otlptracegrpc.WithTimeout(cfg.ExportTimeout),
		}
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		return otlptracegrpc.New(ctx, opts...)
	}
}

// Tracer 返回指定 instrumentation scope 的 Tracer。
// 未 Init 时返回的是全局 noop，所有调用零开销、不会 panic ——
// 这是 otel API 的设计契约，也是"观测是旁路"的兜底。
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

// SpanFromContext 返回 ctx 中当前的 span。无效时返回 noop span。
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}
