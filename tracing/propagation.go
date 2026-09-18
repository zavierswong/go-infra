package tracing

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// InjectHeaders 把当前 trace 上下文写入 HTTP header（W3C
// traceparent + tracestate + baggage）。用于出站请求：
//
//	req.Header 之外的场景（amqp Delivery.Headers、kafka RecordHeaders）
//	没有 http.Header，改用 [InjectMap]。
//
// 未 Init（全局传播器为空）时是 no-op，不会 panic。
func InjectHeaders(ctx context.Context, h http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(h))
}

// ExtractHeaders 从 HTTP header 提取上游 trace 上下文，返回派生 ctx。
// 用于入站请求；提取不到（无 traceparent）时返回携带新根 span 候选的
// 原始链 —— 后续 StartSpan 会成为根。
func ExtractHeaders(ctx context.Context, h http.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(h))
}

// InjectMap 把 trace 上下文写入任意 string→string 载体。
// 典型映射：
//
//   - amqp091 Delivery.Headers（map[string]any）：逐个转成 string 放入，
//     或直接用 tracing.InjectHeaders + http.Header 再转写；
//   - franz-go kgo.RecordHeaders：append(kgo.RecordHeader{
//     Key: k, Value: []byte(v)})。
//
// 只搬 traceparent 与 baggage 两个键，键名由 W3C 规范固定。
func InjectMap(ctx context.Context, m map[string]string) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(m))
}

// ExtractMap 从 string→string 载体提取上游 trace 上下文，返回派生 ctx。
// nil map 安全（提取不到，返回原 ctx）。
func ExtractMap(ctx context.Context, m map[string]string) context.Context {
	if m == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(m))
}
