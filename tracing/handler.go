package tracing

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// NewLogHandler 返回一个会在每条日志上追加 trace_id / span_id 的
// slog.Handler 包装器（字段取自 ctx 中的当前 OTel span）。
//
// 用法：注册进 logger 包即可，进程内所有走 slog 的日志
// （含 logger 包全局函数、Plog 与 GORM 适配器）在 span 存在时
// 自动携带链路字段，不再依赖手工 logger.WithTraceID：
//
//	logger.SetHandlerWrapper(tracing.NewLogHandler)
//
// 用"注册"而不是"自行包装后替换"：包装器在 logger 构建 handler 时才套用，
// 因此注册与 logger.Init 的先后顺序无关，之后重新 Init（配置热重载）
// 也不会把它丢掉。本函数签名恰好是 func(slog.Handler) slog.Handler，
// 可直接作为 logger.HandlerWrapper 传入，无需适配层。
//
// 不经 logger 包、只想影响某一个 logger 时，也可以自行套用：
//
//	lg := slog.New(tracing.NewLogHandler(baseHandler))
//
// 语义细节：
//   - 记录上已存在 trace_id 字段（例如业务先用 logger.WithTraceID
//     注入了自定义 ID）时**不再追加**，避免 JSON 里出现重复键 ——
//     两种机制可以共存，手工 ID 优先。
//   - ctx 无有效 span 时不追加任何字段，日志输出与包装前完全一致。
//   - 纯读包装：WithAttrs / WithGroup / Enabled 全部透传，
//     不影响级别过滤与 caller 定位。
func NewLogHandler(h slog.Handler) slog.Handler {
	return traceHandler{Handler: h}
}

type traceHandler struct {
	slog.Handler
}

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() && !hasTraceID(r) {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{Handler: h.Handler.WithGroup(name)}
}

// hasTraceID 检查记录是否已带 trace_id 字段（只看顶层，进组的字段
// 语义已变，不算重复）。
func hasTraceID(r slog.Record) bool {
	found := false
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "trace_id" {
			found = true
			return false
		}
		return true
	})
	return found
}
