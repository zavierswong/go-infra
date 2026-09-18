package logger

import (
	"context"
	"log/slog"
)

// TraceIDKey 链路追踪字段名，写入每条日志
const TraceIDKey = "trace_id"

type traceIDKey struct{}

// WithTraceID 把 trace ID 注入 ctx，后续经 Plog/Ctx 输出日志会自动带上该字段
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if ctx == nil || traceID == "" {
		return ctx
	}
	return context.WithValue(ctx, traceIDKey{}, traceID)
}

// TraceID 从 ctx 读取 trace ID，不存在时返回空串
func TraceID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if id, ok := ctx.Value(traceIDKey{}).(string); ok {
		return id
	}
	return ""
}

// attrsFromCtx 抽取 ctx 中的通用字段
func attrsFromCtx(ctx context.Context) []slog.Attr {
	if ctx == nil {
		return nil
	}
	if id := TraceID(ctx); id != "" {
		return []slog.Attr{slog.String(TraceIDKey, id)}
	}
	return nil
}
