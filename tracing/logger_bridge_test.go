package tracing

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zavierswong/go-infra/logger"
)

// TestLoggerBridgeCarriesTraceFields 端到端钉住"接缝真的能接上"：
// 用真实的 logger 全局实例 + 真实 span，走 logger 的包级出口，
// 断言落盘日志里带上了 span 的 trace_id / span_id。
//
// 与 TestLogHandlerInjectsTraceFields 的分工：那条验 NewLogHandler 自身
// （把 handler 直接交给 slog.New），这条验**它被注册进 logger 之后**仍然
// 有效。后者才是过去接不上的那一段，也是失败时最安静的形态——
// 日志照常输出，只是字段没了，既没有编译错误也没有运行时报错。
//
// 上游 span 用进程内 SDK provider（不碰 OTLP）：本用例验的是
// logger 接缝与 span 的互通，导出链路由 Init 的用例覆盖。
func TestLoggerBridgeCarriesTraceFields(t *testing.T) {
	_ = setupTestTracer(t)

	path := filepath.Join(t.TempDir(), "app.log")
	if err := logger.Init(logger.Config{Format: "json", Output: path}); err != nil {
		t.Fatalf("logger.Init 失败: %v", err)
	}
	t.Cleanup(func() {
		_ = logger.Close()
		logger.SetHandlerWrapper(nil)
	})

	// 这一行就是要验的接线：go-infra 的 logger 现在有这个入口了。
	logger.SetHandlerWrapper(NewLogHandler)

	ctx, span := Tracer("test").Start(context.Background(), "ingest")
	logger.NewPlog("probe").Info(ctx, "in-span")
	logger.Infof("no-span")

	wantTrace := span.SpanContext().TraceID().String()
	wantSpan := span.SpanContext().SpanID().String()
	span.End()

	// 读盘前先 Sync：文件输出下最后几条可能还在写入路径上。
	if err := logger.Sync(); err != nil {
		t.Fatalf("logger.Sync 失败: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取日志文件失败: %v", err)
	}
	got := string(data)

	if !strings.Contains(got, `"trace_id":"`+wantTrace+`"`) {
		t.Errorf("日志未携带 span 的 trace_id=%s。实际内容:\n%s", wantTrace, got)
	}
	if !strings.Contains(got, `"span_id":"`+wantSpan+`"`) {
		t.Errorf("日志未携带 span 的 span_id=%s。实际内容:\n%s", wantSpan, got)
	}
	// 无 span 的调用不该凭空多出字段：两条日志里只应有一条带 trace_id。
	if n := strings.Count(got, `"trace_id"`); n != 1 {
		t.Errorf("只有带 span 的那条日志应含 trace_id, 实际 %d 条:\n%s", n, got)
	}
}
