package tracing

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// ---- 测试基础设施 -------------------------------------------------------

// memExporter 内存 span 收集器，配合 SimpleSpanProcessor 实现
// "span.End() 即落盘"的同步断言，不需要任何网络。
type memExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (e *memExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, spans...)
	return nil
}

func (e *memExporter) Shutdown(context.Context) error { return nil }

func (e *memExporter) all() []sdktrace.ReadOnlySpan {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdktrace.ReadOnlySpan(nil), e.spans...)
}

// setupTestTracer 用内存 exporter 替换 otel 全局，测试结束复原。
func setupTestTracer(t *testing.T) *memExporter {
	t.Helper()
	exp := &memExporter{}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)),
	)
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return exp
}

// capHandler 捕获 slog 记录，用于断言 trace 字段注入。
type capHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}

func (h *capHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capHandler) WithGroup(string) slog.Handler      { return h }

// snapshot 返回已捕获的前 3 条记录（顺序与日志调用一致）。
func (h *capHandler) snapshot(t *testing.T) []slog.Record {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.records) < 3 {
		t.Fatalf("捕获记录 %d 条, 期望 >= 3", len(h.records))
	}
	return append([]slog.Record(nil), h.records[:3]...)
}

// recordAttrs 把记录的顶层字段转成 map。
func recordAttrs(t *testing.T, r slog.Record) map[string]string {
	t.Helper()
	m := make(map[string]string, 4)
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.String()
		return true
	})
	return m
}

// ---- Config -------------------------------------------------------------

func TestConfigReady(t *testing.T) {
	t.Run("缺 ServiceName", func(t *testing.T) {
		if _, err := (Config{}).ready(); err == nil {
			t.Fatal("期望报错，得到 nil")
		}
	})
	t.Run("非法 Protocol", func(t *testing.T) {
		_, err := Config{ServiceName: "svc", Protocol: "udp"}.ready()
		if err == nil || !strings.Contains(err.Error(), "Protocol") {
			t.Fatalf("期望 Protocol 报错，得到 %v", err)
		}
	})
	t.Run("负采样率", func(t *testing.T) {
		_, err := Config{ServiceName: "svc", SamplingRatio: -0.5}.ready()
		if err == nil {
			t.Fatal("期望报错，得到 nil")
		}
	})
	t.Run("零值回落默认", func(t *testing.T) {
		cfg, err := Config{ServiceName: "svc"}.ready()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Protocol != "grpc" {
			t.Errorf("Protocol = %q, 期望 grpc", cfg.Protocol)
		}
		if cfg.SamplingRatio != 1.0 {
			t.Errorf("SamplingRatio = %v, 期望 1.0", cfg.SamplingRatio)
		}
		if cfg.BatchTimeout != defaultBatchTimeout || cfg.ExportTimeout != defaultExportTimeout {
			t.Errorf("超时默认值未生效: %v / %v", cfg.BatchTimeout, cfg.ExportTimeout)
		}
		if cfg.MaxQueueSize != defaultMaxQueueSize || cfg.MaxExportBatchSize != defaultMaxExportBatch {
			t.Errorf("批量默认值未生效: %v / %v", cfg.MaxQueueSize, cfg.MaxExportBatchSize)
		}
	})
	t.Run("超采样率归一化", func(t *testing.T) {
		cfg, _ := Config{ServiceName: "svc", SamplingRatio: 2}.ready()
		if cfg.SamplingRatio != 1 {
			t.Errorf("SamplingRatio = %v, 期望 1", cfg.SamplingRatio)
		}
	})
	t.Run("负队列", func(t *testing.T) {
		_, err := Config{ServiceName: "svc", MaxQueueSize: -1}.ready()
		if err == nil {
			t.Fatal("期望报错，得到 nil")
		}
	})
}

func TestInitInvalidConfig(t *testing.T) {
	shutdown, err := Init(context.Background(), Config{})
	if err == nil {
		t.Fatal("空配置应报错")
	}
	if shutdown != nil {
		t.Fatal("失败时 shutdown 应为 nil")
	}
}

// ---- 日志 Handler -------------------------------------------------------

func TestLogHandlerInjectsTraceFields(t *testing.T) {
	_ = setupTestTracer(t)

	captured := &capHandler{}
	h := NewLogHandler(captured)
	logger := slog.New(h)

	ctx, span := Tracer("test").Start(context.Background(), "op")
	span.End() // End 不影响 ctx 中 SpanContext 的读取
	logger.InfoContext(ctx, "in-span")
	logger.InfoContext(context.Background(), "no-span")
	logger.InfoContext(ctx, "manual", "trace_id", "custom-id")

	recs := captured.snapshot(t)
	a0 := recordAttrs(t, recs[0])
	if a0["trace_id"] == "" || a0["span_id"] == "" {
		t.Errorf("span ctx 下的日志未注入 trace 字段: %v", a0)
	}
	if a1 := recordAttrs(t, recs[1]); len(a1) != 0 {
		t.Errorf("无 span 日志不应注入任何字段, got %v", a1)
	}
	// 手工 trace_id 存在时不追加任何注入（避免重复键），手工值优先。
	a2 := recordAttrs(t, recs[2])
	if got := a2["trace_id"]; got != "custom-id" {
		t.Errorf("trace_id = %q, 期望手工值 custom-id", got)
	}
	if _, ok := a2["span_id"]; ok {
		t.Error("手工 trace_id 存在时不应注入 span_id")
	}
}

// ---- HTTP 中间件 --------------------------------------------------------

func TestHTTPMiddlewarePropagatesAndRecords(t *testing.T) {
	exp := setupTestTracer(t)

	rootCtx, root := Tracer("test").Start(context.Background(), "upstream")
	root.End()

	var (
		gotValid      bool
		gotTraceMatch bool
	)
	mux := http.NewServeMux()
	mux.Handle("/user/{id}", HTTPMiddleware("")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sc := trace.SpanContextFromContext(r.Context())
		gotValid = sc.IsValid()
		gotTraceMatch = sc.TraceID().String() == rootSpanID(root)
		w.WriteHeader(http.StatusOK)
	})))

	req := httptest.NewRequest(http.MethodGet, "/user/42", nil)
	InjectHeaders(rootCtx, req.Header) // 模拟上游发来的 traceparent
	httptest.NewRecorder()
	mux.ServeHTTP(httptest.NewRecorder(), req)

	if !gotValid {
		t.Fatal("服务端 ctx 中无有效 span")
	}
	if !gotTraceMatch {
		t.Error("服务端 span 的 trace_id 与上游不一致，传播断裂")
	}

	spans := exp.all()
	var found bool
	for _, s := range spans {
		if s.Name() == "/user/{id}" {
			found = true
			if s.SpanKind() != trace.SpanKindServer {
				t.Errorf("SpanKind = %v, 期望 server", s.SpanKind())
			}
		}
	}
	if !found {
		t.Fatalf("未找到名为 /user/{{id}} 的 server span, spans=%v", spanNames(exp.all()))
	}
}

func TestHTTPMiddlewareRecordsStatus(t *testing.T) {
	exp := setupTestTracer(t)

	mux := http.NewServeMux()
	mux.Handle("/boom", HTTPMiddleware("")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})))
	mux.Handle("/ok", HTTPMiddleware("")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})))

	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/boom", nil))
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))

	status := map[string]int{}
	errSpan := map[string]bool{}
	for _, s := range exp.all() {
		switch s.Name() {
		case "/boom", "/ok":
			status[s.Name()] = codeAttr(s)
			errSpan[s.Name()] = s.Status().Code.String() == "Error"
		}
	}
	if status["/boom"] != 500 {
		t.Errorf("/boom 状态码属性 = %d", status["/boom"])
	}
	if !errSpan["/boom"] {
		t.Error("/boom (500) 的 span 状态应为 Error")
	}
	if errSpan["/ok"] {
		t.Error("/ok (200) 的 span 状态不应为 Error")
	}
}

func TestHTTPMiddlewarePanicPropagates(t *testing.T) {
	_ = setupTestTracer(t)

	mux := http.NewServeMux()
	mux.Handle("/panic", HTTPMiddleware("")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})))

	defer func() {
		if rec := recover(); rec == nil {
			t.Error("panic 应原样向外传播，不应被中间件吞掉")
		}
	}()
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/panic", nil))
}

// ---- RoundTripper -------------------------------------------------------

func TestRoundTripperInjectsAndRecords(t *testing.T) {
	exp := setupTestTracer(t)

	var serverTrace string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverTrace = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusTeapot) // 418，验证客户端 4xx 置错
	}))
	defer srv.Close()

	ctx, span := Tracer("test").Start(context.Background(), "root")
	span.End()

	client := &http.Client{Transport: NewRoundTripper(nil)}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/x", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if serverTrace == "" {
		t.Fatal("出站请求未注入 traceparent")
	}
	want := trace.SpanContextFromContext(ctx).TraceID().String()
	if !strings.Contains(serverTrace, want) {
		t.Errorf("traceparent = %q, 不包含根 trace_id %q", serverTrace, want)
	}

	var found bool
	for _, s := range exp.all() {
		if s.SpanKind() == trace.SpanKindClient {
			found = true
			if s.Status().Code.String() != "Error" {
				t.Errorf("客户端 4xx span 状态应为 Error, 得到 %v", s.Status())
			}
			if codeAttr(s) != 418 {
				t.Errorf("状态码属性 = %d, 期望 418", codeAttr(s))
			}
		}
	}
	if !found {
		t.Fatalf("未找到 client span, spans=%v", spanNames(exp.all()))
	}
}

func TestRoundTripperRecordsTransportError(t *testing.T) {
	exp := setupTestTracer(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // 立刻关闭制造连接失败

	client := &http.Client{Transport: NewRoundTripper(nil), Timeout: shortTimeout}
	_, _ = client.Get(url)

	var found bool
	for _, s := range exp.all() {
		if s.SpanKind() == trace.SpanKindClient {
			found = true
			if s.Status().Code.String() != "Error" {
				t.Error("传输失败的 span 状态应为 Error")
			}
			if len(s.Events()) == 0 {
				t.Error("传输失败应记录 exception 事件")
			}
		}
	}
	if !found {
		t.Fatal("未找到 client span")
	}
}

// ---- 通用载体传播 -------------------------------------------------------

func TestMapPropagationRoundTrip(t *testing.T) {
	_ = setupTestTracer(t)

	ctx, span := Tracer("test").Start(context.Background(), "root")
	span.End()

	m := make(map[string]string)
	InjectMap(ctx, m)
	if m["traceparent"] == "" {
		t.Fatal("InjectMap 未写入 traceparent")
	}

	got := ExtractMap(context.Background(), m)
	if trace.SpanContextFromContext(got).TraceID() != trace.SpanContextFromContext(ctx).TraceID() {
		t.Error("ExtractMap 还原的 trace_id 不一致")
	}
	if ExtractMap(context.Background(), nil) == nil {
		t.Error("nil map 应安全返回原 ctx")
	}

	h := http.Header{}
	InjectHeaders(ctx, h)
	if h.Get("traceparent") == "" {
		t.Fatal("InjectHeaders 未写入 traceparent")
	}
	if back := ExtractHeaders(context.Background(), h); trace.SpanContextFromContext(back).TraceID() != trace.SpanContextFromContext(ctx).TraceID() {
		t.Error("ExtractHeaders 还原的 trace_id 不一致")
	}
}

// ---- 小工具 -------------------------------------------------------------

func rootSpanID(s trace.Span) string {
	if sc := s.SpanContext(); sc.IsValid() {
		return sc.TraceID().String()
	}
	return ""
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	names := make([]string, 0, len(spans))
	for _, s := range spans {
		names = append(names, s.Name())
	}
	return names
}

// codeAttr 提取 span 上的 http.response.status_code 属性。
func codeAttr(s sdktrace.ReadOnlySpan) int {
	for _, a := range s.Attributes() {
		if string(a.Key) == "http.response.status_code" {
			return int(a.Value.AsInt64())
		}
	}
	return -1
}

// shortTimeout 测试用 HTTP 客户端超时。
const shortTimeout = 2 * time.Second

// TestStatusWriterConcurrentAccess 回归测试：statusWriter 的字段必须能
// 承受并发访问。
//
// 旧实现是裸的 status / wrote 字段。handler 完全可能在另一个 goroutine
// 里写响应（SSE 推送、异步 flush、Hijack 之后的后台写入），此时
// ServeHTTP 已返回、中间件正在读 status —— 一条真实的 data race。
func TestStatusWriterConcurrentAccess(t *testing.T) {
	rec := &syncRecorder{}
	w := &statusWriter{ResponseWriter: rec, status: http.StatusOK}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				w.WriteHeader(200 + i%5)
				_, _ = w.Write([]byte("x"))
				_ = w.StatusCode()
			}
		}(i)
	}
	wg.Wait()

	if got := w.StatusCode(); got < 200 || got >= 600 {
		t.Fatalf("状态码应在合法区间, got %d", got)
	}
}

// TestHTTPMiddlewareStatusAfterAsyncWrite 端到端回归：handler 在子
// goroutine 里写响应时，中间件读状态码不得与之构成竞争。
func TestHTTPMiddlewareStatusAfterAsyncWrite(t *testing.T) {
	rec := &syncRecorder{}
	var wg sync.WaitGroup
	h := HTTPMiddleware("async")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 模拟流式/异步写入：handler 返回后子 goroutine 仍在写响应，
		// 而中间件此刻正在读 statusWriter 里的状态码。
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte("chunk"))
		}()
	}))

	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stream", nil))
	wg.Wait()

	if got := rec.StatusCode(); got != http.StatusAccepted {
		t.Fatalf("状态码 = %d, want %d", got, http.StatusAccepted)
	}
}

// syncRecorder 并发安全的 ResponseWriter。
//
// httptest.ResponseRecorder 的 body buffer 自身不加锁，直接拿它做并发
// 写测试会先撞上 recorder 自己的 race，掩盖被测对象的真实问题。
type syncRecorder struct {
	mu     sync.Mutex
	status int
	header http.Header
	body   bytes.Buffer
}

func (r *syncRecorder) Header() http.Header {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.header == nil {
		r.header = make(http.Header)
	}
	return r.header
}

func (r *syncRecorder) Write(b []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.Write(b)
}

func (r *syncRecorder) WriteHeader(code int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status == 0 {
		r.status = code
	}
}

func (r *syncRecorder) StatusCode() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status
}

// TestRoundTripperSpanCoversBodyRead 回归测试：client span 必须覆盖响应体
// 读取耗时，而不是在响应头到达时就结束。
//
// 旧实现把 defer span.End() 写在 RoundTrip 里，慢下游 / 大响应的耗时
// 全部落在 span 之外，链路图上看不出卡在哪一跳。
func TestRoundTripperSpanCoversBodyRead(t *testing.T) {
	exp := setupTestTracer(t)

	// 响应头立即返回，body 延迟 150ms 才写完：这段耗时只可能出现在
	// "读响应体"阶段。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(150 * time.Millisecond)
		_, _ = w.Write([]byte("hello"))
	}))
	defer srv.Close()

	cli := &http.Client{Transport: NewRoundTripper(nil)}
	resp, err := cli.Get(srv.URL)
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	if n := len(exp.all()); n != 0 {
		t.Fatalf("响应体尚未读完时 span 不应结束, 已导出 %d 个 span", n)
	}

	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatalf("读响应体: %v", err)
	}
	_ = resp.Body.Close()

	spans := exp.all()
	if len(spans) != 1 {
		t.Fatalf("应有 1 个 client span, got %d", len(spans))
	}
	if d := spans[0].EndTime().Sub(spans[0].StartTime()); d < 100*time.Millisecond {
		t.Fatalf("client span 应覆盖响应体读取耗时, duration=%v", d)
	}
	if spans[0].Status().Code != codes.Unset {
		t.Fatalf("200 响应不应置错误状态, got %v", spans[0].Status())
	}
}

// TestRoundTripperSpanEndsOnCloseWithoutRead 不读 body 直接关闭时，
// span 也必须结束（否则 span 会一直挂到进程退出）。
func TestRoundTripperSpanEndsOnCloseWithoutRead(t *testing.T) {
	exp := setupTestTracer(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("hello"))
	}))
	defer srv.Close()

	cli := &http.Client{Transport: NewRoundTripper(nil)}
	resp, err := cli.Get(srv.URL)
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	_ = resp.Body.Close() // 一个字节都不读

	if n := len(exp.all()); n != 1 {
		t.Fatalf("不读 body 直接 Close 也应结束 span, 已导出 %d 个", n)
	}
}
