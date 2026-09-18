package tracing

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// HTTPMiddleware 返回 net/http 服务端中间件：从请求头提取上游
// trace 上下文 → 创建 SERVER span → 注入请求 ctx → 透传给 next。
//
//   - span 名优先取 r.Pattern（Go 1.22+ ServeMux 路由模板），
//     避免把 URL 里的动态段做成高基数 span 名；无 Pattern 时退化为
//     "METHOD /path"，此时注意 /user/123 这类路径会刷爆 span 名基数，
//     建议给中间件传 operation 参数固定名字，或确保前面有路由模板。
//   - 5xx 置 span 状态为 Error；4xx 只记录状态码属性不置错
//     （OTel HTTP 语义约定：服务端 4xx 是客户端的错误，不是服务的）。
//   - panic 不吞：记录到 span 后原样 re-panic，交给上层 recover。
func HTTPMiddleware(operation string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := ExtractHeaders(r.Context(), r.Header)

			name := operation
			if name == "" {
				if r.Pattern != "" {
					name = r.Pattern
				} else {
					name = r.Method + " " + r.URL.Path
				}
			}

			attrs := []attribute.KeyValue{
				attribute.String("http.request.method", r.Method),
			}
			if r.Pattern != "" {
				attrs = append(attrs, attribute.String("http.route", r.Pattern))
			}

			var span trace.Span
			ctx, span = tracer().Start(ctx, name,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(attrs...),
			)
			defer func() {
				if rec := recover(); rec != nil {
					span.RecordError(fmt.Errorf("panic: %v", rec))
					span.SetStatus(codes.Error, "panic")
					span.End()
					panic(rec)
				}
				span.End()
			}()

			rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r.WithContext(ctx))

			status := rw.StatusCode()
			span.SetAttributes(attribute.Int("http.response.status_code", status))
			if status >= 500 {
				span.SetStatus(codes.Error, strconv.Itoa(status))
			}
		})
	}
}

// statusWriter 捕获响应状态码，其余调用全部透传。
//
// 所有字段都在 mu 下访问：handler 完全可能在另一个 goroutine 里写响应
// （SSE 推送、异步 flush、Hijack 之后的后台写入），那时 ServeHTTP 已经返回、
// 中间件正在读 status —— 裸字段就是一条真实的 data race。
type statusWriter struct {
	http.ResponseWriter

	mu     sync.Mutex
	status int
	wrote  bool
}

// StatusCode 返回已写入的响应状态码（未显式写入时为 200）。
func (w *statusWriter) StatusCode() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status
}

func (w *statusWriter) WriteHeader(code int) {
	w.mu.Lock()
	if !w.wrote {
		w.status = code
		w.wrote = true
	}
	w.mu.Unlock()

	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	if !w.wrote {
		w.status = http.StatusOK
		w.wrote = true
	}
	w.mu.Unlock()

	return w.ResponseWriter.Write(b)
}

// Flush 透传底层 Flusher（SSE / 流式响应需要）。
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// NewRoundTripper 返回注入 trace 上下文的客户端 Transport：
// 为每个出站请求创建 CLIENT span 并把 traceparent 写进请求头。
//
//	base 为 nil 时使用 http.DefaultTransport。
//	典型接法（含 httpclient/ 包）：
//	  t := tracing.NewRoundTripper(http.DefaultTransport)
//	  client := &http.Client{Transport: t}
//
//	内部对请求做了 Clone（浅拷贝 + 复制 Header），不会污染调用方的
//	请求对象；body 指针共享，不影响读语义。
func NewRoundTripper(base http.RoundTripper) http.RoundTripper {
	return &roundTripper{base: base}
}

type roundTripper struct {
	base http.RoundTripper
}

func (t *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, span := tracer().Start(req.Context(), req.Method,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("http.request.method", req.Method),
			attribute.String("server.address", req.URL.Host),
			attribute.String("url.scheme", req.URL.Scheme),
		),
	)
	// 注意这里【不能】写 defer span.End()：RoundTrip 返回只代表响应头到了，
	// 调用方读取/关闭响应体的耗时（大响应、慢下游的主要耗时所在）会被漏在
	// span 之外，链路图里客户端耗时会系统性偏小、看不出慢在哪一跳。
	// 结束动作改由 spanBody 在读完或关闭时触发，见下面。
	var once sync.Once
	endSpan := func(err error) {
		once.Do(func() {
			if err != nil && !errors.Is(err, io.EOF) {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()
		})
	}

	out := req.Clone(ctx)
	InjectHeaders(ctx, out.Header)

	resp, err := t.transport().RoundTrip(out)
	if err != nil {
		endSpan(err)
		return resp, err
	}

	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	if resp.StatusCode >= 400 {
		// 客户端视角 4xx/5xx 都是"对方回了个错误响应"。
		span.SetStatus(codes.Error, strconv.Itoa(resp.StatusCode))
	}

	if resp.Body != nil && resp.Body != http.NoBody {
		resp.Body = &spanBody{ReadCloser: resp.Body, endSpan: endSpan}
	} else {
		// 没有响应体可读（204 / HEAD / 已被上游消费），就地结束。
		endSpan(nil)
	}
	return resp, nil
}

// spanBody 把 client span 的生命周期延长到"响应体读完或关闭"。
//
//   - Read 到 EOF → 正常读完，结束 span；
//   - Read 出错 → 记录错误后结束 span；
//   - Close → 兜底结束：调用方完全可能只读一部分就关掉（流式提前退出、
//     错误分支 return），那种情况下永远等不到 EOF，span 会一直挂着。
//
// endSpan 内部有 once 保护，上述路径重复触发只会生效一次。
type spanBody struct {
	io.ReadCloser
	endSpan func(error)
}

func (b *spanBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.endSpan(err) // io.EOF 由 endSpan 内部识别为"正常读完"
	}
	return n, err
}

func (b *spanBody) Close() error {
	err := b.ReadCloser.Close()
	b.endSpan(err)
	return err
}

func (t *roundTripper) transport() http.RoundTripper {
	if t.base == nil {
		return http.DefaultTransport
	}
	return t.base
}

func tracer() trace.Tracer { return Tracer(tracerName) }
