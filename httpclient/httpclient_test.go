package httpclient_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zavierswong/go-infra/breaker"
	"github.com/zavierswong/go-infra/httpclient"
)

func newTestLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestRetryOn5xx 5xx 触发重试，GetBody 重放请求体，最终拿到成功响应。
// 方法用 PUT（幂等）：默认策略不重试非幂等方法（见 TestNoRetryOnPost）。
func TestRetryOn5xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// 校验重试确实重放了 body。
		body := new(bytes.Buffer)
		if _, err := body.ReadFrom(r.Body); err != nil {
			t.Errorf("read body: %v", err)
		}
		if body.String() != "hello" {
			t.Errorf("重试应重放 body, got %q", body.String())
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := httpclient.New(httpclient.Config{
		Name:       "t-retry",
		MaxRetries: 3,
		Backoff:    time.Millisecond,
	})

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPut,
		srv.URL, bytes.NewBufferString("hello"))
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("应拿到 200, got %d", resp.StatusCode)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("应尝试 3 次, got %d", got)
	}
}

// TestNoRetryOnPost 回归测试：非幂等的 POST 即使请求体可重放
// （GetBody 非 nil，甚至没有 body）也不得被默认策略重试 ——
// 5xx 意味着下游可能已经执行，重试等于重复副作用。
// 旧实现只检查"可重放性"从不读 req.Method，POST 会被打 3 次。
func TestNoRetryOnPost(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	c := httpclient.New(httpclient.Config{MaxRetries: 5, Backoff: time.Millisecond})

	// 无 body 的 POST：GetBody 检查直接通过，必须被幂等检查拦下。
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, nil)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()
	if got := attempts.Load(); got != 1 {
		t.Fatalf("非幂等 POST 不应重试, attempts=%d", got)
	}

	// 带 GetBody 的 POST（bytes.Reader 会自动生成）同样不重试。
	attempts.Store(0)
	req2, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
		srv.URL, bytes.NewBufferString("payload"))
	resp2, err := c.Do(req2)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp2.Body.Close()
	if got := attempts.Load(); got != 1 {
		t.Fatalf("可重放的 POST 同样不应重试, attempts=%d", got)
	}
}

// TestDefaultMaxRetries 回归测试：零值 Config 必须启用默认 2 次重试。
// 旧实现 retryEnabled() 要求 MaxRetries > 0，把整层重试直接不装配，
// 与 Config.MaxRetries 的文档（"<=0 按 DefaultMaxRetries 处理"）矛盾。
func TestDefaultMaxRetries(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	c := httpclient.New(httpclient.Config{}) // 零值：未设 MaxRetries、未禁用
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()
	if got := attempts.Load(); got != 1+httpclient.DefaultMaxRetries {
		t.Fatalf("零值 Config 应默认重试 %d 次, attempts=%d", httpclient.DefaultMaxRetries, got)
	}
}

// TestCloseIdleConnectionsPassthrough 回归测试：包装层必须把
// CloseIdleConnections 透传到底层 Transport。
// http.Client 的实现依赖运行时类型断言，包装层不实现该方法时
// 整个调用被静默吞掉，优雅退出时空闲连接无法释放。
func TestCloseIdleConnectionsPassthrough(t *testing.T) {
	var closed atomic.Int32
	base := &countingCloseTransport{inner: http.DefaultTransport, closed: &closed}

	c := httpclient.New(httpclient.Config{
		RetryDisabled: true,
		Breaker:       breaker.New(breaker.Config{}),
		Transport:     base,
	})
	c.CloseIdleConnections()

	if got := closed.Load(); got != 1 {
		t.Fatalf("CloseIdleConnections 应透传到底层 Transport, 调用 %d 次", got)
	}
}

type countingCloseTransport struct {
	inner  http.RoundTripper
	closed *atomic.Int32
}

func (t *countingCloseTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.inner.RoundTrip(req)
}

func (t *countingCloseTransport) CloseIdleConnections() { t.closed.Add(1) }

// TestNoRetryOnPostWithoutGetBody 带体但 GetBody 为 nil 的非幂等请求不重试。
func TestNoRetryOnPostWithoutGetBody(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	c := httpclient.New(httpclient.Config{MaxRetries: 3, Backoff: time.Millisecond})

	// 用自定义 ReadCloser 构造：NewRequest 不会为它设置 GetBody。
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
		srv.URL, io.NopCloser(strings.NewReader("payload")))
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	if got := attempts.Load(); got != 1 {
		t.Fatalf("不可重放的 POST 不应重试, attempts=%d", got)
	}
}

// TestRetryDisabled 显式关闭重试。
func TestRetryDisabled(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	c := httpclient.New(httpclient.Config{RetryDisabled: true})
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()
	if got := attempts.Load(); got != 1 {
		t.Fatalf("RetryDisabled 应只尝试 1 次, got %d", got)
	}
}

// TestNoRetryOnContextCancel 调用方取消后不再重试。
func TestNoRetryOnContextCancel(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	c := httpclient.New(httpclient.Config{MaxRetries: 10, Backoff: time.Second})
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	_, err := c.Do(req) // 退避 1s > ctx 50ms，第二次尝试前被取消
	if err == nil {
		t.Fatal("应返回 ctx 超时错误")
	}
	if got := attempts.Load(); got > 2 {
		t.Fatalf("取消后不应继续重试, attempts=%d", got)
	}
}

// TestBreakerOpens 连续 5xx 熔断后，后续请求不再打到下游。
func TestBreakerOpens(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	br := breaker.New(breaker.Config{
		Name:             "t-breaker",
		FailureThreshold: 1,
		OpenTimeout:      time.Hour,
	})
	c := httpclient.New(httpclient.Config{
		RetryDisabled: true,
		Breaker:       br,
	})

	// 第一次：真实请求，5xx 记失败并熔断。
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("第一次请求不应失败: %v", err)
	}
	resp.Body.Close()
	if attempts.Load() != 1 {
		t.Fatalf("第一次应打到下游, attempts=%d", attempts.Load())
	}

	// 第二次：熔断放不开，请求不到下游。
	_, err = c.Get(srv.URL)
	if !errors.Is(err, breaker.ErrOpen) {
		t.Fatalf("应返回 breaker.ErrOpen, got %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("熔断后不应打到下游, attempts=%d", got)
	}
}

// TestCallerCancelDoesNotTripBreaker 回归测试：调用方主动取消/超时
// 不得被记成下游失败。
//
// 旧实现里熔断层对任何 err 都 Record，于是"上游网关频繁掐请求"这种
// 常见场景会把一个健康的下游熔断掉 —— 与重试层排除 ctx 取消的处理不对称。
func TestCallerCancelDoesNotTripBreaker(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	br := breaker.New(breaker.Config{
		Name:             "t-cancel",
		FailureThreshold: 1, // 一次失败就熔断：只要被误记必然立刻可见
		OpenTimeout:      time.Hour,
	})
	c := httpclient.New(httpclient.Config{
		RetryDisabled: true,
		Breaker:       br,
	})

	// 已取消的 ctx：请求必然以 context.Canceled 失败。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if _, err := c.Do(req); !errors.Is(err, context.Canceled) {
		t.Fatalf("应返回 context.Canceled, got %v", err)
	}

	// 关键：熔断器不得被这次"调用方取消"打开。
	if got := br.State(); got != breaker.StateClosed {
		t.Fatalf("调用方取消不应熔断下游, state = %v", got)
	}

	// 下游依然可用：正常请求应当放行。
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("健康下游应可正常调用: %v", err)
	}
	resp.Body.Close()
}

// TestLoggingInjection 每次尝试写一条日志，含 name/method/status 字段。
func TestLoggingInjection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	buf := new(bytes.Buffer)
	c := httpclient.New(httpclient.Config{
		Name:   "order-svc",
		Logger: newTestLogger(buf),
	})
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	out := buf.String()
	for _, want := range []string{"client=order-svc", "method=GET", "status=200"} {
		if !strings.Contains(out, want) {
			t.Errorf("日志应包含 %q, got:\n%s", want, out)
		}
	}
}

// TestCustomRetryShould 自定义判定覆盖默认策略。
func TestCustomRetryShould(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusForbidden) // 403 默认不重试
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := httpclient.New(httpclient.Config{
		MaxRetries: 2,
		Backoff:    time.Millisecond,
		RetryShould: func(resp *http.Response, err error) bool {
			return err == nil && resp.StatusCode == http.StatusForbidden
		},
	})
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()
	if got := attempts.Load(); got != 2 {
		t.Fatalf("自定义判定应重试 403 一次, attempts=%d", got)
	}
}

// TestDefaults 归一化默认值可读回。
func TestDefaults(t *testing.T) {
	c := httpclient.New(httpclient.Config{Name: "d"})
	cfg := c.Config()
	if cfg.Timeout != httpclient.DefaultTimeout {
		t.Errorf("默认总超时 = %v, want %v", cfg.Timeout, httpclient.DefaultTimeout)
	}
	if cfg.Backoff != httpclient.DefaultBackoff {
		t.Errorf("默认初始退避 = %v, want %v", cfg.Backoff, httpclient.DefaultBackoff)
	}
	if cfg.MaxBackoff != httpclient.DefaultMaxBackoff {
		t.Errorf("默认退避上限 = %v, want %v", cfg.MaxBackoff, httpclient.DefaultMaxBackoff)
	}
}
