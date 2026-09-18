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

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
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
