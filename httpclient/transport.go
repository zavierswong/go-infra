package httpclient

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/zavierswong/go-infra/breaker"
)

// ---- 熔断层：请求粒度 Allow/Record ----

type breakerTransport struct {
	next http.RoundTripper
	br   *breaker.Breaker
}

func (t breakerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tkt, err := t.br.AllowTicket()
	if err != nil {
		return nil, err
	}
	resp, err := t.next.RoundTrip(req)
	switch {
	case err != nil:
		// 调用方自己的取消/超时**不算下游故障**：上游（网关、LB、前端）
		// 高频掐请求是常态，把它记进失败窗口会熔断一个实际健康的下游。
		// 但探测名额必须归还（RecordSkipped）：AllowTicket 放行的探测
		// 若无人归还名额，HalfOpen 会永久卡死在 ErrOpen —— 旧实现
		// 在这里直接 return，正是这个泄漏路径。
		if canceledByCaller(req.Context(), err) {
			t.br.RecordSkipped(tkt)
			return nil, err
		}
		t.br.RecordTicket(tkt, err)
	case resp.StatusCode >= http.StatusInternalServerError:
		// 5xx 记失败：下游已不可靠，继续放行只会雪崩。
		t.br.RecordTicket(tkt, errors.New("httpclient: upstream returned "+resp.Status))
	default:
		t.br.RecordTicket(tkt, nil)
	}
	return resp, err
}

// CloseIdleConnections 透传给底层 Transport。
//
// http.Client.CloseIdleConnections 的实现依赖运行时类型断言
// （interface{ CloseIdleConnections() }），包装层不实现它时整个调用
// 会被静默吞掉 —— 优雅退出 / 配置热更新时空闲连接无法释放。
func (t breakerTransport) CloseIdleConnections() { passCloseIdle(t.next) }

func passCloseIdle(next http.RoundTripper) {
	if c, ok := next.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

// canceledByCaller 判断错误是否来自调用方自己的取消/超时。
//
// 判定必须同时看 ctx 是否已结束：单看错误的话，下游返回的、恰好包装了
// 同名哨兵的错误会被误判成"调用方取消"。
// 与重试层的处理（shouldRetry 里排除 Canceled/DeadlineExceeded）对称。
func canceledByCaller(ctx context.Context, err error) bool {
	if ctx == nil || ctx.Err() == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// ---- 重试层：幂等方法 + 指数退避 ----

type retryTransport struct {
	next http.RoundTripper
	cfg  Config
}

func (t retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	maxRetries := t.cfg.maxRetries()

	backoff := t.cfg.Backoff
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			if err := rewindBody(req); err != nil {
				return nil, err
			}
		}

		resp, err := t.next.RoundTrip(req)
		if attempt >= maxRetries || !t.shouldRetry(req, resp, err) {
			return resp, err
		}
		drainAndClose(resp)

		if werr := sleepCtx(ctx, backoff); werr != nil {
			// 调用方取消/超时：重试循环立即终止。
			return nil, ctx.Err()
		}
		backoff *= 2
		if backoff > t.cfg.MaxBackoff {
			backoff = t.cfg.MaxBackoff
		}
	}
}

func (t retryTransport) CloseIdleConnections() { passCloseIdle(t.next) }

// shouldRetry 判定本轮结果是否值得重试。可重放性检查始终生效：
// 带请求体但 GetBody 为 nil 的请求永远不重试（body 已被读完，
// 盲目重放会把空体发给下游）。
//
// 默认策略还要求方法幂等：POST/PATCH 等非幂等方法不自动重试 ——
// "请求体可重放"只解决传输层可行性，不代表业务语义安全（5xx 意味着
// 下游可能已经执行，重试等于重复副作用）。确实需要重试非幂等请求时，
// 请显式配置 RetryShould（它不受幂等检查约束）。
func (t retryTransport) shouldRetry(req *http.Request, resp *http.Response, err error) bool {
	if req.Body != nil && req.Body != http.NoBody && req.GetBody == nil {
		return false
	}
	if t.cfg.RetryShould != nil {
		return t.cfg.RetryShould(resp, err)
	}
	if !idempotentMethod(req.Method) {
		return false
	}
	if err != nil {
		// 调用方主动取消/超时不是可重试故障。
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return false
		}
		// 熔断器拒绝放行也不重试（下层直接透传 ErrOpen）。
		if errors.Is(err, breaker.ErrOpen) {
			return false
		}
		return true
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	}
	return false
}

// idempotentMethod 默认重试策略认可的幂等方法。
// POST/PATCH 缺席是有意的：见 shouldRetry 的说明。
func idempotentMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut,
		http.MethodDelete, http.MethodOptions, http.MethodTrace:
		return true
	}
	return false
}

// rewindBody 用 GetBody 重放请求体。幂等方法通常无 body，此路径
// 主要覆盖带体的 PUT/DELETE/OPTIONS（http.NewRequest 对
// *bytes.Buffer / *bytes.Reader / *strings.Reader 自动设置 GetBody）。
func rewindBody(req *http.Request) error {
	if req.Body == nil || req.Body == http.NoBody {
		return nil
	}
	if req.GetBody == nil {
		return errUnreplayable
	}
	body, err := req.GetBody()
	if err != nil {
		return err
	}
	req.Body = body
	return nil
}

// sleepCtx 可被 ctx 取消的退避等待。
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// drainAndClose 读完并关闭响应体。重试前必须 drain，
// 否则底层连接无法复用（连接泄漏）。
func drainAndClose(resp *http.Response) {
	if resp == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	_ = resp.Body.Close()
}

// ---- 日志层：每次尝试一条记录 ----

type loggingTransport struct {
	next   http.RoundTripper
	name   string
	logger *slog.Logger
}

func (t loggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := t.next.RoundTrip(req)

	attrs := []any{
		"client", t.name,
		"method", req.Method,
		"url", req.URL.String(),
		"duration", time.Since(start).String(),
	}
	switch {
	case err != nil:
		attrs = append(attrs, "error", err.Error())
		t.logger.Log(req.Context(), slog.LevelWarn, "httpclient: attempt failed", attrs...)
	case resp.StatusCode >= http.StatusInternalServerError:
		attrs = append(attrs, "status", resp.StatusCode)
		t.logger.Log(req.Context(), slog.LevelWarn, "httpclient: attempt", attrs...)
	default:
		attrs = append(attrs, "status", resp.StatusCode)
		t.logger.Log(req.Context(), slog.LevelInfo, "httpclient: attempt", attrs...)
	}
	return resp, err
}

func (t loggingTransport) CloseIdleConnections() { passCloseIdle(t.next) }
