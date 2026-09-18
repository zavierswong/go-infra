// Package shutdown 提供统一的优雅退出：signal 收口 → 通知业务退出 →
// 按注册的逆序执行清理钩子（每钩子独立超时，受总预算约束）。
//
// 典型 main：
//
//	r := shutdown.New(shutdown.Config{Logger: logger.Default()})
//	r.Add("http-server", srv.Shutdown)
//	r.Add("mq-consumer", consumer.Close)
//	r.Add("db-pool", db.Close)          // 最先注册 → 最后关闭（依赖顺序）
//	if err := r.Run(func(ctx context.Context) error {
//	    return srv.ListenAndServe()      // 业务主循环
//	}); err != nil && !errors.Is(err, http.ErrServerClosed) {
//	    log.Fatal(err)
//	}
//
// 关闭顺序约定：与注册顺序相反（后注册的先关）—— 依赖上层资源的
// 组件（HTTP server、consumer）先停，最底层的连接池最后关。
package shutdown

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
)

// 默认信号与超时。
var (
	DefaultSignals = []os.Signal{syscall.SIGINT, syscall.SIGTERM}
	DefaultTimeout = 30 * time.Second
	// DefaultHookTimeout 单个钩子未显式指定超时时使用。
	DefaultHookTimeout = 10 * time.Second
)

// HookFunc 清理钩子。ctx 带钩子超时（与总预算取小），
// 超时后应尽快放弃等待资源、返回错误。
type HookFunc func(ctx context.Context) error

type hook struct {
	name    string
	timeout time.Duration // 0 用 DefaultHookTimeout
	fn      HookFunc
}

// Config 优雅退出配置。零值字段取默认值，可直接 New(Config{})。
type Config struct {
	// Signals 监听的退出信号，nil 用 SIGINT + SIGTERM。
	Signals []os.Signal

	// Timeout 是"收到信号 → 全部钩子执行完"的总预算，<=0 用 DefaultTimeout。
	// 业务主循环的退出也在预算内。
	Timeout time.Duration

	// HookTimeout 单个钩子的默认超时，<=0 用 DefaultHookTimeout。
	// 可被 AddTimeout 覆盖。
	HookTimeout time.Duration

	// Logger 钩子执行与信号日志；nil 用 slog.Default()。
	Logger *slog.Logger
}

// Runner 优雅退出协调器。注册钩子后调用 Run。
// 一个进程一个实例即可；Run 不可重入。
type Runner struct {
	cfg   Config
	log   *slog.Logger
	hooks []hook

	mu     sync.Mutex
	called bool
}

// New 构造 Runner。
func New(cfg Config) *Runner {
	if len(cfg.Signals) == 0 {
		cfg.Signals = DefaultSignals
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.HookTimeout <= 0 {
		cfg.HookTimeout = DefaultHookTimeout
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Runner{cfg: cfg, log: log}
}

// Add 注册清理钩子，使用默认钩子超时。后注册的先执行。
func (r *Runner) Add(name string, fn HookFunc) {
	r.AddTimeout(name, 0, fn)
}

// AddTimeout 注册带独立超时的清理钩子。后注册的先执行。
// 须在 Run 之前完成全部注册。
func (r *Runner) AddTimeout(name string, timeout time.Duration, fn HookFunc) {
	if fn == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hooks = append(r.hooks, hook{name: name, timeout: timeout, fn: fn})
}

// Run 运行业务主循环并接管退出流程：
//
//  1. 监听 Config.Signals；
//  2. 业务正常返回 → 直接执行钩子（清理语义统一）；
//  3. 收到信号 → cancel 业务 ctx（业务应监听并尽快返回）；
//  4. 第二次信号 → 立即 os.Exit(1)（强制兜底，不再优雅）；
//  5. 业务返回（或总预算耗尽）→ 逆序执行钩子，每钩子超时取
//     min(钩子超时, 总预算剩余)。
//
// 返回值 = 业务错误 + 各钩子错误（errors.Join）；业务从未返回且
// 预算耗尽时，其错误为"budget exceeded"。
//
// 预算耗尽时 Run 会返回，但业务 goroutine 若没有响应 ctx 取消就仍会存活
// （Go 无法从外部杀掉 goroutine）—— 只能由业务自己遵守 ctx 约定。
func (r *Runner) Run(run func(ctx context.Context) error) error {
	r.mu.Lock()
	if r.called {
		r.mu.Unlock()
		return errors.New("shutdown: Run is not reentrant")
	}
	r.called = true
	hooks := r.hooks
	r.mu.Unlock()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, r.cfg.Signals...)
	defer signal.Stop(sigCh)

	// 业务 ctx：收到信号即取消。
	bizCtx, cancelBiz := context.WithCancel(context.Background())
	defer cancelBiz()

	runErr := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				runErr <- fmt.Errorf("shutdown: run panicked: %v\n%s", p, debug.Stack())
			}
		}()
		runErr <- run(bizCtx)
	}()

	// 第二次信号兜底：任何优雅流程都可能卡死，给运维一个硬退出。
	//
	// 关键：本协程必须在主流程收到「第一个」信号之后才武装（armed），
	// 不能与主线程同时阻塞在同一个 sigCh 上 —— Go 只会把到达的信号
	// 投递给其中一个接收者，且选择是随机的，两者各有一半概率拿到首个
	// 信号。兜底协程一旦抢到首信号就会直接 os.Exit(1)，优雅退出整体被
	// 跳过。因此这里先等 armed，确保它只消费「第二个及以后」的信号。
	armed := make(chan struct{})
	quit := make(chan struct{})
	defer close(quit)
	go func() {
		select {
		case <-armed: // 主流程已进入排空：开始监听强制退出信号。
		case <-quit: // 业务自行结束，Run 返回，无需兜底。
			return
		}
		select {
		case s := <-sigCh:
			r.log.Error("shutdown: second signal, forcing exit", "signal", s.String())
			os.Exit(1)
		case <-quit:
		}
	}()

	select {
	case s := <-sigCh:
		r.log.Info("shutdown: signal received, draining", "signal", s.String())
		close(armed)
		cancelBiz()
	case err := <-runErr:
		// 业务自行结束（正常或出错）：仍执行钩子，保证清理统一。
		if err != nil {
			r.log.Warn("shutdown: run returned with error", "error", err)
		}
		return errors.Join(err, r.executeHooks(hooks))
	}

	// 等业务退出，受总预算约束。
	budget, bcancel := context.WithTimeout(context.Background(), r.cfg.Timeout)
	defer bcancel()

	var runFailure error
	select {
	case err := <-runErr:
		if err != nil {
			r.log.Warn("shutdown: run returned with error", "error", err)
		}
	case <-budget.Done():
		runFailure = fmt.Errorf("shutdown: run did not exit within budget %v", r.cfg.Timeout)
		r.log.Error(runFailure.Error())
	}

	return errors.Join(runFailure, r.executeBudgetedHooks(budget, hooks))
}

// executeHooks 独立预算版（业务自行结束路径）：每钩子超时 = 显式值或默认值。
func (r *Runner) executeHooks(hooks []hook) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.Timeout)
	defer cancel()
	return r.executeBudgetedHooks(ctx, hooks)
}

// executeBudgetedHooks 逆序执行钩子。每钩子超时 = min(显式超时, 剩余预算)，
// 用 context.WithTimeout(budget, ...) 的"双截止日"语义自然实现：
// 剩余预算不足时以预算为准。
func (r *Runner) executeBudgetedHooks(budget context.Context, hooks []hook) error {
	var errs []error
	for i := len(hooks) - 1; i >= 0; i-- {
		h := hooks[i]
		timeout := h.timeout
		if timeout <= 0 {
			timeout = r.cfg.HookTimeout
		}
		hctx, cancel := context.WithTimeout(budget, timeout)

		start := time.Now()
		err := safeHook(hctx, h.fn)
		cancel()

		switch {
		case err == nil:
			r.log.Info("shutdown: hook done",
				"hook", h.name, "duration", time.Since(start).String())
		case errors.Is(err, context.DeadlineExceeded):
			errs = append(errs, fmt.Errorf("hook %q: %w", h.name, err))
			r.log.Error("shutdown: hook timed out",
				"hook", h.name, "duration", time.Since(start).String())
		default:
			errs = append(errs, fmt.Errorf("hook %q: %w", h.name, err))
			r.log.Error("shutdown: hook failed",
				"hook", h.name, "duration", time.Since(start).String(), "error", err)
		}
	}
	return errors.Join(errs...)
}

// safeHook 执行钩子并 recover panic —— 清理代码的 bug 不应
// 阻断后续钩子。
func safeHook(ctx context.Context, fn HookFunc) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("hook panicked: %v\n%s", p, debug.Stack())
		}
	}()
	return fn(ctx)
}
