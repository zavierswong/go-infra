// Package idempotency 提供基于 Redis 的幂等执行器。
//
// 核心语义：同一 key 的操作在结果保留窗口内至多成功执行一次；
// 窗口内的重复请求直接复用缓存结果（Replayed=true），不产生重复副作用。
// 典型场景：MQ 消费去重（at-least-once 重投）、支付回调去重、
// 重复提交拦截、httpclient 重试 + 非幂等请求的兜底。
//
// 实现建立在 lock 包之上：「执行权」由分布式锁互斥（带看门狗续期，
// 慢任务不丢锁；持有者崩溃后锁按 TTL 自动释放，等待方接管重试），
// 「结果」以 JSON envelope 缓存并附带 TTL。
//
// 错误语义（重要）：
//   - fn 失败 → 结果【不】缓存，下一次请求会重新执行（失败可重试）；
//   - fn 成功但结果写 Redis 失败 → 返回错误（宁可让调用方重试，
//     也不能谎报成功而下次重复执行）。
package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zavierswong/go-infra/lock"
)

// 默认配置值。
const (
	DefaultKeyPrefix    = "idem:"
	DefaultLockTTL      = 30 * time.Second
	DefaultResultTTL    = 24 * time.Hour
	DefaultPollInterval = 50 * time.Millisecond
)

// Config 幂等执行器配置。
type Config struct {
	// KeyPrefix 是所有幂等 key 的统一前缀。默认 "idem:"。
	KeyPrefix string

	// LockTTL 是执行锁的存活上限（持有者崩溃后锁最长残留这么久）。
	// 开启看门狗后不必为「业务慢」调大。默认 30s。
	LockTTL time.Duration

	// ResultTTL 是成功结果的保留窗口；窗口内的重复请求直接重放。
	// 默认 24h。按业务幂等窗口需求设置（如支付回调通常 24h 起步）。
	ResultTTL time.Duration

	// PollInterval 是等待他人执行完成时的轮询间隔。默认 50ms。
	PollInterval time.Duration
}

// ErrInProgress 表示等待其他实例执行完成时超时/被取消。
// 用 errors.Is 判断；其携带的底层错误是 ctx.Err()。
var ErrInProgress = errors.New("idempotency: 另一次执行仍在进行中")

// ErrResultNotStored 表示 fn 已成功执行（副作用已发生），但结果写入 Redis
// 失败（内部已重试 resultWriteAttempts 次仍失败）。
//
// 调用方【不要】直接重试 Do：此时执行锁已经释放，重试会再执行一次 fn，
// 把「结果没写进去」升级成「副作用发生了两次」。正确处置：
//  1. 结果可重建 → 调用 [Store] 把已知结果补写进缓存，后续请求照常重放；
//  2. 结果不可重建（fn 非幂等）→ 记录告警转人工/对账，ResultTTL 之后
//     该 key 自然失效。
var ErrResultNotStored = errors.New("idempotency: 结果写入失败（副作用已发生）")

// resultWriteAttempts 结果写入的尝试次数。
//
// 结果写不进去的代价远高于多试两次：副作用已经发生，而后续请求查不到
// 结果就会重新执行 fn。因此在 sysTimeout 预算内重试几次，把「Redis
// 瞬时抖动」这一类可自愈失败挡掉。
const resultWriteAttempts = 3

// pollJitterRatio 轮询间隔的抖动幅度（±30%）。
//
// 所有等待者用同一个固定间隔会同时醒来抢锁（惊群）：每轮只有 1 个成功，
// 其余 N-1 个在下一轮继续同时冲上来 —— 锁竞争与 Redis QPS 都被放大。
const pollJitterRatio = 0.3

// jittered 给轮询间隔叠加 ±pollJitterRatio 的随机抖动。
func jittered(base time.Duration) time.Duration {
	if base <= 0 {
		return base
	}
	delta := int64(float64(base) * pollJitterRatio)
	if delta <= 0 {
		return base
	}
	return base + time.Duration(rand.Int64N(2*delta+1)) - time.Duration(delta)
}

// sysTimeout 是「系统动作」（写结果、释放锁、复核执行权）的操作上限。
//
// 这些动作不能绑调用方 ctx —— 请求取消不应导致「副作用已发生但结果没写
// 进去」，否则下一次请求会重复执行副作用，幂等承诺直接被打破。
// 但也不能无限等待：Redis 不可达时应尽快失败，而不是把 goroutine 钉死。
const sysTimeout = 3 * time.Second

// sysCtx 返回与调用方 ctx 解耦、但带超时上限的系统动作 ctx。
func sysCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(context.Background()), sysTimeout)
}

// envelope 是结果缓存条目的 JSON 结构。
// 执行权由锁 key 表达，结果 key 只会出现 "ok" 状态。
type envelope struct {
	Status string          `json:"s"` // "ok"
	Value  json.RawMessage `json:"v,omitempty"`
}

// Processor 幂等执行器。
type Processor struct {
	cli   redis.UniversalClient
	cfg   Config
	locks *lock.Client
}

// New 创建幂等执行器。cli 可复用 access/redis 的底层连接。
func New(cli redis.UniversalClient, cfg Config) (*Processor, error) {
	if cli == nil {
		return nil, errors.New("idempotency: nil redis client")
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = DefaultKeyPrefix
	}
	if cfg.LockTTL <= 0 {
		cfg.LockTTL = DefaultLockTTL
	}
	if cfg.ResultTTL <= 0 {
		cfg.ResultTTL = DefaultResultTTL
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	return &Processor{
		cli: cli,
		cfg: cfg,
		// 锁客户端不加前缀：锁键统一由 Do 拼好（full+":lock"）传入，
		// 避免 KeyPrefix 叠加两次。
		locks: lock.New(cli, lock.WithAutoRenew(0)), // ttl/3 看门狗，慢任务不丢锁

	}, nil
}

// Result 幂等执行结果。
type Result[T any] struct {
	// Value 是本次执行（或重放）的业务结果。
	Value T
	// Replayed 为 true 表示结果来自此前成功执行的缓存，fn 未被调用。
	Replayed bool
}

// Do 以 key 为幂等键执行 fn。
//
// 流程：查结果缓存（命中 → 重放）→ 抢执行锁（失败 → 轮询等待，
// 期间不断复查结果与锁）→ 双重检查 → 执行 fn → 缓存结果 → 释放锁。
//
//   - fn 失败：错误原样上抛，结果不缓存，下次请求重新执行；
//   - fn panic：锁经 defer 释放后 panic 继续向调用方传播；
//   - 等待期间 ctx 取消/超时：返回包裹 ErrInProgress 的错误；
//   - fn 收到的 ctx 会在执行锁意外丢失（如 Redis 长时间不可用导致
//     看门狗续期失败）时被取消，fn 应及时响应退出。
//
// T 需可 JSON 序列化（结果经 Redis 缓存）。
func Do[T any](ctx context.Context, p *Processor, key string, fn func(context.Context) (T, error)) (Result[T], error) {
	if key == "" {
		return Result[T]{}, errors.New("idempotency: empty key")
	}
	full := p.cfg.KeyPrefix + key
	lockKey := full + ":lock"

	// 快路径：结果已在窗口内，直接重放。
	if res, ok, err := tryReplay[T](ctx, p.cli, full); ok || err != nil {
		return res, err
	}

	// 抢执行锁。
	l, err := p.locks.Acquire(ctx, lockKey, p.cfg.LockTTL)
	switch {
	case err == nil:
		return execute[T](ctx, p, l, full, fn)
	case errors.Is(err, lock.ErrLocked):
		// 进入轮询：持有者成功 → 重放；失败/崩溃 → 接管。
	default:
		return Result[T]{}, fmt.Errorf("idempotency: acquire lock: %w", err)
	}

	ticker := time.NewTicker(jittered(p.cfg.PollInterval))
	defer ticker.Stop()
	for {
		// ctx 可能在 tryReplay / Acquire 的 Redis 往返**期间**被取消，
		// 那时拿到的是裸的 "context canceled"，调用方无从区分
		// 「另一次执行在进行中」与「Redis 故障」。在这里统一收敛。
		if err := ctx.Err(); err != nil {
			return Result[T]{}, fmt.Errorf("%w: %v", ErrInProgress, err)
		}

		res, ok, err := tryReplay[T](ctx, p.cli, full)
		if ok || err != nil {
			return res, err
		}
		l, err := p.locks.Acquire(ctx, lockKey, p.cfg.LockTTL)
		switch {
		case err == nil:
			return execute[T](ctx, p, l, full, fn)
		case errors.Is(err, lock.ErrLocked):
			// 仍被持有，继续等。
		default:
			return Result[T]{}, fmt.Errorf("idempotency: acquire lock: %w", err)
		}
		select {
		case <-ctx.Done():
			return Result[T]{}, fmt.Errorf("%w: %v", ErrInProgress, ctx.Err())
		case <-ticker.C:
			// 下一轮换成新的抖动间隔，把等待者的唤醒时刻摊开。
			ticker.Reset(jittered(p.cfg.PollInterval))
		}
	}
}

// tryReplay 查询结果缓存并反序列化。命中返回 (结果, true, nil)；
// 未命中返回 (零值, false, nil)；Redis 故障返回 err。
func tryReplay[T any](ctx context.Context, cli redis.UniversalClient, full string) (Result[T], bool, error) {
	b, err := cli.Get(ctx, full).Bytes()
	if err == redis.Nil {
		return Result[T]{}, false, nil
	}
	if err != nil {
		return Result[T]{}, false, fmt.Errorf("idempotency: get result: %w", err)
	}
	var env envelope
	if err := json.Unmarshal(b, &env); err != nil || env.Status != "ok" {
		// 结构性脏数据（写入被截断、外部误写、envelope 版本不兼容）：
		// 只当作未命中的话，它会一直占着这个 key 直到 TTL，期间的每次
		// 请求都要多解析一次垃圾。这里顺手删掉 —— 删除失败无所谓，
		// 反正还有 TTL 兜底，且下次成功后会被覆盖。
		sys, cancel := sysCtx()
		defer cancel()
		_ = cli.Del(sys, full).Err()
		return Result[T]{}, false, nil
	}
	var v T
	if err := json.Unmarshal(env.Value, &v); err != nil {
		return Result[T]{}, false, nil
	}
	return Result[T]{Value: v, Replayed: true}, true, nil
}

// execute 持有执行锁执行 fn：双重检查 → 运行 → 缓存结果 → 释放锁。
func execute[T any](ctx context.Context, p *Processor, l *lock.Lock, full string, fn func(context.Context) (T, error)) (Result[T], error) {
	// 解锁不依赖调用方 ctx（请求已取消时锁仍要释放），但要有超时上限。
	defer func() {
		sys, cancel := sysCtx()
		defer cancel()
		// ErrLost 是预期结局之一（执行权已被接管，锁本就不属于我们）；
		// 其他错误只意味着继任者要多等一个 LockTTL，锁会自行过期。
		// 两者都不改变「结果已经写好」这一事实，因此不上抛。
		_ = l.Unlock(sys)
	}()

	// 双重检查：上一个持有者可能在「写完结果、释放锁之前」崩溃。
	if res, ok, err := tryReplay[T](ctx, p.cli, full); ok || err != nil {
		return res, err
	}

	// 锁意外丢失（看门狗续期失败）时取消 fn，防止双执行。
	fnCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-l.Lost():
			cancel()
		case <-fnCtx.Done():
		}
	}()

	v, err := fn(fnCtx)
	if err != nil {
		return Result[T]{}, err // 不缓存失败结果，下次重新执行
	}

	data, err := json.Marshal(v)
	if err != nil {
		return Result[T]{}, fmt.Errorf("idempotency: marshal result: %w", err)
	}
	env, err := json.Marshal(envelope{Status: "ok", Value: data})
	if err != nil {
		return Result[T]{}, fmt.Errorf("idempotency: marshal envelope: %w", err)
	}
	// 写结果前复核执行权：fn 可能跑得比锁 TTL 久（或看门狗续期失败），
	// 此刻锁也许已被别人接管。不做这层校验的话，我们仍会把结果写进去、
	// 覆盖新持有者稍后写入的结果 —— 第二次执行的产出凭空消失，比单纯的
	// 双执行更难排查。fnCtx 取消只是「尽力通知」，fn 不响应 ctx 时
	// （README 明确承认这种情况）这层复核是唯一防线。
	sys, cancel := sysCtx()
	defer cancel()
	if err := l.Refresh(sys, p.cfg.LockTTL); err != nil {
		if errors.Is(err, lock.ErrLost) {
			// 副作用可能已经发生，且我们无从写入结果：必须让调用方知道
			// 这次是「悬空成功」，需要人工/对账介入，而不是重试。
			return Result[T]{}, fmt.Errorf("idempotency: 结果写回前执行权已被接管（副作用可能已发生）: %w", lock.ErrLost)
		}
		return Result[T]{}, fmt.Errorf("idempotency: verify ownership: %w", err)
	}

	// 结果必须先落 Redis 再返回成功：写失败时宁可报错让调用方重试，
	// 也不能谎报成功而下次请求重复执行副作用。
	if err := p.storeResult(full, env); err != nil {
		return Result[T]{}, err
	}
	return Result[T]{Value: v}, nil
}

// storeResult 写入结果信封；失败时在 sysTimeout 预算内重试。
//
// 返回的错误携带 [ErrResultNotStored]，调用方据此走补偿而不是盲目重试 Do。
func (p *Processor) storeResult(full string, env []byte) error {
	var err error
	for i := 0; i < resultWriteAttempts; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i) * 20 * time.Millisecond)
		}
		sys, cancel := sysCtx()
		err = p.cli.Set(sys, full, env, p.cfg.ResultTTL).Err()
		cancel()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("%w（key=%s，已尝试 %d 次）: %v", ErrResultNotStored, full, resultWriteAttempts, err)
}

// Store 把已知结果直接写入幂等缓存，用于 [ErrResultNotStored] 之后的补偿：
// fn 的副作用已经发生，只是结果没落库，把结果补写进去即可让后续请求
// 重放，避免重复执行。
//
//   - 已存在结果时不覆盖（SetNX 语义）：补偿路径上「别人已经写好」与
//     「我们写成功」等价；
//   - 写入不绑定调用方 ctx（补偿常常发生在请求已结束之后），
//     ctx 只用于让调用方提前放弃。
//
// 只有在确认副作用已发生、且结果可重建时才应该调用它。
func Store[T any](ctx context.Context, p *Processor, key string, v T) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if key == "" {
		return errors.New("idempotency: empty key")
	}
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("idempotency: marshal result: %w", err)
	}
	env, err := json.Marshal(envelope{Status: "ok", Value: data})
	if err != nil {
		return fmt.Errorf("idempotency: marshal envelope: %w", err)
	}
	sys, cancel := sysCtx()
	defer cancel()
	if _, err := p.cli.SetNX(sys, p.cfg.KeyPrefix+key, env, p.cfg.ResultTTL).Result(); err != nil {
		return fmt.Errorf("idempotency: store result: %w", err)
	}
	return nil
}
