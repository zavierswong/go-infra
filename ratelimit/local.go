// Package ratelimit 提供两种限流器：
//
//   - **本地令牌桶**（LocalLimiter）：进程内多 key 限流，零网络开销。
//     适合单实例保护（防本机被自己的业务打挂）。
//   - **Redis 滑动窗口**（RedisLimiter）：跨实例共享配额，
//     适合集群级保护（接口总 QPS、每用户全局配额）。
//
// 两者实现同一个 Limiter 接口，可以按场景替换：
//
//	type Limiter interface {
//		Allow(ctx context.Context, key string) (bool, error)
//	}
//
// key 的粒度由调用方决定：接口名、用户 ID、IP …… 都是常见选择。
// Redis 版的 key 会自动加前缀并拼接，调用方传业务语义即可。
//
// # 算法选型
//
// 令牌桶允许突发（桶里有存量），适合"平均速率受限、允许脉冲"的场景；
// 滑动窗口严格限制任意窗口内的请求数，没有边界突刺，
// 适合"配额"语义（每月 100 次、每分钟 60 条）。
//
// # 最小用法
//
//	rl := ratelimit.NewLocal(ratelimit.LocalConfig{Rate: 100, Burst: 200})
//	ok, _ := rl.Allow(ctx, "user:42")
//	if !ok {
//		return ErrTooManyRequests
//	}
package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Limiter 是两种限流器的统一接口。
//
// Redis 版会把网络错误从第二个返回值抛出 —— 想要「Redis 挂了就放行」
// 的降级语义由调用方决定，本包不替你决定。
type Limiter interface {
	// Allow 报告该 key 本次是否放行（不等待、不消耗未来配额）。
	Allow(ctx context.Context, key string) (bool, error)
}

// ---- 本地令牌桶 ----

const (
	defaultIdleTTL = 10 * time.Minute
	// sweepIntervalFactor 决定惰性清扫的间隔：每次访问检查
	//「距上次清扫是否超过 IdleTTL 的一半」，超过才扫，摊薄成本。
	sweepIntervalFactor = 2
)

// LocalConfig 是本地令牌桶配置。
type LocalConfig struct {
	// Rate 是每个 key 每秒补充的令牌数（平均速率）。
	Rate float64
	// Burst 是每个 key 的桶容量，决定允许的最大瞬时突发。
	Burst int
	// IdleTTL 是 key 连续空闲多久后被回收，防止 map 无界增长。
	// <=0 时取默认 10m。
	IdleTTL time.Duration
}

// LocalLimiter 是进程内的多 key 令牌桶，零值不可用，用 NewLocal 创建。
//
// 惰性清扫：不启动后台协程，key 回收发生在 Allow/Wait 路径上
// （间隔超过 IdleTTL/2 的首次访问触发一次全量扫描）。
type LocalLimiter struct {
	cfg LocalConfig

	mu        sync.Mutex
	buckets   map[string]*localBucket
	lastSweep time.Time
}

type localBucket struct {
	lim      *rate.Limiter
	lastSeen time.Time

	// waiters 记录正在 b.lim.Wait 中阻塞的调用数。
	// > 0 时 sweep 不得回收本桶：Wait 期间桶被删的话，随后的 Allow 会
	// 新建一个空桶，同一 key 同时存在两个限流器，实际放行速率翻倍。
	waiters int
}

// NewLocal 创建本地令牌桶。
//
// 一个 LocalLimiter 管理任意多个 key；每个 key 独立计数。
func NewLocal(cfg LocalConfig) *LocalLimiter {
	if cfg.IdleTTL <= 0 {
		cfg.IdleTTL = defaultIdleTTL
	}
	return &LocalLimiter{
		cfg:     cfg,
		buckets: make(map[string]*localBucket),
	}
}

// Allow 实现 Limiter。
func (l *LocalLimiter) Allow(_ context.Context, key string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.sweepLocked(time.Now())

	b := l.buckets[key]
	if b == nil {
		b = &localBucket{lim: rate.NewLimiter(rate.Limit(l.cfg.Rate), l.cfg.Burst)}
		l.buckets[key] = b
	}
	b.lastSeen = time.Now()
	return b.lim.Allow(), nil
}

// Wait 阻塞等待直到拿到令牌或 ctx 结束。
//
// 适合"不丢弃，只排队"的场景；Redis 版没有对应实现 ——
// 分布式排队要么靠队列要么靠重试，不该在限流器里做。
func (l *LocalLimiter) Wait(ctx context.Context, key string) error {
	l.mu.Lock()
	b := l.buckets[key]
	if b == nil {
		b = &localBucket{lim: rate.NewLimiter(rate.Limit(l.cfg.Rate), l.cfg.Burst)}
		l.buckets[key] = b
	}
	b.lastSeen = time.Now()
	// 注册为等待者：sweep 期间不得回收本桶（见 localBucket.waiters）。
	b.waiters++
	l.mu.Unlock()

	err := b.lim.Wait(ctx)

	// 收尾：注销等待者并刷新 lastSeen —— 后者同样关键，否则等待时长
	// 会被算进"空闲"，桶一醒过来就可能被判定为可回收。
	l.mu.Lock()
	b.waiters--
	b.lastSeen = time.Now()
	l.mu.Unlock()

	return err
}

// sweepLocked 回收空闲 key。调用方必须持有 mu。
func (l *LocalLimiter) sweepLocked(now time.Time) {
	if now.Sub(l.lastSweep) < l.cfg.IdleTTL/sweepIntervalFactor {
		return
	}
	l.lastSweep = now
	cutoff := now.Add(-l.cfg.IdleTTL)
	for k, b := range l.buckets {
		if b.waiters > 0 {
			continue // 有调用方正阻塞在这个桶上，回收会造成状态分裂
		}
		if b.lastSeen.Before(cutoff) {
			delete(l.buckets, k)
		}
	}
}
