// Package lock 提供基于 Redis 的分布式锁。
//
// # 正确性来源
//
// 安全性靠三件事凑齐，缺一不可：
//
//  1. **唯一令牌**：每次 Acquire 生成 crypto/rand 随机 token，存为键值。
//     没有它，A 超时后会把 B 刚拿到的锁删掉。
//  2. **原子释放/续期**：释放与续期都是「GET 比对 token 再操作」，
//     必须在 Lua 脚本里完成 —— GET 和 DEL 之间锁易主的话，
//     非原子的两步照样会删错锁。
//  3. **TTL 强制存在**：SET NX 必须带 TTL。持锁进程崩溃后锁要能自行消失，
//     这是可用性兜底；代价是业务可能没跑完锁就过期了。
//
// # 过期问题与看门狗
//
// TTL 是双刃剑：设太短业务没跑完锁先过期，设太长持有者崩溃后阻塞别人太久。
// 自动续期（看门狗）解这个矛盾：先用较短的 TTL 拿锁，后台协程按 ttl/3
// 周期续期；进程崩溃则续期停止，锁按 TTL 自然过期。
//
//	clk := lock.New(rdb, lock.WithAutoRenew(0)) // 0 = 默认 ttl/3
//	l, err := clk.Acquire(ctx, "order:123", 10*time.Second)
//	if err != nil { return err }
//	defer l.Unlock(context.Background())
//	// 业务期间锁被看门狗自动续期；崩溃后 10s 内自动释放。
//
// # 最小用法
//
//	clk := lock.New(rdb)
//	l, err := clk.Acquire(ctx, "order:123", 10*time.Second)
//	if errors.Is(err, lock.ErrLocked) {
//		return ErrBusy // 别人持有，按业务处理
//	}
//	defer l.Unlock(context.Background())
package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrLocked 表示锁正被别人持有。用 errors.Is 判断。
	ErrLocked = errors.New("lock: already held")
	// ErrLost 表示锁已不属于本次持有：可能过期后被别人抢走，
	// 也可能已被自己释放。续期与解锁遇到时都返回它。
	ErrLost = errors.New("lock: lost")
)

// releaseScript / refreshScript 都以 token 比对为前提，
// 「检查 + 操作」两步必须原子，这正是必须用 Lua 的原因。
var (
	releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0`)

	refreshScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0`)
)

// Client 是锁的入口。零值不可用，请用 New 创建。
//
// 非单例设计：可以为一个 Redis 创建多个 Client（不同前缀、不同续期策略），
// 也可以直接共用一个。
type Client struct {
	cli    redis.UniversalClient
	prefix string

	// autoRenew 是看门狗开关，renewInterval 是续期周期。
	//
	// 两者必须分开：只用 renewInterval 一个字段无法区分
	// 「没调用过 WithAutoRenew（默认关闭）」与「WithAutoRenew(0)
	// （开启，周期按 ttl/3 算）」—— 而 0 恰恰是 Duration 的零值。
	autoRenew     bool
	renewInterval time.Duration
}

// Option 定制 Client。
type Option func(*Client)

// WithKeyPrefix 给所有锁键加前缀，默认无前缀。
//
// 多套业务共用一个 Redis 时建议设置，避免键冲突。
func WithKeyPrefix(p string) Option {
	return func(c *Client) { c.prefix = p }
}

// WithAutoRenew 开启自动续期（看门狗）。
//
//   - d > 0：固定周期续期；但周期大于等于锁 TTL 时会被钳到 ttl/3
//     （下限 100ms）—— 否则首次续期之前锁就过期了，续期必然失败。
//   - d <= 0：按 ttl/3 动态计算（下限 100ms）—— 推荐写 0 走这条。
//
// 续期失败的语义见 Lock.Lost。
func WithAutoRenew(d time.Duration) Option {
	return func(c *Client) {
		c.autoRenew = true
		c.renewInterval = d
	}
}

// New 创建锁客户端。
//
// cli 接受 go-redis 的 UniversalClient（*redis.Client /
// *redis.ClusterClient / *redis.Ring 均可），可直接传入
// access/redis 的 Client().UniversalClient()。
func New(cli redis.UniversalClient, opts ...Option) *Client {
	c := &Client{cli: cli}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Acquire 尝试获取一把分布式锁；被别人持有时立即返回 ErrLocked
// （不会阻塞等待，忙等轮询请调用方自己加 ticker）。
//
// ttl 是锁的存活上限：持有者崩溃后锁最长残留这么久。
// 开了自动续期时它同时是续期基准，不必为「业务可能很慢」而调大。
//
// ctx 只约束获取动作本身（网络往返），不影响锁的持有期。
func (c *Client) Acquire(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	token, err := newToken()
	if err != nil {
		return nil, err
	}

	full := c.prefix + key
	ok, err := c.cli.SetNX(ctx, full, token, ttl).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrLocked
	}

	l := &Lock{
		c:     c,
		key:   full,
		token: token,
		ttl:   ttl,
		lost:  make(chan struct{}),
	}
	l.startRenewal()
	return l, nil
}

// Lock 是一把已持有的分布式锁。
//
// 惯用法是 defer Unlock；开启自动续期后，Unlock / 丢失都会停掉续期协程。
type Lock struct {
	c     *Client
	key   string
	token string
	ttl   time.Duration

	// lost 在锁确定不再属于本次持有时关闭（续期失败、或确认过期）。
	lost     chan struct{}
	lostOnce sync.Once

	// 看门狗生命周期。
	//
	// renewMu 把「判空 → close → 置空」包成原子操作：早先版本是裸的
	// check-then-act，并发 Unlock 会各自读到非 nil 从而 double close
	// panic（close of closed channel），且 renewStop 本身构成数据竞争。
	//
	// stopped 在 Unlock 开始时置位，供在途续期区分「锁丢了」与
	// 「锁刚被自己主动释放」—— 解锁成功后 key 已不存在，在途 Refresh
	// 必然返回 ErrLost，若不加以区分就会向 Lost() 读方发出假丢失信号。
	renewMu     sync.Mutex
	renewStop   chan struct{}
	renewCancel context.CancelFunc
	stopped     atomic.Bool
}

// Key / Token 返回锁键与本次持有的令牌（排查用）。
func (l *Lock) Key() string   { return l.key }
func (l *Lock) Token() string { return l.token }

// Lost 返回一个 channel，锁确定丢失时被关闭。
//
// 开了自动续期才有意义：读方可以 select 它感知「业务还没跑完
// 但锁已易主」并尽快中止。未开启续期时永远不关闭 —— 没有续期
// 就没有失败的续期，过期丢失只能靠业务自查（用 Token GET）。
func (l *Lock) Lost() <-chan struct{} { return l.lost }

// Unlock 释放锁。
//
// 只会释放自己持有的锁（token 比对）；锁已易主或过期时返回 ErrLost，
// 但看门狗仍会停止 —— 锁已经不是你的了，续期没有意义。
//
// 可并发、可重复调用：重复 Unlock 不会 panic，只是后续调用会拿到
// ErrLost（key 已被第一次调用删掉）。
// 解锁动作建议用独立的 context（如 context.Background()），
// 不要用即将取消的请求 ctx。
func (l *Lock) Unlock(ctx context.Context) error {
	l.stopRenewal()

	n, err := releaseScript.Run(ctx, l.c.cli, []string{l.key}, l.token).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLost
	}
	return nil
}

// Refresh 把锁续期到 ttl。
//
// 用于手动续期（未开看门狗时）；锁已易主或过期返回 ErrLost。
func (l *Lock) Refresh(ctx context.Context, ttl time.Duration) error {
	n, err := refreshScript.Run(ctx, l.c.cli, []string{l.key}, l.token, ttl.Milliseconds()).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLost
	}
	return nil
}

// startRenewal 按客户端配置启动看门狗（未开启则为空操作）。
func (l *Lock) startRenewal() {
	if !l.c.autoRenew {
		return // 未开启看门狗
	}

	interval := l.c.renewInterval
	if interval <= 0 {
		// 传 0 或负数：按 ttl/3 动态计算（下限 100ms）。
		interval = l.ttl / 3
		if interval < 100*time.Millisecond {
			interval = 100 * time.Millisecond
		}
	} else if interval >= l.ttl && l.ttl > 0 {
		// 固定周期不能长于 TTL：否则首次续期之前锁就已过期，
		// 之后每次续期都失败并误报丢失。这里退化为 ttl/3。
		interval = l.ttl / 3
		if interval < 100*time.Millisecond {
			interval = 100 * time.Millisecond
		}
	}

	// 续期用独立 ctx：请求 ctx 取消不应导致锁失效。
	// 但 ctx 由 stopRenewal 取消，好让在途 Refresh 尽快返回。
	ctx, cancel := context.WithCancel(context.Background())

	// 通道先落局部变量再交给协程：此后 stopRenewal 会在锁内改写
	// l.renewStop 字段，协程只读局部变量，避免对这个字段的读写竞争。
	stop := make(chan struct{})
	l.renewStop, l.renewCancel = stop, cancel

	go func() {
		defer cancel()

		// 续期用独立 ctx：请求 ctx 取消不应导致锁失效。
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				err := l.Refresh(ctx, l.ttl)
				if err != nil {
					// 已主动解锁：key 不存在是预期结果，不是丢失。
					// 这一步判断必须与 Unlock 的 stopped 置位配对，
					// 否则一次成功的解锁会误触发 Lost()。
					if l.stopped.Load() {
						return
					}
					// 丢失（易主/过期）或 Redis 不可达都视为续期失败。
					// 保守处理：声明丢失并停止续期，让持有方尽快感知。
					l.lostOnce.Do(func() { close(l.lost) })
					return
				}
			}
		}
	}()
}

// stopRenewal 停掉看门狗（幂等，可并发调用）。
//
// 持有 renewMu 完成「取通道 → 置空」的原子切换，close 因而只可能发生
// 一次；同时取消续期 ctx，让在途 Refresh 尽快返回、协程尽快退出。
// 本函数不阻塞等待协程退出（Unlock 不应被 Redis 往返拖住），协程会在
// 下一个 tick 或当前 Refresh 返回后自行结束。
func (l *Lock) stopRenewal() {
	// 先立标志：告诉在途续期「这是主动释放，不是丢失」。
	l.stopped.Store(true)

	l.renewMu.Lock()
	stop, cancel := l.renewStop, l.renewCancel
	l.renewStop, l.renewCancel = nil, nil
	l.renewMu.Unlock()

	if stop == nil {
		return // 未开启续期，或已停过一次
	}
	if cancel != nil {
		cancel()
	}
	close(stop)
}

// newToken 生成 128-bit 随机令牌。
func newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
