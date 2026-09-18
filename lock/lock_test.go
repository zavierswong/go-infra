package lock

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestClient 起一个 miniredis 并返回直连客户端。
// Lua 脚本、TTL、SetNX 都由 miniredis 支持，测试确定且快。
func newTestClient(t *testing.T) (*Client, *redis.Client) {
	t.Helper()

	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })

	return New(cli), cli
}

// TestAcquireAndUnlock 基本获取/释放/再获取。
func TestAcquireAndUnlock(t *testing.T) {
	c, _ := newTestClient(t)
	ctx := context.Background()

	l, err := c.Acquire(ctx, "job:1", 10*time.Second)
	if err != nil {
		t.Fatalf("首次获取应成功: %v", err)
	}

	// 同一把锁第二次获取必须失败。
	if _, err := c.Acquire(ctx, "job:1", 10*time.Second); !errors.Is(err, ErrLocked) {
		t.Errorf("重复获取应返回 ErrLocked, got %v", err)
	}

	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("解锁应成功: %v", err)
	}

	// 释放后可再次获取，且令牌不同。
	l2, err := c.Acquire(ctx, "job:1", 10*time.Second)
	if err != nil {
		t.Fatalf("释放后应可重新获取: %v", err)
	}
	if l2.Token() == l.Token() {
		t.Error("两次持有的令牌不应相同")
	}
}

// TestAcquireExpiry 持有者不解锁时锁按 TTL 过期（可用性兜底）。
func TestAcquireExpiry(t *testing.T) {
	c, cli := newTestClient(t)
	ctx := context.Background()

	if _, err := c.Acquire(ctx, "job:2", 10*time.Second); err != nil {
		t.Fatalf("获取失败: %v", err)
	}

	// TTL 真实生效。
	ttl, err := cli.TTL(ctx, "job:2").Result()
	if err != nil || ttl <= 0 {
		t.Fatalf("锁应带 TTL, got %v, err %v", ttl, err)
	}
}

// TestUnlockWrongOwner 令牌不匹配时不得删除别人的锁。
func TestUnlockWrongOwner(t *testing.T) {
	c, cli := newTestClient(t)
	ctx := context.Background()

	if _, err := c.Acquire(ctx, "job:3", 10*time.Second); err != nil {
		t.Fatalf("获取失败: %v", err)
	}

	// 模拟锁已易主（过期后被别人抢走）。
	if err := cli.Set(ctx, "job:3", "someone-else", 10*time.Second).Err(); err != nil {
		t.Fatal(err)
	}

	l := &Lock{c: c, key: "job:3", token: "mine", lost: make(chan struct{})}
	if err := l.Unlock(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("易主后解锁应返回 ErrLost, got %v", err)
	}
	// 别人的锁还活着。
	if got := cli.Get(ctx, "job:3").Val(); got != "someone-else" {
		t.Errorf("不得删除他人的锁, got %q", got)
	}
}

// TestRefresh 锁还在时续期生效，易主/过期后返回 ErrLost。
func TestRefresh(t *testing.T) {
	c, cli := newTestClient(t)
	ctx := context.Background()

	l, err := c.Acquire(ctx, "job:4", 10*time.Second)
	if err != nil {
		t.Fatalf("获取失败: %v", err)
	}
	if err := l.Refresh(ctx, 30*time.Second); err != nil {
		t.Fatalf("续期应成功: %v", err)
	}
	if ttl, _ := cli.TTL(ctx, "job:4").Result(); ttl < 29*time.Second {
		t.Errorf("续期后 TTL 应接近 30s, got %v", ttl)
	}

	// 锁过期后续期失败。
	if err := cli.Set(ctx, "job:4", "other", time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	if err := l.Refresh(ctx, 10*time.Second); !errors.Is(err, ErrLost) {
		t.Errorf("易主后续期应返回 ErrLost, got %v", err)
	}
}

// TestKeyPrefix 前缀选项真实生效。
func TestKeyPrefix(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })

	c := New(cli, WithKeyPrefix("go-infra:lock:"))
	ctx := context.Background()

	l, err := c.Acquire(ctx, "job:5", 10*time.Second)
	if err != nil {
		t.Fatalf("获取失败: %v", err)
	}
	if !strings.HasPrefix(l.Key(), "go-infra:lock:job:5") {
		t.Errorf("键应带前缀, got %q", l.Key())
	}
}

// TestAutoRenew 看门狗在 TTL 内持续续期，锁不会中途过期。
func TestAutoRenew(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })

	// ttl 200ms，若不续期 200ms 后就没了；续期周期 ttl/3 ≈ 66ms。
	c := New(cli, WithAutoRenew(0))
	ctx := context.Background()

	l, err := c.Acquire(ctx, "job:6", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("获取失败: %v", err)
	}

	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := c.Acquire(ctx, "job:6", 200*time.Millisecond); !errors.Is(err, ErrLocked) {
			t.Fatalf("看门狗存活期间锁不应被抢走, got %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 解锁后立即可被他人获取（续期协程已停）。
	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("解锁应成功: %v", err)
	}
	if _, err := c.Acquire(ctx, "job:6", 10*time.Second); err != nil {
		t.Errorf("解锁后应可获取: %v", err)
	}
}

// TestLostChannel 续期失败（锁被外部删掉）时 Lost 被关闭。
func TestLostChannel(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })

	c := New(cli, WithAutoRenew(30*time.Millisecond))
	ctx := context.Background()

	l, err := c.Acquire(ctx, "job:7", time.Minute)
	if err != nil {
		t.Fatalf("获取失败: %v", err)
	}

	// 外部删锁 → 下一次续期找不到自己的 token → 声明丢失。
	if err := cli.Del(ctx, "job:7").Err(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-l.Lost():
		// 预期：续期协程感知丢失。
	case <-time.After(2 * time.Second):
		t.Fatal("锁被删除后 Lost 应被关闭")
	}

	// 丢失后解锁应返回 ErrLost，且不再 panic（看门狗已退出）。
	if err := l.Unlock(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("锁已不在, 解锁应返回 ErrLost, got %v", err)
	}
}

// TestUnlockIdempotentStop Unlock 可重复调用且停掉看门狗。
func TestUnlockIdempotentStop(t *testing.T) {
	c, _ := newTestClient(t)
	ctx := context.Background()

	l, err := c.Acquire(ctx, "job:8", time.Minute)
	if err != nil {
		t.Fatalf("获取失败: %v", err)
	}
	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("第一次解锁应成功: %v", err)
	}
	if err := l.Unlock(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("第二次解锁应返回 ErrLost, got %v", err)
	}
}

// TestConcurrentUnlock 回归测试：并发 Unlock 不得 panic。
//
// 旧实现里 stopRenewal 是裸的 check-then-act（if renewStop != nil { close }），
// 两个 goroutine 同时通过判空就会 double close → "close of closed channel"，
// 且 renewStop 字段本身是无保护的数据竞争。
func TestConcurrentUnlock(t *testing.T) {
	c, _ := newTestClient(t)
	c = New(c.cli, WithAutoRenew(20*time.Millisecond))
	ctx := context.Background()

	const n = 8
	for round := 0; round < 20; round++ {
		l, err := c.Acquire(ctx, "job:concurrent", time.Minute)
		if err != nil {
			t.Fatalf("round %d: 获取失败: %v", round, err)
		}

		var wg sync.WaitGroup
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				_ = l.Unlock(ctx) // 只关心不 panic
			}()
		}
		wg.Wait()
	}
}

// refreshDelayHook 把「续期脚本」卡在发往 Redis 之前，从而确定性地
// 制造出「在途续期」—— 这是复现假丢失信号的必要条件。
//
// 续期脚本比释放脚本多一个 ttl 参数（EVAL 系列共 6 个参数），据此区分。
type refreshDelayHook struct {
	entered chan struct{}   // 进入续期时通知（缓冲 1，只通知一次）
	release <-chan struct{} // 放行信号（测试中通过 close 放行）
}

func (h refreshDelayHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h refreshDelayHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h refreshDelayHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		name := cmd.Name()
		if (name == "eval" || name == "evalsha") && len(cmd.Args()) == 6 {
			select {
			case h.entered <- struct{}{}:
			default:
			}
			<-h.release
		}
		return next(ctx, cmd)
	}
}

// TestUnlockNoFalseLost 回归测试：一次成功的 Unlock 不得触发 Lost()。
//
// 旧实现中 Unlock 只关闭停止通道、不等看门狗退出，在途的 Refresh 会在
// key 被删除之后到达 Redis、拿到 0 而返回 ErrLost，于是向读方发出一次
// 假丢失信号。本用例用钩子把续期卡在发送前，让该时序 100% 复现。
func TestUnlockNoFalseLost(t *testing.T) {
	mr := miniredis.RunT(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})

	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })
	cli.AddHook(refreshDelayHook{entered: entered, release: release})

	c := New(cli, WithAutoRenew(20*time.Millisecond))
	ctx := context.Background()

	l, err := c.Acquire(ctx, "job:nolost", time.Minute)
	if err != nil {
		t.Fatalf("获取失败: %v", err)
	}

	// 等看门狗进入第一次续期，并卡在发往 Redis 之前。
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("看门狗未发起续期")
	}

	// 此刻续期在途：解锁删除 key，再放行续期。
	// 旧实现下续期随后返回 ErrLost 并关闭 lost。
	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("解锁应成功: %v", err)
	}
	close(release)

	select {
	case <-l.Lost():
		t.Fatal("主动解锁误触发了 Lost()")
	case <-time.After(300 * time.Millisecond):
	}
}

// TestWithAutoRenewZeroEnablesWatchdog 回归测试：WithAutoRenew(0) 必须真的
// 启用看门狗。
//
// 旧实现用 renewInterval == 0 同时表达「默认不开启」和「WithAutoRenew(0)」，
// 于是 startRenewal 在 interval==0 时直接 return —— 文档承诺的「d <= 0 按
// ttl/3 续期」从未生效。后果不只是锁会过期：Lost() 只在续期失败时关闭，
// 看门狗没启动就永远不关闭，依赖 Lost() 做「丢失即取消」的调用方
// （如 idempotency）整条保护链变成死代码。
//
// 注意：miniredis 的 TTL 走虚拟时钟，必须显式 FastForward 才会推进，
// 所以这里起一个协程让虚拟时钟跟着真实时钟走。
func TestWithAutoRenewZeroEnablesWatchdog(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				mr.FastForward(5 * time.Millisecond)
			}
		}
	}()
	defer func() { close(stop); wg.Wait() }()

	const ttl = 600 * time.Millisecond
	// 文档承诺：0 表示「开启，周期按 ttl/3 计算」。
	c := New(cli, WithAutoRenew(0))

	l, err := c.Acquire(context.Background(), "job:watchdog", ttl)
	if err != nil {
		t.Fatalf("获取失败: %v", err)
	}
	defer func() { _ = l.Unlock(context.Background()) }()

	// 跨越 2 个 TTL：没有看门狗的话键早已过期消失。
	time.Sleep(2 * ttl)

	if !mr.Exists("job:watchdog") {
		t.Fatal("WithAutoRenew(0) 未启用看门狗：锁在 TTL 后过期了")
	}
	select {
	case <-l.Lost():
		t.Fatal("看门狗续期正常，却误报了 Lost")
	default:
	}
}

// TestWithoutAutoRenewExpires 对照组：未开启自动续期时锁必须按 TTL 过期。
// 保证上一条的断言不是因为「虚拟时钟没推进」而假通过。
func TestWithoutAutoRenewExpires(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				mr.FastForward(5 * time.Millisecond)
			}
		}
	}()
	defer func() { close(stop); wg.Wait() }()

	const ttl = 300 * time.Millisecond
	c := New(cli) // 不开启自动续期

	l, err := c.Acquire(context.Background(), "job:nowatchdog", ttl)
	if err != nil {
		t.Fatalf("获取失败: %v", err)
	}
	defer func() { _ = l.Unlock(context.Background()) }()

	time.Sleep(2 * ttl)

	if mr.Exists("job:nowatchdog") {
		t.Fatal("未开启自动续期时，锁不应在 2 倍 TTL 后仍存活")
	}
}
