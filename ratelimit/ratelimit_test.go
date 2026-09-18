package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// ---- 本地令牌桶 ----

// TestLocalBurst 令牌桶允许突发 burst 个请求，之后拒绝。
func TestLocalBurst(t *testing.T) {
	rl := NewLocal(LocalConfig{Rate: 0.0001, Burst: 3}) // 补充慢到可忽略
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		ok, err := rl.Allow(ctx, "u1")
		if err != nil || !ok {
			t.Fatalf("第 %d 次突发应放行, ok=%v err=%v", i+1, ok, err)
		}
	}
	ok, err := rl.Allow(ctx, "u1")
	if ok || err != nil {
		t.Fatalf("突发耗尽后应拒绝, ok=%v err=%v", ok, err)
	}
}

// TestLocalPerKeyIsolation 各 key 独立计数。
func TestLocalPerKeyIsolation(t *testing.T) {
	rl := NewLocal(LocalConfig{Rate: 0.0001, Burst: 1})
	ctx := context.Background()

	if ok, _ := rl.Allow(ctx, "a"); !ok {
		t.Fatal("key a 应放行")
	}
	if ok, _ := rl.Allow(ctx, "a"); ok {
		t.Fatal("key a 应已耗尽")
	}
	if ok, _ := rl.Allow(ctx, "b"); !ok {
		t.Fatal("key b 不应受 key a 影响")
	}
}

// TestLocalRefill 令牌按速率随时间补充。
func TestLocalRefill(t *testing.T) {
	rl := NewLocal(LocalConfig{Rate: 20, Burst: 1}) // 20/s ≈ 每 50ms 一个
	ctx := context.Background()

	if ok, _ := rl.Allow(ctx, "k"); !ok {
		t.Fatal("首令牌应放行")
	}
	if ok, _ := rl.Allow(ctx, "k"); ok {
		t.Fatal("桶空后应拒绝")
	}
	time.Sleep(60 * time.Millisecond) // 补充约 1 个令牌
	if ok, _ := rl.Allow(ctx, "k"); !ok {
		t.Fatal("等待后应重新放行")
	}
}

// TestLocalIdleRecycle 空闲 key 被回收后重新开始满桶计数。
func TestLocalIdleRecycle(t *testing.T) {
	rl := NewLocal(LocalConfig{Rate: 0.0001, Burst: 2, IdleTTL: 30 * time.Millisecond})
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if ok, _ := rl.Allow(ctx, "k"); !ok {
			t.Fatalf("第 %d 次应放行", i+1)
		}
	}
	if ok, _ := rl.Allow(ctx, "k"); ok {
		t.Fatal("桶应已耗尽")
	}

	time.Sleep(50 * time.Millisecond) // 超过 IdleTTL
	if ok, _ := rl.Allow(ctx, "k"); !ok {
		t.Fatal("key 回收后应视为新桶放行")
	}
}

// TestLocalWait 排队等待令牌而不是直接失败。
func TestLocalWait(t *testing.T) {
	rl := NewLocal(LocalConfig{Rate: 200, Burst: 1}) // 5ms 一个令牌
	ctx := context.Background()

	if ok, _ := rl.Allow(ctx, "k"); !ok {
		t.Fatal("首令牌应放行")
	}
	start := time.Now()
	if err := rl.Wait(ctx, "k"); err != nil {
		t.Fatalf("Wait 不应失败: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Errorf("Wait 应在补充周期内返回, 耗时 %v", time.Since(start))
	}
}

// TestLocalConcurrentKeys 并发访问不同 key 不竞态、不串数。
func TestLocalConcurrentKeys(t *testing.T) {
	rl := NewLocal(LocalConfig{Rate: 1000, Burst: 1000})
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ok, _ := rl.Allow(ctx, "key-"+string(rune('a'+n)))
				if !ok {
					t.Errorf("高 burst 不应拒绝")
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

// ---- Redis 滑动窗口 ----

func newTestRedis(t *testing.T) (*RedisLimiter, *redis.Client) {
	t.Helper()

	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })

	return NewRedis(cli, RedisConfig{Window: time.Second, Limit: 3}), cli
}

// TestRedisLimit 窗口内限流到 Limit 次。
func TestRedisLimit(t *testing.T) {
	rl, _ := newTestRedis(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		ok, err := rl.Allow(ctx, "u1")
		if err != nil || !ok {
			t.Fatalf("前 %d 次应放行, ok=%v err=%v", i+1, ok, err)
		}
	}
	ok, err := rl.Allow(ctx, "u1")
	if ok || err != nil {
		t.Fatalf("配额满后应拒绝, ok=%v err=%v", ok, err)
	}
}

// TestRedisPerKeyIsolation 与滑动窗口各 key 独立。
func TestRedisPerKeyIsolation(t *testing.T) {
	rl, _ := newTestRedis(t)
	ctx := context.Background()

	if ok, _ := rl.Allow(ctx, "a"); !ok {
		t.Fatal("key a 应放行")
	}
	if ok, _ := rl.Allow(ctx, "b"); !ok {
		t.Fatal("key b 不应受 key a 影响")
	}
}

// TestRedisWindowExpiry key 过期（超过窗口）后配额重置。
func TestRedisWindowExpiry(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })

	rl := NewRedis(cli, RedisConfig{Window: 200 * time.Millisecond, Limit: 1})
	ctx := context.Background()

	if ok, err := rl.Allow(ctx, "k"); err != nil || !ok {
		t.Fatalf("首次应放行, ok=%v err=%v", ok, err)
	}
	if ok, _ := rl.Allow(ctx, "k"); ok {
		t.Fatal("配额满后应拒绝")
	}

	mr.FastForward(300 * time.Millisecond) // key 已过期
	if ok, err := rl.Allow(ctx, "k"); err != nil || !ok {
		t.Fatalf("窗口滑过后应重新放行, ok=%v err=%v", ok, err)
	}
}

// TestRedisPrefix 前缀选项生效且不同前缀互不影响。
func TestRedisPrefix(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })

	a := NewRedis(cli, RedisConfig{Window: time.Second, Limit: 1, Prefix: "svc-a:"})
	b := NewRedis(cli, RedisConfig{Window: time.Second, Limit: 1, Prefix: "svc-b:"})
	ctx := context.Background()

	if ok, _ := a.Allow(ctx, "k"); !ok {
		t.Fatal("svc-a 应放行")
	}
	if ok, _ := a.Allow(ctx, "k"); ok {
		t.Fatal("svc-a 应已耗尽")
	}
	if ok, _ := b.Allow(ctx, "k"); !ok {
		t.Fatal("svc-b 不应受 svc-a 配额影响")
	}
}

// TestRedisErrorPassthrough Redis 故障时错误原样上抛，由调用方决定降级。
func TestRedisErrorPassthrough(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rl := NewRedis(cli, RedisConfig{Window: time.Second, Limit: 1})
	ctx := context.Background()

	mr.Close() // 直接关掉，制造网络错误

	if _, err := rl.Allow(ctx, "k"); err == nil {
		t.Fatal("Redis 不可达时应返回错误")
	}
}

// TestLimiterInterface 两种限流器可互换（编译期约束）。
func TestLimiterInterface(t *testing.T) {
	var _ Limiter = NewLocal(LocalConfig{Rate: 1, Burst: 1})
	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })
	var _ Limiter = NewRedis(cli, RedisConfig{Limit: 1})
}
