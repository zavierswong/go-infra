package cache_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/zavierswong/go-infra/cache"
)

func newMiniRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })
	return mr, cli
}

// --- Local ---

func TestLocalSetGet(t *testing.T) {
	l := cache.NewLocal(cache.LocalConfig{})
	defer l.Close()

	l.Set("a", 1)
	v, ok := l.Get("a")
	if !ok || v != 1 {
		t.Fatalf("Get(a) = %v, %v", v, ok)
	}

	if _, ok := l.Get("missing"); ok {
		t.Fatal("未设置 key 不应命中")
	}
	if l.Len() != 1 {
		t.Errorf("Len = %d, want 1", l.Len())
	}

	l.Delete("a")
	if _, ok := l.Get("a"); ok {
		t.Fatal("删除后不应命中")
	}
}

func TestLocalLRUEviction(t *testing.T) {
	l := cache.NewLocal(cache.LocalConfig{Size: 2})
	defer l.Close()

	l.Set("a", 1)
	l.Set("b", 2)
	_, _ = l.Get("a") // a 变为最近使用，b 成为最久未用
	l.Set("c", 3)     // 应淘汰 b

	if _, ok := l.Get("b"); ok {
		t.Fatal("b 应被 LRU 淘汰")
	}
	if _, ok := l.Get("a"); !ok {
		t.Fatal("a 不应被淘汰")
	}
	if _, ok := l.Get("c"); !ok {
		t.Fatal("c 不应被淘汰")
	}
	if l.Len() != 2 {
		t.Errorf("Len = %d, want 2", l.Len())
	}
}

func TestLocalTTLExpiry(t *testing.T) {
	l := cache.NewLocal(cache.LocalConfig{TTL: 30 * time.Millisecond})
	defer l.Close()

	l.Set("a", "x")
	if _, ok := l.Get("a"); !ok {
		t.Fatal("TTL 内应命中")
	}
	time.Sleep(40 * time.Millisecond)
	if _, ok := l.Get("a"); ok {
		t.Fatal("过期后不应命中")
	}
	if l.Misses() == 0 {
		t.Error("过期未命中应计入 Misses")
	}
}

func TestLocalConcurrent(t *testing.T) {
	l := cache.NewLocal(cache.LocalConfig{Size: 64})
	defer l.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				k := fmt.Sprintf("k%d", (base+j)%100)
				l.Set(k, j)
				_, _ = l.Get(k)
				if j%50 == 0 {
					l.Delete(k)
				}
			}
		}(i)
	}
	wg.Wait()
}

// --- Redis ---

func TestRedisRoundtrip(t *testing.T) {
	_, cli := newMiniRedis(t)
	r := cache.NewRedis(cli, cache.RedisConfig{KeyPrefix: "app1:", TTL: time.Minute})
	ctx := context.Background()

	if _, ok, err := r.Get(ctx, "k"); ok || err != nil {
		t.Fatalf("初始未命中: ok=%v err=%v", ok, err)
	}
	if err := r.Set(ctx, "k", []byte("v1"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	b, ok, err := r.Get(ctx, "k")
	if err != nil || !ok || string(b) != "v1" {
		t.Fatalf("Get = %s, %v, %v", b, ok, err)
	}

	// 前缀隔离：另一个前缀读不到。
	r2 := cache.NewRedis(cli, cache.RedisConfig{KeyPrefix: "app2:"})
	if _, ok, _ := r2.Get(ctx, "k"); ok {
		t.Fatal("前缀不应穿透")
	}

	if err := r.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, _ := r.Get(ctx, "k"); ok {
		t.Fatal("删除后不应命中")
	}
}

// --- Multi ---

func TestMultiGetOrLoadLevels(t *testing.T) {
	_, cli := newMiniRedis(t)
	m := cache.NewMulti(cli, cache.MultiConfig{
		Local: cache.LocalConfig{TTL: time.Minute},
		Redis: cache.RedisConfig{KeyPrefix: "m:"},
	})
	defer m.Close()
	ctx := context.Background()

	calls := 0
	loader := func(context.Context) (map[string]int, error) {
		calls++
		return map[string]int{"n": calls}, nil
	}

	v1, err := cache.GetOrLoad(ctx, m, "user:1", time.Minute, loader)
	if err != nil {
		t.Fatalf("首次加载: %v", err)
	}
	if v1["n"] != 1 || calls != 1 {
		t.Fatalf("首次应执行 loader, calls=%d", calls)
	}

	// 第二次：L1 命中，loader 不执行。
	v2, _ := cache.GetOrLoad(ctx, m, "user:1", time.Minute, loader)
	if v2["n"] != 1 || calls != 1 {
		t.Fatalf("第二次应 L1 命中, calls=%d", calls)
	}

	// 清掉 L1 后：L2 命中并回填 L1，loader 仍不执行。
	m.InvalidateLocal("user:1")
	v3, _ := cache.GetOrLoad(ctx, m, "user:1", time.Minute, loader)
	if v3["n"] != 1 || calls != 1 {
		t.Fatalf("第三次应 L2 命中, calls=%d", calls)
	}

	st := m.Stats()
	if st.L1Hits != 1 || st.L2Hits != 1 || st.Loads != 1 {
		t.Errorf("Stats = %+v", st)
	}
}

func TestMultiGetOrLoadConcurrentSingleFlight(t *testing.T) {
	_, cli := newMiniRedis(t)
	m := cache.NewMulti(cli, cache.MultiConfig{
		Redis: cache.RedisConfig{KeyPrefix: "sf:"},
	})
	defer m.Close()
	ctx := context.Background()

	var calls atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			v, err := cache.GetOrLoad(ctx, m, "hot", time.Minute, func(context.Context) (int, error) {
				calls.Add(1)
				time.Sleep(20 * time.Millisecond) // 放大并发窗口
				return 42, nil
			})
			if err != nil || v != 42 {
				t.Errorf("GetOrLoad = %v, %v", v, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Errorf("loader 应只执行 1 次, got %d", got)
	}
}

func TestMultiInvalidate(t *testing.T) {
	_, cli := newMiniRedis(t)
	m := cache.NewMulti(cli, cache.MultiConfig{
		Redis: cache.RedisConfig{KeyPrefix: "inv:"},
	})
	defer m.Close()
	ctx := context.Background()

	calls := 0
	load := func(context.Context) (int, error) {
		calls++
		return calls * 100, nil
	}
	v1, _ := cache.GetOrLoad(ctx, m, "k", time.Minute, load)
	if v1 != 100 || calls != 1 {
		t.Fatalf("首次加载错误: v=%d calls=%d", v1, calls)
	}

	if err := m.Invalidate(ctx, "k"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	v2, _ := cache.GetOrLoad(ctx, m, "k", time.Minute, load)
	if v2 != 200 || calls != 2 {
		t.Fatalf("失效后应重新加载: v=%d calls=%d", v2, calls)
	}
}

func TestMultiLoaderErrorNotCached(t *testing.T) {
	_, cli := newMiniRedis(t)
	m := cache.NewMulti(cli, cache.MultiConfig{
		Redis: cache.RedisConfig{KeyPrefix: "err:"},
	})
	defer m.Close()
	ctx := context.Background()

	n := 0
	if _, err := cache.GetOrLoad(ctx, m, "k", time.Minute, func(context.Context) (int, error) {
		n++
		return 0, errors.New("db down")
	}); err == nil {
		t.Fatal("loader 错误应上抛")
	}
	if _, err := cache.GetOrLoad(ctx, m, "k", time.Minute, func(context.Context) (int, error) {
		n++
		return 7, nil
	}); err != nil {
		t.Fatalf("重试加载: %v", err)
	}
	if n != 2 {
		t.Errorf("失败不应被缓存, calls=%d", n)
	}
}

func TestMultiFailOpenVsFailClosed(t *testing.T) {
	// L2 故障（直接关掉 miniredis，后续所有 Get 都得到连接错误）。
	mr, cli := newMiniRedis(t)
	mr.Close()

	m := cache.NewMulti(cli, cache.MultiConfig{
		Redis: cache.RedisConfig{KeyPrefix: "fo:"},
	})
	defer m.Close()

	v, err := cache.GetOrLoad(context.Background(), m, "k", time.Minute,
		func(context.Context) (string, error) { return "fallback", nil })
	if err != nil || v != "fallback" {
		t.Fatalf("fail-open 应走 loader 成功: v=%q err=%v", v, err)
	}
	if m.Stats().L2Errors == 0 {
		t.Error("L2 故障应计入 Stats.L2Errors")
	}

	// fail-close：同样的故障直接报错。
	m2 := cache.NewMulti(cli, cache.MultiConfig{
		Redis:      cache.RedisConfig{KeyPrefix: "fc:"},
		FailClosed: true,
	})
	defer m2.Close()
	if _, err := cache.GetOrLoad(context.Background(), m2, "k", time.Minute,
		func(context.Context) (string, error) {
			t.Fatal("fail-close 不应执行 loader")
			return "", nil
		}); err == nil {
		t.Fatal("fail-close 应上抛 L2 错误")
	}
}

func TestMultiDirtyL2DataTreatedAsMiss(t *testing.T) {
	_, cli := newMiniRedis(t)
	m := cache.NewMulti(cli, cache.MultiConfig{
		Local: cache.LocalConfig{},
		Redis: cache.RedisConfig{KeyPrefix: "dirty:"},
	})
	defer m.Close()
	ctx := context.Background()

	// 预埋一份无法反序列化成目标类型的脏数据。
	if err := cli.Set(ctx, "dirty:k", []byte("not-json"), 0).Err(); err != nil {
		t.Fatal(err)
	}

	calls := 0
	v, err := cache.GetOrLoad(ctx, m, "k", time.Minute, func(context.Context) (int, error) {
		calls++
		return 9, nil
	})
	if err != nil || v != 9 || calls != 1 {
		t.Fatalf("脏数据应当作未命中: v=%d calls=%d err=%v", v, calls, err)
	}
}

// TestGetOrLoadMixedTypesNoPanic 回归测试：同一 key 被不同类型使用时不得 panic。
//
// 旧实现里 singleflight 的 key 不带类型，两个用不同 T 的调用共享同一次加载，
// 「谁先到谁当 leader」决定了结果值的动态类型，follower 的 v.(T) 断言
// 失败就直接 panic —— 而且是并发时序决定的，测试环境极难复现、线上随机炸。
func TestGetOrLoadMixedTypesNoPanic(t *testing.T) {
	_, cli := newMiniRedis(t)
	m := cache.NewMulti(cli, cache.MultiConfig{
		Local: cache.LocalConfig{Size: 64, TTL: time.Minute},
	})
	defer m.Close()

	ctx := context.Background()
	key := "mixed:type"

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				got, err := cache.GetOrLoad(ctx, m, key, time.Minute,
					func(context.Context) (int, error) { return 42, nil })
				if err != nil {
					t.Errorf("int 加载失败: %v", err)
					return
				}
				if got != 42 {
					t.Errorf("int 结果 = %v, want 42", got)
				}
			} else {
				got, err := cache.GetOrLoad(ctx, m, key, time.Minute,
					func(context.Context) (string, error) { return "hello", nil })
				if err != nil {
					t.Errorf("string 加载失败: %v", err)
					return
				}
				if got != "hello" {
					t.Errorf("string 结果 = %q, want hello", got)
				}
			}
		}(i)
	}
	wg.Wait()
}

// TestGetOrLoadNilInterface 回归测试：T 为接口类型且 loader 返回 nil 时
// 不得 panic。
//
// 旧实现把 loader 的裸值作为 any 传给 singleflight；T 是接口时
// loaded 为 nil，any 就成了 nil interface，v.(T) 对 nil interface 断言
// 必然失败 → panic。而 nil 明明是这个 T 的合法值。
func TestGetOrLoadNilInterface(t *testing.T) {
	_, cli := newMiniRedis(t)
	m := cache.NewMulti(cli, cache.MultiConfig{
		Local: cache.LocalConfig{Size: 64, TTL: time.Minute},
	})
	defer m.Close()

	got, err := cache.GetOrLoad[any](context.Background(), m, "nil:value", time.Minute,
		func(context.Context) (any, error) { return nil, nil })
	if err != nil {
		t.Fatalf("loader 返回 nil 不应当作错误: %v", err)
	}
	if got != nil {
		t.Fatalf("结果 = %v, want nil", got)
	}
}
