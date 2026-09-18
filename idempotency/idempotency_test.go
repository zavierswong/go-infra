package idempotency_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/zavierswong/go-infra/idempotency"
	"github.com/zavierswong/go-infra/lock"
)

func newProc(t *testing.T, cfg idempotency.Config) (*miniredis.Miniredis, *redis.Client, *idempotency.Processor) {
	t.Helper()
	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })
	p, err := idempotency.New(cli, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return mr, cli, p
}

func TestDoExecutesAndReplays(t *testing.T) {
	_, cli, p := newProc(t, idempotency.Config{})
	ctx := context.Background()

	calls := 0
	run := func() (idempotency.Result[string], error) {
		return idempotency.Do(ctx, p, "pay:1001", func(context.Context) (string, error) {
			calls++
			return "ok-" + fmt.Sprint(calls), nil
		})
	}

	r1, err := run()
	if err != nil {
		t.Fatalf("首次执行: %v", err)
	}
	if r1.Replayed || r1.Value != "ok-1" || calls != 1 {
		t.Fatalf("首次应真实执行: %+v calls=%d", r1, calls)
	}

	// 结果已在 Redis 且带 TTL。
	ttl, err := cli.TTL(ctx, "idem:pay:1001").Result()
	if err != nil || ttl <= 0 {
		t.Fatalf("结果应带 TTL: %v %v", ttl, err)
	}

	r2, err := run()
	if err != nil {
		t.Fatalf("重放: %v", err)
	}
	if !r2.Replayed || r2.Value != "ok-1" || calls != 1 {
		t.Fatalf("第二次应重放且不执行 fn: %+v calls=%d", r2, calls)
	}
}

func TestDoConcurrentDedup(t *testing.T) {
	_, _, p := newProc(t, idempotency.Config{})
	ctx := context.Background()

	var calls atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := idempotency.Do(ctx, p, "order:42", func(context.Context) (int, error) {
				calls.Add(1)
				time.Sleep(30 * time.Millisecond) // 放大并发窗口
				return 42, nil
			})
			if err != nil || res.Value != 42 {
				t.Errorf("Do = %+v, %v", res, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Errorf("fn 应只执行 1 次, got %d", got)
	}
}

func TestDoErrorNotCached(t *testing.T) {
	_, _, p := newProc(t, idempotency.Config{})
	ctx := context.Background()

	calls := 0
	// 第一次失败：错误上抛且不缓存。
	if _, err := idempotency.Do(ctx, p, "job:1", func(context.Context) (int, error) {
		calls++
		return 0, errors.New("boom")
	}); err == nil {
		t.Fatal("fn 错误应上抛")
	}
	// 第二次重新执行并成功。
	r, err := idempotency.Do(ctx, p, "job:1", func(context.Context) (int, error) {
		calls++
		return 7, nil
	})
	if err != nil || r.Replayed || r.Value != 7 {
		t.Fatalf("失败后应重新执行: %+v %v", r, err)
	}
	// 第三次重放成功结果。
	r, err = idempotency.Do(ctx, p, "job:1", func(context.Context) (int, error) {
		calls++
		return 0, nil
	})
	if err != nil || !r.Replayed || r.Value != 7 || calls != 2 {
		t.Fatalf("成功结果应重放: %+v calls=%d", r, calls)
	}
}

func TestDoPanicReleasesLock(t *testing.T) {
	_, _, p := newProc(t, idempotency.Config{})
	ctx := context.Background()

	calls := 0
	func() {
		defer func() { _ = recover() }() // 预期 panic
		_, _ = idempotency.Do(ctx, p, "panic:1", func(context.Context) (int, error) {
			calls++
			panic("worker exploded")
		})
	}()
	if calls != 1 {
		t.Fatalf("panic 前应已执行, calls=%d", calls)
	}

	// 锁已被 defer 释放：立刻重试可以执行。
	r, err := idempotency.Do(ctx, p, "panic:1", func(context.Context) (int, error) {
		calls++
		return 9, nil
	})
	if err != nil || r.Replayed || r.Value != 9 {
		t.Fatalf("panic 后锁应已释放: %+v %v", r, err)
	}
	if calls != 2 {
		t.Errorf("calls = %d, want 2", calls)
	}
}

func TestDoWaitsForExternalHolder(t *testing.T) {
	_, cli, p := newProc(t, idempotency.Config{})
	ctx := context.Background()

	// 外部先抢走执行锁，模拟另一实例正在执行。
	extLocks := lock.New(cli)
	l, err := extLocks.Acquire(ctx, "idem:pay:2002:lock", 10*time.Second)
	if err != nil {
		t.Fatalf("外部抢锁: %v", err)
	}

	calls := 0
	done := make(chan struct{})
	var res idempotency.Result[string]
	go func() {
		defer close(done)
		res, err = idempotency.Do(ctx, p, "pay:2002", func(context.Context) (string, error) {
			calls++
			return "ran", nil
		})
	}()

	// 外部持有期间：不执行、不返回。
	time.Sleep(150 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("外部持锁期间 Do 不应完成")
	default:
	}
	if calls != 0 {
		t.Fatalf("外部持锁期间不应执行 fn, calls=%d", calls)
	}

	// 释放后：接管并执行。
	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("外部解锁: %v", err)
	}
	<-done
	if err != nil || res.Replayed || res.Value != "ran" || calls != 1 {
		t.Fatalf("解锁后应接管执行: %+v %v calls=%d", res, err, calls)
	}
}

func TestDoTakesOverCrashedHolder(t *testing.T) {
	mr, cli, p := newProc(t, idempotency.Config{})
	ctx := context.Background()

	// 模拟持有者崩溃：抢锁后不释放（无看门狗），锁按 TTL 过期。
	// miniredis 的 TTL 走虚拟时钟，需在后台显式推进。
	extLocks := lock.New(cli)
	if _, err := extLocks.Acquire(ctx, "idem:job:3:lock", 150*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(80 * time.Millisecond)
		mr.FastForward(500 * time.Millisecond)
	}()

	calls := 0
	res, err := idempotency.Do(ctx, p, "job:3", func(context.Context) (int, error) {
		calls++
		return 1, nil
	})
	if err != nil {
		t.Fatalf("接管执行: %v", err)
	}
	if res.Replayed || calls != 1 {
		t.Fatalf("崩溃持有者应被接管: %+v calls=%d", res, calls)
	}
}

func TestDoWaitTimeoutReturnsInProgress(t *testing.T) {
	_, cli, p := newProc(t, idempotency.Config{})

	extLocks := lock.New(cli)
	l, err := extLocks.Acquire(context.Background(), "idem:pay:4:lock", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Unlock(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	_, err = idempotency.Do(ctx, p, "pay:4", func(context.Context) (int, error) {
		t.Fatal("不应执行")
		return 0, nil
	})
	if !errors.Is(err, idempotency.ErrInProgress) {
		t.Fatalf("等待超时应返回 ErrInProgress, got %v", err)
	}
}

func TestDoResultTTLWindow(t *testing.T) {
	mr, cli, p := newProc(t, idempotency.Config{ResultTTL: 100 * time.Millisecond})
	ctx := context.Background()

	calls := 0
	run := func() (idempotency.Result[int], error) {
		return idempotency.Do(ctx, p, "k", func(context.Context) (int, error) {
			calls++
			return 1, nil
		})
	}
	if _, err := run(); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(200 * time.Millisecond) // 结果过期（miniredis 需显式推进时钟）
	r, err := run()
	if err != nil || r.Replayed || calls != 2 {
		t.Fatalf("结果过期后应重新执行: %+v calls=%d %v", r, calls, err)
	}
	_ = cli
}

func TestNewValidation(t *testing.T) {
	if _, err := idempotency.New(nil, idempotency.Config{}); err == nil {
		t.Fatal("nil client 应报错")
	}
	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })
	p, err := idempotency.New(cli, idempotency.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idempotency.Do(context.Background(), p, "", func(context.Context) (int, error) {
		return 0, nil
	}); err == nil {
		t.Fatal("空 key 应报错")
	}
}

// TestDoLostOwnershipDoesNotStoreResult 回归测试：执行权在 fn 运行期间被
// 接管时，结果【不得】写入 Redis。
//
// 旧实现在 fn 返回后直接 SET 结果，只靠 fnCtx 取消来「尽力阻止」。fn 不响应
// ctx 时（README 自己承认这种情况）旧持有者仍会写入，覆盖新持有者稍后写入
// 的结果 —— 第二次执行的产出凭空消失，比单纯的双执行更难排查。
//
// 这里让 fn 在内部把锁换成别人的 token，模拟「锁已过期被接管」。
func TestDoLostOwnershipDoesNotStoreResult(t *testing.T) {
	_, cli, p := newProc(t, idempotency.Config{})
	ctx := context.Background()

	const key = "pay:lost"
	lockKey := "idem:" + key + ":lock"

	_, err := idempotency.Do(ctx, p, key, func(context.Context) (string, error) {
		// 模拟：执行期间锁过期，被另一个实例用新 token 接管。
		// （fn 故意不响应 fnCtx，正是本用例要覆盖的场景。）
		if err := cli.Set(ctx, lockKey, "someone-else", time.Minute).Err(); err != nil {
			return "", err
		}
		return "stale-result", nil
	})

	if err == nil {
		t.Fatal("执行权已被接管时，Do 应当返回错误")
	}
	if !errors.Is(err, lock.ErrLost) {
		t.Fatalf("错误应可被 errors.Is(err, lock.ErrLost) 识别, got %v", err)
	}

	// 关键断言：陈旧结果不得落库，否则会污染后续重放。
	if n, _ := cli.Exists(ctx, "idem:"+key).Result(); n != 0 {
		v, _ := cli.Get(ctx, "idem:"+key).Result()
		t.Fatalf("执行权已丢失却仍写入了结果: %s", v)
	}
}

// TestDoStoreResultUsesDetachedCtx 回归测试：调用方 ctx 已取消时，
// 结果仍应写入（副作用已发生，不写会导致下次重复执行）。
//
// 旧实现用 context.Background() 虽然也能写进去，但没有任何超时上限；
// 这里验证的是「请求取消不影响结果落库」这条语义仍然成立。
func TestDoStoreResultUsesDetachedCtx(t *testing.T) {
	_, cli, p := newProc(t, idempotency.Config{})
	reqCtx, cancel := context.WithCancel(context.Background())

	_, err := idempotency.Do(reqCtx, p, "pay:cancel", func(ctx context.Context) (string, error) {
		cancel() // 执行途中调用方断开
		return "done", nil
	})
	if err != nil {
		t.Fatalf("调用方取消不应导致结果写失败: %v", err)
	}

	if n, _ := cli.Exists(context.Background(), "idem:pay:cancel").Result(); n == 0 {
		t.Fatal("副作用已发生，结果必须落库；否则下次请求会重复执行")
	}
}

// TestDirtyResultKeyCleaned 回归测试：结构性脏数据（写坏的 envelope）
// 被清理，而不是一直占着 key 让每次请求重复解析垃圾。
func TestDirtyResultKeyCleaned(t *testing.T) {
	mr, cli, p := newProc(t, idempotency.Config{})
	ctx := context.Background()

	if err := cli.Set(ctx, "idem:dirty", "not-a-json", 0).Err(); err != nil {
		t.Fatalf("准备脏数据失败: %v", err)
	}

	// fn 失败不会写结果，正好单独观察脏键是否被清掉。
	if _, err := idempotency.Do(ctx, p, "dirty", func(context.Context) (string, error) {
		return "", errors.New("boom")
	}); err == nil {
		t.Fatal("fn 失败应返回错误")
	}

	if mr.Exists("idem:dirty") {
		t.Fatal("结构性脏数据应被删除")
	}
}

// TestResultWriteFailureCompensated 回归测试：结果写失败时返回
// ErrResultNotStored（而不是谎报成功），并可用 Store 补偿 ——
// 补偿后后续请求重放，不再重复执行 fn。
// failSetHook 让 SET / SETNX 失败。只拦截写结果这一步：
// 抢锁（SET NX）发生在 fn 之前，续期走 EVAL，二者不受影响。
type failSetHook struct{ fail atomic.Bool }

func (h *failSetHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *failSetHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.fail.Load() && strings.HasPrefix(cmd.FullName(), "set") {
			return errors.New("mock: SET failed")
		}
		return next(ctx, cmd)
	}
}

func (h *failSetHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestResultWriteFailureCompensated(t *testing.T) {
	_, cli, p := newProc(t, idempotency.Config{})
	hook := &failSetHook{}
	cli.AddHook(hook)
	ctx := context.Background()

	// fn 成功之后让写结果的 SET 失败：模拟"副作用已发生、结果写不进去"。
	_, err := idempotency.Do(ctx, p, "pay:2002", func(context.Context) (string, error) {
		hook.fail.Store(true)
		return "paid", nil
	})
	if !errors.Is(err, idempotency.ErrResultNotStored) {
		t.Fatalf("应返回 ErrResultNotStored, got %v", err)
	}

	hook.fail.Store(false) // Redis 恢复
	if err := idempotency.Store(ctx, p, "pay:2002", "paid"); err != nil {
		t.Fatalf("Store 补偿失败: %v", err)
	}

	calls := 0
	res, err := idempotency.Do(ctx, p, "pay:2002", func(context.Context) (string, error) {
		calls++
		return "again", nil
	})
	if err != nil {
		t.Fatalf("补偿后 Do: %v", err)
	}
	if !res.Replayed || res.Value != "paid" || calls != 0 {
		t.Fatalf("补偿后应重放且不再执行 fn: %+v calls=%d", res, calls)
	}
}
