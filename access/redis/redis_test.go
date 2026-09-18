package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// 本文件是连接**真实 Redis** 的集成测试。
//
// 默认连接本机开发环境（127.0.0.1:6379，密码 123456），可用环境变量覆盖：
//
//	TEST_REDIS_ADDR      默认 127.0.0.1:6379
//	TEST_REDIS_PASSWORD  默认 123456
//	TEST_REDIS_DB        默认 0
//
// 跳过策略：`go test -short` 跳过全部集成用例；Redis **不可达**时跳过
// （跳过信息里写明地址与如何覆盖）；但一旦连得上而后续操作失败，
// 用例会直接失败 —— 那属于真实缺陷，不该被当成环境问题。

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return def
}

// testConfig 返回指向测试 Redis 的基础配置。
func testConfig(t *testing.T) Config {
	t.Helper()
	if testing.Short() {
		t.Skip("集成测试需要真实 Redis，-short 模式下跳过")
	}

	cfg := Config{
		Addr:         envOr("TEST_REDIS_ADDR", "127.0.0.1:6379"),
		Password:     envOr("TEST_REDIS_PASSWORD", "123456"),
		DB:           envInt("TEST_REDIS_DB", 0),
		ClientName:   "go-infra-test",
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolSize:     16,
		MinIdleConns: 2,
		MaxRetries:   1,
	}
	probeReachable(t, cfg)
	return cfg
}

// probeReachable 只验证 TCP 是否可达，不涉及认证。
// 只有真的不可达才 skip —— 认证失败、命令报错都必须让用例失败。
func probeReachable(t *testing.T, cfg Config) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", cfg.Addr, 2*time.Second)
	if err != nil {
		t.Skipf("Redis 不可达（%s）: %v\n"+
			"请启动 Redis，或用 TEST_REDIS_ADDR/PASSWORD/DB 覆盖", cfg.Addr, err)
	}
	_ = conn.Close()
}

func openClient(t *testing.T, cfg Config) *RDS {
	t.Helper()
	r, err := Open(cfg)
	if err != nil {
		t.Fatalf("Redis 可达但初始化失败（这是真实问题，不是环境问题）: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// keyPrefix 让每次运行使用独立的 key 前缀，避免并行运行或残留数据互相干扰。
var keyPrefix = fmt.Sprintf("gointra:test:%d:", time.Now().UnixNano())

func key(name string) string { return keyPrefix + name }

// cleanupKeys 删除本次用例写入的 key。
func cleanupKeys(t *testing.T, r *RDS, names ...string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		client := r.Client()
		if client == nil {
			return
		}
		if len(names) > 0 {
			ks := make([]string, len(names))
			for i, n := range names {
				ks[i] = key(n)
			}
			_ = client.Del(ctx, ks...).Err()
			return
		}
		// 没指定就按前缀扫（测试数据量很小）
		iter := client.Scan(ctx, 0, keyPrefix+"*", 100).Iterator()
		for iter.Next(ctx) {
			_ = client.Del(ctx, iter.Val()).Err()
		}
	})
}

// ---------------------------------------------------------------------------
// 连接与连接池
// ---------------------------------------------------------------------------

func TestOpenAndHealth(t *testing.T) {
	r := openClient(t, testConfig(t))

	if r.Closed() {
		t.Error("刚创建的客户端不应是已关闭状态")
	}
	if err := r.HealthCheck(context.Background()); err != nil {
		t.Errorf("HealthCheck 失败: %v", err)
	}
	if r.Client() == nil {
		t.Error("Client() 不应返回 nil")
	}
}

// TestPoolSettingsActuallyApplied 断言池参数真的落到了 go-redis 上。
func TestPoolSettingsActuallyApplied(t *testing.T) {
	cfg := testConfig(t)
	cfg.PoolSize = 12
	cfg.MinIdleConns = 3
	cfg.MaxActiveConns = 24

	r := openClient(t, cfg)

	opts := r.Client().Options()
	if opts.PoolSize != 12 {
		t.Errorf("PoolSize 期望 12，实际 %d", opts.PoolSize)
	}
	if opts.MinIdleConns != 3 {
		t.Errorf("MinIdleConns 期望 3，实际 %d", opts.MinIdleConns)
	}
	if opts.MaxActiveConns != 24 {
		t.Errorf("MaxActiveConns 期望 24，实际 %d", opts.MaxActiveConns)
	}
	// 默认必须尊重 ctx 超时（DisableContextTimeout 的零值语义）
	if !opts.ContextTimeoutEnabled {
		t.Error("ContextTimeoutEnabled 应默认为 true")
	}
	// 重试必须真的开起来了（旧版是 -1，等于禁用）
	if opts.MaxRetries <= 0 {
		t.Errorf("MaxRetries 应为正数，实际 %d（旧版 -1 会禁用重试）", opts.MaxRetries)
	}
}

// TestPoolStatsAvailable 池统计应可用，便于接入监控。
func TestPoolStatsAvailable(t *testing.T) {
	r := openClient(t, testConfig(t))
	ctx := context.Background()

	if err := r.Client().Set(ctx, key("stats"), "1", time.Minute).Err(); err != nil {
		t.Fatalf("Set 失败: %v", err)
	}
	cleanupKeys(t, r, "stats")

	stats := r.Stats()
	if stats == nil {
		t.Fatal("Stats() 不应返回 nil")
	}
	// 刚执行过命令，池里应该至少建立过一个连接并已归还
	if stats.TotalConns == 0 {
		t.Errorf("执行过命令后 TotalConns 不应为 0: %+v", stats)
	}
}

// TestMinIdleConnsKeepsConnectionsWarm 预热连接：执行几轮命令后，
// 空闲连接数应稳定在 MinIdleConns 附近，而不是掉回 0。
func TestMinIdleConnsKeepsConnectionsWarm(t *testing.T) {
	cfg := testConfig(t)
	cfg.PoolSize = 8
	cfg.MinIdleConns = 4

	r := openClient(t, cfg)
	ctx := context.Background()

	// 先打一波并发把连接池撑起来
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = r.Client().Set(ctx, key(fmt.Sprintf("warm%d", i)), "x", time.Minute).Err()
		}(i)
	}
	wg.Wait()
	cleanupKeys(t, r)

	// 给 go-redis 一点时间归还连接
	deadline := time.Now().Add(2 * time.Second)
	var idle uint32
	for time.Now().Before(deadline) {
		if s := r.Stats(); s != nil {
			idle = s.IdleConns
			if idle > 0 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if idle == 0 {
		t.Errorf("MinIdleConns=4 时空闲连接不应为 0，实际 %d", idle)
	}
}

// ---------------------------------------------------------------------------
// 功能
// ---------------------------------------------------------------------------

// TestNilIsNotAnError 缓存未命中是正常业务结果，必须能被 redis.Nil 区分出来。
func TestNilIsNotAnError(t *testing.T) {
	r := openClient(t, testConfig(t))
	ctx := context.Background()
	client := r.Client()

	missing := key("definitely-missing")

	_, err := client.Get(ctx, missing).Result()
	if !errors.Is(err, goredis.Nil) {
		t.Errorf("读取不存在的 key 应返回 goredis.Nil，实际: %v", err)
	}
	// 并且必须被本包的 isRealErr 判定为"不需要记错误"
	if isRealErr(err) {
		t.Error("goredis.Nil 不应被视为真实错误")
	}

	// 写入后再读应成功
	if err := client.Set(ctx, key("present"), "hello", time.Minute).Err(); err != nil {
		t.Fatalf("Set 失败: %v", err)
	}
	cleanupKeys(t, r, "present")

	got, err := client.Get(ctx, key("present")).Result()
	if err != nil {
		t.Fatalf("Get 失败: %v", err)
	}
	if got != "hello" {
		t.Errorf("期望 hello，实际 %q", got)
	}
}

// TestPipelineUsesHook 走一次 pipeline，覆盖 ProcessPipelineHook 的执行路径。
func TestPipelineUsesHook(t *testing.T) {
	r := openClient(t, testConfig(t))
	ctx := context.Background()
	cleanupKeys(t, r)

	_, err := r.Client().Pipelined(ctx, func(pipe goredis.Pipeliner) error {
		pipe.Set(ctx, key("p1"), "a", time.Minute)
		pipe.Set(ctx, key("p2"), "b", time.Minute)
		pipe.Get(ctx, key("p1"))
		pipe.Get(ctx, key("p2"))
		return nil
	})
	if err != nil {
		t.Fatalf("Pipelined 失败: %v", err)
	}

	if got := r.Client().Get(ctx, key("p2")).Val(); got != "b" {
		t.Errorf("pipeline 写入的值不对: %q", got)
	}
}

// TestTTLAndExpire 基本的键生命周期。
func TestTTLAndExpire(t *testing.T) {
	r := openClient(t, testConfig(t))
	ctx := context.Background()
	client := r.Client()
	cleanupKeys(t, r, "ttl")

	if err := client.Set(ctx, key("ttl"), "v", 2*time.Second).Err(); err != nil {
		t.Fatalf("Set 失败: %v", err)
	}
	ttl, err := client.TTL(ctx, key("ttl")).Result()
	if err != nil {
		t.Fatalf("TTL 失败: %v", err)
	}
	if ttl <= 0 || ttl > 2*time.Second {
		t.Errorf("TTL 应在 (0, 2s] 内，实际 %s", ttl)
	}
}

// ---------------------------------------------------------------------------
// 并发
// ---------------------------------------------------------------------------

// TestConcurrentCommands 在 -race 下并发使用客户端，覆盖 Client()/Stats() 与命令并发。
func TestConcurrentCommands(t *testing.T) {
	r := openClient(t, testConfig(t))
	ctx := context.Background()
	cleanupKeys(t, r)

	const workers, rounds = 16, 20
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				// 同时读取状态，制造与命令执行的并发
				_ = r.Stats()
				_ = r.Closed()

				k := key(fmt.Sprintf("c%d-%d", w, i))
				if err := r.Client().Set(ctx, k, w*rounds+i, time.Minute).Err(); err != nil {
					errCh <- fmt.Errorf("worker %d 第 %d 轮 Set: %w", w, i, err)
					return
				}
				got, err := r.Client().Get(ctx, k).Int()
				if err != nil {
					errCh <- fmt.Errorf("worker %d 第 %d 轮 Get: %w", w, i, err)
					return
				}
				if got != w*rounds+i {
					errCh <- fmt.Errorf("worker %d 第 %d 轮值错误: %d", w, i, got)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// ---------------------------------------------------------------------------
// 生命周期
// ---------------------------------------------------------------------------

// TestCloseIsIdempotentAndClean Close 幂等，且关闭后状态明确。
//
// 旧版 Close 不重置全局变量，关闭后 Get 会返回一个已关闭的 client，
// 而 New 又因为"已存在"直接返回 nil，进程再也无法恢复 Redis 能力。
func TestCloseIsIdempotentAndClean(t *testing.T) {
	r := openClient(t, testConfig(t))

	done := make(chan error, 1)
	go func() {
		_ = r.Close()
		_ = r.Close()
		done <- r.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("重复 Close 不应报错: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close 超过 5s 未返回")
	}

	if !r.Closed() {
		t.Error("Close 后 Closed() 应为 true")
	}
	if r.Client() != nil {
		t.Error("Close 后 Client() 应返回 nil")
	}
	if err := r.HealthCheck(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("Close 后 HealthCheck 应返回 ErrClosed，实际: %v", err)
	}
	if stats := r.Stats(); stats != nil {
		t.Errorf("Close 后 Stats() 应返回 nil，实际 %#v", stats)
	}
	if err := r.Close(); err != nil {
		t.Errorf("再次 Close 应返回 nil，实际: %v", err)
	}
}

// TestHealthCheckRespectsContext ctx 取消时 HealthCheck 应尽快返回。
func TestHealthCheckRespectsContext(t *testing.T) {
	r := openClient(t, testConfig(t))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := r.HealthCheck(ctx); err == nil {
		t.Error("ctx 已取消时 HealthCheck 应返回错误")
	}
}

// TestLegacyNewGetCloseWorks 兼容层：New 幂等、Get 拿到可用客户端、
// Close 之后可以重新 Open（旧版做不到）。
func TestLegacyNewGetCloseWorks(t *testing.T) {
	cfg := testConfig(t)

	if err := New(cfg); err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	// 再调一次 New 应直接返回 nil（已有实例）
	if err := New(cfg); err != nil {
		t.Fatalf("重复 New 应返回 nil，实际: %v", err)
	}

	client := Get(cfg)
	if client == nil {
		t.Fatal("Get 不应返回 nil")
	}

	ctx := context.Background()
	if err := client.Set(ctx, key("legacy"), "1", time.Minute).Err(); err != nil {
		t.Fatalf("兼容层 Set 失败: %v", err)
	}
	t.Cleanup(func() { _ = Close() })

	if err := Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}

	// 关键差异：关闭后可以重新初始化（旧版不重置全局变量，这里会返回已关闭的 client）
	if err := New(cfg); err != nil {
		t.Fatalf("Close 之后应能重新 New，实际: %v", err)
	}
	reopened := Get(cfg)
	if reopened == nil {
		t.Fatal("重新初始化后 Get 不应返回 nil")
	}
	if err := reopened.Ping(ctx).Err(); err != nil {
		t.Errorf("重新初始化后 Ping 应成功，实际: %v", err)
	}
}
