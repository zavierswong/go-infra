package redis_test

// 本文件是**可编译**的用法示例。
//
// 它们没有 `// Output:` 注释，因此 `go test` 只做**编译检查**、不会真正执行，
// 也就不需要 Redis 在跑 —— 保证文档里的代码不会随 API 演进而失效。
//
// 需要真正跑通行为的示例请见 redis_test.go 里的集成用例（连真实 Redis）。

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	goredis "github.com/redis/go-redis/v9"
	infraredis "github.com/zavierswong/go-infra/access/redis"
)

// 连接、读写、关闭的最小完整流程。
func Example() {
	r, err := infraredis.Open(infraredis.Config{
		Addr:     "127.0.0.1:6379",
		Password: "123456",
		DB:       0,

		// 连接池：这四项决定吞吐与延迟
		PoolSize:       100, // 基准连接数（默认 GOMAXPROCS × 10）
		MinIdleConns:   20,  // 始终保留的热连接，避免流量波峰时临时建连
		MaxActiveConns: 200, // 硬上限（PoolSize 不是上限！）
		PoolTimeout:    4 * time.Second,

		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		MaxRetries:   3, // 注意：-1 表示**禁用**重试
	})
	if err != nil {
		log.Fatalf("连接 Redis 失败: %v", err)
	}
	defer func() { _ = r.Close() }() // 可重复调用

	ctx := context.Background()
	client := r.Client()

	// 业务代码直接拿原生 go-redis 客户端用
	if err := client.Set(ctx, "user:1001", `{"name":"zavier"}`, 10*time.Minute).Err(); err != nil {
		log.Fatalf("写入失败: %v", err)
	}

	val, err := client.Get(ctx, "user:1001").Result()
	if errors.Is(err, goredis.Nil) {
		// key 不存在是正常业务结果，不是错误
		log.Println("未命中")
		return
	}
	if err != nil {
		log.Fatalf("读取失败: %v", err)
	}

	fmt.Println(val)
}

// 连接池调参：三种典型场景。
//
// 最关键的一条：MinIdleConns 决定"流量波峰到来时是否需要现场建连"。
// 跨可用区、带 TLS 或有代理的链路，一次建连可能就要几毫秒，
// 表现成 P99 毛刺；把它设成 PoolSize 的 1/4 ~ 1/2 通常就够。
func ExampleConfig_poolTuning() {
	// 场景一：常规在线服务——预热点连接，抗住波峰
	online := infraredis.Config{
		Addr:            "redis.internal:6379",
		PoolSize:        100,
		MinIdleConns:    25,
		MaxActiveConns:  200, // 硬上限：保证 实例数 × 200 < Redis maxclients
		PoolTimeout:     4 * time.Second,
		ConnMaxIdleTime: 5 * time.Minute,
		ConnMaxLifetime: time.Hour,
	}

	// 场景二：极低延迟敏感——池开大、热连接留足
	lowLatency := infraredis.Config{
		Addr:         "redis.internal:6379",
		PoolSize:     200,
		MinIdleConns: 100,
		PoolTimeout:  100 * time.Millisecond, // 快速失败，交给上层降级
		MaxRetries:   3,
	}

	// 场景三：低频后台任务——小池、不预热，省连接
	job := infraredis.Config{
		Addr:         "redis.internal:6379",
		PoolSize:     4,
		MinIdleConns: 0,
		MaxRetries:   1,
	}

	_ = online
	_ = lowLatency
	_ = job
}

// 把连接池指标接到监控。
func ExampleRDS_Stats() {
	r, err := infraredis.Open(infraredis.Config{Addr: "127.0.0.1:6379"})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	stats := r.Stats()
	if stats == nil {
		return
	}

	// 这些指标决定要不要调参：
	//   Misses 持续增长 → MinIdleConns / PoolSize 太小，一直在新建连接
	//   Timeouts > 0    → 池满后在排队，需要调大 MaxActiveConns 或 PoolTimeout
	//   StaleConns 占比高 → ConnMaxIdleTime / ConnMaxLifetime 太短
	log.Printf("命中=%d 未命中=%d 超时=%d 连接总数=%d 空闲=%d 过期=%d",
		stats.Hits, stats.Misses, stats.Timeouts,
		stats.TotalConns, stats.IdleConns, stats.StaleConns)
}

// 健康检查接口。
func ExampleRDS_HealthCheck() {
	r, err := infraredis.Open(infraredis.Config{Addr: "127.0.0.1:6379"})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := r.HealthCheck(ctx); err != nil {
		log.Printf("Redis 不可用: %v", err)
		return
	}
	log.Println("Redis 正常")
}

// 慢命令与失败命令的日志：默认只记失败，按需开启慢命令告警。
func ExampleConfig_logging() {
	r, err := infraredis.Open(infraredis.Config{
		Addr: "127.0.0.1:6379",

		// 超过 10ms 的命令记为 Warn —— 用来发现大 key、全量 SCAN 这类问题
		LogSlowThreshold: 10 * time.Millisecond,

		// 记录每一条命令（Debug 级别），仅用于排查，
		// 高 QPS 下会显著增加日志量
		LogCommands: false,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	// 注意：key 不存在（goredis.Nil）不会被记为错误，
	// 否则缓存未命中的正常流量会把告警淹没。
}

// 用 TLS 连接云托管 Redis。
func ExampleConfig_tls() {
	r, err := infraredis.Open(infraredis.Config{
		Addr:     "redis.example.com:6380",
		Username: "app", // Redis 6+ ACL 用户名
		Password: "s3cret",
		TLS: infraredis.TLSConfig{
			Enable:   true,
			CAFile:   "/etc/ssl/certs/redis-ca.pem", // 留空则用系统信任链
			CertFile: "/etc/ssl/certs/client.pem",   // 需要双向认证时提供
			KeyFile:  "/etc/ssl/certs/client-key.pem",
			// ServerName 留空则从 Addr 的 host 推导
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = r.Close() }()
}

// 优雅关闭。
func ExampleRDS_Close() {
	r, err := infraredis.Open(infraredis.Config{Addr: "127.0.0.1:6379"})
	if err != nil {
		log.Fatal(err)
	}

	// 收到 SIGTERM：关闭连接池。可重复调用，第二次起直接返回 nil。
	if err := r.Close(); err != nil {
		log.Printf("关闭 Redis 出错: %v", err)
	}
	_ = r.Close() // 幂等

	if r.Closed() {
		log.Println("已关闭")
	}
}
