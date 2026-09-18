package redis_test

// 本文件是**可编译**的 metrics 用法示例。
//
// 与 example_test.go 一样不带 `// Output:`，`go test` 只做编译检查、不执行，
// 因此不需要 Redis 在跑 —— 文档代码不会随 API 演进而失效。

import (
	"context"
	"log"

	"github.com/zavierswong/go-infra/access/redis"
	"github.com/zavierswong/go-infra/metrics"
)

// 连接池快照：与 mysql / postgres 同一个形状。
//
// 注意 Redis 侧有两个**永久为 0** 的字段（MaxIdleClosed / MaxLifetimeClosed），
// 因为 go-redis 的池子不做这两类回收；不要为它们建指标，
// 否则看板上会永远是一条贴 0 的直线，容易误判成"采集坏了"。
func ExampleRDS_PoolStats() {
	r, err := redis.Open(redis.Config{
		Addr:     "127.0.0.1:6379",
		Password: "123456",
		DB:       0,

		// 与 ClientName 的分工：
		//   ClientName 随连接上报给服务端，出现在 CLIENT LIST 里，给运维看；
		//   Name 只在本进程内用，作为指标的实例标签，给监控看。
		ClientName: "order-api",
		Name:       "session-cache",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	s := r.PoolStats()
	log.Printf("component=%s instance=%s", s.Component, s.Instance)
	log.Printf("总连接=%d 空闲=%d 使用中=%d 排队=%d", s.Open, s.Idle, s.InUse, s.Pending)

	// 命中率！go-redis 的 Stats 里叫 Hits / Misses，
	// 这是 Redis 侧最该盯的曲线之一：命中率掉下去，
	// 后端数据库的 QPS 会立刻被放大。
	if total := s.Hits + s.Misses; total > 0 {
		log.Printf("命中率=%.4f", float64(s.Hits)/float64(total))
	}

	// 等连接超时次数：> 0 说明连接池或网络已经成为瓶颈。
	if s.Timeouts > 0 {
		log.Printf("等连接超时 %d 次，考虑调大 MaxActiveConns", s.Timeouts)
	}
}

// 接入事件流：每条命令产生一个 Event，Op 就是命令名的大写形式。
//
// 之所以用命令名而不是笼统的 "cmd"：~200 个命令的基数对 Prometheus 完全可接受，
// 而 get/set/hgetall 的延迟曲线必须分开看 —— 混在一起的平均值没有意义。
func ExampleRDS_observer() {
	obs := metrics.ObserverFunc(func(e metrics.Event) {
		if !e.IsError() {
			return
		}
		// e.Op 形如 "GET" / "SET" / "PIPELINE"。
		//
		// e.Detail 已经做过两件事：
		//   1. 脱敏 —— AUTH / HELLO / ACL / CONFIG / MIGRATE 只留命令名，
		//      参数全部省略（否则 Redis 口令会被写进日志）；
		//   2. 截断 —— SET 的**值**永远不出现，只留键名。
		// 因此 Detail 可以直接进日志，但落库前仍建议再评估一次。
		log.Printf("op=%s instance=%s 耗时=%s reason=%s err=%v detail=%s",
			e.Op, e.Instance, e.Duration, e.Reason, e.Err, e.Detail)
	})

	r, err := redis.Open(redis.Config{
		Addr:     "127.0.0.1:6379",
		Password: "123456",
		Name:     "session-cache",
		Observer: obs,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	ctx := context.Background()

	// 单条命令 → 一条 Event（Op = "SET" / "GET" ...）
	_ = r.Client().Set(ctx, "session:1001", "token", 0).Err()

	// 管道 → **一条** Event（Op = "PIPELINE"），而不是每条命令一条：
	// 拆开的话要么只能编造单条命令的耗时，要么把整段耗时重复计入每条命令。
	pipe := r.Client().Pipeline()
	pipe.Get(ctx, "session:1001")
	pipe.Get(ctx, "session:1002")
	_, _ = pipe.Exec(ctx)
}
