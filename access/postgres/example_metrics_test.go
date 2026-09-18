package postgres_test

// 本文件是**可编译**的 metrics 用法示例。
//
// 与 example_test.go 一样不带 `// Output:`，`go test` 只做编译检查、不执行，
// 因此不需要 PostgreSQL 在跑 —— 文档代码不会随 API 演进而失效。

import (
	"context"
	"log"

	"github.com/zavierswong/go-infra/access/postgres"
	"github.com/zavierswong/go-infra/metrics"
)

// 连接池快照：与 mysql / redis 同一个形状，上层可以共用同一个导出器。
//
// PostgreSQL 侧要特别留意 Open 这个数：它对应服务端每个连接 fork 出来的
// 一个**后端进程**，常驻内存数 MB 起。所以这里的长连接代价比 MySQL 高，
// Open 持续贴近 MaxOpen 时优先考虑上 PgBouncer，而不是继续加连接。
func ExamplePostgres_PoolStats() {
	p, err := postgres.Open(postgres.Config{
		Dsn: "postgres://app:123456@127.0.0.1:5432/demo?sslmode=disable",

		// Name 与 ApplicationName 的分工：
		//   ApplicationName 随建连下发给服务端，出现在 pg_stat_activity，给 DBA 看；
		//   Name 只在本进程内用，作为指标的实例标签，给监控看。
		ApplicationName: "order-api",
		Name:            "order-db",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	s := p.PoolStats()
	log.Printf("component=%s instance=%s", s.Component, s.Instance)
	log.Printf("打开=%d 使用中=%d 空闲=%d 排队=%d", s.Open, s.InUse, s.Idle, s.Pending)

	// WaitCount > 0 就说明已经有请求在排队等连接，是调大 MaxOpenConns 的硬信号
	// （MySQL 侧同理，这个判断与驱动无关）。
	if s.WaitCount > 0 {
		log.Printf("等连接次数=%d 总时长=%s", s.WaitCount, s.WaitDuration)
	}
}

// 接入事件流：每条语句产生一个 Event，失败时带 SQLSTATE 归类后的 Reason。
//
// Reason 的价值在于把"业务冲突"和"系统故障"分开：
// 23505（唯一键冲突）归 conflict、40P01（死锁）归 conflict、
// 08006（连接中断）归 connect。如果这些混成一个 unknown 标签，
// 看板上"用户重复下单"和"数据库挂了"会长成一模一样的曲线。
func ExamplePostgres_observer() {
	obs := metrics.ObserverFunc(func(e metrics.Event) {
		if !e.IsError() {
			return
		}
		log.Printf("op=%s instance=%s 耗时=%s reason=%s err=%v sql=%s",
			e.Op, e.Instance, e.Duration, e.Reason, e.Err, e.Detail)
	})

	p, err := postgres.Open(postgres.Config{
		Dsn:      "postgres://app:123456@127.0.0.1:5432/demo?sslmode=disable",
		Name:     "order-db",
		Observer: obs,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	ctx := context.Background()

	// 每条语句产生一条 Event，Op 取值：
	// query / row / raw / create / update / delete。
	var count int64
	_ = p.DB().WithContext(ctx).
		Table("orders").Where("user_id = ?", 1001).Count(&count).Error

	// 写操作的事件锚在事务**两端**（BEGIN 与 COMMIT），
	// 因此 Duration 覆盖了两次额外往返，且提交阶段才爆的错误
	// （延迟约束、序列化冲突）也能被捕获到 —— 这是 PostgreSQL 场景
	// 最容易漏报的一类错误。

	// SQLSTATE 分类可以直接复用在业务错误映射上。
	err = p.DB().WithContext(ctx).Exec(
		"INSERT INTO orders (id, user_id) VALUES (?, ?)", 1, 1001).Error
	switch {
	case postgres.IsUniqueViolation(err):
		log.Println("订单已存在（23505）")
	case postgres.IsDeadlock(err):
		log.Println("死锁，整个事务重试（40P01）")
	case postgres.IsRetryable(err):
		log.Println("瞬时故障，可原样重试")
	case err != nil:
		log.Printf("其他错误: %v", err)
	}
}
