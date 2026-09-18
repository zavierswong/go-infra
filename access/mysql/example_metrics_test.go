package mysql_test

// 本文件是**可编译**的 metrics 用法示例。
//
// 与 example_test.go 一样不带 `// Output:`，`go test` 只做编译检查、不执行，
// 因此不需要 MySQL 在跑 —— 文档代码不会随 API 演进而失效。

import (
	"context"
	"log"

	"github.com/zavierswong/go-infra/access/mysql"
	"github.com/zavierswong/go-infra/metrics"
)

// 连接池快照：与 Stats 同源，只是换成了与 redis / postgres 统一的形状。
//
// 统一形状的意义：上层只需要写**一个**导出器，就能同时给
// MySQL、PostgreSQL、Redis 打点，不必为每个驱动各写一遍字段搬运。
func ExampleMySQL_PoolStats() {
	m, err := mysql.Open(mysql.Config{
		Dsn: "root:123456@tcp(127.0.0.1:3306)/demo?parseTime=True",

		// 多实例部署务必填 Name：不填的话所有库的曲线会叠成一条，
		// 出问题时无法判断是哪个实例在恶化。
		Name: "order-db",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = m.Close() }()

	s := m.PoolStats()

	// 本包刻意**不**引入 prometheus —— 由调用方把这个快照喂给任意后端。
	// 理由见 metrics 包文档：驱动的职责是暴露事实，不是选择后端。
	log.Printf("component=%s instance=%s", s.Component, s.Instance)
	log.Printf("打开=%d 使用中=%d 空闲=%d 排队=%d", s.Open, s.InUse, s.Idle, s.Pending)
	log.Printf("等连接次数=%d 等连接总时长=%s", s.WaitCount, s.WaitDuration)

	// 饱和 = 使用中已经顶到上限，新请求正在排队等连接。
	if s.Saturated() {
		log.Println("连接池已饱和，考虑调大 MaxOpenConns")
	}
	log.Printf("空闲占比=%.2f", s.IdleRatio())
}

// 接入事件流：实现 metrics.Observer，拿到每条语句的耗时与失败原因。
func ExampleMySQL_observer() {
	// 只需要实现 ObserveOp 一个方法。两条硬性要求：
	//
	//  1. 必须并发安全 —— GORM 回调会被多个业务协程同时调用；
	//  2. 绝不能阻塞 —— 它跑在请求的关键路径上，要落盘或发网络
	//     请写进带缓冲的 channel，由独立协程消费。
	obs := metrics.ObserverFunc(func(e metrics.Event) {
		// 用 IsError() 而不是 Failed()：
		// IsError() 会把"零行结果"排除掉，否则"按条件查不到记录"
		// 这种正常业务常态会把错误率直接顶上去。
		if !e.IsError() {
			return
		}
		log.Printf("op=%s instance=%s 耗时=%s reason=%s err=%v sql=%s",
			e.Op, e.Instance, e.Duration, e.Reason, e.Err, e.Detail)
	})

	m, err := mysql.Open(mysql.Config{
		Dsn:      "root:123456@tcp(127.0.0.1:3306)/demo?parseTime=True",
		Name:     "order-db",
		Observer: obs,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = m.Close() }()

	// 之后的每条语句都会产生一条 Event，
	// Op 取值：query / row / raw / create / update / delete。
	var count int64
	_ = m.DB().WithContext(context.Background()).
		Table("orders").Where("user_id = ?", 1001).Count(&count).Error
}
