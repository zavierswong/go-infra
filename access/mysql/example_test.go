package mysql_test

// 本文件是**可编译**的用法示例。
//
// 它们没有 `// Output:` 注释，因此 `go test` 只做**编译检查**、不会真正执行，
// 也就不需要 MySQL 在跑 —— 保证文档里的代码不会随 API 演进而失效。
//
// 需要真正跑通行为的示例请见 mysql_test.go 里的集成用例（连真实 MySQL）。

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/zavierswong/go-infra/access/mysql"
)

// 连接、查询、关闭的最小完整流程。
func Example() {
	m, err := mysql.Open(mysql.Config{
		Dsn: "root:123456@tcp(127.0.0.1:3306)/demo" +
			"?charset=utf8mb4&parseTime=True&loc=Local",

		// 连接池：这四项决定了吞吐与延迟
		MaxOpenConns:    50,               // 并发上限，建议 CPU 核数 × 2~4
		MaxIdleConns:    50,               // 热连接数，与 MaxOpenConns 对齐可避免反复拨号
		ConnMaxLifetime: 30 * time.Minute, // 必须小于 MySQL 的 wait_timeout
		ConnMaxIdleTime: 5 * time.Minute,  // 低频时段回收冗余连接

		LogLevel:      "warn",
		SlowThreshold: 200 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("连接 MySQL 失败: %v", err)
	}
	defer func() { _ = m.Close() }() // 可重复调用

	ctx := context.Background()

	// 业务代码直接拿 *gorm.DB 用
	var count int64
	if err := m.DB().WithContext(ctx).
		Table("orders").
		Where("user_id = ?", 1001).
		Count(&count).Error; err != nil {
		log.Fatalf("查询失败: %v", err)
	}

	fmt.Println(count)
}

// 连接池调参：三种典型场景。
func ExampleConfig_poolTuning() {
	// 场景一：Web API，中等并发
	api := mysql.Config{
		Dsn:             "root:123456@tcp(127.0.0.1:3306)/app?parseTime=True",
		MaxOpenConns:    40, // ≈ CPU 核数 × 4
		MaxIdleConns:    40, // 与上限一致：QPS 高峰不需要临时拨号
		ConnMaxLifetime: 30 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
	}

	// 场景二：高吞吐批处理，需要更大的池、更短的寿命
	batch := mysql.Config{
		Dsn:             "root:123456@tcp(127.0.0.1:3306)/dw?parseTime=True",
		MaxOpenConns:    100,
		MaxIdleConns:    20, // 批处理空闲期长，不必留太多热连接
		ConnMaxLifetime: 10 * time.Minute,
		ConnMaxIdleTime: time.Minute,
	}

	// 场景三：低频后台任务，小池即可，避免占用数据库连接
	job := mysql.Config{
		Dsn:             "root:123456@tcp(127.0.0.1:3306)/app?parseTime=True",
		MaxOpenConns:    4,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
	}

	_ = api
	_ = batch
	_ = job
}

// 把连接池指标接到监控。
func ExampleMySQL_Stats() {
	m, err := mysql.Open(mysql.Config{
		Dsn: "root:123456@tcp(127.0.0.1:3306)/demo?parseTime=True",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = m.Close() }()

	stats := m.Stats()

	// 这些指标决定要不要调参：
	//   WaitCount > 0        → MaxOpenConns 太小，请求在排队等连接
	//   OpenConnections 长期贴顶 → 同样说明池子偏小
	//   MaxIdleClosed 占比高  → MaxIdleConns 太小
	//   MaxLifetimeClosed 占比高 → ConnMaxLifetime 太短
	log.Printf("打开=%d 使用中=%d 空闲=%d 等待次数=%d 等待时长=%s",
		stats.OpenConnections, stats.InUse, stats.Idle,
		stats.WaitCount, stats.WaitDuration)
	log.Printf("因空闲回收=%d 因寿命回收=%d 打开上限=%d",
		stats.MaxIdleClosed, stats.MaxLifetimeClosed, stats.MaxOpenConnections)
}

// 健康检查接口。
func ExampleMySQL_HealthCheck() {
	m, err := mysql.Open(mysql.Config{
		Dsn: "root:123456@tcp(127.0.0.1:3306)/demo?parseTime=True",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = m.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := m.HealthCheck(ctx); err != nil {
		// 探活失败：交给上层决定是返回 503 还是重试
		log.Printf("MySQL 不可用: %v", err)
		return
	}
	log.Println("MySQL 正常")
}

// 让连接监控在连续失败后主动重建连接池。
func ExampleMySQL_Reconnect() {
	m, err := mysql.Open(mysql.Config{
		Dsn: "root:123456@tcp(127.0.0.1:3306)/demo?parseTime=True",

		HealthCheckInterval: 30 * time.Second,
		// 默认 0 = 不重建。sql.DB 本身是连接池，服务端恢复后下一次查询
		// 会自动拨号，探活失败通常不需要重建。
		// 只有在"连接池确实卡死"时才打开它。
		RebuildAfterFailures: 3,

		// 建连重试：0 表示默认 3 次，1 表示失败即返回，负值表示无限重试
		DialAttempts:   3,
		DialBackoff:    time.Second,
		DialMaxBackoff: 30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = m.Close() }()

	// 也可以手动触发：先建新池、就绪后再关旧池，期间旧池仍可服务
	if err := m.Reconnect(); err != nil {
		log.Printf("重建连接池失败: %v", err)
	}
}

// 优雅关闭。
func ExampleMySQL_Close() {
	m, err := mysql.Open(mysql.Config{
		Dsn: "root:123456@tcp(127.0.0.1:3306)/demo?parseTime=True",
	})
	if err != nil {
		log.Fatal(err)
	}

	// 收到 SIGTERM：关闭连接池并停止监控协程。
	// 可重复调用，第二次起直接返回 nil；不会因持锁而阻塞其他操作。
	if err := m.Close(); err != nil {
		log.Printf("关闭 MySQL 出错: %v", err)
	}
	_ = m.Close() // 幂等
}
