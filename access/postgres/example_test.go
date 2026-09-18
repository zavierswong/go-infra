package postgres_test

// 本文件是**可编译**的用法示例。
//
// 它们没有 `// Output:` 注释，因此 `go test` 只做**编译检查**、不会真正执行，
// 也就不需要 PostgreSQL 在跑 —— 保证文档里的代码不会随 API 演进而失效。
//
// 需要真正跑通行为的示例请见 postgres_test.go 里的集成用例（连真实 PostgreSQL）。

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"gorm.io/gorm"

	"github.com/zavierswong/go-infra/access/postgres"
)

// 连接、查询、关闭的最小完整流程。
func Example() {
	p, err := postgres.Open(postgres.Config{
		// 两种写法都可以，本包都支持，并且会保持你写的形式
		Dsn: "postgres://postgres:123456@127.0.0.1:5432/demo?sslmode=disable",

		// 连接池：这四项决定了吞吐与延迟
		MaxOpenConns:    50,               // 并发上限
		MaxIdleConns:    50,               // 热连接数，与上限对齐可避免反复 fork 后端进程
		ConnMaxLifetime: 30 * time.Minute, // 必须小于服务端/中间件的空闲超时
		ConnMaxIdleTime: 5 * time.Minute,  // 低频时段回收冗余连接

		// PostgreSQL 专有的会话参数（连上以后改不了已有连接，只能在建连时下发）
		ApplicationName:  "order-service",
		TimeZone:         "Asia/Shanghai",
		StatementTimeout: 30 * time.Second,

		LogLevel:      "warn",
		SlowThreshold: 200 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("连接 PostgreSQL 失败: %v", err)
	}
	defer func() { _ = p.Close() }() // 可重复调用

	ctx := context.Background()

	// 业务代码直接拿 *gorm.DB 用。注意占位符会被 GORM 改写成 $1、$2
	var count int64
	if err := p.DB().WithContext(ctx).
		Table("orders").
		Where("user_id = ?", 1001).
		Count(&count).Error; err != nil {
		log.Fatalf("查询失败: %v", err)
	}

	fmt.Println(count)
}

// keyword/value 形式的连接串（libpq 传统写法），含空格的值要加单引号。
func Example_keywordValueDSN() {
	p, err := postgres.Open(postgres.Config{
		Dsn: "host=127.0.0.1 port=5432 user=postgres password=123456 dbname=demo sslmode=disable",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	// DSN() 返回**抹掉密码**后的最终连接串，适合打日志或上报到运维面板
	log.Printf("实际连接: %s", p.DSN())
}

// 主从读写分离：多个 host + target_session_attrs，客户端直接路由，无需代理层。
func Example_targetSessionAttrs() {
	// 只连主库（写）—— 用 read-write
	writer, err := postgres.Open(postgres.Config{
		Dsn:                "host=pg-primary,pg-standby port=5432,5432 user=app password=secret dbname=demo sslmode=require",
		TargetSessionAttrs: "read-write",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = writer.Close() }()

	// 只连备库（读）—— 用 read-only 或 prefer-standby
	reader, err := postgres.Open(postgres.Config{
		Dsn:                "host=pg-primary,pg-standby port=5432,5432 user=app password=secret dbname=demo sslmode=require",
		TargetSessionAttrs: "read-only",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
}

// PostgreSQL 专有的会话参数：每一个都会在建连时下发给服务端。
func ExampleConfig_postgresSettings() {
	p, err := postgres.Open(postgres.Config{
		Dsn: "host=127.0.0.1 port=5432 user=postgres password=123456 dbname=demo sslmode=disable",

		// 显示在 pg_stat_activity.application_name，排查"谁在压库"必备
		ApplicationName: "report-worker",

		// 服务端单条语句超时：客户端 ctx 超时只是放弃等待，服务端那条语句还在跑；
		// statement_timeout 才会真正取消它，是保护数据库的最后一道闸门
		StatementTimeout: 30 * time.Second,

		// 掐掉"开着事务忘了提交"的连接：它持有锁并挡住 vacuum 回收死元组，
		// 是 PostgreSQL 最经典的线上事故来源
		IdleInTransactionTimeout: 5 * time.Minute,

		// 时区：决定 timestamptz 的显示，也决定 timestamp 的 ScanLocation。
		// 容器里通常是 UTC，不设置的话会出现"写进去读出来差 8 小时"
		TimeZone: "Asia/Shanghai",

		// 多租户按 schema 隔离
		SearchPath: "tenant_42,public",

		// 上面没列举到的服务端参数走逃生通道（键会被校验，不能注入连接串）
		Settings: map[string]string{
			"lock_timeout":                  "3s",
			"work_mem":                      "16MB",
			"default_transaction_isolation": "read committed",
		},

		// 服务端前面挂了 PgBouncer 且 pool_mode=transaction 时必须打开，
		// 否则预备语句缓存会跨连接失效（prepared statement ... does not exist）
		PreferSimpleProtocol: false,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = p.Close() }()
}

// 连接池调参：三种典型场景。
func ExampleConfig_poolTuning() {
	// 场景一：Web API，中等并发
	api := postgres.Config{
		Dsn:             "host=127.0.0.1 dbname=app user=app password=secret sslmode=disable",
		MaxOpenConns:    40, // ≈ CPU 核数 × 4
		MaxIdleConns:    40, // 与上限一致：波峰时无需临时 fork 后端进程
		ConnMaxLifetime: 30 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
	}

	// 场景二：高吞吐批处理 —— 池大、寿命短。注意 PostgreSQL 每个连接是一个
	// 独立后端进程（常驻内存 5~10MB），100 个连接就是约 1GB 常驻内存
	batch := postgres.Config{
		Dsn:             "host=127.0.0.1 dbname=dw user=app password=secret sslmode=disable",
		MaxOpenConns:    100,
		MaxIdleConns:    20, // 批处理空闲期长，不必留太多热连接
		ConnMaxLifetime: 10 * time.Minute,
		ConnMaxIdleTime: time.Minute,
	}

	// 场景三：低频后台任务 —— 小池即可，别占数据库连接
	job := postgres.Config{
		Dsn:             "host=127.0.0.1 dbname=app user=app password=secret sslmode=disable",
		MaxOpenConns:    4,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
	}

	_ = api
	_ = batch
	_ = job
}

// 把连接池指标接到监控。
func ExamplePostgres_Stats() {
	p, err := postgres.Open(postgres.Config{
		Dsn: "host=127.0.0.1 dbname=demo user=postgres password=123456 sslmode=disable",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	stats := p.Stats()

	// 这些指标决定要不要调参：
	//   WaitCount > 0            → MaxOpenConns 太小，请求在排队等连接
	//   OpenConnections 长期贴顶  → 同样说明池子偏小
	//   MaxIdleClosed 占比高     → MaxIdleConns 太小
	//   MaxLifetimeClosed 占比高 → ConnMaxLifetime 太短或小于中间件空闲超时
	log.Printf("打开=%d 使用中=%d 空闲=%d 等待次数=%d 等待时长=%s",
		stats.OpenConnections, stats.InUse, stats.Idle,
		stats.WaitCount, stats.WaitDuration)
	log.Printf("因空闲回收=%d 因寿命回收=%d 打开上限=%d",
		stats.MaxIdleClosed, stats.MaxLifetimeClosed, stats.MaxOpenConnections)
}

// 健康检查接口。
func ExamplePostgres_HealthCheck() {
	p, err := postgres.Open(postgres.Config{
		Dsn: "host=127.0.0.1 dbname=demo user=postgres password=123456 sslmode=disable",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := p.HealthCheck(ctx); err != nil {
		// 探活失败：交给上层决定是返回 503 还是重试
		log.Printf("PostgreSQL 不可用: %v", err)
		return
	}
	log.Println("PostgreSQL 正常")
}

// 让连接监控在连续失败后主动重建连接池。
func ExamplePostgres_Reconnect() {
	p, err := postgres.Open(postgres.Config{
		Dsn: "host=127.0.0.1 dbname=demo user=postgres password=123456 sslmode=disable",

		HealthCheckInterval: 30 * time.Second,
		// 默认 0 = 不重建。sql.DB 本身是连接池，服务端恢复后下一次查询
		// 会自动拨号，探活失败通常不需要重建。
		RebuildAfterFailures: 3,

		// 建连重试：0 表示默认 3 次，1 表示失败即返回，负值表示无限重试
		DialAttempts:   3,
		DialBackoff:    time.Second,
		DialMaxBackoff: 30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	// 也可以手动触发：先建新池、就绪后再关旧池，期间旧池仍可服务
	if err := p.Reconnect(); err != nil {
		log.Printf("重建连接池失败: %v", err)
	}
}

// 优雅关闭。
func ExamplePostgres_Close() {
	p, err := postgres.Open(postgres.Config{
		Dsn: "host=127.0.0.1 dbname=demo user=postgres password=123456 sslmode=disable",
	})
	if err != nil {
		log.Fatal(err)
	}

	// 收到 SIGTERM：关闭连接池并停止监控协程。
	// 可重复调用，第二次起直接返回 nil；不会因持锁而阻塞其他操作。
	if err := p.Close(); err != nil {
		log.Printf("关闭 PostgreSQL 出错: %v", err)
	}
	_ = p.Close() // 幂等
}

// 用 SQLSTATE 区分"该重试"与"该报错"。
//
// 判据是服务端返回的五字符错误码，**不要匹配错误文本** ——
// 文本受 lc_messages 与版本影响，没有稳定性保证。
func ExampleIsRetryable() {
	p, err := postgres.Open(postgres.Config{
		Dsn: "host=127.0.0.1 dbname=demo user=postgres password=123456 sslmode=disable",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	ctx := context.Background()
	const maxAttempts = 3

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// SERIALIZABLE 隔离级别下必须整个事务重试（拿新快照），
		// 单独重试某条语句没有意义
		txErr := p.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Exec("UPDATE accounts SET balance = balance - 100 WHERE id = ?", 1).Error
		})
		if txErr == nil {
			return
		}

		if !postgres.IsRetryable(txErr) {
			log.Fatalf("不可重试的错误（SQLSTATE=%s）: %v", postgres.SQLState(txErr), txErr)
		}
		// 40001 序列化失败 / 40P01 死锁 / 08006 连接断开 / 53300 连接数打满 …
		log.Printf("第 %d 次失败可重试（SQLSTATE=%s）", attempt, postgres.SQLState(txErr))
		time.Sleep(time.Duration(attempt) * 50 * time.Millisecond)
	}
	log.Printf("重试 %d 次仍未成功", maxAttempts)
}

// 把数据库约束错误翻译成业务错误。
func ExampleIsUniqueViolation() {
	p, err := postgres.Open(postgres.Config{
		Dsn: "host=127.0.0.1 dbname=demo user=postgres password=123456 sslmode=disable",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	var errEmailTaken = errors.New("邮箱已被占用")

	writeErr := p.DB().Exec("INSERT INTO users (email) VALUES (?)", "a@b.com").Error
	if writeErr == nil {
		return
	}

	// ConstraintName 让你能区分"是哪个唯一键炸了"，
	// 前提是建表时给约束起了可读的名字：
	//   CONSTRAINT uk_users_email UNIQUE (email)
	if postgres.IsUniqueViolation(writeErr) {
		if postgres.ConstraintName(writeErr) == "uk_users_email" {
			writeErr = errEmailTaken
		}
	}
	log.Printf("写入失败: %v", writeErr)
}
