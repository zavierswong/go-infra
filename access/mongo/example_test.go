package mongo_test

// 本文件是**可编译**的用法示例。
//
// 它们没有 `// Output:` 注释，因此 `go test` 只做**编译检查**、不会真正执行，
// 也就不需要 MongoDB 在跑 —— 保证文档里的代码不会随 API 演进而失效。
//
// 需要真正跑通行为的示例请见 mongo_test.go 里的集成用例（连真实 MongoDB）。

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	// 驱动与本包同名（都叫 mongo），外部测试包里必须给其中一个起别名：
	// 本包在示例中是主角，保持 mongo，驱动退让为 driver。
	driver "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zavierswong/go-infra/access/mongo"
	"github.com/zavierswong/go-infra/logger"
	"github.com/zavierswong/go-infra/metrics"
)

// 连接、读写、关闭的最小完整流程。
func Example() {
	// 日志底座是可选的，但初始化后本包内部的生命周期日志
	// （连接成功、健康检查失败、重连）才会按统一格式输出。
	if err := logger.Init(logger.Config{
		Level:   "info",
		Format:  "json",
		Service: "order-api",
	}); err != nil {
		log.Fatalf("初始化日志失败: %v", err)
	}
	defer logger.Close()

	client, err := mongo.Open(mongo.Config{
		URI:     "mongo://app:secret@127.0.0.1:27017/shop?authSource=admin",
		Name:    "order",
		AppName: "order-api@prod",
	})
	if err != nil {
		// 初始化失败已经做过有限次重试，耗时上界是
		// DialAttempts × DialProbeTimeout，不会无限等待。
		log.Fatalf("连接 MongoDB 失败: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// DefaultDatabase 用的是 URI 路径里的库名。
	// 没写库名时它会返回 ErrInvalidConfig —— 而不是悄悄退化成 test / admin。
	db, err := client.DefaultDatabase()
	if err != nil {
		log.Fatalf("URI 未指定默认库: %v", err)
	}

	coll := db.Collection("orders")

	if _, err := coll.InsertOne(ctx, bson.M{
		"order_id": "A1001",
		"user_id":  1001,
		"status":   "paid",
		"amount":   9900,
	}); err != nil {
		log.Fatalf("写入失败: %v", err)
	}

	var out bson.M
	if err := coll.FindOne(ctx, bson.M{"order_id": "A1001"}).Decode(&out); err != nil {
		log.Fatalf("查询失败: %v", err)
	}
	fmt.Printf("订单状态: %v\n", out["status"])
}

// 完整配置：连接池、超时、读写关注、日志。
//
// 每一项都写在**该不该调**的语境里，因为这几个旋钮的默认值
// 大多不是"适合生产"的那个。
func ExampleConfig() {
	client, err := mongo.Open(mongo.Config{
		URI: "mongo://app:secret@h1:27017,h2:27017,h3:27017/shop?replicaSet=rs0",

		// ---- 标识 ----
		// Name 只进指标标签。多集群 / 读写分离时不填它，
		// metrics.Event.Instance 就是空串，两条曲线会叠在一起。
		Name: "order",
		// AppName 会随建连握手上报给服务端。它是排查问题时最有性价比的一项：
		// db.currentOp()、mongod 日志、$currentOp 里都能看到，
		// 于是"这条慢查询是哪个服务发的"一眼就有答案。
		AppName: "order-api@prod",

		// ---- 连接池（四项全是"每台服务器"口径）----
		// ⚠️ MaxPoolSize 是**每节点**上限：3 节点副本集 + 10 个实例
		// = 最多 3 × 10 × MaxPoolSize 条到服务端的连接。
		MaxPoolSize: 100,
		// 0 = 懒建连（驱动默认）。设 >0 能削掉首次请求的抖动，
		// 代价是每个实例常驻这些连接，实例多时并不划算。
		MinPoolSize: 0,
		// 同时在建的连接数上限（驱动默认 2，与 mongosh 一致）。
		// 它保护的是服务端：池空了以后几十个 goroutine 同时握手会把 mongod 打慢。
		MaxConnecting: 2,
		// ⚠️ 0 = **不因空闲而关闭**（驱动默认）。低频服务或实例很多时
		// 建议设成 5m~10m，让波谷时段把连接还回去。
		MaxConnIdleTime: 5 * time.Minute,

		// ---- 超时（四个字段职责完全不同，最常见的踩坑点都在这）----
		// 单次建连（TCP + TLS + 握手 + 认证）的超时。默认 30s。
		ConnectTimeout: 5 * time.Second,
		// 选不到可用节点时等多久才放弃。默认 30s ——
		// 意味着主节点宕机后请求会**挂起 30 秒**才失败，
		// 而在线上 30s 往往比上游超时还长。
		ServerSelectionTimeout: 3 * time.Second,

		// ---- 建连行为（本包自己的旋钮，不是驱动参数）----
		// 启动期每次尝试的探活上限。默认 5s。
		// 它与 ServerSelectionTimeout 分开，于是
		// 「启动最坏耗时 = DialAttempts × DialProbeTimeout」是可算可控的；
		// 若复用 30s 的选节点超时，3 次重试就要挂 90 秒才启动失败。
		DialProbeTimeout: 5 * time.Second,
		DialAttempts:     3,

		// ---- 读写关注 ----
		// 读写分离到从节点时要清楚代价：从节点可能有复制延迟，
		// 读到的数据会落后，"读己之写"会失效。
		ReadPreference: "primaryPreferred",
		// 需要"读到的数据不会被回滚"时应至少用 majority。
		ReadConcern: "majority",
		// "0" 表示**根本不等待服务端确认**（调用方拿不到写入结果，丢了也不知道）。
		// 除日志、埋点这类可丢数据外不要用它。
		WriteConcern: "majority",

		// ---- 日志 ----
		// silent / error / warn / info；空值按 info。
		// 生产建议 warn 或 error：warn/info 下命令日志的量与业务 QPS 成正比。
		LogLevel:      "warn",
		SlowThreshold: 200 * time.Millisecond,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
}

// URI 里的参数**永远覆盖** Config 里的同名字段。
//
// 这不是本包自己定的规则，而是驱动的语义（后调用的 Set* 覆盖先调用的，
// 包括 ApplyURI），本包把 ApplyURI 放在最后一步调用，于是天然得到这个行为。
//
// 好处很实际：URI 是运维手里临时改的那个旋钮（改连接串比重发配置快），
// 覆盖语义与直觉一致，不会出现"改了 URI 却不生效"。
func ExampleConfig_uriOverride() {
	client, err := mongo.Open(mongo.Config{
		// URI 说 maxPoolSize=7
		URI:         "mongo://app:secret@127.0.0.1:27017/shop?maxPoolSize=7",
		MaxPoolSize: 50, // Config 说 50 —— 会被 URI 覆盖掉
		Name:        "order",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// ⚠️ 两个语义差异要记牢：
	//
	//	Config.MaxPoolSize = 0  →  "用驱动默认值 100"
	//	URI 的 maxPoolSize=0    →  "不限制"（没有硬上限）
	//
	// 要不限制只能写在 URI 里。
	//
	// Config() 返回的是**生效值**，因此这里读到的是 7 而不是 50；
	// PoolStats().MaxOpen 也是 7 —— 指标必须反映驱动真正使用的那个数，
	// 否则"池吃满"的告警会基于错误的上限。
	fmt.Printf("生效的每节点池上限: %d\n", client.Config().MaxPoolSize)
}

// 连接池快照。
//
// ⚠️ 驱动**不提供任何池统计 API**（database/sql 有 sql.DBStats、
// go-redis 有 PoolStats，mongo-driver 只有 CMAP 事件流）。
// 本包订阅事件自己计数，于是能在 Prometheus 里看到 MongoDB 的池水位。
//
// 两个必须知道的语义：
//   - MaxOpen 是**每台服务器**的上限，其余字段是**所有服务器聚合**的
//     （多节点下 Open 可能大于 MaxOpen，这是正确的）；
//   - WaitCount / WaitDuration 的口径与 database/sql 不同：那边只统计
//     "被阻塞的请求"，这边包含命中空闲连接、耗时近乎为 0 的快路径。
func ExampleMongoDB_PoolStats() {
	client, err := mongo.Open(mongo.Config{
		URI:  "mongo://app:secret@127.0.0.1:27017/shop",
		Name: "order",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	stats := client.PoolStats()

	// 瞬时量 → Gauge
	fmt.Printf("池上限/节点=%d 已建=%d 使用中=%d 空闲=%d 排队=%d\n",
		stats.MaxOpen, stats.Open, stats.InUse, stats.Idle, stats.Pending)

	// 累计量 → Counter。WaitCount 是最该盯的那个：
	// 只要它在涨，就说明上限偏小，或者单次操作太慢占着连接不放。
	fmt.Printf("累计等待次数=%d 累计等待时长=%s\n", stats.WaitCount, stats.WaitDuration)

	// ⚠️ 这几个字段**恒为 0**，不要拿去建指标 —— 驱动没有这些概念，
	// 建了只会得到一堆永远为 0 的曲线：
	//   MaxIdleClosed（没有空闲连接数上限）
	//   MaxLifetimeClosed（没有连接最长寿命）
	//   Hits / Misses（不暴露是否命中空闲连接）
	fmt.Printf("恒为 0 的字段: %d %d %d %d\n",
		stats.MaxIdleClosed, stats.MaxLifetimeClosed, stats.Hits, stats.Misses)

	if stats.Saturated() {
		// 池吃满本身不是错误，但它意味着后续请求只能排队。
		// 拿它做告警比拿"有没有报错"更早 —— 通常在用户可见的失败之前就触发了。
		log.Println("连接池已吃满，后续请求将排队")
	}
}

// 瞬时状态快照。
//
// 与事件流互补：事件告诉你**发生了什么**，快照告诉你**此刻处于什么状态**。
func ExampleMongoDB_Status() {
	client, err := mongo.Open(mongo.Config{
		URI:  "mongo://app:secret@127.0.0.1:27017/shop",
		Name: "order",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	st := client.Status()

	fmt.Printf("实例=%s 已关闭=%v 健康=%v\n", st.Instance, st.Closed, st.Healthy)
	fmt.Printf("主机=%s 默认库=%s\n", st.Hosts, st.Database)

	// 驱动对每次操作默认使用**隐式会话**、操作结束即归还，
	// 所以这个数约等于「在途操作数」——判断"是不是有一批慢操作把连接占住了"
	// 最直接的信号，比看 Open 更有指向性。
	fmt.Printf("在途会话数=%d\n", st.SessionsInProgress)

	// 池被清空的累计次数（主节点变更、maxPoolSize 变更、网络错误都会触发）。
	// 它陡增通常对应一次主从切换或网络抖动，是解释"刚才为什么有一批超时"的关键证据。
	// 它是**事件性**的而不是水位指标，所以放在 Status 里而不是 PoolStats 里。
	fmt.Printf("池清空累计次数=%d\n", st.PoolCleared)
}

// 健康检查：接进 HTTP 的 /healthz。
func ExampleMongoDB_HealthCheck() {
	client, err := mongo.Open(mongo.Config{
		URI:  "mongo://app:secret@127.0.0.1:27017/shop",
		Name: "order",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// 每次探活给一个短超时，别让健康检查接口被数据库拖住。
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	switch err := client.HealthCheck(ctx); {
	case err == nil:
		fmt.Println("ok")
	case errors.Is(err, mongo.ErrClosed):
		// 客户端已关闭 —— 这是停机流程，不是故障。
		fmt.Println("closed")
	default:
		fmt.Printf("unhealthy: %v\n", err)
	}
}

// 读取生效配置与**脱敏后**的连接串。
func ExampleMongoDB_URI() {
	client, err := mongo.Open(mongo.Config{
		URI: "mongo://app:secret@h1:27017/shop?authSource=admin&tlsCertificateKeyFilePassword=pkpass",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// 脱敏覆盖**两处**口令：userinfo 里的密码，以及
	// tlsCertificateKeyFilePassword 这类查询参数。
	// 只抹 userinfo 的脱敏函数会把私钥口令原样写进日志。
	//
	// 输入：mongo://app:secret@h1:27017/shop?authSource=admin&tlsCertificateKeyFilePassword=pkpass
	// 输出：mongo://app:***@h1:27017/shop?authSource=admin&tlsCertificateKeyFilePassword=***
	fmt.Println(client.URI())
}

// 故障恢复：Reconnect 的**正确用法与注意事项**。
func ExampleMongoDB_Reconnect() {
	client, err := mongo.Open(mongo.Config{
		URI:  "mongo://app:secret@127.0.0.1:27017/shop",
		Name: "order",
		// 打开"连续失败 N 次后自动重建"。
		// 默认 0（不重建）—— 因为驱动本来就在后台维护拓扑、自动重连，
		// 服务端恢复后会自行可用，探活失败通常并不需要重建。
		RebuildAfterFailures: 3,
		HealthCheckInterval:  10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.Reconnect(); err != nil {
		log.Fatalf("重建客户端失败: %v", err)
	}

	// ⚠️ 这里是最容易踩的地方：**重连会让之前取到的句柄失效。**
	//
	// Database / Collection 句柄是挂在某个具体的 *mongo.Client 上的，
	// 而 Reconnect 换掉了那个客户端并断开了它 —— 旧句柄再用会报
	// `client is disconnected`。
	//
	// 这一点与 mysql / postgres 包不同：SQL 侧换的是连接池，
	// *gorm.DB 本身稳定，句柄一直有效。
	//
	// 结论：**不要缓存 Collection，每次用时现取**（取句柄没有网络开销）。
	coll := client.Database("shop").Collection("orders")
	_ = coll
}

// 接入 metrics：把命令事件翻译成 Prometheus 指标。
//
// 本包只负责把 MongoDB 的事件流**归一化**成 metrics.Event，
// 翻译成具体指标格式是独立适配包的职责 ——
// access/* 不 import prometheus，这条分层避免了
// "换一个可观测性后端就要改数据库封装"。
func ExampleConfig_observer() {
	client, err := mongo.Open(mongo.Config{
		URI:  "mongo://app:secret@127.0.0.1:27017/shop",
		Name: "order",

		// Observer 收到每个命令结束后的事件。
		//
		// Op 取**命令名原文**（find / insert / aggregate / getMore …），
		// 与 mongodb_exporter、db.currentOp() 的输出对齐。
		// 只有 Op 适合做指标标签：Detail 每条都不同，作为标签会基数爆炸。
		Observer: metrics.ObserverFunc(func(e metrics.Event) {
			fmt.Printf("op=%s instance=%s duration=%s detail=%q failed=%v reason=%s\n",
				e.Op, e.Instance, e.Duration, e.Detail, e.Failed(), e.Reason)
		}),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// 传 nil 表示不观测：**此时一个监听器都不注册**，热路径上零额外开销。
	//
	// 这不是无关紧要的优化 —— 只要注册了 Started 回调，驱动就必须把命令文档
	// 复制并序列化成 bson.Raw 才能交给回调，那是一次与命令大小成正比的分配。
	// 所以「不需要观测就不要开着」在本包是一条硬约定。
	//
	// 用 IsError() 而不是 Failed() 过滤错误率：
	// mongo.ErrNoDocuments（查不到文档）在业务上通常是**正常结果**，
	// Failed() 为 true 但 IsError() 为 false，不该污染错误率。
	_ = client
}

// 错误判定：**按服务端错误码与标签**，不要匹配错误文本。
//
// 文本受服务端版本与 locale 影响，没有稳定性保证；
// 而错误码与标签是 MongoDB 的公开契约。
func ExampleIsNoDocuments() {
	var err error // 假装来自一次 FindOne

	switch {
	case mongo.IsNoDocuments(err):
		// 两条最重要的判定之一。
		// 它对应 SQL 的 sql.ErrNoRows、Redis 的 goredis.Nil，
		// 在业务上通常是正常结果（"这个用户还没建过档案"）。
		// 把它计入错误率，任何一个"查了但没有"的正常路径都会表现为故障。
		fmt.Println("没查到，走默认逻辑")

	case mongo.IsDuplicateKey(err):
		// 唯一索引冲突。它是**业务冲突**而非瞬时故障，重试只会再撞一次。
		fmt.Println("已存在，走幂等分支")

	case mongo.IsRetryable(err):
		// 瞬时故障：主从切换、节点重启、网络抖动。
		// ⚠️ 客户端 ctx 超时**不在**这一类 —— 超时的写操作可能已经落库，
		// 重试有重复写入风险，必须由业务侧用幂等键解决。
		fmt.Println("可重试")

	default:
		fmt.Printf("服务端错误码=%d 名称=%s 文案=%s\n",
			mongo.ServerErrorCode(err),
			mongo.ServerErrorName(err),
			mongo.ServerErrorMessage(err))
	}
}

// 事务的错误处理：两种标签的区别至关重要，混用会造成重复写入。
func ExampleIsTransactionRetryable() {
	txErr := error(nil) // 假装来自一次事务

	switch {
	case mongo.IsTransactionRetryable(txErr):
		// TransientTransactionError：事务快照已失效，
		// **必须重跑整个事务**（单独重试某条语句没有意义）。
		fmt.Println("丢弃当前事务，重新 StartTransaction 并重跑全部语句")

	case mongo.IsCommitRetryable(txErr):
		// UnknownTransactionCommitResult：COMMIT 的响应丢了，
		// 事务**可能已经提交**。
		// 重跑整个事务会造成重复写入；正确做法是只重新执行 commit。
		fmt.Println("只重试 commit，不要重跑事务")

	default:
		fmt.Println("不可重试，直接失败")
	}
}

// 超时配置：三个字段的职责完全不同，最常见的踩坑点都在这。
//
//	go-redis 那一套里只有"一个超时"的直觉在 MongoDB 上会直接翻车。
func ExampleConfig_timeouts() {
	client, err := mongo.Open(mongo.Config{
		URI: "mongo://app:secret@127.0.0.1:27017/shop",

		// ① ConnectTimeout：**单次建连**（TCP + TLS + 握手 + 认证）的超时。
		//    它不是"单次操作"的超时。
		ConnectTimeout: 5 * time.Second,

		// ② ServerSelectionTimeout：**选不到可用节点**时等多久才放弃。
		//    它决定了"MongoDB 挂了"多久之后业务才能拿到错误。
		ServerSelectionTimeout: 3 * time.Second,

		// ③ OperationTimeout：客户端侧的**全操作**超时（CSOT，URI 的 timeoutMS）。
		//
		//    ⚠️ 这是本包最容易误用的一个配置。驱动的实现是：
		//
		//        if timeout == nil || IsTimeoutContext(parent) {
		//            return parent, cancel   // 什么都不做
		//        }
		//
		//    而 IsTimeoutContext 判定的是"ctx 上**有没有** deadline"（不看长短）。
		//    也就是说：**只要调用方的 ctx 带了任意 deadline，本字段就完全失效**，
		//    由那个 deadline 单独说了算。举两个会踩到的例子：
		//
		//        // 例子一：HTTP 请求的 ctx 常有很长的 deadline
		//        ctx, _ := context.WithTimeout(r.Context(), 5*time.Minute)
		//        coll.Find(ctx, ...)   // OperationTimeout=5s 不生效，这个操作可以跑 5 分钟
		//
		//        // 例子二：只有完全不设 deadline 时，它才起作用
		//        coll.Find(context.Background(), ...)   // 受 5s 限制 ✅
		//
		//    所以它是"兜底"而不是"上限"：想让 5s 一定生效，
		//    必须自己给每次操作套 ctx。
		OperationTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// 正确的做法：无论有没有配 OperationTimeout，都给操作套一个 ctx。
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var out bson.M
	_ = client.Database("shop").Collection("orders").
		FindOne(ctx, bson.M{"order_id": "A1001"}).Decode(&out)
}

// 逃生舱：需要驱动的完整能力时直接用 Client。
//
// 本包只负责**连接生命周期与可观测性**，刻意不把驱动 API 再包一遍 ——
// 会话、事务、Change Stream、GridFS、聚合游标、自定义 BSON 编解码
// 都直接用驱动原生写法。
func ExampleMongoDB_Client() {
	client, err := mongo.Open(mongo.Config{
		URI:  "mongo://app:secret@127.0.0.1:27017/shop?replicaSet=rs0",
		Name: "order",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	raw := client.Client()
	if raw == nil {
		log.Fatal("客户端已关闭")
	}

	ctx := context.Background()
	coll := raw.Database("shop").Collection("orders")

	// 聚合 + 游标遍历。
	cursor, err := coll.Aggregate(ctx, driver.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{{Key: "status", Value: "paid"}}}},
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$user_id"},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$amount"}}},
		}}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	for cursor.Next(ctx) {
		var row bson.M
		if err := cursor.Decode(&row); err != nil {
			log.Fatal(err)
		}
		fmt.Println(row)
	}
	// 游标可能会中途失效（服务端默认 10 分钟空闲即回收，主从切换也会）。
	// 它不是数据错误，而是"这个游标得重开"，所以别当成查询失败上报。
	if err := cursor.Err(); err != nil {
		if mongo.IsCursorNotFound(err) {
			log.Println("游标已失效，需要重新发起查询")
		} else {
			log.Fatal(err)
		}
	}

	// 事务（driver v2 里 Session 是结构体，不再是接口）。
	sess, err := raw.StartSession()
	if err != nil {
		log.Fatal(err)
	}
	defer sess.EndSession(ctx)

	if _, err := sess.WithTransaction(ctx, func(sc context.Context) (any, error) {
		if _, err := coll.InsertOne(sc, bson.M{"order_id": "A1002", "status": "pending"}); err != nil {
			return nil, err
		}
		if _, err := coll.UpdateOne(sc,
			bson.M{"order_id": "A1001"},
			bson.M{"$set": bson.M{"status": "shipped"}},
		); err != nil {
			return nil, err
		}
		return nil, nil
	}); err != nil {
		log.Fatalf("事务失败: %v", err)
	}

	// 注意事务需要副本集或分片集群；单节点 mongod 会报
	// IllegalOperation: Transaction numbers are only allowed on a replica set member or mongos。

	// 唯一索引（用于让重复写入得到可判定的 11000 错误）。
	_, err = coll.Indexes().CreateOne(ctx, driver.IndexModel{
		Keys:    bson.D{{Key: "order_id", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("uq_order_id"),
	})
	if err != nil {
		log.Fatal(err)
	}
}

// 优雅停机：先把实例从负载均衡摘掉，再关连接。
func ExampleMongoDB_Close() {
	client, err := mongo.Open(mongo.Config{
		URI:  "mongo://app:secret@127.0.0.1:27017/shop",
		Name: "order",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Close 幂等、可重复调用，第二次起直接返回 nil。
	// 所以停机流程里可以放心 defer Close()。
	defer client.Close()

	// Close 会等"在途操作归还连接"，上限是内部固定的 5s；
	// 超时后强制关闭它们，于是正在进行中的读写会失败。
	// 这个上界是有意为之：停机流程里"关不掉"比"有几个请求失败"严重得多。
	if err := client.Close(); err != nil {
		log.Printf("关闭时出错（连接已断开，仅表示有在途请求被中断）: %v", err)
	}
	if client.Closed() {
		log.Println("已关闭")
	}
}
