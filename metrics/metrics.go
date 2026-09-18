// Package metrics 定义与具体监控后端无关的「可观测事实」契约。
//
// # 为什么单独一个包
//
// access/mysql、access/postgres、access/mongo、access/redis、access/rabbitmq
// 需要一套**统一**的事件与快照形状，上层才能用同一个适配器给所有组件打点。
// 这套形状放在任何一个 access 包里，都会让其余几个反过来依赖它，
// 所以独立成包，位置与 access/、logger/ 平级。
//
// # 为什么不在这里封装 Prometheus
//
// 本包**刻意只依赖标准库**，不 import prometheus / opentelemetry / statsd。
// 驱动层的职责是把发生的事实结构化地抛出来；翻译成哪个后端的指标是使用方的选择。
// 业界惯例也是如此：redis/go-redis 本体不带 otel，extra/redisotel 是独立 module；
// gorm.io/gorm 不带 telemetry，gorm.io/plugin/opentelemetry 是独立 module。
//
// 好处很实际：不使用监控库的项目，引入 access/mysql 时不会被迫把
// prometheus/client_golang 及其依赖（common、client_model、protobuf）拖进依赖图。
//
// # 两条数据通道
//
//  1. **事件（推）**：每个操作结束时回调一次 Observer，用来做直方图与错误计数。
//     直方图**只能**靠事件 —— 拉模式两个采样点相除只能得到平均值，会把 P99 抹平。
//  2. **快照（拉)**：随时调用各包的 PoolStats / Status 拿瞬时状态，用来做 Gauge。
//     连接池水位这类指标本来就是瞬时量，不需要事件。
//
// # 最小用法
//
//	cfg := mysql.Config{
//		Dsn:  dsn,
//		Name: "order", // 实例名，会成为指标标签
//		Observer: metrics.ObserverFunc(func(e metrics.Event) {
//			queryDuration.
//				WithLabelValues(string(e.Component), e.Instance, string(e.Op), string(e.Reason)).
//				Observe(e.Duration.Seconds())
//		}),
//	}
//	db, err := mysql.Open(cfg)
//
// 完整的 Prometheus 适配器写法见各包 README 的「metrics 暴露」章节。
package metrics

// Component 标识产生事件的组件，用作指标里的低基数标签。
type Component string

const (
	// ComponentMySQL 对应 access/mysql。
	ComponentMySQL Component = "mysql"
	// ComponentPostgres 对应 access/postgres。
	ComponentPostgres Component = "postgres"
	// ComponentMongoDB 对应 access/mongo。
	ComponentMongoDB Component = "mongo"
	// ComponentRedis 对应 access/redis。
	ComponentRedis Component = "redis"
	// ComponentRabbitMQ 对应 access/rabbitmq。
	ComponentRabbitMQ Component = "rabbitmq"
	// ComponentKafka 对应 access/kafka。
	ComponentKafka Component = "kafka"
)

// Op 是操作类别的低基数名称。
//
// 只有 Op 适合做指标标签。Event.Detail（SQL 文本、Redis 键、路由键）每条都不同，
// 一旦作为标签就会造成基数爆炸，甚至把 Prometheus 打挂。
//
// 这里有两处**刻意**不用本文件常量、而是直接用协议里的原生名字：
//
//   - **Redis**：Op 填命令名（GET / SET / HGETALL …）。Redis 命令总数约 200 个，
//     作为标签是可接受的，而且比笼统的 "cmd" 有用得多。
//   - **MongoDB**：Op 填驱动上报的命令名原文（find / insert / aggregate / getMore …），
//     大小写**原样保留**（是 getMore 不是 GETMORE）。理由与 Redis 相同，
//     而且这样产出的指标能与 mongodb_exporter、`db.currentOp()` 的输出对齐。
//
// 两者的共同约束仍然是低基数：命令名是**受规范约束的有限集合**，
// 而集合名、过滤条件、文档内容都不是 —— 那些只能出现在 Event.Detail 里。
type Op string

const (
	// OpQuery 是 SELECT 类查询（GORM 的 Query callback）。
	OpQuery Op = "query"
	// OpRow 是单行查询（GORM 的 Row callback）。
	OpRow Op = "row"
	// OpCreate 是 INSERT（GORM 的 Create callback）。
	OpCreate Op = "create"
	// OpUpdate 是 UPDATE（GORM 的 Update callback）。
	OpUpdate Op = "update"
	// OpDelete 是 DELETE（GORM 的 Delete callback）。
	OpDelete Op = "delete"
	// OpRaw 是 db.Raw / db.Exec 这类绕过模型的语句。
	OpRaw Op = "raw"

	// 注意这里**没有事务类 Op**。GORM 默认给每条写语句都套一层隐式事务
	// （SkipDefaultTransaction 为 false），而 callback 层拿不到"这是显式事务"
	// 这个信息 —— 埋点结果会是"写次数 == 提交次数"的噪声曲线。
	// 需要事务级指标时，请在应用层包装 db.Transaction 自行埋点。

	// OpPublish 是投递一条消息（含 broker confirm 等待）。
	OpPublish Op = "publish"
	// OpConsume 是一次消息投递到 handler 并处理完成。
	OpConsume Op = "consume"
	// OpAck 是显式确认。
	OpAck Op = "ack"
	// OpNack 是显式否认（可带 requeue）。
	OpNack Op = "nack"
	// OpCommit 是提交消费位移（Kafka 消费组）。
	//
	// Kafka 的"确认"不是逐条 ack，而是把"已处理到哪"写成消费组的位移；
	// 提交失败意味着重启后消息会被重复消费（或按策略回退），
	// 因此它值得独立于 consume 成为一条速率曲线。
	OpCommit Op = "commit"
	// OpReturn 是 mandatory 模式下 broker 退回的不可路由消息。
	OpReturn Op = "return"

	// OpPipeline 是一次 pipeline 往返（多条命令合并发送）。
	//
	// pipeline 只有一次网络往返，go-redis 也只给出总耗时，因此整条 pipeline
	// 只上报一个事件。它不会出现在按命令名统计的曲线里 —— 这是有意的，
	// 因为把总耗时按条数平摊是编造数据，每条都记总耗时又会重复计数。
	OpPipeline Op = "pipeline"

	// OpReconnect 是重建 TCP 连接。
	OpReconnect Op = "reconnect"
	// OpChannelRebuild 是重建 AMQP 通道（连接仍在，但通道被 broker 关了）。
	OpChannelRebuild Op = "channel_rebuild"
	// OpDeclare 是声明交换机 / 队列 / 绑定。
	OpDeclare Op = "declare"
	// OpPing 是一次探活往返。
	//
	// 它有三个来源，语义一致、都**不是**业务流量：
	// 本库各包的 HealthCheck、后台 monitor 的周期探活、以及 mongo 客户端
	// 建连时的那次 Verify。做业务 QPS / 延迟曲线时应当把它过滤掉，
	// 否则低频服务上会看到一条"恒定小速率"的 ping 曲线；
	// 反过来，ping 的失败率才是真正的健康信号。
	OpPing Op = "ping"
)
