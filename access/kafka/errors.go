// Package kafka 提供拿来即用、默认即高可靠的 Kafka 客户端。
//
// 底层是 franz-go（github.com/twmb/franz-go/pkg/kgo）：纯 Go、无 CGO、
// 幂等生产 + 批量发送 + 消费组再均衡全部内建，是当前 Go 生态里
// 性能与维护活跃度最好的 Kafka 客户端之一。
//
// 与本仓库其他 access 包的约定一致：
//
//   - 非单例：Open 返回独立 Client，可多实例共存（靠 Config.Name 区分指标）；
//   - 可观测性中立：只暴露事件流（Config.Observer）与状态快照（Status），
//     不 import prometheus / otel，翻译成哪种指标由适配层决定；
//   - 默认值即生产可用：acks=all、幂等生产、处理成功才提交位移。
//
// # 高并发要点
//
//   - 生产端：批量发送（Linger + BatchMaxBytes）、幂等重试、
//     MaxBufferedRecords 背压，ProduceAsync 提供零阻塞路径；
//   - 消费端：消费组自动再均衡，GroupConfig.Concurrency 可把 handler
//     撑到任意并发度（代价是分区内顺序，见该字段文档）。
package kafka

import "errors"

// 本包的哨兵错误。调用方应使用 errors.Is 判定，而不是匹配错误文本。
var (
	// ErrInvalidConfig 配置非法（brokers 缺失、取值越界、认证配置不完整等）。
	ErrInvalidConfig = errors.New("kafka: 配置非法")

	// ErrClosed 客户端已关闭。
	ErrClosed = errors.New("kafka: 客户端已关闭")

	// ErrNotConnected 无法连接集群（bootstrap 失败、探活超时等）。
	ErrNotConnected = errors.New("kafka: 无法连接集群")

	// ErrProduce 消息投递失败。内部错误会用 %w 链接，
	// 因此 errors.As(err, *kerr.Error) 仍能取出 Kafka 协议错误码。
	ErrProduce = errors.New("kafka: 生产失败")

	// ErrConsume 消费者启动或运行失败（加入消费组失败、订阅的主题不存在等）。
	ErrConsume = errors.New("kafka: 消费失败")

	// ErrCommit 提交消费位移失败。提交失败意味着重启后会重复消费，
	// 不会丢消息（位移没前进），但也意味着处理逻辑必须幂等。
	ErrCommit = errors.New("kafka: 提交位移失败")

	// ErrTopic 主题管理操作失败（EnsureTopic / CreateTopic / DeleteTopic）。
	ErrTopic = errors.New("kafka: 主题操作失败")
)
