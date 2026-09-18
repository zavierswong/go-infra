package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/zavierswong/go-infra/metrics"
)

// 本文件把客户端的发布 / 消费 / 提交路径接到 metrics.Observer 上。
//
// 与 rabbitmq 包的事件切分对照：
//
//	publish  一次投递（ProduceBatch 整批一个事件）
//	consume  一条记录被 handler 处理完成
//	commit   一次消费位移提交（对应 AMQP 的 ack，失败 = 重启后重复消费）
//	reconnect  broker 建连失败（成功的建连不上报）
//	ping     探活往返（HealthCheck）
//
// Kafka 没有 per-message 的 nack：处理失败的处理策略是"不标记提交位移"，
// 由消费组语义决定消息去向，见 consume.go 的 Handler 文档。

// classifyErr 把错误细化成 metrics 的低基数原因。
//
// Kafka 的协议错误码（kerr）比 AMQP 的更琐碎，这里只挑生产上
// 真正需要区分处理的几类；其余 retriable 错误统一归 connect ——
// 它们的共同特征是"等一会就好"，告警意义一致。
func classifyErr(err error) metrics.Reason {
	if err == nil {
		return metrics.ReasonNone
	}

	// 超时 / 取消排最前：DeliveryTimeout 超时会以 ErrProduce 包装出现，
	// 必须先把它识别成 timeout 而不是笼统的 produce 失败。
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return metrics.ClassifyErr(err)
	}

	switch {
	case errors.Is(err, ErrClosed):
		return metrics.ReasonClosed
	case errors.Is(err, ErrInvalidConfig):
		return metrics.ReasonInvalid
	}

	var ke *kerr.Error
	if errors.As(err, &ke) {
		switch ke.Code {
		case kerr.MessageTooLarge.Code,
			kerr.RecordListTooLarge.Code,
			kerr.CorruptMessage.Code,
			kerr.InvalidTopicException.Code,
			kerr.InvalidRequiredAcks.Code,
			kerr.UnsupportedForMessageFormat.Code,
			kerr.UnsupportedCompressionType.Code:
			return metrics.ReasonInvalid
		case kerr.UnknownTopicOrPartition.Code,
			kerr.OffsetOutOfRange.Code:
			return metrics.ReasonNotFound
		case kerr.TopicAlreadyExists.Code:
			return metrics.ReasonConflict
		}
		if kerr.IsRetriable(err) {
			// NotLeaderForPartition / NotCoordinator / CoordinatorNotAvailable /
			// NetworkException……共同点：客户端会自动重试，
			// 持续出现才说明集群侧有问题。
			return metrics.ReasonConnect
		}
	}

	// ErrNotConnected / ErrProduce / ErrCommit / ErrTopic 等本包哨兵
	// 与未识别的 kerr 错误，交回通用归类兜底。
	return metrics.ClassifyErr(err)
}

// observe 派发一个事件。时间起点由调用方给出。
//
// obs 为 nil 的防御性判断：hook 回调会在任意时刻（包括 Close 之后）从
// 客户端内部协程触发，此时不能假设 Client 已被 Open 完整初始化。
func (c *Client) observe(op metrics.Op, start time.Time, err error, detail string) {
	if c.obs == nil {
		return
	}
	c.obs.ObserveOp(metrics.Event{
		Component: metrics.ComponentKafka,
		Instance:  c.cfg.Name,
		Op:        op,
		Duration:  time.Since(start),
		Err:       err,
		Reason:    classifyErr(err),
		Detail:    detail,
	})
}

// observeEvent 派发一个已构造好的事件（消费侧使用）。
func (c *Client) observeEvent(e metrics.Event) {
	if c.obs == nil {
		return
	}
	e.Component = metrics.ComponentKafka
	e.Instance = c.cfg.Name
	e.Reason = classifyErr(e.Err)
	c.obs.ObserveOp(e)
}
