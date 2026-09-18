package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/zavierswong/go-infra/metrics"
)

// 本文件把客户端的发布 / 消费 / 自愈路径接到 metrics.Observer 上。
//
// 与数据库、缓存两个包最大的不同：MQ 的一条消息会经过**多条**路径
// （投递 → 处理 → 确认），因此产生的事件不止一个。具体约定见 README：
//
//	publish  一次投递尝试（含 confirm 等待与通道重建后的重投）
//	consume  一条消息被 handler 处理完成
//	ack/nack 对一条消息的确认动作
//	return   被 broker 退回的不可路由消息
//	reconnect / channel_rebuild  链路自愈
//
// 也就是说一条正常消息会产生 2 个事件（consume + ack）。这样切分的好处是
// 每个 Op 都有唯一明确的含义，能各自独立地做速率与耗时曲线；
// 代价是事件条数约为消息数的两倍 —— 观察者务必按"非阻塞 + 有界缓冲"实现。

// Status 是客户端的瞬时状态快照，适合映射成 Gauge。
//
// 它回答的是"这个客户端现在能不能用"，与事件流互补：
// 事件告诉你发生了什么，快照告诉你此刻处于什么状态。
type Status struct {
	// Instance 是实例名，来自 Config.Name。
	Instance string
	// Closed 表示客户端是否已关闭。
	Closed bool
	// Connected 表示底层 TCP 连接是否仍然可用。
	Connected bool
	// PublishChannelReady 表示发布通道是否可用。
	// 连接健康但通道被 broker 关闭是完全可能的（例如队列被删导致的 404），
	// 所以这两个状态必须分开看。
	PublishChannelReady bool
	// Host 与 VHost 便于在监控里定位到具体实例。
	Host  string
	VHost string
}

// Status 返回当前状态快照。
func (c *Client) Status() Status {
	c.mu.RLock()
	conn, pub, closed := c.conn, c.pub, c.closed
	c.mu.RUnlock()

	// amqp 的 IsClosed 不是 nil 安全的（内部直接解引用 atomic 字段），
	// 必须先判空 —— liveConn / livePub 里已经有同样的注意点。
	connected := conn != nil && !conn.IsClosed()
	ready := pub != nil && !pub.IsClosed()

	return Status{
		Instance:            c.cfg.Name,
		Closed:              closed,
		Connected:           connected,
		PublishChannelReady: ready,
		Host:                c.cfg.Host,
		VHost:               c.cfg.VHost,
	}
}

// classifyErr 把错误细化成 metrics 的低基数原因。
//
// 这里能做得比数据库侧更细，因为 AMQP 的错误码是协议规范的一部分：
// 404（队列不存在）、406（同名实体属性不一致）、320（被运维强制关闭）
// 是生产上最常见的三类，混成一个 "unknown" 就完全没法定位了。
func classifyErr(err error) metrics.Reason {
	if err == nil {
		return metrics.ReasonNone
	}

	// 超时必须排在 ErrNotConfirmed 之前判定。
	//
	// 原因：ErrNotConfirmed 同时覆盖"broker 返回 nack"和"等确认超时"两种情况，
	// 而超时的错误链里带着 context.DeadlineExceeded（来自 WaitContext）。
	// 顺序反了会把"broker 太慢"误报成"消息被拒收"，
	// 于是告警指向"检查路由配置"，而真正该做的是调大 ConfirmTimeout。
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return metrics.ClassifyErr(err)
	}

	switch {
	case errors.Is(err, ErrClosed):
		return metrics.ReasonClosed
	case errors.Is(err, ErrNotConnected), errors.Is(err, ErrPublish):
		// ErrPublish 包装的是"写帧失败 / 连接中途断"这类链路问题。
		return metrics.ReasonConnect
	case errors.Is(err, ErrInvalidConfig), errors.Is(err, ErrTopology):
		return metrics.ReasonInvalid
	case errors.Is(err, ErrNotConfirmed), errors.Is(err, ErrUnroutable):
		// 消息没能被正常接走：nack 或不可路由被退回。
		return metrics.ReasonRejected
	}

	// AMQP 协议错误码。这些是 broker 明确告诉我们的原因，比错误文本可靠。
	var amqpErr *amqp.Error
	if errors.As(err, &amqpErr) {
		switch amqpErr.Code {
		case 320: // CONNECTION_FORCED：被管理员或运维工具强制关闭
			return metrics.ReasonConnect
		case 402, 403: // INVALID_PATH / ACCESS_REFUSED：vhost 或权限不对
			return metrics.ReasonInvalid
		case 404: // NOT_FOUND：队列/交换机不存在（消费时最常见）
			return metrics.ReasonNotFound
		case 405: // RESOURCE_LOCKED：队列被独占
			return metrics.ReasonConflict
		case 406: // PRECONDITION_FAILED：同名实体的属性与声明不一致
			return metrics.ReasonInvalid
		}
		if amqpErr.Code >= 500 {
			// 501/502/503/504/505/506：帧、语法、命令、通道、资源类错误，
			// 都意味着链路已经不可用，需要重建。
			return metrics.ReasonConnect
		}
	}

	return metrics.ClassifyErr(err)
}

// observe 派发一个事件。
//
// 时间起点由调用方给出：本包的耗时口径是**整个公开方法**，
// 例如 publish 的 Duration 覆盖 confirm 等待与失败后的重建重投，
// 这样它才等于调用方真实感受到的阻塞时间。
func (c *Client) observe(op metrics.Op, start time.Time, err error, detail string) {
	c.obs.ObserveOp(metrics.Event{
		Component: metrics.ComponentRabbitMQ,
		Instance:  c.cfg.Name,
		Op:        op,
		Duration:  time.Since(start),
		Err:       err,
		Reason:    classifyErr(err),
		Detail:    detail,
	})
}

// publishDetail 描述一次投递的目标。
//
// 刻意在"延迟改写之前"调用，于是拿到的是调用方本意的目标 ——
// 排查问题时想知道的是"我想投到 sms.send"，而不是"投到了延迟交换机
// amq.delay.30s"这个实现细节。
func publishDetail(o *publishOptions) string {
	target := o.routingKey
	if o.exchange != "" {
		target = o.exchange + " / " + o.routingKey
	}
	if o.delay > 0 {
		return fmt.Sprintf("%s (延迟 %s)", target, o.delay)
	}
	return target
}
