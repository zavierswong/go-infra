package rabbitmq

import "errors"

// 本包的哨兵错误。调用方应使用 errors.Is 判定，而不是匹配错误文本。
var (
	// ErrInvalidConfig 配置非法（字段缺失、取值越界、拓扑引用不存在的交换机/队列等）。
	ErrInvalidConfig = errors.New("rabbitmq: 配置非法")

	// ErrClosed 客户端已关闭。
	ErrClosed = errors.New("rabbitmq: 客户端已关闭")

	// ErrNotConnected 在超时窗口内无法取得可用连接/通道。
	ErrNotConnected = errors.New("rabbitmq: 无法建立连接")

	// ErrPublish 消息未能交给 broker（写帧失败、连接中断等）。
	ErrPublish = errors.New("rabbitmq: 发布失败")

	// ErrNotConfirmed broker 返回 nack，或在超时前未确认。此时消息可能已丢失，
	// 也可能只是确认延迟；调用方应按业务幂等决定是否重发。
	ErrNotConfirmed = errors.New("rabbitmq: broker 未确认消息")

	// ErrUnroutable 消息带 mandatory 标记但没有任何队列匹配，已被 broker 退回
	// （basic.return）。消息不会进入任何队列。
	ErrUnroutable = errors.New("rabbitmq: 消息无法路由到任何队列")

	// ErrTopology 声明交换机/队列/绑定失败。
	ErrTopology = errors.New("rabbitmq: 拓扑声明失败")

	// ErrConsume 消费者注册失败。
	ErrConsume = errors.New("rabbitmq: 消费者注册失败")
)
