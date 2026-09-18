package metrics

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net"
	"os"
)

// Reason 是错误原因的低基数归类，用来做错误计数的标签值。
//
// 直接拿 err.Error() 做标签有两个问题：一是每个错误的文案都不同，
// 基数会随错误数量增长；二是文案里往往带具体的值（表名、主键、地址），
// 一旦进到指标里就可能泄露业务数据，还会撑爆时间序列。
type Reason string

const (
	// ReasonNone 表示操作成功、没有错误。
	ReasonNone Reason = "none"
	// ReasonTimeout 是超时：ctx deadline、网络读写超时、连接池等待超时。
	ReasonTimeout Reason = "timeout"
	// ReasonCanceled 是 ctx 被主动取消：调用方放弃、上游断开、优雅退出。
	ReasonCanceled Reason = "canceled"
	// ReasonClosed 是使用了一个已经关闭的客户端或连接。
	ReasonClosed Reason = "closed"
	// ReasonConnect 是建立连接失败或连接中途断开（ECONNREFUSED / EOF）。
	ReasonConnect Reason = "connect"
	// ReasonNotFound 是目标不存在：SQL 查出零行、Redis 键不存在。
	// 这类"错误"在业务上经常是正常路径（缓存未命中），告警时要排除掉。
	ReasonNotFound Reason = "not_found"
	// ReasonInvalid 是参数或语句本身非法：DSN 错误、SQL 语法错误、配置校验失败。
	ReasonInvalid Reason = "invalid"
	// ReasonConflict 是并发或约束冲突：唯一键冲突、外键约束、死锁、序列化失败。
	// 这类错误通常值得重试，是"错误率上升但服务仍健康"的典型来源。
	ReasonConflict Reason = "conflict"
	// ReasonRejected 是消息被拒收：broker 的 basic.nack / mandatory 退回，
	// 或消费者主动 nack 且不重入队。它衡量的是"消息没能被成功处理"。
	ReasonRejected Reason = "rejected"
	// ReasonUnknown 是兜底，表示没能归入以上任何一类。
	//
	// 这个值持续增长是好信号的反面：它说明出现了库没预料到的错误，
	// 值得去翻日志看看究竟是什么。
	ReasonUnknown Reason = "unknown"
)

// ClassifyErr 把错误归入低基数类别。
//
// 它只识别**通用**原因：context 的两种取消、网络超时、连接断开、零行。
// 各包自己的语义（postgres 的唯一键冲突、rabbitmq 的 nack、redis 的 Nil、
// mongo 的 ErrNoDocuments）由各包在抛出事件前先行细化 ——
// 那些包能同时 import 本包和各自的驱动，而本包不能反向依赖它们。
//
// 所以这里的 sql.ErrNoRows 分支只覆盖 SQL 数据库。
// mongo 的“查不到文档”（mongo.ErrNoDocuments）与 redis 的 goredis.Nil
// 都在各自包里映射成 ReasonNotFound，语义与这里的零行完全对齐。
//
// 这里**不设全局注册表**。看起来很方便，实际会让测试之间互相污染，
// 而且库的全局可变状态在大型项目里迟早会变成事故源。
//
// 各包需要额外词汇时直接定义新的 Reason 常量（Reason 是字符串类型），
// 只要保证取值全局唯一且低基数即可 —— access/postgres 的各类 SQLSTATE 归类就是这么做的。
func ClassifyErr(err error) Reason {
	if err == nil {
		return ReasonNone
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return ReasonTimeout
	case errors.Is(err, os.ErrDeadlineExceeded):
		return ReasonTimeout
	case errors.Is(err, context.Canceled):
		return ReasonCanceled
	case errors.Is(err, sql.ErrNoRows):
		return ReasonNotFound
	case errors.Is(err, net.ErrClosed):
		return ReasonClosed
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		// 连接在读写中途消失。SQL 驱动与 AMQP 客户端都会以这种方式报断连。
		return ReasonConnect
	}

	// net.Error 的 Timeout 覆盖了 i/o timeout 这类不包装 ctx 的底层超时。
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return ReasonTimeout
	}

	return ReasonUnknown
}
