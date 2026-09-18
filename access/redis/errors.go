package redis

import "errors"

// 本包的哨兵错误。调用方应使用 errors.Is 判定，而不是匹配错误文本。
var (
	// ErrInvalidConfig 配置非法（Addr 缺失、取值为负、时长单位疑似写错、TLS 配置不完整等）。
	ErrInvalidConfig = errors.New("redis: 配置非法")

	// ErrConnect 无法连接 Redis（网络不通、认证失败、TLS 握手失败等）。
	ErrConnect = errors.New("redis: 无法连接 Redis")

	// ErrClosed 客户端已关闭。
	ErrClosed = errors.New("redis: 客户端已关闭")

	// ErrNotConnected 客户端处于未连接状态。
	ErrNotConnected = errors.New("redis: 尚未建立连接")
)
