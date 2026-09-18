package redis

import (
	"fmt"
	"sync"

	goredis "github.com/redis/go-redis/v9"
)

// 本文件是旧版 API 的兼容层，仅为平滑迁移保留，均已标记 Deprecated。

var (
	legacyMu   sync.Mutex
	legacyInst *RDS
)

// New 创建进程内共享的 Redis 连接（已有实例时直接返回 nil）。
//
// Deprecated: 单例的问题与 mysql 包相同 —— 首个 cfg 决定一切、后续调用的 cfg
// 被静默忽略、并行测试互相污染、无法按业务维度隔离连接池。
// 请改用 Open，由调用方持有返回的 *RDS。
//
// 保留的旧语义：初始化失败时下次调用会**重新尝试**（不会像 sync.Once 那样
// 把一次失败永久固化）。
func New(cfg Config) error {
	legacyMu.Lock()
	defer legacyMu.Unlock()

	if legacyInst != nil {
		return nil
	}
	inst, err := Open(cfg)
	if err != nil {
		return err
	}
	legacyInst = inst
	return nil
}

// Get 获取全局 Redis 客户端。
//
// Deprecated: 改用 Open + (*RDS).Client。
//
// ⚠️ 注意它**会在初始化失败时 panic**：该签名没有 error 返回值，
// 失败时唯一的出路就是 panic。新代码请用 Open 并正常处理 error。
func Get(cfg Config) *goredis.Client {
	if err := New(cfg); err != nil {
		panic(fmt.Sprintf(
			"redis: 初始化失败: %v（Deprecated API，新代码请改用 Open 并处理返回的 error）", err))
	}

	legacyMu.Lock()
	inst := legacyInst
	legacyMu.Unlock()

	return inst.Client()
}

// Close 关闭全局 Redis 连接。
//
// Deprecated: 改用 (*RDS).Close。
//
// 与旧版的行为差异：关闭后会把全局实例重置，因此可以重新 Open。
// 旧版不重置全局变量，Close 之后 Get 会返回一个**已关闭**的客户端，
// 而 New 又因为"实例已存在"直接返回 nil，整个进程再也无法恢复 Redis 能力。
func Close() error {
	legacyMu.Lock()
	inst := legacyInst
	legacyInst = nil
	legacyMu.Unlock()

	if inst == nil {
		return nil
	}
	return inst.Close()
}
