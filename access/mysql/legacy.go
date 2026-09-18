package mysql

import (
	"database/sql"
	"sync"

	"gorm.io/gorm"
)

// 本文件是旧版 API 的兼容层，仅为平滑迁移保留，均已标记 Deprecated。

var (
	legacyMu   sync.Mutex
	legacyInst *MySQL
)

// Get 返回进程内共享的 MySQL 客户端（懒初始化）。
//
// Deprecated: 单例有四处理由不该用 ——
//   - 首个 cfg 决定一切，后续调用传入的 cfg 被静默忽略，多库/读写分离无法共存；
//   - 并行测试无法隔离，会互相污染；
//   - 关闭后无法重新初始化（旧版把字段永久置 nil 且单例无法重置）；
//   - 无法按业务维度做资源隔离（例如报表库的连接池不应与交易库抢连接）。
//
// 请改用 Open，由调用方持有返回的 *MySQL。
//
// 与旧版的行为差异：初始化失败会**返回错误**，而不是在 sync.Once 里无限重连
// 把首个调用方永久阻塞。重试次数由 Config.DialAttempts 控制；
// 失败也不会被永久固化（下次调用会重新尝试）。
func Get(cfg Config) (*MySQL, error) {
	legacyMu.Lock()
	defer legacyMu.Unlock()

	if legacyInst != nil {
		return legacyInst, nil
	}
	inst, err := Open(cfg)
	if err != nil {
		return nil, err
	}
	legacyInst = inst
	return inst, nil
}

// Close 关闭全局客户端并重置单例，使其可以再次初始化。
//
// Deprecated: 改用 (*MySQL).Close。
//
// 不重置全局变量的话，Close 之后 Get 会返回一个**已关闭**的客户端，
// 而后续调用又因为"实例已存在"直接复用它 —— 整个进程再也无法恢复。
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

// GetDB 返回 GORM 实例。
//
// Deprecated: 改用 (*MySQL).DB。
func (m *MySQL) GetDB() *gorm.DB { return m.DB() }

// GetSql 返回底层 sql.DB。
//
// Deprecated: 改用 (*MySQL).SQL。
func (m *MySQL) GetSql() *sql.DB { return m.SQL() }
