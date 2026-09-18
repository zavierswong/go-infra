package metrics

import (
	"database/sql"
	"time"
)

// PoolStats 是连接池的瞬时状态快照。
//
// 它把三种形状完全不同的池统计归一成一份结构：database/sql（mysql / postgres）、
// go-redis（redis）、以及 mongo-driver 的 CMAP 事件流（mongo）——
// 这样上层只需要一个导出器就能同时给所有组件打点。
//
// 字段语义以 database/sql.DBStats 为准。各组件不提供的能力留 0，
// **所以导出时不要给恒为 0 的字段建指标** —— 那样只会得到一堆永远为 0 的曲线。
// 各组件的可用性矩阵：
//
//	字段                  mysql/postgres  redis   mongo
//	--------------------  --------------  ------  -------
//	MaxOpen                     ✅         ✅       ✅
//	Open / InUse / Idle         ✅         ✅       ✅（InUse 是检出数）
//	Pending                     ❌0        ✅       ✅
//	WaitCount / WaitDuration    ✅         ✅       ⚠️ 口径不同，见下
//	MaxIdleClosed               ✅         ❌0      ❌0
//	MaxIdleTimeClosed           ✅         ❌0      ✅
//	MaxLifetimeClosed           ✅         ❌0      ❌0
//	Hits / Misses               ❌0        ✅       ❌0
//	Timeouts                    ❌0        ✅       ✅
//	Unusable / Stale            ❌0        ✅       ✅
//
// ⚠️ **WaitCount / WaitDuration 在 mongo 上口径不同**：driver 只给出
// "每次成功检出耗时"，不区分"是否排过队"，所以那边统计的是**全部检出**
// （含命中空闲连接、耗时近乎为 0 的快路径），而不是 database/sql 那种
// "只统计被阻塞的请求"。跨组件对齐阈值时要注意这一点；
// 判断 MongoDB 的池是否吃紧，看 Pending 比看 WaitDuration 更准。
type PoolStats struct {
	// Component 是组件名。
	Component Component
	// Instance 是实例名，来自各包的 Config.Name。
	Instance string

	// ---- 瞬时量：适合 Gauge ----

	// MaxOpen 是连接数上限。
	//
	// Redis 侧取自 MaxActiveConns，**0 表示没有硬上限**
	// （这正是 go-redis 的默认行为：它只用 PoolSize 做软目标，池不够时会继续超额新建）。
	//
	// MongoDB 侧取自 Config.MaxPoolSize，注意它是**每台服务器**的上限：
	// 副本集有 3 个节点时，实际总连接上限是 3 × MaxPoolSize。
	MaxOpen int
	// Open 是已建立的连接总数（含空闲与使用中）。
	Open int
	// InUse 是正在被使用的连接数。
	InUse int
	// Idle 是空闲连接数。长期为 0 说明 MinIdleConns / MaxIdleConns 偏小。
	Idle int
	// Pending 是正在排队等待连接的请求数。Redis 与 MongoDB 提供，
	// SQL 数据库侧恒为 0（database/sql 不暴露排队深度）。
	//
	// 这个值 > 0 且持续不降，就是"池不够用"最直接的证据 ——
	// 它比任何累计量都更早地反映问题。
	Pending int

	// ---- 累计量：适合 Counter ----

	// WaitCount 与 WaitDuration 是"拿不到连接而排队等待"的次数与总时长。
	//
	// 对整个池来说这是最该盯的指标：只要 WaitCount 在涨，
	// 就说明 MaxOpen / MaxActiveConns 偏小，或者单次操作太慢占着连接不放。
	// （MongoDB 的口径差异见 PoolStats 顶部的说明。）
	WaitCount    int64
	WaitDuration time.Duration

	// MaxIdleClosed 是因空闲连接数超过上限而被关闭的次数 → MaxIdleConns 偏小。
	MaxIdleClosed int64
	// MaxIdleTimeClosed 是因空闲超时被关闭的次数 → ConnMaxIdleTime / MaxConnIdleTime 偏短。
	MaxIdleTimeClosed int64
	// MaxLifetimeClosed 是因达到最长寿命被关闭的次数 → ConnMaxLifetime 偏短。
	// MongoDB 的驱动没有"连接最长寿命"这个概念，该项恒为 0。
	MaxLifetimeClosed int64

	// ---- Redis 专有，其余组件恒为 0 ----

	// Hits 与 Misses 是"在空闲池里拿到连接"与"没拿到"的次数。
	// Misses 持续增长说明 MinIdleConns 偏小，请求总是要走新建连接这条慢路径。
	Hits   int64
	Misses int64
	// Timeouts 是等待连接超时的次数，直接对应用户可见的失败。
	Timeouts int64
	// Unusable 是取到连接后才发现已失效、被丢弃的次数。
	// 它偏高通常意味着服务端有连接回收策略（如 MySQL 的 wait_timeout、
	// 中间的 NAT/LB 空闲断流）而客户端的 ConnMaxLifetime / ConnMaxIdleTime 设得太长。
	// MongoDB 侧取自 CMAP 的 ConnectionCheckOutFailed{reason=connectionError}。
	Unusable int64
	// Stale 是被清理掉的失效连接数。
	// MongoDB 侧取自 ConnectionClosed{reason=stale}（副本集主从切换、SDAM 判死等
	// 拓扑变化导致的连接失效）。
	Stale int64
}

// FromDBStats 把 database/sql 的统计归一成 PoolStats。
//
// mysql 与 postgres 两个包共用它，保证两个组件的指标形状完全一致 ——
// 否则同一个 dashboard 得为 MySQL 和 PostgreSQL 各写一套查询。
func FromDBStats(component Component, instance string, s sql.DBStats) PoolStats {
	return PoolStats{
		Component:         component,
		Instance:          instance,
		MaxOpen:           s.MaxOpenConnections,
		Open:              s.OpenConnections,
		InUse:             s.InUse,
		Idle:              s.Idle,
		WaitCount:         s.WaitCount,
		WaitDuration:      s.WaitDuration,
		MaxIdleClosed:     s.MaxIdleClosed,
		MaxIdleTimeClosed: s.MaxIdleTimeClosed,
		MaxLifetimeClosed: s.MaxLifetimeClosed,
	}
}

// Saturated 报告池是否已经吃满：正在使用的连接数达到上限。
//
// 池吃满本身不是错误，但它意味着后续请求只能排队 ——
// 拿它做告警比拿"有没有报错"更早，通常在用户可见的失败之前就触发了。
//
// ⚠️ 对 MongoDB 要留意：MaxOpen 是**单台服务器**的上限，而 InUse 是所有服务器
// 的检出总数。副本集下这个比较会**偏保守**（3 节点、每节点 100 时，
// InUse 到 100 就报满，实际还远没满）。多节点部署请改用 Pending 判断。
func (p PoolStats) Saturated() bool {
	return p.MaxOpen > 0 && p.InUse >= p.MaxOpen
}

// IdleRatio 返回空闲连接在已建立连接中的占比，取值范围 [0,1]。
//
// 没有已建立连接时返回 0（而不是 NaN），避免监控侧出现无意义的数。
// 这个值长期接近 0 说明连接几乎总是忙碌的；长期接近 1 说明池开得太大了。
func (p PoolStats) IdleRatio() float64 {
	if p.Open <= 0 {
		return 0
	}
	return float64(p.Idle) / float64(p.Open)
}
