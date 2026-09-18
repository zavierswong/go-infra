package mysql

import (
	"context"
	"time"

	"gorm.io/gorm"

	"github.com/zavierswong/go-infra/metrics"
)

// 本文件把 GORM 的 callback 链接到 metrics.Observer 上。
//
// 设计要点三条，都是踩过才明白的：
//
//  1. **开始时间存在 Statement.Settings 里，不塞 context。**
//     processor.Execute 全程复用同一个 Statement，所以 Before 写进去的值
//     在 After 里读得到；而往 Statement.Context 里塞值会污染使用者自己的
//     context 链，还会让 ctx 的相等性判断失效。
//
//  2. **没有开始时间就不上报。**
//     如果 Before 回调没能注册上，After 回调会读不到开始时间。
//     此时宁可一个事件都不发，也不要发一个 Duration 恒为 0 的假指标 ——
//     假指标会让 P99 曲线看起来完美，比没有指标更危险。
//
//  3. **注册失败不影响连接。**
//     metrics 是旁路。回调注册不上最多是少一类指标，绝不能让 Open 失败。
const metricsStartKey = "infra:metrics:op_start"

// GORM 回调链里的锚点名字，取自 gorm.io/gorm/callbacks 的默认注册。
//
// 写类操作锚在事务两端而不是 gorm:create / gorm:after_create：
// 一是写操作的耗时里 BEGIN 和 COMMIT 各占一次网络往返，不锚在两端会少算；
// 二是提交阶段的错误（延迟约束、磁盘满）只在 gorm:commit_or_rollback_transaction
// 里才被写进 db.Error，锚在 gorm:after_create 上会把它漏掉。
const (
	anchorTxBegin     = "gorm:begin_transaction"
	anchorTxCommit    = "gorm:commit_or_rollback_transaction"
	anchorQueryBegin  = "gorm:query"
	anchorQueryEnd    = "gorm:after_query"
	anchorCreateBegin = "gorm:before_create"
	anchorUpdateBegin = "gorm:setup_reflect_value"
	anchorDeleteBegin = "gorm:before_delete"
	anchorRow         = "gorm:row"
	anchorRaw         = "gorm:raw"
)

// PoolStats 返回归一化后的连接池快照。
//
// 与 Stats 的区别：Stats 返回 database/sql 的原生结构，适合直接读字段；
// PoolStats 返回 metrics.PoolStats，与 redis 包是同一个形状，
// 因此上层只需要一个导出器就能同时给 MySQL、PostgreSQL、Redis 打点。
func (m *MySQL) PoolStats() metrics.PoolStats {
	return metrics.FromDBStats(metrics.ComponentMySQL, m.cfg.Name, m.Stats())
}

// registerMetrics 在每个操作类别的回调链首尾各挂一个回调。
func (m *MySQL) registerMetrics(db *gorm.DB) {
	obs := metrics.OrNop(m.cfg.Observer)
	instance := m.cfg.Name
	cb := db.Callback()

	begin := func(tx *gorm.DB) {
		if tx.Statement == nil {
			return
		}
		tx.Statement.Settings.Store(metricsStartKey, time.Now())
	}
	end := func(op metrics.Op) func(*gorm.DB) {
		return m.endHook(op, obs, instance)
	}
	// track 只记日志。指标注册不上属于"少一类曲线"，不是连接故障，
	// 因此绝不向上返回错误、也绝不让 Open 失败。
	track := func(op metrics.Op, err error) {
		if err != nil {
			m.log.Errorf(context.Background(),
				"注册 %s 的 metrics 回调失败，该类操作将不上报指标（连接本身不受影响）: %v", op, err)
		}
	}

	// ---- 写类：锚在事务两端 ----
	//
	// gorm:begin_transaction 与 gorm:commit_or_rollback_transaction 都带
	// Match(enableTransaction) 条件，而本包内部构造的 gorm.Config 没有打开
	// SkipDefaultTransaction，所以它们一定存在。若将来有人改动这个前提，
	// 注册会失败 → 该类操作静默不上报（begin 没执行，end 读不到开始时间），
	// 不会产生错误数据。
	//
	// 这六条没法像读类那样抽成表：GORM 的 processor / callback 类型都未导出，
	// 链式调用无法用接口接住（方法返回的是未导出类型，Go 没有返回类型协变）。
	if err := cb.Create().Before(anchorTxBegin).
		Register(metricsCallbackName(metrics.OpCreate, "begin"), begin); err != nil {
		track(metrics.OpCreate, err)
	}
	if err := cb.Create().After(anchorTxCommit).
		Register(metricsCallbackName(metrics.OpCreate, "end"), end(metrics.OpCreate)); err != nil {
		track(metrics.OpCreate, err)
	}

	if err := cb.Update().Before(anchorUpdateBegin).
		Register(metricsCallbackName(metrics.OpUpdate, "begin"), begin); err != nil {
		track(metrics.OpUpdate, err)
	}
	if err := cb.Update().After(anchorTxCommit).
		Register(metricsCallbackName(metrics.OpUpdate, "end"), end(metrics.OpUpdate)); err != nil {
		track(metrics.OpUpdate, err)
	}

	if err := cb.Delete().Before(anchorTxBegin).
		Register(metricsCallbackName(metrics.OpDelete, "begin"), begin); err != nil {
		track(metrics.OpDelete, err)
	}
	if err := cb.Delete().After(anchorTxCommit).
		Register(metricsCallbackName(metrics.OpDelete, "end"), end(metrics.OpDelete)); err != nil {
		track(metrics.OpDelete, err)
	}

	// ---- 读类：没有事务回调可用，锚在各自唯一的回调上 ----
	//
	// Query 的链是 gorm:query → gorm:preload → gorm:after_query。
	// 锚在 after_query 上，Preload 触发的额外查询会由它们自己的 Execute
	// 单独上报一次，与 GORM 自身慢日志的粒度一致。
	if err := cb.Query().Before(anchorQueryBegin).
		Register(metricsCallbackName(metrics.OpQuery, "begin"), begin); err != nil {
		track(metrics.OpQuery, err)
	}
	if err := cb.Query().After(anchorQueryEnd).
		Register(metricsCallbackName(metrics.OpQuery, "end"), end(metrics.OpQuery)); err != nil {
		track(metrics.OpQuery, err)
	}

	if err := cb.Row().Before(anchorRow).
		Register(metricsCallbackName(metrics.OpRow, "begin"), begin); err != nil {
		track(metrics.OpRow, err)
	}
	if err := cb.Row().After(anchorRow).
		Register(metricsCallbackName(metrics.OpRow, "end"), end(metrics.OpRow)); err != nil {
		track(metrics.OpRow, err)
	}

	if err := cb.Raw().Before(anchorRaw).
		Register(metricsCallbackName(metrics.OpRaw, "begin"), begin); err != nil {
		track(metrics.OpRaw, err)
	}
	if err := cb.Raw().After(anchorRaw).
		Register(metricsCallbackName(metrics.OpRaw, "end"), end(metrics.OpRaw)); err != nil {
		track(metrics.OpRaw, err)
	}
}

// metricsCallbackName 生成注册名。加前缀是为了在 GORM 报错时一眼看出是本包注册的。
func metricsCallbackName(op metrics.Op, phase string) string {
	return "infra:metrics:" + string(op) + ":" + phase
}

// endHook 生成操作结束时的回调。
//
// 关于 Duration 的口径：它覆盖从第一个回调到最后一个回调的整段时间，
// 即「GORM 构建语句 + 数据库往返 + 结果扫描 + 事务提交」。
// 这比只掐 ExecContext 的那一段更接近调用方真实感受到的延迟，
// 但也意味着它**不**等于 GORM 慢日志里的数字（那边只量了单次调用）。
// 两处阈值不要互相套用。
func (m *MySQL) endHook(op metrics.Op, obs metrics.Observer, instance string) func(*gorm.DB) {
	return func(tx *gorm.DB) {
		if tx.Statement == nil {
			return
		}

		raw, ok := tx.Statement.Settings.Load(metricsStartKey)
		if !ok {
			// begin 回调没跑（例如注册失败）。不产生事件，避免上报 Duration=0 的假数据。
			return
		}
		tx.Statement.Settings.Delete(metricsStartKey)

		start, ok := raw.(time.Time)
		if !ok {
			return
		}

		err := tx.Error
		obs.ObserveOp(metrics.Event{
			Component: metrics.ComponentMySQL,
			Instance:  instance,
			Op:        op,
			Duration:  time.Since(start),
			Err:       err,
			Reason:    classifyErr(err),
			// GORM 在构造阶段已经建好了这条 SQL，String() 不产生拷贝。
			// 这条字符串在 Execute 收尾时被丢弃但不会被覆写，保留它是安全的。
			Detail: tx.Statement.SQL.String(),
		})
	}
}
