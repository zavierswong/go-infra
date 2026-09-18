package prometheus

import (
	"github.com/zavierswong/go-infra/metrics"
)

// ObserveOp 实现 metrics.Observer，把一条操作事件翻译成指标。
//
// 翻译规则：
//
//   - 耗时直方图**收全部事件**（成功与失败），所以
//     errors_total / duration_seconds_count 就是错误率；
//   - errors_total 只在 IsError() 为真时递增 —— 按契约，
//     not_found（SQL 零行、Redis 键不存在）是业务的正常结果，
//     混进错误率会把"缓存命中率下降"误报成"错误率飙升"；
//   - not_found 单独进 not_found_total，给需要观察未命中的场景留一条曲线。
//
// 满足 Observer 的两个约束：client_golang 的 metric 都是并发安全的；
// Observe/Set 只写内存，不做任何 IO，不会阻塞业务路径。
func (e *Exporter) ObserveOp(ev metrics.Event) {
	// 零值 Exporter 没有任何 metric，直接返回而不是 panic ——
	// Observer 会被并发调用，宁可静默也不炸业务。
	if e == nil || e.opDuration == nil {
		return
	}

	component, instance := string(ev.Component), orPlaceholder(ev.Instance)

	e.opDuration.WithLabelValues(component, instance, string(ev.Op)).
		Observe(ev.Duration.Seconds())

	if ev.Err == nil {
		return
	}
	if ev.Reason == metrics.ReasonNotFound {
		e.opNotFound.WithLabelValues(component, instance, string(ev.Op)).Inc()
		return
	}
	e.opErrors.WithLabelValues(component, instance, string(ev.Op), string(ev.Reason)).Inc()
}

// orPlaceholder 按 metrics 契约把空实例名替换成固定占位符。
func orPlaceholder(instance string) string {
	if instance == "" {
		return instancePlaceholder
	}
	return instance
}
