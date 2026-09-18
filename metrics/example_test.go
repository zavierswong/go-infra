package metrics_test

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/zavierswong/go-infra/metrics"
)

// Example 演示如何把事件接成指标。
//
// 这里用 fmt 代替真正的 prometheus 客户端 —— 本包不依赖任何监控库，
// 实际项目里把 Observe 那一行换成 HistogramVec.WithLabelValues(...).Observe 即可。
func Example() {
	var events []string

	obs := metrics.ObserverFunc(func(e metrics.Event) {
		// 注意只用低基数维度做标签：组件、实例、操作、原因。
		// e.Detail 里是 SQL 原文，绝不能进标签。
		line := fmt.Sprintf("%s/%s/%s/%s",
			e.Component, e.Instance, e.Op, e.Reason)
		if e.Failed() {
			line += " 失败"
		}
		events = append(events, line)
	})

	obs.ObserveOp(metrics.Event{
		Component: metrics.ComponentMySQL,
		Instance:  "order",
		Op:        metrics.OpQuery,
		Duration:  12 * time.Millisecond,
		Reason:    metrics.ReasonNone,
		Detail:    "SELECT * FROM orders WHERE user_id = ?",
	})

	obs.ObserveOp(metrics.Event{
		Component: metrics.ComponentRedis,
		Instance:  "session",
		Op:        metrics.Op("GET"),
		Duration:  900 * time.Microsecond,
		Err:       context.DeadlineExceeded,
		Reason:    metrics.ReasonTimeout,
	})

	fmt.Println(events)
}

// ExampleMultiObserver 演示同一次操作同时接指标与慢日志。
func ExampleMultiObserver() {
	toMetrics := metrics.ObserverFunc(func(e metrics.Event) {
		_ = e.Duration.Seconds()
	})
	toSlowLog := metrics.ObserverFunc(func(e metrics.Event) {
		if e.Duration > 500*time.Millisecond {
			fmt.Printf("慢操作: %s %s %s\n", e.Component, e.Op, e.Duration)
		}
	})

	// nil 会被忽略，所以可以安全地把"没配置"的选项直接塞进来。
	obs := metrics.MultiObserver(toMetrics, nil, toSlowLog)
	obs.ObserveOp(metrics.Event{Component: metrics.ComponentPostgres, Op: metrics.OpQuery})

	fmt.Println("已广播")
	// Output: 已广播
}

// ExampleClassifyErr 演示把错误归成低基数标签值。
func ExampleClassifyErr() {
	fmt.Println(metrics.ClassifyErr(nil))
	fmt.Println(metrics.ClassifyErr(context.DeadlineExceeded))
	fmt.Println(metrics.ClassifyErr(sql.ErrNoRows))

	// Output:
	// none
	// timeout
	// not_found
}

// ExampleFromDBStats 演示把 database/sql 的统计归一成通用快照。
//
// mysql 与 postgres 两个包都走这个函数，所以两者的指标形状完全一致。
func ExampleFromDBStats() {
	stats := metrics.FromDBStats(metrics.ComponentMySQL, "order", sql.DBStats{
		MaxOpenConnections: 32,
		OpenConnections:    12,
		InUse:              7,
		Idle:               5,
		WaitCount:          0,
	})

	fmt.Println(stats.Open, stats.Idle, stats.Saturated())
	// Output: 12 5 false
}

// ExamplePoolStats_Saturated 演示用池水位做"早于用户可见失败"的告警。
func ExamplePoolStats_Saturated() {
	full := metrics.PoolStats{MaxOpen: 32, InUse: 32}
	fmt.Println("已吃满:", full.Saturated())
	fmt.Println("空闲占比:", full.IdleRatio())

	// Output:
	// 已吃满: true
	// 空闲占比: 0
}
