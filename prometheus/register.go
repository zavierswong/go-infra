package prometheus

import (
	"context"
	"sync"
	"time"

	"github.com/zavierswong/go-infra/metrics"
)

type statusSource func() MQStatus

type healthCheck struct {
	component string
	instance  string
	check     func(ctx context.Context) error
}

// MQStatus 是与具体包解耦的组件状态快照，适合映射成 Gauge。
//
// 它的形状对齐 access/rabbitmq 的 Status：使用者把
// `client.Status()` 的结果搬运过来即可。刻意不直接 import
// access/rabbitmq，让本包保持"只认识 metrics 契约"的分层姿态。
type MQStatus struct {
	// Component 是组件名，通常填 metrics.ComponentRabbitMQ。
	Component metrics.Component
	// Instance 是实例名，来自 Config.Name。
	Instance string
	// Closed 表示客户端是否已被应用层 Close。
	Closed bool
	// Connected 表示底层传输连接是否存活。
	Connected bool
	// PublishChannelReady 表示发布通道是否可用。
	PublishChannelReady bool
}

// RegisterPool 注册一个池快照来源。
//
// db.PoolStats / cli.PoolStats 这类方法值直接传入即可：
//
//	pw.RegisterPool(db.PoolStats)
//
// 快照在每次 scrape 时现取，不做缓存；同一来源注册多次没有意义。
func (e *Exporter) RegisterPool(source func() metrics.PoolStats) {
	if source == nil {
		return
	}
	e.mu.Lock()
	e.pools = append(e.pools, source)
	e.mu.Unlock()
}

// RegisterStatus 注册一个组件状态快照来源（目前对应 rabbitmq 的 Status）。
func (e *Exporter) RegisterStatus(source func() MQStatus) {
	if source == nil {
		return
	}
	e.mu.Lock()
	e.statuses = append(e.statuses, source)
	e.mu.Unlock()
}

// KafkaStatus 是与 access/kafka 解耦的缓冲水位快照，
// 形状对齐 kafka.Client.Status() 与 kafka.Group.Status()：
// 使用者把 Status 的字段搬运过来即可，本包刻意不 import access/kafka。
//
// Kafka 没有"连接池"意义上的水位，这里给的是 franz-go 内部的
// 生产/消费缓冲积压 —— 缓冲持续增长（逼近 MaxBufferedRecords）
// 就是 broker 处理不过来或分区不可用的最早信号。
type KafkaStatus struct {
	// Component 是组件名，填 metrics.ComponentKafka。
	Component metrics.Component
	// Instance 是实例标签。生产端来自 Config.Name；
	// 注册消费组水位时建议带上组名（如 "order"+"-consumer"），
	// 避免与父 Client 的生产端序列混叠。
	Instance string
	// Closed 表示客户端是否已关闭（消费组侧是 Run 是否已退出）。
	Closed bool

	// ProduceBufferedRecords 是已入队但尚未被 broker 确认的记录数。
	ProduceBufferedRecords int64
	// ProduceBufferedBytes 是生产缓冲中的字节量。
	ProduceBufferedBytes int64
	// FetchBufferedRecords 是已拉取、handler 尚未处理完的记录数（消费组侧）。
	FetchBufferedRecords int64
	// FetchBufferedBytes 是待消费缓冲中的字节量。
	FetchBufferedBytes int64
}

type kafkaSource func() KafkaStatus

// RegisterKafkaStatus 注册一个 kafka 缓冲水位来源。
//
// 生产端与消费组各自注册一次（Group 用 NewGroup 后的 Status）：
//
//	pw.RegisterKafkaStatus(func() infraprom.KafkaStatus {
//		s := cli.Status()
//		return infraprom.KafkaStatus{ ... }
//	})
//
// 水位 Gauge 遵循"恒为 0 不建序列"约定：首次非零才建序列；
// Closed 落在共享的 status_closed 上，始终写入。
func (e *Exporter) RegisterKafkaStatus(source func() KafkaStatus) {
	if source == nil {
		return
	}
	e.mu.Lock()
	e.kafkas = append(e.kafkas, source)
	e.mu.Unlock()
}

// RegisterHealth 注册一个探活来源，结果反映在 component_up 上。
//
// 各包的 HealthCheck(ctx) error 是方法值，直接传入：
//
//	pw.RegisterHealth(metrics.ComponentMySQL, "order", db.HealthCheck)
//
// 第一次注册会自动启动后台探活协程（周期默认 30s，可用
// WithHealthInterval 调整）；没有注册任何探活来源时不会启动任何协程。
// 探活在后台执行、带独立超时，与 scrape 路径完全隔离。
func (e *Exporter) RegisterHealth(component metrics.Component, instance string, check func(ctx context.Context) error) {
	if check == nil {
		return
	}

	e.mu.Lock()
	e.checks = append(e.checks, healthCheck{
		component: string(component),
		instance:  orPlaceholder(instance),
		check:     check,
	})
	started := e.healthStop != nil
	if !started {
		e.healthStop = make(chan struct{})
	}
	stop := e.healthStop
	e.mu.Unlock()

	// 只在第一次注册时启动；多个 goroutine 并发注册时，
	// 只有看到 started == false 的那一个会走到这里。
	// loop 开头的立即探测让第一条曲线在启动后立刻有值，
	// 不用等一个周期。
	if !started {
		go e.runHealthLoop(stop)
	}
}

// runHealthLoop 周期性执行全部探活。启动时先立即探一次，
// 让 component_up 不必等第一个周期才出现。
func (e *Exporter) runHealthLoop(stop <-chan struct{}) {
	e.runHealthOnce()

	ticker := time.NewTicker(e.opts.healthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			e.runHealthOnce()
		}
	}
}

// runHealthOnce 并发执行当前注册的全部探活。
//
// 单个探活失败或超时只影响自己的 component_up，彼此隔离；
// 结果写入 Gauge，scrape 侧只读，不存在竞争。
func (e *Exporter) runHealthOnce() {
	e.mu.Lock()
	checks := make([]healthCheck, len(e.checks))
	copy(checks, e.checks)
	e.mu.Unlock()

	var wg sync.WaitGroup
	for _, c := range checks {
		wg.Add(1)
		go func(c healthCheck) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), e.opts.healthTimeout)
			defer cancel()

			value := 0.0
			if c.check(ctx) == nil {
				value = 1
			}
			e.componentUp.WithLabelValues(c.component, c.instance).Set(value)
		}(c)
	}
	wg.Wait()
}

// Close 停掉后台探活协程。
//
// 没有注册过探活时是空操作；重复调用安全。指标本身不会失效，
// 只是 component_up 不再更新。
func (e *Exporter) Close() {
	e.mu.Lock()
	stop := e.healthStop
	e.healthStop = nil
	e.mu.Unlock()

	if stop != nil {
		close(stop)
	}
}
