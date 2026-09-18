// Package prometheus 是可观测性的 Prometheus 适配包：把 metrics 契约抛出的
// 「可观测事实」（Event 事件流、PoolStats 快照、HealthCheck 探活、MQ 状态）
// 翻译成 prometheus/client_golang 的指标。
//
// # 分层职责
//
// 本仓库的铁律是 access/* 不 import prometheus —— 驱动层只负责结构化地
// 抛出事实，翻译成哪个后端的指标是适配层的选择。本包就是那个适配层，
// 也是整个仓库里 prometheus/client_golang 唯一的出现位置。
//
// 对应 metrics 契约的两条数据通道：
//
//  1. **事件（推）**：Exporter 自身实现 metrics.Observer，把它塞进各包的
//     Config.Observer 即可。事件被翻译成耗时直方图与错误计数器 ——
//     直方图只能靠事件，拉模式两个采样点相除得不到 P99。
//  2. **快照（拉）**：RegisterPool / RegisterStatus / RegisterHealth 注册
//     各包的 PoolStats / Status / HealthCheck。池水位在 scrape 时现取现报，
//     永远新鲜；HealthCheck 是网络探针，由后台协程按周期执行，
//     绝不在 scrape 路径上跑。
//
// # 指标清单
//
//	go_infra_operation_duration_seconds{component,instance,op}
//	go_infra_operation_errors_total{component,instance,op,reason}
//	go_infra_operation_not_found_total{component,instance,op}
//	go_infra_pool_max_open_connections{component,instance}
//	go_infra_pool_open_connections{component,instance}
//	go_infra_pool_in_use_connections{component,instance}
//	go_infra_pool_idle_connections{component,instance}
//	go_infra_pool_pending_requests{component,instance}
//	go_infra_pool_wait_total{component,instance}
//	go_infra_pool_wait_duration_seconds_total{component,instance}
//	go_infra_pool_closed_total{component,instance,reason}
//	go_infra_pool_hits_total / misses_total / timeouts_total /
//	    unusable_total / stale_total{component,instance}   （Redis/MongoDB 专有）
//	go_infra_component_up{component,instance}
//	go_infra_status_closed / connected / publish_ready{component,instance}
//
// 按 metrics.PoolStats 的约定，恒为 0 的字段**不会产生时间序列**：
// 池计数器只在数值增长时递增，池水位 Gauge 首次非零才建序列 ——
// MySQL 上不会出现一条永远是 0 的 pending_requests 曲线。
//
// # 基本用法
//
//	pw := prometheus.New()
//	prometheus.MustRegister(pw) // pw 自身实现 prometheus.Collector
//
//	cfg := mysql.Config{
//		Dsn:      dsn,
//		Name:     "order",
//		Observer: pw, // 事件通道
//	}
//	db, err := mysql.Open(cfg)
//	...
//	pw.RegisterPool(db.PoolStats) // 池水位
//	// 探活：
//	pw.RegisterHealth(metrics.ComponentMySQL, "order", db.HealthCheck)
//
// 完整的可编译示例见 example_test.go。
package prometheus

import (
	"sync"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"

	"github.com/zavierswong/go-infra/metrics"
)

// defaultNamespace 是全部指标的默认前缀。
const defaultNamespace = "go_infra"

// instancePlaceholder 是 Instance 为空时的占位值。
//
// metrics.Event.Instance 的注释明确要求：为空时适配器应当使用固定占位值，
// 不要让标签变成空字符串 —— 空标签会让没填 Config.Name 的多实例曲线
// 悄悄叠在一起，且无法用标签匹配区分。
const instancePlaceholder = "-"

// defaultBuckets 是操作耗时直方图的默认桶。
//
// Redis 单命令在毫秒级、SQL 查询在毫秒到秒级、MQ 的 publish 含 confirm
// 等待可能到秒级，桶从 0.5ms 铺到 10s 能同时覆盖三者。
var defaultBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
	0.25, 0.5, 1, 2.5, 5, 10,
}

const (
	defaultHealthInterval = 30 * time.Second
	defaultHealthTimeout  = 5 * time.Second
)

// Option 定制 Exporter。
type Option func(*options)

type options struct {
	namespace      string
	buckets        []float64
	healthInterval time.Duration
	healthTimeout  time.Duration
}

func newOptions(opts []Option) options {
	o := options{
		namespace:      defaultNamespace,
		buckets:        defaultBuckets,
		healthInterval: defaultHealthInterval,
		healthTimeout:  defaultHealthTimeout,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if o.namespace == "" {
		o.namespace = defaultNamespace
	}
	if len(o.buckets) == 0 {
		o.buckets = defaultBuckets
	}
	if o.healthInterval <= 0 {
		o.healthInterval = defaultHealthInterval
	}
	if o.healthTimeout <= 0 {
		o.healthTimeout = defaultHealthTimeout
	}
	return o
}

// WithNamespace 覆盖指标前缀，默认 "go_infra"。
//
// 只有同一个 Registry 里需要区分两套本库指标时才需要改，一般不用动。
func WithNamespace(ns string) Option {
	return func(o *options) { o.namespace = ns }
}

// WithBuckets 覆盖耗时直方图的桶边界，默认覆盖 0.5ms ~ 10s。
//
// 传 nil 恢复默认。桶是序列定义的一部分，无法中途调整，
// 请在服务启动时定好。
func WithBuckets(buckets []float64) Option {
	return func(o *options) { o.buckets = append([]float64(nil), buckets...) }
}

// WithHealthInterval 覆盖探活周期，默认 30s。
func WithHealthInterval(d time.Duration) Option {
	return func(o *options) { o.healthInterval = d }
}

// WithHealthTimeout 覆盖单次探活的超时，默认 5s。
//
// 探活超时只影响 component_up 报 0，不影响任何业务调用。
func WithHealthTimeout(d time.Duration) Option {
	return func(o *options) { o.healthTimeout = d }
}

// Exporter 把 metrics 契约翻译成 Prometheus 指标。
//
// 它同时扮演三个角色：
//
//   - prometheus.Collector：直接注册进自己的 Registry，
//     scrape 时现取池水位与组件状态；
//   - metrics.Observer：作为各包 Config.Observer 的事件出口；
//   - 探活调度器：注册了 RegisterHealth 后自动启动后台协程，
//     Close 可以停掉它。
//
// 一个 Exporter 只应注册进**一个** Registry：池计数器（wait、closed 等）
// 靠相邻两次 Collect 的差值递增，被两个 Registry 交替 scrape 会把增量
// 在两个 Registry 之间瓜分（各看到一部分）。
type Exporter struct {
	opts options

	// mu 保护 pools / statuses / kafkas / checks / lastPool / lastKafka
	// 与探活协程的运行状态。
	mu         sync.Mutex
	pools      []poolSource
	statuses   []statusSource
	kafkas     []kafkaSource
	checks     []healthCheck
	lastPool   map[string]metrics.PoolStats
	lastKafka  map[string]KafkaStatus
	healthStop chan struct{}

	// collectMu 串行化一次 scrape 的全过程（见 Collect）：
	// 让快照与倾倒不被并发 scrape 交错，行为更确定。
	// 注意它不解决"重复累加"—— 那是「读 last → 存本拍」必须在 e.mu 内
	// 原子完成来保证的，两者职责不同。
	collectMu sync.Mutex

	// ---- 事件通道（推） ----

	opDuration *prom.HistogramVec
	opErrors   *prom.CounterVec
	opNotFound *prom.CounterVec

	// ---- 池快照（拉）：瞬时量 Gauge ----

	poolMaxOpen *prom.GaugeVec
	poolOpen    *prom.GaugeVec
	poolInUse   *prom.GaugeVec
	poolIdle    *prom.GaugeVec
	poolPending *prom.GaugeVec

	// ---- 池快照（拉）：累计量 Counter，Collect 时按差值递增 ----

	poolWaitCount   *prom.CounterVec
	poolWaitSeconds *prom.CounterVec
	poolClosed      *prom.CounterVec
	poolHits        *prom.CounterVec
	poolMisses      *prom.CounterVec
	poolTimeouts    *prom.CounterVec
	poolUnusable    *prom.CounterVec
	poolStale       *prom.CounterVec

	// ---- 探活与组件状态 ----

	componentUp    *prom.GaugeVec
	statusClosed   *prom.GaugeVec
	statusConnect  *prom.GaugeVec
	statusPubReady *prom.GaugeVec

	// ---- Kafka 缓冲水位（拉）：Gauge，scrape 时现取 ----

	kafkaProduceRecords *prom.GaugeVec
	kafkaProduceBytes   *prom.GaugeVec
	kafkaFetchRecords   *prom.GaugeVec
	kafkaFetchBytes     *prom.GaugeVec
}

type poolSource func() metrics.PoolStats

// New 创建 Exporter。
//
// 返回值必须注册进 prometheus Registry 才会被 scrape：
//
//	pw := prometheus.New()
//	prometheus.MustRegister(pw)
//
// 指标前缀、桶边界与探活参数可通过 Option 调整。
func New(opts ...Option) *Exporter {
	o := newOptions(opts)

	e := &Exporter{
		opts:      o,
		lastPool:  make(map[string]metrics.PoolStats),
		lastKafka: make(map[string]KafkaStatus),
	}

	// ---- 事件通道：直方图观察全部事件，错误单独计数 ----
	//
	// 直方图同时收成功与失败（_count 是总量），错误率从
	// errors_total / duration_count 得出；not_found 按契约单独一条曲线，
	// 绝不混进 errors_total —— 否则缓存未命中会表现为"Redis 错误率飙升"。
	e.opDuration = prom.NewHistogramVec(prom.HistogramOpts{
		Namespace: o.namespace,
		Name:      "operation_duration_seconds",
		Help:      "Duration of one finished operation, including pool wait and network round trip.",
		Buckets:   o.buckets,
	}, []string{"component", "instance", "op"})
	e.opErrors = prom.NewCounterVec(prom.CounterOpts{
		Namespace: o.namespace,
		Name:      "operation_errors_total",
		Help:      "Failed operations by low-cardinality reason. Excludes not_found: misses are normal results for caches.",
	}, []string{"component", "instance", "op", "reason"})
	e.opNotFound = prom.NewCounterVec(prom.CounterOpts{
		Namespace: o.namespace,
		Name:      "operation_not_found_total",
		Help:      "Operations that reported not_found (empty result / missing key). Observed separately from errors.",
	}, []string{"component", "instance", "op"})

	// ---- 池水位 Gauge ----
	e.poolMaxOpen = newGaugeVec(o.namespace, "pool_max_open_connections",
		"Maximum number of open connections (MaxActiveConns for redis, per-server MaxPoolSize for mongo).")
	e.poolOpen = newGaugeVec(o.namespace, "pool_open_connections",
		"Currently established connections, including idle and in use.")
	e.poolInUse = newGaugeVec(o.namespace, "pool_in_use_connections",
		"Connections currently checked out.")
	e.poolIdle = newGaugeVec(o.namespace, "pool_idle_connections",
		"Idle connections. Consistently near zero means MinIdleConns/MaxIdleConns is too small.")
	e.poolPending = newGaugeVec(o.namespace, "pool_pending_requests",
		"Requests waiting for a connection. Only reported by redis and mongo; always fresh at scrape time.")

	// ---- 池累计量 Counter ----
	e.poolWaitCount = newCounterVec(o.namespace, "pool_wait_total",
		"Times a request had to wait for a connection. Growing means the pool is too small or ops hold connections too long.")
	e.poolWaitSeconds = newCounterVec(o.namespace, "pool_wait_duration_seconds_total",
		"Total time spent waiting for a connection.")
	e.poolClosed = prom.NewCounterVec(prom.CounterOpts{
		Namespace: o.namespace,
		Name:      "pool_closed_total",
		Help: "Connections closed by pool policy. reason=max_idle means MaxIdleConns is too small, " +
			"reason=max_idle_time means ConnMaxIdleTime is too short, reason=max_lifetime means ConnMaxLifetime is too short.",
	}, []string{"component", "instance", "reason"})
	e.poolHits = newCounterVec(o.namespace, "pool_hits_total",
		"Times a connection was reused from the idle pool (redis only).")
	e.poolMisses = newCounterVec(o.namespace, "pool_misses_total",
		"Times no idle connection was available and a new one had to be dialed (redis only). Growing means MinIdleConns is too small.")
	e.poolTimeouts = newCounterVec(o.namespace, "pool_timeouts_total",
		"Times waiting for a connection timed out (redis / mongo only).")
	e.poolUnusable = newCounterVec(o.namespace, "pool_unusable_total",
		"Connections dropped because they were already broken on checkout (redis / mongo only). Usually means server-side idle reaping outlives client lifetimes.")
	e.poolStale = newCounterVec(o.namespace, "pool_stale_total",
		"Connections dropped by topology changes such as replica-set failover (mongo only).")

	// ---- 探活与组件状态 ----
	e.componentUp = newGaugeVec(o.namespace, "component_up",
		"1 if the last health check succeeded, 0 otherwise. Checked in the background, never on the scrape path.")
	e.statusClosed = newGaugeVec(o.namespace, "status_closed",
		"1 if the client was closed by the application (Close), 0 otherwise.")
	e.statusConnect = newGaugeVec(o.namespace, "status_connected",
		"1 if the underlying transport is alive, 0 otherwise.")
	e.statusPubReady = newGaugeVec(o.namespace, "status_publish_ready",
		"1 if the publishing channel is ready, 0 otherwise. A live connection with a dead channel is possible (e.g. queue deleted -> 404).")

	// ---- Kafka 缓冲水位 ----
	e.kafkaProduceRecords = newGaugeVec(o.namespace, "kafka_buffered_produce_records",
		"Kafka records queued for produce but not yet acknowledged by a broker. Sustained growth toward MaxBufferedRecords means brokers cannot keep up or partitions are unavailable.")
	e.kafkaProduceBytes = newGaugeVec(o.namespace, "kafka_buffered_produce_bytes",
		"Bytes buffered for produce, not yet acknowledged by a broker.")
	e.kafkaFetchRecords = newGaugeVec(o.namespace, "kafka_buffered_fetch_records",
		"Kafka records fetched but not yet handed to the consume handler (consume-group side). Sustained growth means the handler or its downstream is the bottleneck.")
	e.kafkaFetchBytes = newGaugeVec(o.namespace, "kafka_buffered_fetch_bytes",
		"Bytes buffered on the fetch side, waiting to be processed by the consume handler.")

	return e
}

func newGaugeVec(namespace, name, help string) *prom.GaugeVec {
	return prom.NewGaugeVec(prom.GaugeOpts{
		Namespace: namespace,
		Name:      name,
		Help:      help,
	}, []string{"component", "instance"})
}

func newCounterVec(namespace, name, help string) *prom.CounterVec {
	return prom.NewCounterVec(prom.CounterOpts{
		Namespace: namespace,
		Name:      name,
		Help:      help,
	}, []string{"component", "instance"})
}
