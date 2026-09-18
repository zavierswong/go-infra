package prometheus

import (
	"time"

	prom "github.com/prometheus/client_golang/prometheus"

	"github.com/zavierswong/go-infra/metrics"
)

// Describe 实现 prometheus.Collector。
//
// 用 DescribeByCollect 实现：序列按需创建（首次非零才建），
// 静态 Describe 无法枚举"将来会出现"的序列，干脆以一次真实
// Collect 的结果作为描述 —— 这是 client_golang 官方认可的
// 非确定性 Collector 写法。
func (e *Exporter) Describe(ch chan<- *prom.Desc) {
	prom.DescribeByCollect(e, ch)
}

// Collect 实现 prometheus.Collector：scrape 时现取池水位与组件状态，
// 再把全部 metric 倾倒进通道。
//
// 池快照函数与状态函数只做内存读取（各包内部有各自的锁），
// 不会阻塞 scrape；探活**永远不在这里执行** —— 它是网络探针，
// 由后台协程负责。
//
// 整个 Collect 由 collectMu 串行化，让一次 scrape 的快照与倾倒不被另一次
// 并发 scrape 交错（client_golang 不会替你串行化同一 Collector 的 Collect）。
func (e *Exporter) Collect(ch chan<- prom.Metric) {
	e.collectMu.Lock()
	defer e.collectMu.Unlock()

	// 说明：这里加锁不是为了防"重复累加"—— 「读上一拍 → 存本拍」在
	// e.mu 内原子完成，增量天然是望远镜式累加（后到的 Collect 读到
	// 已被更新的 last，差值为 0）。加这把锁是为了让一次 scrape 的
	// 快照与倾倒不被另一次并发 scrape 交错，行为更确定。
	e.collectPools()
	e.collectStatuses()
	e.collectKafkaStatuses()

	e.opDuration.Collect(ch)
	e.opErrors.Collect(ch)
	e.opNotFound.Collect(ch)

	e.poolMaxOpen.Collect(ch)
	e.poolOpen.Collect(ch)
	e.poolInUse.Collect(ch)
	e.poolIdle.Collect(ch)
	e.poolPending.Collect(ch)

	e.poolWaitCount.Collect(ch)
	e.poolWaitSeconds.Collect(ch)
	e.poolClosed.Collect(ch)
	e.poolHits.Collect(ch)
	e.poolMisses.Collect(ch)
	e.poolTimeouts.Collect(ch)
	e.poolUnusable.Collect(ch)
	e.poolStale.Collect(ch)

	e.componentUp.Collect(ch)
	e.statusClosed.Collect(ch)
	e.statusConnect.Collect(ch)
	e.statusPubReady.Collect(ch)

	e.kafkaProduceRecords.Collect(ch)
	e.kafkaProduceBytes.Collect(ch)
	e.kafkaFetchRecords.Collect(ch)
	e.kafkaFetchBytes.Collect(ch)
}

// collectPools 取一遍全部池快照，更新 Gauge 并把累计量按差值递增。
//
// 瞬时量（Gauge）只在「当前值非零」或「上一拍非零（序列已存在，
// 需要回落到 0）」时写入 —— 恒为 0 的字段永远不会建出序列，
// 这正是 metrics.PoolStats 注释里"不要给恒为 0 的字段建指标"的落实。
//
// 累计量（Counter）在相邻两次 Collect 之间取差值递增；
// 首次见到某来源时，把当前值整体当作增量（Exporter 晚于客户端
// 启动是常态，从 0 追到当前值是合理近似）。
//
// 「读上一拍 → 存本拍」在 e.mu 内原子完成，因此并发 scrape 不会把同一
// 增量重复累加（后到者读到的 last 已被更新，差值为 0）。
func (e *Exporter) collectPools() {
	e.mu.Lock()
	pools := make([]poolSource, len(e.pools))
	copy(pools, e.pools)
	e.mu.Unlock()

	for _, source := range pools {
		stats := source()
		key := poolKey(stats.Component, stats.Instance)

		e.mu.Lock()
		last, seen := e.lastPool[key]
		e.lastPool[key] = stats
		e.mu.Unlock()

		e.updatePoolGauges(stats, last, seen)
		e.updatePoolCounters(stats, last, seen)
	}
}

func (e *Exporter) updatePoolGauges(p, last metrics.PoolStats, seen bool) {
	c, i := string(p.Component), orPlaceholder(p.Instance)

	setGauge(e.poolMaxOpen, c, i, p.MaxOpen, last.MaxOpen, seen)
	setGauge(e.poolOpen, c, i, p.Open, last.Open, seen)
	setGauge(e.poolInUse, c, i, p.InUse, last.InUse, seen)
	setGauge(e.poolIdle, c, i, p.Idle, last.Idle, seen)
	setGauge(e.poolPending, c, i, p.Pending, last.Pending, seen)
}

func (e *Exporter) updatePoolCounters(p, last metrics.PoolStats, seen bool) {
	c, i := string(p.Component), orPlaceholder(p.Instance)

	addCounter(e.poolWaitCount, c, i, p.WaitCount, last.WaitCount, seen)
	addCounterSeconds(e.poolWaitSeconds, c, i, p.WaitDuration, last.WaitDuration, seen)

	for _, item := range []struct {
		reason    string
		cur, last int64
	}{
		{"max_idle", p.MaxIdleClosed, last.MaxIdleClosed},
		{"max_idle_time", p.MaxIdleTimeClosed, last.MaxIdleTimeClosed},
		{"max_lifetime", p.MaxLifetimeClosed, last.MaxLifetimeClosed},
	} {
		addCounterLabeled(e.poolClosed, c, i, item.reason, item.cur, item.last, seen)
	}

	addCounter(e.poolHits, c, i, p.Hits, last.Hits, seen)
	addCounter(e.poolMisses, c, i, p.Misses, last.Misses, seen)
	addCounter(e.poolTimeouts, c, i, p.Timeouts, last.Timeouts, seen)
	addCounter(e.poolUnusable, c, i, p.Unusable, last.Unusable, seen)
	addCounter(e.poolStale, c, i, p.Stale, last.Stale, seen)
}

// collectStatuses 取一遍组件状态快照。
//
// 与池水位不同，状态的 false 是有信息量的（客户端被 Close、
// 连接断开、发布通道被 broker 关掉），所以**始终**写入、
// 首次 scrape 就建序列。
func (e *Exporter) collectStatuses() {
	e.mu.Lock()
	statuses := make([]statusSource, len(e.statuses))
	copy(statuses, e.statuses)
	e.mu.Unlock()

	for _, source := range statuses {
		s := source()
		c, i := string(s.Component), orPlaceholder(s.Instance)

		e.statusClosed.WithLabelValues(c, i).Set(boolToFloat(s.Closed))
		e.statusConnect.WithLabelValues(c, i).Set(boolToFloat(s.Connected))
		e.statusPubReady.WithLabelValues(c, i).Set(boolToFloat(s.PublishChannelReady))
	}
}

// collectKafkaStatuses 取一遍 Kafka 缓冲水位快照。
//
// 水位 Gauge 沿用池水位的约定：0 是空闲的正常值、没有信息量，
// 首次非零才建序列（纯生产端不消费，fetch 侧永远不该出现序列）；
// 与池水位不同，Closed 落在共享的 status_closed 上 —— 它是状态语义，
// 0（存活）也有信息量，始终写入。
func (e *Exporter) collectKafkaStatuses() {
	e.mu.Lock()
	sources := make([]kafkaSource, len(e.kafkas))
	copy(sources, e.kafkas)
	e.mu.Unlock()

	for _, source := range sources {
		s := source()
		key := poolKey(s.Component, s.Instance)

		e.mu.Lock()
		last, seen := e.lastKafka[key]
		e.lastKafka[key] = s
		e.mu.Unlock()

		c, i := string(s.Component), orPlaceholder(s.Instance)

		e.statusClosed.WithLabelValues(c, i).Set(boolToFloat(s.Closed))

		setGauge64(e.kafkaProduceRecords, c, i, s.ProduceBufferedRecords, last.ProduceBufferedRecords, seen)
		setGauge64(e.kafkaProduceBytes, c, i, s.ProduceBufferedBytes, last.ProduceBufferedBytes, seen)
		setGauge64(e.kafkaFetchRecords, c, i, s.FetchBufferedRecords, last.FetchBufferedRecords, seen)
		setGauge64(e.kafkaFetchBytes, c, i, s.FetchBufferedBytes, last.FetchBufferedBytes, seen)
	}
}

// setGauge 写入池水位 Gauge，落实"恒为 0 不建序列"。
//
// 当前值为 0 且上一拍也是 0（或从未见过）时跳过：前者是
// 没必要建，后者说明序列本就不存在，跳过才能让它继续不存在。
func setGauge(g *prom.GaugeVec, component, instance string, cur, last int, seen bool) {
	if cur == 0 && (!seen || last == 0) {
		return
	}
	g.WithLabelValues(component, instance).Set(float64(cur))
}

// setGauge64 是 setGauge 的 int64 版本（Kafka 水位是字节数，int 装不下大缓冲）。
func setGauge64(g *prom.GaugeVec, component, instance string, cur, last int64, seen bool) {
	if cur == 0 && (!seen || last == 0) {
		return
	}
	g.WithLabelValues(component, instance).Set(float64(cur))
}

// addCounter 把累计量按差值递增。差值<=0 时不动 ——
// 计数器不允许回退，出现回退只可能是组件被重连重建，
// 让序列停在旧值比制造负增量更诚实。
func addCounter(ctr *prom.CounterVec, component, instance string, cur, last int64, seen bool) {
	if !seen {
		if cur > 0 {
			ctr.WithLabelValues(component, instance).Add(float64(cur))
		}
		return
	}
	if d := cur - last; d > 0 {
		ctr.WithLabelValues(component, instance).Add(float64(d))
	}
}

// addCounterSeconds 是 addCounter 的时长版本，换算成秒。
func addCounterSeconds(ctr *prom.CounterVec, component, instance string, cur, last time.Duration, seen bool) {
	if !seen {
		if cur > 0 {
			ctr.WithLabelValues(component, instance).Add(cur.Seconds())
		}
		return
	}
	if d := cur - last; d > 0 {
		ctr.WithLabelValues(component, instance).Add(d.Seconds())
	}
}

// addCounterLabeled 是带额外标签（reason）的 addCounter。
func addCounterLabeled(ctr *prom.CounterVec, component, instance, label string, cur, last int64, seen bool) {
	if !seen {
		if cur > 0 {
			ctr.WithLabelValues(component, instance, label).Add(float64(cur))
		}
		return
	}
	if d := cur - last; d > 0 {
		ctr.WithLabelValues(component, instance, label).Add(float64(d))
	}
}

func poolKey(component metrics.Component, instance string) string {
	return string(component) + "\x00" + instance
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
