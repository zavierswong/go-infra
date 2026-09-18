// Package breaker 实现三态熔断器（Closed / Open / HalfOpen）。
//
// 它保护的不是连接，而是**失败会传染的调用**：下游抖动时，与其让每个
// 请求都去撞超时，不如熔断器直接快速失败，把故障挡在门外，等下游恢复
// 后再放行探测。
//
// # 状态机
//
//	Closed（放行，滑动窗口内失败数达阈值）
//	  → Open（拒绝一切，快速失败 ErrOpen）
//	  → 冷却 OpenTimeout 后 → HalfOpen（只放行少量探测请求）
//	  → 探测成功 → Closed（清零重来）
//	  → 探测失败 → Open（重新冷却）
//
// # 为什么用"窗口内失败数"而不是"连续失败数"
//
// 连续失败数会被偶发成功清零：失败 4 次 → 成功 1 次 → 又要重新数 5 次，
// 下游持续劣化时熔断器永远不开。滑动窗口内的失败计数没有这个漏洞。
//
// # 最小用法
//
//	b := breaker.New(breaker.Config{Name: "payment"})
//	err := b.Do(func() error { return callPayment() })
//	if errors.Is(err, breaker.ErrOpen) {
//		return fallback() // 熔断中，走降级
//	}
//
// 与 metrics 契约的打通（State 变化进指标）留给后续版本；
// 本包刻意零依赖，不 import prometheus。
package breaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrOpen 是熔断期间的快速失败信号。
//
// 调用方应使用 errors.Is 判断并走降级路径，而不是把它当普通错误重试 ——
// 重试只会延长 Open 状态。
var ErrOpen = errors.New("breaker: circuit is open")

// State 是熔断器的三态。
type State int32

const (
	// StateClosed 放行全部请求，统计滑动窗口内的失败。
	StateClosed State = iota
	// StateOpen 拒绝全部请求（Allow 返回 ErrOpen），冷却等待。
	StateOpen
	// StateHalfOpen 冷却结束，只放行少量探测请求。
	StateHalfOpen
)

// String 返回状态名（closed / open / half_open）。
func (s State) String() string {
	switch s {
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

const (
	// numBuckets 是滑动窗口的分桶数。桶越细粒度越准，代价只是
	// 计数遍历 O(numBuckets)，10 个足够平滑。
	numBuckets = 10

	defaultWindow           = time.Minute
	defaultFailureThreshold = 5
	defaultOpenTimeout      = 30 * time.Second
	defaultHalfOpenProbes   = 1
)

// Config 是熔断器配置，零值字段取默认值。
type Config struct {
	// Name 是实例名，用于日志与未来的指标标签。可空。
	Name string

	// Window 是滑动窗口长度，默认 1m。
	Window time.Duration
	// FailureThreshold 是窗口内失败数阈值，达到即熔断，默认 5。
	//
	// 刻意用绝对失败数而不是失败率：窗口刚起步时（前几个请求）
	// 失败率没有统计意义，1 个请求失败 1 次就是 100%。
	FailureThreshold int
	// OpenTimeout 是熔断冷却时长，到期转入 HalfOpen，默认 30s。
	OpenTimeout time.Duration
	// HalfOpenMaxProbes 是半开状态允许的并发探测数，默认 1。
	// 大于 1 会更快确认恢复，但探测期间的下游压力也更大。
	HalfOpenMaxProbes int
}

func (c *Config) normalize() {
	if c.Window <= 0 {
		c.Window = defaultWindow
	}
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = defaultFailureThreshold
	}
	if c.OpenTimeout <= 0 {
		c.OpenTimeout = defaultOpenTimeout
	}
	if c.HalfOpenMaxProbes <= 0 {
		c.HalfOpenMaxProbes = defaultHalfOpenProbes
	}
}

// bucket 是滑动窗口的一个分桶。
//
// start 是桶的起始时刻；写入时若桶已过期（start 不等于当前桶起点），
// 直接清零复用，不需要后台协程做滚动。
type bucket struct {
	start           time.Time
	total, failures int64
}

// Breaker 是三态熔断器，零值不可用，请用 New 创建。
//
// 所有方法并发安全；内部只有一把互斥锁，临界区内全是内存操作，
// 不构成瓶颈。
type Breaker struct {
	cfg Config

	mu      sync.Mutex
	buckets [numBuckets]bucket
	state   State

	// openedAt 是进入 Open 的时刻，用于冷却计时。
	openedAt time.Time
	// probes 是 HalfOpen 已放行的探测数。
	probes int

	// inflight 是"已放行、尚未 Record"的调用数（用于识别陈旧结果）。
	inflight int
	// stale 是"熔断发生前已放行、结果尚未上报"的调用数。
	// 这些结果返回时不能裁决 HalfOpen —— 它们属于上一代。
	stale int
}

// New 创建熔断器。零值配置字段取默认值（见 Config）。
func New(cfg Config) *Breaker {
	cfg.normalize()
	return &Breaker{cfg: cfg}
}

// Allow 报告当前是否放行一次调用。
//
// 返回 ErrOpen 表示熔断中，调用方应当走降级而不是重试。
// 被放行的调用**必须**调用 Record 上报结果，否则半开探测的
// 计数会泄漏（放行后没有结果，探测名额无法回收）。
func (b *Breaker) Allow() error {
	now := time.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateOpen:
		if now.Sub(b.openedAt) < b.cfg.OpenTimeout {
			return ErrOpen
		}
		// 冷却结束：转半开，放行第一个探测。
		b.state = StateHalfOpen
		b.probes = 0
		fallthrough
	case StateHalfOpen:
		if b.probes >= b.cfg.HalfOpenMaxProbes {
			return ErrOpen
		}
		b.probes++
		b.inflight++
		return nil
	default:
		b.inflight++
		return nil
	}
}

// Record 上报一次调用的结果。
//
// Closed 状态下计入滑动窗口并检查阈值；HalfOpen 状态下一次结果
// 就定生死（成功关闭、失败重开）；Open 状态下忽略（结果对应的
// 调用不可能被放行过）。
//
// 熔断发生**之前**就已放行、此刻才返回的调用（慢请求）会被判定为陈旧：
// 它只做减法、不参与状态裁决 —— 否则一个 Closed 时代的慢请求成功返回，
// 就能在真正的探测出结果之前把熔断器合上，或反之用一次陈旧失败把它重新打开。
func (b *Breaker) Record(err error) {
	now := time.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.inflight > 0 {
		b.inflight--
	}
	if b.stale > 0 {
		// 陈旧结果：来自上一代（熔断前）的放行，不能裁决 HalfOpen。
		b.stale--
		return
	}

	switch b.state {
	case StateHalfOpen:
		if err == nil {
			b.resetLocked()
		} else {
			b.tripLocked(now)
		}
	case StateClosed:
		idx, start := b.bucketAt(now)
		bk := &b.buckets[idx]
		if bk.start != start {
			*bk = bucket{start: start}
		}
		bk.total++
		if err != nil {
			bk.failures++
			if b.windowFailuresLocked(now) >= int64(b.cfg.FailureThreshold) {
				b.tripLocked(now)
			}
		}
	}
}

// Do 是 Allow + 调用 + Record 的组合：放行则执行 fn 并上报结果。
//
// ErrOpen 会直接返回，fn 不会执行；这也是大多数调用方的用法。
// fn 自己的长耗时/超时控制由调用方负责（熔断器只看结果，不看耗时）。
//
// fn panic 时本方法会先按失败上报再重新 panic：处于 HalfOpen 时探测名额
// （probes）只由 Record 归零，不记录就会永久泄漏，此后所有请求都被
// ErrOpen 挡住，即使下游早已恢复 —— 熔断器被一次 panic 卡死。
func (b *Breaker) Do(fn func() error) (err error) {
	if err := b.Allow(); err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			b.Record(fmt.Errorf("breaker: fn panicked: %v", p))
			panic(p) // 不吞 panic：调用方仍需感知，只是不再丢失统计
		}
	}()
	err = fn()
	b.Record(err)
	return err
}

// DoContext 在 Do 的基础上先尊重 ctx 取消：ctx 已结束时不再放行，
// 避免熔断器放行了请求、下游却直接被 ctx 拒绝的浪费。
func (b *Breaker) DoContext(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return b.Do(fn)
}

// State 返回当前（惰性推进后的）状态。
//
// Open 冷却到期后返回 HalfOpen，即使还没有请求来触发转移 ——
// 监控读这个值不会滞后一个请求周期。
func (b *Breaker) State() State {
	now := time.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == StateOpen && now.Sub(b.openedAt) >= b.cfg.OpenTimeout {
		return StateHalfOpen
	}
	return b.state
}

// Name 返回配置的实例名。
func (b *Breaker) Name() string { return b.cfg.Name }

// bucketAt 返回 now 所属的桶下标与桶起点。
//
// idx 必须由 start 推导，与 start 共用同一个时间基准。早先 start 用
// time.Truncate（以**公元 1 年**为零点）、idx 却用 now.UnixNano()
// （以 **1970 年**为零点），两者相差 62135596800 秒。当 width 不能整除
// 这个偏移时（例如 Window=7s → width=700ms），同一个 Truncate 窗口内的
// 前后时刻会算出**不同的 idx**，于是写入另一个桶、并把刚累计的失败数
// 清零 —— 熔断阈值被稀释、甚至永远达不到。
func (b *Breaker) bucketAt(now time.Time) (int, time.Time) {
	width := b.cfg.Window / numBuckets
	if width <= 0 {
		width = time.Nanosecond
	}
	start := now.Truncate(width)
	idx := int(start.UnixNano()/int64(width)) % numBuckets
	if idx < 0 {
		idx += numBuckets
	}
	return idx, start
}

// windowFailuresLocked 统计仍落在窗口内的失败总数。
func (b *Breaker) windowFailuresLocked(now time.Time) int64 {
	var n int64
	cutoff := now.Add(-b.cfg.Window)
	for i := range b.buckets {
		bk := &b.buckets[i]
		if bk.start.After(cutoff) {
			n += bk.failures
		}
	}
	return n
}

func (b *Breaker) tripLocked(now time.Time) {
	b.state = StateOpen
	b.openedAt = now
	b.probes = 0
	// 此刻仍在途的调用属于"上一代"：它们返回时不能裁决 HalfOpen，
	// 只能做减法（见 Record 的 stale 分支）。
	b.stale = b.inflight
	// 清窗：熔断期间旧失败不再有意义，也避免重开后残留计数。
	b.buckets = [numBuckets]bucket{}
}

func (b *Breaker) resetLocked() {
	b.state = StateClosed
	b.probes = 0
	// 回到 Closed 后结果不再决定状态迁移，陈旧标记可以作废。
	b.stale = 0
	b.buckets = [numBuckets]bucket{}
}
