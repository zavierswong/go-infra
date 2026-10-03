package breaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func newTestBreaker(cfg Config) *Breaker {
	// 测试用小窗口小超时，避免依赖真实时长。
	if cfg.Window == 0 {
		cfg.Window = 200 * time.Millisecond
	}
	if cfg.OpenTimeout == 0 {
		cfg.OpenTimeout = 60 * time.Millisecond
	}
	return New(cfg)
}

// TestTripAfterThreshold 窗口内失败达到阈值即熔断。
func TestTripAfterThreshold(t *testing.T) {
	b := newTestBreaker(Config{FailureThreshold: 3})

	for i := 0; i < 3; i++ {
		if err := b.Allow(); err != nil {
			t.Fatalf("第 %d 次不应被拒绝: %v", i+1, err)
		}
		b.Record(errors.New("boom"))
	}

	if got := b.State(); got != StateOpen {
		t.Errorf("达到阈值后应进入 Open, got %v", got)
	}
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Errorf("Open 状态 Allow 应返回 ErrOpen, got %v", err)
	}
}

// TestSuccessKeepsClosed 只有失败计入，成功不会触发熔断。
func TestSuccessKeepsClosed(t *testing.T) {
	b := newTestBreaker(Config{FailureThreshold: 3})

	for i := 0; i < 100; i++ {
		if err := b.Allow(); err != nil {
			t.Fatalf("成功路径不应被拒绝: %v", err)
		}
		b.Record(nil)
	}

	if got := b.State(); got != StateClosed {
		t.Errorf("全部成功应保持 Closed, got %v", got)
	}
}

// TestOpenToHalfOpenAfterTimeout 冷却到期转半开并限量放行探测。
func TestOpenToHalfOpenAfterTimeout(t *testing.T) {
	b := newTestBreaker(Config{FailureThreshold: 1, HalfOpenMaxProbes: 2})

	b.Record(errors.New("boom"))
	if got := b.State(); got != StateOpen {
		t.Fatalf("应为 Open, got %v", got)
	}

	time.Sleep(80 * time.Millisecond) // > OpenTimeout(60ms)

	if got := b.State(); got != StateHalfOpen {
		t.Errorf("冷却到期应为 HalfOpen, got %v", got)
	}
	if err := b.Allow(); err != nil {
		t.Errorf("半开应放行第 1 个探测: %v", err)
	}
	if err := b.Allow(); err != nil {
		t.Errorf("半开应放行第 2 个探测: %v", err)
	}
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Errorf("探测名额用尽后应拒绝, got %v", err)
	}
}

// TestHalfOpenSuccessCloses 探测成功回到 Closed 并清零窗口。
func TestHalfOpenSuccessCloses(t *testing.T) {
	b := newTestBreaker(Config{FailureThreshold: 1})

	b.Record(errors.New("boom"))
	time.Sleep(80 * time.Millisecond)

	if err := b.Allow(); err != nil {
		t.Fatalf("半开探测应放行: %v", err)
	}
	b.Record(nil)

	if got := b.State(); got != StateClosed {
		t.Fatalf("探测成功应回到 Closed, got %v", got)
	}
	// 回到 Closed 后放行恢复正常（窗口已清零，需要重新累计）。
	if err := b.Allow(); err != nil {
		t.Errorf("Closed 状态应正常放行, got %v", err)
	}
}

// TestHalfOpenFailureReopens 探测失败重新进入 Open（重新冷却）。
func TestHalfOpenFailureReopens(t *testing.T) {
	b := newTestBreaker(Config{FailureThreshold: 1, OpenTimeout: 10 * time.Millisecond})

	b.Record(errors.New("boom"))
	time.Sleep(20 * time.Millisecond)

	if err := b.Allow(); err != nil {
		t.Fatalf("半开探测应放行: %v", err)
	}
	b.Record(errors.New("boom"))

	if got := b.State(); got != StateOpen {
		t.Fatalf("探测失败应重回 Open, got %v", got)
	}
	// 重新冷却：立刻 Allow 必须被拒。
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Errorf("重开后应重新冷却, got %v", err)
	}
}

// TestWindowExpiryForgetsFailures 窗口滑过之后旧失败不再计数。
func TestWindowExpiryForgetsFailures(t *testing.T) {
	b := newTestBreaker(Config{FailureThreshold: 5, Window: 150 * time.Millisecond})

	for i := 0; i < 4; i++ {
		b.Record(errors.New("boom"))
	}

	time.Sleep(200 * time.Millisecond) // 窗口滑过

	b.Record(errors.New("boom")) // 只剩这 1 个在窗口内

	if got := b.State(); got != StateClosed {
		t.Errorf("过期失败不应计数, 期望仍 Closed, got %v", got)
	}
}

// TestDo 组合入口：放行则执行并记录，拒绝则不执行。
func TestDo(t *testing.T) {
	b := newTestBreaker(Config{FailureThreshold: 1})

	called := 0
	if err := b.Do(func() error { called++; return nil }); err != nil {
		t.Fatalf("Do 不应失败: %v", err)
	}
	if called != 1 {
		t.Fatalf("fn 应被执行 1 次, got %d", called)
	}

	b.Record(errors.New("boom")) // 熔断
	if err := b.Do(func() error { called++; return nil }); !errors.Is(err, ErrOpen) {
		t.Errorf("熔断中 Do 应返回 ErrOpen, got %v", err)
	}
	if called != 1 {
		t.Errorf("熔断中 fn 不应执行, called = %d", called)
	}
}

// TestDoContext ctx 已取消时不放行。
func TestDoContext(t *testing.T) {
	b := newTestBreaker(Config{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := 0
	err := b.DoContext(ctx, func() error { called++; return nil })
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ctx 取消应返回 context.Canceled, got %v", err)
	}
	if called != 0 {
		t.Errorf("fn 不应执行, called = %d", called)
	}
}

// TestDoPanicDoesNotLeakProbe 回归测试：fn panic 不得让 HalfOpen 的探测
// 名额永久泄漏。
//
// 旧实现里 Do 没有 recover，panic 直接上抛、Record 永不执行；而 HalfOpen
// 的 probes 只由 Record 归零，于是熔断器此后再也放不出探测请求，即使
// 下游已经恢复也一直返回 ErrOpen。
func TestDoPanicDoesNotLeakProbe(t *testing.T) {
	b := newTestBreaker(Config{
		FailureThreshold:  1,
		HalfOpenMaxProbes: 1,
		OpenTimeout:       20 * time.Millisecond,
	})

	// 触发熔断：一次失败即达阈值。
	b.Record(errors.New("boom"))
	if got := b.State(); got != StateOpen {
		t.Fatalf("应进入 Open, got %v", got)
	}

	// 等冷却结束进入 HalfOpen，然后让探测请求 panic。
	time.Sleep(40 * time.Millisecond)

	func() {
		defer func() {
			if p := recover(); p == nil {
				t.Error("panic 应当继续上抛给调用方，不能被吞掉")
			}
		}()
		_ = b.Do(func() error { panic("handler bug") })
	}()

	// 关键：panic 之后熔断器必须能重新放行探测（probes 已归零）。
	// 旧实现下这里会永久卡在 ErrOpen。
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := b.Allow()
		if err == nil {
			b.Record(nil) // 探测成功 → 回到 Closed
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("panic 之后 HalfOpen 探测名额被泄漏，Allow 始终返回 %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := b.State(); got != StateClosed {
		t.Errorf("探测成功后应回到 Closed, got %v", got)
	}
}

// TestConcurrentRecords 并发记录不丢数、不竞态。
func TestConcurrentRecords(t *testing.T) {
	b := newTestBreaker(Config{FailureThreshold: 1000, Window: time.Second})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = b.Allow()
				b.Record(nil)
			}
		}()
	}
	wg.Wait()

	if got := b.State(); got != StateClosed {
		t.Errorf("高并发成功应保持 Closed, got %v", got)
	}
}

// TestBucketAtStableWithinWindow 同一个 Truncate 窗口内的所有时刻必须落在
// 同一个桶上。
//
// 旧实现里 start 用 time.Truncate（公元 1 年基准）、idx 用 UnixNano
// （1970 基准），两者相差 62135596800 秒。width 不能整除该偏移时
// （Window=7s → width=700ms 就是），同一窗口内前后时刻会算出不同 idx：
// 计数被写到另一个桶、本窗口已累计的失败数被清零，熔断阈值被稀释。
func TestBucketAtStableWithinWindow(t *testing.T) {
	b := newTestBreaker(Config{Window: 7 * time.Second})
	width := 7 * time.Second / time.Duration(numBuckets)

	base := time.Date(2026, 9, 16, 22, 0, 0, 0, time.Local)
	for i := 0; i < 500; i++ {
		// 步长故意与 width 不成整数倍，覆盖窗口内的各个位置。
		now := base.Add(time.Duration(i) * 137 * time.Millisecond)
		idx, start := b.bucketAt(now)

		wantStart := now.Truncate(width)
		if !start.Equal(wantStart) {
			t.Fatalf("bucketAt(%v).start = %v, want %v", now, start, wantStart)
		}
		// 窗口起点自身必须落在同一个桶：这是"同一窗口 = 同一 idx"的判据。
		idxAtStart, startAtStart := b.bucketAt(wantStart)
		if idxAtStart != idx || !startAtStart.Equal(start) {
			t.Fatalf("窗口 %v 内 idx 不稳定: bucketAt(%v)=(%d,%v), bucketAt(%v)=(%d,%v)",
				wantStart, now, idx, start, wantStart, idxAtStart, startAtStart)
		}
	}
}

// TestStaleResultDoesNotDecideHalfOpen 回归测试：熔断前放行的慢请求，
// 其结果不得裁决 HalfOpen。
//
// 旧实现里 Record 只看当前状态：Closed 时代放行的慢调用若恰好在 HalfOpen
// 期间返回，一次陈旧成功就能在真正的探测出结果之前把熔断器合上（反之
// 一次陈旧失败会立即重开）。现在这些结果被标记为陈旧，只做减法。
func TestStaleResultDoesNotDecideHalfOpen(t *testing.T) {
	b := newTestBreaker(Config{
		FailureThreshold:  1,
		HalfOpenMaxProbes: 1,
		OpenTimeout:       30 * time.Millisecond,
	})

	// Closed 时代放行两个调用：一个慢调用（尚未返回）、一个即将失败。
	if err := b.Allow(); err != nil {
		t.Fatalf("Closed 应放行: %v", err)
	}
	if err := b.Allow(); err != nil {
		t.Fatalf("Closed 应放行: %v", err)
	}
	// 第二个调用失败并触发熔断 —— 此刻第一个（慢）调用仍在途。
	b.Record(errors.New("boom"))
	if got := b.State(); got != StateOpen {
		t.Fatalf("应进入 Open, got %v", got)
	}

	// 等冷却进入 HalfOpen，并放行一个真正的探测（尚未返回结果）。
	time.Sleep(50 * time.Millisecond)
	if err := b.Allow(); err != nil {
		t.Fatalf("HalfOpen 应放行探测: %v", err)
	}

	// 此刻陈旧结果返回（成功）：不得替探测做决定、把熔断器合上。
	b.Record(nil)
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("陈旧成功结果不应关闭熔断器, got %v", got)
	}

	// 真正的探测失败 → 必须重新熔断。
	b.Record(errors.New("probe failed"))
	if got := b.State(); got != StateOpen {
		t.Fatalf("探测失败应重新熔断, got %v", got)
	}
}

// TestLegacyStaleResultDoesNotLeakProbe 回归测试（Critical 修复）：
// 熔断前在途的慢请求先于探测结果返回时，不得让探测名额泄漏。
//
// 旧实现里 stale 是"按到达顺序数个数"的近似：探测结果若先到，会被
// 误当陈旧结果吞掉且名额不回收，Allow 从此永久返回 ErrOpen —— 熔断器
// 卡死在 HalfOpen，即使下游早已恢复。
func TestLegacyStaleResultDoesNotLeakProbe(t *testing.T) {
	b := newTestBreaker(Config{
		FailureThreshold:  1,
		OpenTimeout:       30 * time.Millisecond,
		HalfOpenMaxProbes: 1,
	})

	// Closed 代放行慢调用 A（一直不返回）。
	if err := b.Allow(); err != nil {
		t.Fatalf("Closed 应放行: %v", err)
	}
	// B 失败触发熔断，此刻 stale = 1。
	b.Record(errors.New("boom"))
	time.Sleep(50 * time.Millisecond)

	// 半开放行探测 C（legacy 路径）。
	if err := b.Allow(); err != nil {
		t.Fatalf("HalfOpen 应放行探测: %v", err)
	}
	// 探测 C 先成功返回：不得被当陈旧结果吞掉后卡死。
	b.Record(nil)
	if err := b.Allow(); err != nil {
		t.Fatalf("探测名额被泄漏，Allow = %v（旧实现的永久卡死路径）", err)
	}
	// 新探测失败 → 正常重开，而不是永久 ErrOpen。
	b.Record(errors.New("probe failed"))
	if got := b.State(); got != StateOpen {
		t.Fatalf("探测失败应重新熔断, got %v", got)
	}
}

// TestTicketIgnoresStaleResults 票据路径必须精确区分代际：
// 陈旧结果不得裁决 HalfOpen，也不得写进新一代的窗口。
func TestTicketIgnoresStaleResults(t *testing.T) {
	b := newTestBreaker(Config{
		FailureThreshold:  1,
		OpenTimeout:       30 * time.Millisecond,
		HalfOpenMaxProbes: 1,
	})

	// Closed 代放行两个票据调用：A（慢）、B（立刻失败）。
	tA, err := b.AllowTicket()
	if err != nil {
		t.Fatalf("Closed 应放行: %v", err)
	}
	tB, err := b.AllowTicket()
	if err != nil {
		t.Fatalf("Closed 应放行: %v", err)
	}
	b.RecordTicket(tB, errors.New("boom")) // trip → gen+1
	time.Sleep(50 * time.Millisecond)

	// 半开探测成功 —— 尽管陈旧的 A 尚未返回。
	tP, err := b.AllowTicket()
	if err != nil {
		t.Fatalf("HalfOpen 应放行探测: %v", err)
	}
	b.RecordTicket(tP, nil)
	if got := b.State(); got != StateClosed {
		t.Fatalf("探测成功应回到 Closed, got %v", got)
	}

	// 陈旧的 A 此刻才返回（失败）：代数不匹配，必须被完整忽略。
	b.RecordTicket(tA, errors.New("stale boom"))
	if got := b.State(); got != StateClosed {
		t.Errorf("上一代结果不得裁决当前状态, got %v", got)
	}
}

// TestRecordSkippedReturnsProbeSlot RecordSkipped 只归还探测名额、
// 不参与裁决 —— 对应 httpclient"调用方取消不算下游故障"的路径。
func TestRecordSkippedReturnsProbeSlot(t *testing.T) {
	b := newTestBreaker(Config{
		FailureThreshold:  1,
		OpenTimeout:       20 * time.Millisecond,
		HalfOpenMaxProbes: 1,
	})

	b.Record(errors.New("boom")) // trip
	time.Sleep(40 * time.Millisecond)

	tP, err := b.AllowTicket()
	if err != nil {
		t.Fatalf("HalfOpen 应放行探测: %v", err)
	}
	b.RecordSkipped(tP) // 调用方取消：归还名额，不裁决
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("跳过的结果不应改变状态, got %v", got)
	}
	// 名额已归还：下一个探测立刻可以放行（旧实现下这里永久 ErrOpen）。
	if _, err := b.AllowTicket(); err != nil {
		t.Fatalf("探测名额应已归还, Allow = %v", err)
	}
}

// TestStateString 状态名的稳定性（可能进监控/日志）。
func TestStateString(t *testing.T) {
	cases := map[State]string{
		StateClosed:   "closed",
		StateOpen:     "open",
		StateHalfOpen: "half_open",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("State(%d).String() = %q, want %q", s, got, want)
		}
	}
}
