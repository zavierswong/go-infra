package metrics

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// stubTimeoutErr 模拟底层网络超时：它不包装 ctx，只有 net.Error 的 Timeout 标记。
// ClassifyErr 必须靠 errors.As 才能识别它。
type stubTimeoutErr struct{}

func (stubTimeoutErr) Error() string   { return "i/o timeout" }
func (stubTimeoutErr) Timeout() bool   { return true }
func (stubTimeoutErr) Temporary() bool { return true }

func TestClassifyErr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want Reason
	}{
		{"nil", nil, ReasonNone},
		{"ctx deadline", context.DeadlineExceeded, ReasonTimeout},
		{"包装后的 ctx deadline", fmt.Errorf("查询失败: %w", context.DeadlineExceeded), ReasonTimeout},
		{"os deadline", os.ErrDeadlineExceeded, ReasonTimeout},
		{"网络超时", stubTimeoutErr{}, ReasonTimeout},
		{"包装后的网络超时", fmt.Errorf("拨号失败: %w", stubTimeoutErr{}), ReasonTimeout},
		{"ctx canceled", context.Canceled, ReasonCanceled},
		{"包装后的 ctx canceled", fmt.Errorf("上游断开: %w", context.Canceled), ReasonCanceled},
		{"零行", sql.ErrNoRows, ReasonNotFound},
		{"连接已关", net.ErrClosed, ReasonClosed},
		{"EOF", io.EOF, ReasonConnect},
		{"意外 EOF", io.ErrUnexpectedEOF, ReasonConnect},
		{"包装后的 EOF", fmt.Errorf("读响应: %w", io.EOF), ReasonConnect},
		{"普通错误", errors.New("boom"), ReasonUnknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyErr(tc.err); got != tc.want {
				t.Fatalf("ClassifyErr(%v) = %q, 期望 %q", tc.err, got, tc.want)
			}
		})
	}
}

// TestClassifyErrPrefersDeadlineOverCanceled 覆盖一个容易写反的顺序：
// context.DeadlineExceeded 与 context.Canceled 是两个不同的哨兵，
// 但"超时导致取消"的错误里两者可能同时可匹配，必须先判 Deadline。
func TestClassifyErrPrefersDeadlineOverCanceled(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("外层: %w", fmt.Errorf("内层: %w", context.DeadlineExceeded))
	if got := ClassifyErr(err); got != ReasonTimeout {
		t.Fatalf("多层包装的 deadline 应判为 timeout，实际 %q", got)
	}
}

// TestReasonVocabulary 保证词汇表本身是干净的：取值唯一、非空、且没有
// 大小写或连字符不统一的写法（这类问题只会在 dashboard 上才被发现）。
func TestReasonVocabulary(t *testing.T) {
	t.Parallel()

	all := map[Reason]string{
		ReasonNone:     "ReasonNone",
		ReasonTimeout:  "ReasonTimeout",
		ReasonCanceled: "ReasonCanceled",
		ReasonClosed:   "ReasonClosed",
		ReasonConnect:  "ReasonConnect",
		ReasonNotFound: "ReasonNotFound",
		ReasonInvalid:  "ReasonInvalid",
		ReasonConflict: "ReasonConflict",
		ReasonRejected: "ReasonRejected",
		ReasonUnknown:  "ReasonUnknown",
	}

	seen := make(map[Reason]string, len(all))
	for reason, goName := range all {
		if reason == "" {
			t.Errorf("%s 的取值为空字符串", goName)
			continue
		}
		if prev, dup := seen[reason]; dup {
			t.Errorf("取值 %q 被 %s 与 %s 重复使用", reason, prev, goName)
		}
		seen[reason] = goName

		for _, r := range reason {
			ok := (r >= 'a' && r <= 'z') || r == '_'
			if !ok {
				t.Errorf("%s 的取值 %q 含非法字符 %q（只允许小写字母与下划线）", goName, reason, r)
			}
		}
	}
}

func TestEventFailed(t *testing.T) {
	t.Parallel()

	if (Event{}).Failed() {
		t.Fatal("零值 Event 不应算失败")
	}
	if !(Event{Err: errors.New("x")}).Failed() {
		t.Fatal("带 Err 的 Event 应算失败")
	}
}

func TestObserverFunc(t *testing.T) {
	t.Parallel()

	var got []Event
	var mu sync.Mutex
	obs := ObserverFunc(func(e Event) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, e)
	})

	obs.ObserveOp(Event{Component: ComponentMySQL, Op: OpQuery, Duration: time.Millisecond})

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("应收到 1 个事件，实际 %d", len(got))
	}
	if got[0].Component != ComponentMySQL || got[0].Op != OpQuery {
		t.Fatalf("事件内容被改写: %+v", got[0])
	}
}

// TestObserverFuncNilIsSafe 覆盖"值为 nil 的函数"这条容易 panic 的路径。
func TestObserverFuncNilIsSafe(t *testing.T) {
	t.Parallel()

	var f ObserverFunc
	f.ObserveOp(Event{})
}

func TestNopObserver(t *testing.T) {
	t.Parallel()

	// 不 panic 即为通过。
	NopObserver{}.ObserveOp(Event{Detail: "任意内容"})
}

func TestOrNop(t *testing.T) {
	t.Parallel()

	if _, ok := OrNop(nil).(NopObserver); !ok {
		t.Fatalf("nil 应被替换成 NopObserver，实际 %T", OrNop(nil))
	}

	called := false
	real := ObserverFunc(func(Event) { called = true })
	OrNop(real).ObserveOp(Event{})
	if !called {
		t.Fatal("非 nil 的 Observer 不应被替换")
	}
}

// TestOrNopWithTypedNil 记录一个 Go 的经典陷阱：值为 nil 的具体类型
// 装进接口后不等于 nil 接口，OrNop 识别不出来 —— 兜底靠 ObserverFunc 自身。
func TestOrNopWithTypedNil(t *testing.T) {
	t.Parallel()

	var f ObserverFunc
	got := OrNop(f)
	if _, isNop := got.(NopObserver); isNop {
		t.Fatal("本用例想演示的是 typed-nil 无法被识别，行为变了就该更新这段说明")
	}
	got.ObserveOp(Event{}) // 仍然安全：ObserverFunc 内部判了 nil
}

func TestMultiObserver(t *testing.T) {
	t.Parallel()

	t.Run("忽略 nil 元素并保持顺序", func(t *testing.T) {
		t.Parallel()

		var order []string
		a := ObserverFunc(func(Event) { order = append(order, "a") })
		b := ObserverFunc(func(Event) { order = append(order, "b") })

		MultiObserver(a, nil, b).ObserveOp(Event{})

		if len(order) != 2 || order[0] != "a" || order[1] != "b" {
			t.Fatalf("广播顺序错误: %v", order)
		}
	})

	t.Run("只有一个有效观察者时直接返回它", func(t *testing.T) {
		t.Parallel()

		// 注意：不能写 `MultiObserver(a) == Observer(a)` —— 函数类型不可比较，
		// 比较含函数的接口会在运行期 panic。改为断言它没有被包成 multiObserver。
		a := ObserverFunc(func(Event) {})
		if _, wrapped := MultiObserver(a).(multiObserver); wrapped {
			t.Fatal("单元素不应被包装成 multiObserver，那会多一层间接调用")
		}
	})

	t.Run("全为 nil 时退化为 Nop", func(t *testing.T) {
		t.Parallel()

		if _, ok := MultiObserver(nil, nil).(NopObserver); !ok {
			t.Fatalf("应返回 NopObserver，实际 %T", MultiObserver(nil, nil))
		}
		if _, ok := MultiObserver().(NopObserver); !ok {
			t.Fatalf("空参数应返回 NopObserver，实际 %T", MultiObserver())
		}
	})
}

func TestFromDBStats(t *testing.T) {
	t.Parallel()

	in := sql.DBStats{
		MaxOpenConnections: 32,
		OpenConnections:    12,
		InUse:              7,
		Idle:               5,
		WaitCount:          99,
		WaitDuration:       1500 * time.Millisecond,
		MaxIdleClosed:      3,
		MaxIdleTimeClosed:  4,
		MaxLifetimeClosed:  5,
	}

	got := FromDBStats(ComponentPostgres, "report", in)

	if got.Component != ComponentPostgres || got.Instance != "report" {
		t.Fatalf("组件或实例名未透传: %+v", got)
	}
	if got.MaxOpen != 32 || got.Open != 12 || got.InUse != 7 || got.Idle != 5 {
		t.Fatalf("瞬时量映射错误: %+v", got)
	}
	if got.WaitCount != 99 || got.WaitDuration != 1500*time.Millisecond {
		t.Fatalf("等待量映射错误: %+v", got)
	}
	if got.MaxIdleClosed != 3 || got.MaxIdleTimeClosed != 4 || got.MaxLifetimeClosed != 5 {
		t.Fatalf("回收计数映射错误: %+v", got)
	}

	// 数据库侧不存在的字段必须保持 0，否则会在监控上出现假曲线。
	if got.Pending != 0 || got.Hits != 0 || got.Misses != 0 ||
		got.Timeouts != 0 || got.Unusable != 0 || got.Stale != 0 {
		t.Fatalf("数据库侧不应填充 Redis 专有字段: %+v", got)
	}
}

func TestPoolStatsSaturated(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   PoolStats
		want bool
	}{
		{"未吃满", PoolStats{MaxOpen: 32, InUse: 31}, false},
		{"刚好吃满", PoolStats{MaxOpen: 32, InUse: 32}, true},
		{"超限", PoolStats{MaxOpen: 32, InUse: 40}, true},
		{"没有上限时永不判定吃满", PoolStats{MaxOpen: 0, InUse: 100}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.in.Saturated(); got != tc.want {
				t.Fatalf("Saturated() = %v, 期望 %v", got, tc.want)
			}
		})
	}
}

func TestPoolStatsIdleRatio(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   PoolStats
		want float64
	}{
		{"无一空闲", PoolStats{Open: 8, Idle: 0}, 0},
		{"一半空闲", PoolStats{Open: 8, Idle: 4}, 0.5},
		{"全部空闲", PoolStats{Open: 8, Idle: 8}, 1},
		// 没有已建立连接时必须返回 0 而不是 NaN，否则监控侧会画出空洞。
		{"无已建立连接", PoolStats{Open: 0, Idle: 0}, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.in.IdleRatio(); got != tc.want {
				t.Fatalf("IdleRatio() = %v, 期望 %v", got, tc.want)
			}
		})
	}
}
