package utils

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func mustSnowflake(t *testing.T, cfg SnowflakeConfig) *Snowflake {
	t.Helper()
	s, err := NewSnowflake(cfg)
	if err != nil {
		t.Fatalf("NewSnowflake: %v", err)
	}
	return s
}

// TestSnowflakeDefaults 默认配置：63 位、正数、19 位以内十进制。
func TestSnowflakeDefaults(t *testing.T) {
	s := mustSnowflake(t, SnowflakeConfig{Node: 1})

	id, err := s.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if id <= 0 {
		t.Fatalf("ID 应为正数, got %d", id)
	}
	if id < 0 { // 63 位内不会触顶，符号位恒 0
		t.Fatal("ID 应在 int64 正数范围")
	}
	str := strconv.FormatInt(id, 10)
	if len(str) > 19 {
		t.Fatalf("默认位宽应 ≤19 位数字, got %s", str)
	}

	p := s.Parse(id)
	if p.Node != 1 {
		t.Errorf("Parse 应还原 node=1, got %d", p.Node)
	}
	if d := time.Since(p.Time); d < 0 || d > time.Minute {
		t.Errorf("Parse 还原的时间应接近 now, diff %v", d)
	}
}

// TestSnowflakeUniqueConcurrent 并发发号唯一性 + 单调递增（每 goroutine 内）。
func TestSnowflakeUniqueConcurrent(t *testing.T) {
	s := mustSnowflake(t, SnowflakeConfig{Node: 7})

	const workers = 8
	const perWorker = 5000

	var mu sync.Mutex
	all := make(map[int64]struct{}, workers*perWorker)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var last int64
			for j := 0; j < perWorker; j++ {
				id, err := s.Next()
				if err != nil {
					t.Errorf("Next: %v", err)
					return
				}
				if id <= last {
					t.Errorf("ID 应回退: last=%d id=%d", last, id)
					return
				}
				last = id
				mu.Lock()
				if _, dup := all[id]; dup {
					t.Errorf("重复 ID: %d", id)
				}
				all[id] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(all) != workers*perWorker {
		t.Fatalf("应生成 %d 个不同 ID, got %d", workers*perWorker, len(all))
	}
}

// TestSnowflakeCustomBits53 53 位定制：可过 JS Number 的安全整数范围。
func TestSnowflakeCustomBits53(t *testing.T) {
	s := mustSnowflake(t, SnowflakeConfig{
		TimeBits: 41, NodeBits: 5, SeqBits: 7, Node: 3,
	})

	for i := 0; i < 1000; i++ {
		id, err := s.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if id >= 1<<53 {
			t.Fatalf("53 位 ID 应 < 2^53, got %d", id)
		}
		str := strconv.FormatInt(id, 10)
		if len(str) > 16 {
			t.Fatalf("53 位 ID 应 ≤16 位数字, got %s", str)
		}
		p := s.Parse(id)
		if p.Node != 3 {
			t.Fatalf("Parse node = %d, want 3", p.Node)
		}
	}
}

// TestSnowflakeNextString 纯数字字符串：无符号无前缀，且与 int64 对应。
func TestSnowflakeNextString(t *testing.T) {
	s := mustSnowflake(t, SnowflakeConfig{Node: 2})

	str, err := s.NextString()
	if err != nil {
		t.Fatalf("NextString: %v", err)
	}
	if strings.ContainsAny(str, "-+.e ") || str == "" {
		t.Fatalf("应为纯十进制数字, got %q", str)
	}
	id, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		t.Fatalf("字符串应可解析回 int64: %v", err)
	}
	p := s.Parse(id)
	if p.Node != 2 {
		t.Fatalf("Parse node = %d, want 2", p.Node)
	}
}

// fakeClock 可脚本化的时钟：依次返回预置时刻，耗尽后停在最后一个。
type fakeClock struct {
	mu   sync.Mutex
	ts   []time.Time
	idx  int
	base time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{base: start, ts: []time.Time{start}}
}

func (c *fakeClock) setSeq(ts ...time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ts = ts
	c.idx = 0
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.idx < len(c.ts) {
		c.idx++
	}
	return c.ts[min(c.idx-1, len(c.ts)-1)]
}

// TestSnowflakeClockRollbackSmall 时钟小幅回拨：等待追上后继续单调发号。
func TestSnowflakeClockRollbackSmall(t *testing.T) {
	base := time.Now().Truncate(time.Millisecond)
	clock := newFakeClock(base)
	s := mustSnowflake(t, SnowflakeConfig{Node: 1, StartTime: base, MaxRollbackWait: time.Second})
	s.now = clock.Now

	id1, err := s.Next() // t0
	if err != nil {
		t.Fatal(err)
	}

	// 回拨 100ms 再前进 200ms。
	clock.setSeq(base.Add(-100*time.Millisecond), base.Add(200*time.Millisecond))
	id2, err := s.Next()
	if err != nil {
		t.Fatalf("小幅回拨应等待追上, got %v", err)
	}
	if id2 <= id1 {
		t.Fatalf("追上后 ID 应继续递增: %d -> %d", id1, id2)
	}
}

// TestSnowflakeClockRollbackLarge 回拨超过容忍窗口：拒绝生成。
func TestSnowflakeClockRollbackLarge(t *testing.T) {
	base := time.Now().Truncate(time.Millisecond)
	clock := newFakeClock(base)
	s := mustSnowflake(t, SnowflakeConfig{Node: 1, StartTime: base, MaxRollbackWait: time.Second})
	s.now = clock.Now

	if _, err := s.Next(); err != nil {
		t.Fatal(err)
	}
	clock.setSeq(base.Add(-5 * time.Second))
	if _, err := s.Next(); err == nil {
		t.Fatal("大幅回拨应返回错误")
	}
}

// TestSnowflakeSeqExhaustion 单毫秒序列耗尽：自动进位到下一毫秒。
func TestSnowflakeSeqExhaustion(t *testing.T) {
	base := time.Now().Truncate(time.Millisecond)
	clock := newFakeClock(base)
	s := mustSnowflake(t, SnowflakeConfig{TimeBits: 41, NodeBits: 2, SeqBits: 2, Node: 0, StartTime: base})
	s.now = clock.Now

	// 前 5 次调用都在同一毫秒（1 次 currentMS 校验 + 4 次发号），
	// 之后每次调用前进 1ms，触发序列进位路径。
	var times []time.Time
	for i := 0; i < 8; i++ {
		times = append(times, base)
	}
	for i := 0; i < 4; i++ {
		times = append(times, base.Add(time.Duration(i+1)*time.Millisecond))
	}
	clock.setSeq(times...)

	seen := map[int64]struct{}{}
	var last int64
	for i := 0; i < 7; i++ { // 2^2 = 4/ms：同毫秒 4 个后进位
		id, err := s.Next()
		if err != nil {
			t.Fatalf("Next #%d: %v", i, err)
		}
		if id <= last {
			t.Fatalf("ID 应递增: %d -> %d", last, id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("重复 ID %d", id)
		}
		seen[id] = struct{}{}
		last = id
	}
	if len(seen) != 7 {
		t.Fatalf("应 7 个不同 ID, got %d", len(seen))
	}
}

// TestSnowflakeInvalidConfig 非法配置逐项报错。
func TestSnowflakeInvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  SnowflakeConfig
	}{
		{"位宽总和超 63", SnowflakeConfig{TimeBits: 50, NodeBits: 10, SeqBits: 12}},
		{"Node 溢出", SnowflakeConfig{TimeBits: 41, NodeBits: 5, SeqBits: 7, Node: 32}},
		{"Node 为负", SnowflakeConfig{Node: -1}},
		{"TimeBits 非法", SnowflakeConfig{TimeBits: 41, NodeBits: -1, SeqBits: 12}},
		{"纪元在未来", SnowflakeConfig{StartTime: time.Now().Add(time.Hour)}},
		{"纪元早于 1970", SnowflakeConfig{StartTime: time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewSnowflake(tc.cfg); err == nil {
				t.Fatalf("%s 应返回错误", tc.name)
			}
		})
	}
}

// TestSnowflakeMultiNode 不同节点 ID 产出互不相同（同毫秒同序列）。
func TestSnowflakeMultiNode(t *testing.T) {
	base := time.Now().Truncate(time.Millisecond)
	clock := newFakeClock(base)

	var ids []int64
	for node := int64(0); node < 4; node++ {
		s := mustSnowflake(t, SnowflakeConfig{Node: node, StartTime: base})
		s.now = clock.Now
		id, err := s.Next()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	uniq := map[int64]struct{}{}
	for _, id := range ids {
		if _, dup := uniq[id]; dup {
			t.Fatalf("不同节点产出相同 ID: %v", ids)
		}
		uniq[id] = struct{}{}
	}
}

// TestSnowflakeClockStallDoesNotBlockForever 回归测试：回拨后时钟停止推进
// 时，Next() 必须在容忍窗口内返回错误，而不是永久阻塞。
//
// 旧实现只在进入自旋前判断一次 behind（落后量），循环条件只有
// ms >= s.lastMS —— 墙钟一旦冻结就永远不成立。而且这是**持锁**自旋，
// 整个进程所有 ID 生成会被永久卡死，Next() 又没有 ctx 可取消。
func TestSnowflakeClockStallDoesNotBlockForever(t *testing.T) {
	base := time.Now().Truncate(time.Millisecond)
	clock := newFakeClock(base)
	const wait = 200 * time.Millisecond
	s := mustSnowflake(t, SnowflakeConfig{Node: 1, StartTime: base, MaxRollbackWait: wait})
	s.now = clock.Now

	if _, err := s.Next(); err != nil {
		t.Fatal(err)
	}

	// 小幅回拨（落在容忍窗口内，会进入自旋），但此后时钟不再前进。
	clock.setSeq(base.Add(-100 * time.Millisecond))

	done := make(chan error, 1)
	go func() {
		_, err := s.Next()
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("时钟停滞后应返回错误")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Next() 在时钟停滞后永久阻塞（旧实现会持锁自旋，卡死整个进程）")
	}
}

// TestSnowflakeTimestampBitsExhausted 回归测试：时间戳位宽耗尽时必须拒发，
// 而不是静默产出负数 ID。
//
// 旧实现只校验三段位宽之和 ≤ 63，没有为 ms 建立上界；ms 超过 2^TimeBits
// 后会溢出到符号位，BIGINT 主键出现负值、NextString 出现 '-'。
func TestSnowflakeTimestampBitsExhausted(t *testing.T) {
	base := time.Now().Truncate(time.Millisecond)
	clock := newFakeClock(base)
	// TimeBits=2 → 时间戳字段上界为 3ms。
	s := mustSnowflake(t, SnowflakeConfig{TimeBits: 2, NodeBits: 1, SeqBits: 1, Node: 0, StartTime: base})
	s.now = clock.Now

	// 时钟推进到上界之外。
	clock.setSeq(base.Add(4 * time.Millisecond))

	id, err := s.Next()
	if err == nil {
		t.Fatalf("时间戳位宽耗尽时应拒发，而不是产出 ID: %d（负数 ID 会污染 BIGINT 主键）", id)
	}
}

// TestSnowflakePartialBitWidths 回归测试：只给出部分位宽时，未给出的段按
// 默认值补齐，而不是把 TimeBits 静默当成 1。
//
// 旧实现里 {NodeBits: 5, SeqBits: 7} 得到的是 1+5+7=13 位 —— 时间戳只有
// 1 位，1ms 之后就无法发号；而调用方的本意是 41+5+7=53 位的
// JS 安全整数方案。
func TestSnowflakePartialBitWidths(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	s := mustSnowflake(t, SnowflakeConfig{NodeBits: 5, SeqBits: 7, Node: 3, StartTime: base})

	id, err := s.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}

	// TimeBits 被当成 1 的话，解析出的时刻会落回纪元附近，与真实时刻
	// 相差约 1 小时。
	if d := s.Parse(id).Time.Sub(time.Now()); d > time.Second || d < -time.Second {
		t.Fatalf("TimeBits 应按默认值 41 补齐, 解析时刻与当前相差 %v", d)
	}
	if got := s.Parse(id).Node; got != 3 {
		t.Fatalf("node=%d, want 3", got)
	}
	// 41+5+7=53 位：ID 必须能安全通过 JS Number。
	if id >= 1<<53 {
		t.Fatalf("53 位方案不应达到 2^53, got %d", id)
	}
}
