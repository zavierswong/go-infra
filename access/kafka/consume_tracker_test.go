package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// newTestTracker 构造一个可用的 tracker。client 不连任何 broker：
// MarkCommitOffsets 只是内存操作（未配置消费组时为 no-op），
// 单测里直接读 partProgress.marked 断言水位。
func newTestTracker(t *testing.T) *groupTracker {
	t.Helper()
	cli, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	t.Cleanup(cli.Close)
	return newGroupTracker(cli)
}

func trec(topic string, part int32, offset int64) *kgo.Record {
	return &kgo.Record{Topic: topic, Partition: part, Offset: offset}
}

func markedOf(t *testing.T, tr *groupTracker, topic string, part int32) int64 {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	p := tr.parts[tpKey{topic, part}]
	if p == nil {
		return -1
	}
	return p.marked
}

// TestGroupTrackerContiguousWatermark 失败记录必须成为不可提交的栅栏：
// rec2 失败后，rec3/rec4 即使全部成功，提交水位也只能停在 2。
// 旧实现直接 MarkCommitRecords（marks 取最大 offset），提交点会越过
// rec2 → 消息永久丢失。
func TestGroupTrackerContiguousWatermark(t *testing.T) {
	tr := newTestTracker(t)

	for i := int64(0); i < 5; i++ {
		tr.dispatch(trec("t", 0, i))
	}
	tr.settle(trec("t", 0, 2), false) // 失败：栅栏
	tr.settle(trec("t", 0, 0), true)
	if got := markedOf(t, tr, "t", 0); got != 1 {
		t.Fatalf("rec0 成功后水位应为 1, got %d", got)
	}
	tr.settle(trec("t", 0, 1), true)
	if got := markedOf(t, tr, "t", 0); got != 2 {
		t.Fatalf("水位应停在栅栏 rec2, got %d", got)
	}
	// 栅栏之后的记录全部成功，水位也不得前进。
	tr.settle(trec("t", 0, 3), true)
	tr.settle(trec("t", 0, 4), true)
	if got := markedOf(t, tr, "t", 0); got != 2 {
		t.Fatalf("栅栏后的成功不得推进水位, got %d", got)
	}
}

// TestGroupTrackerOutOfOrderSettle 并发乱序完成时，水位只按连续成功段
// 前进：5/6 先完成也不能推进水位。
func TestGroupTrackerOutOfOrderSettle(t *testing.T) {
	tr := newTestTracker(t)

	for i := int64(0); i < 8; i++ {
		tr.dispatch(trec("t", 0, i))
	}
	// 乱序：先完成 7、5、6。
	tr.settle(trec("t", 0, 7), true)
	tr.settle(trec("t", 0, 5), true)
	tr.settle(trec("t", 0, 6), true)
	if got := markedOf(t, tr, "t", 0); got != 0 {
		t.Fatalf("乱序完成不得推进水位（最小在途 0 未结算）, got %d", got)
	}
	// 按序补齐 0..4 后水位一路推到 8。
	for i := int64(0); i <= 4; i++ {
		tr.settle(trec("t", 0, i), true)
	}
	if got := markedOf(t, tr, "t", 0); got != 8 {
		t.Fatalf("全部结算后水位应为 8, got %d", got)
	}
}

// TestGroupTrackerOffsetGap offset 空洞（如被驱动过滤的事务控制记录）
// 不得卡死水位。
func TestGroupTrackerOffsetGap(t *testing.T) {
	tr := newTestTracker(t)

	tr.dispatch(trec("t", 0, 0))
	tr.dispatch(trec("t", 0, 3)) // 空洞 1、2
	tr.settle(trec("t", 0, 0), true)
	if got := markedOf(t, tr, "t", 0); got != 3 {
		t.Fatalf("空洞视为已消费, 水位应到 3, got %d", got)
	}
	tr.settle(trec("t", 0, 3), true)
	if got := markedOf(t, tr, "t", 0); got != 4 {
		t.Fatalf("全部结算后水位应为 4, got %d", got)
	}
}

// TestGroupTrackerAllSuccess 无失败路径：水位随最后一条成功记录推进。
func TestGroupTrackerAllSuccess(t *testing.T) {
	tr := newTestTracker(t)

	for i := int64(0); i < 3; i++ {
		tr.dispatch(trec("t", 1, i))
		tr.settle(trec("t", 1, i), true)
	}
	if got := markedOf(t, tr, "t", 1); got != 3 {
		t.Fatalf("水位应为 3, got %d", got)
	}
}
