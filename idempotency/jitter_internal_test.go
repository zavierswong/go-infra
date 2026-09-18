package idempotency

import (
	"testing"
	"time"
)

// TestJitteredWithinRange 抖动幅度应落在 ±30% 内，并且真的产生分散的取值
// （固定间隔会让所有等待者同时醒来抢锁，即惊群）。
func TestJitteredWithinRange(t *testing.T) {
	base := 100 * time.Millisecond

	seen := make(map[time.Duration]struct{})
	for i := 0; i < 500; i++ {
		got := jittered(base)
		if got < base*7/10 || got > base*13/10 {
			t.Fatalf("抖动超出 ±30%%: %v", got)
		}
		seen[got] = struct{}{}
	}
	if len(seen) < 20 {
		t.Fatalf("抖动取值过于集中（只有 %d 种），起不到摊开唤醒时刻的作用", len(seen))
	}

	if got := jittered(0); got != 0 {
		t.Fatalf("零间隔应保持零, got %v", got)
	}
}
