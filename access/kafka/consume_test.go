package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// newRaceTestGroup 构造一个消费组但不真正连 broker：
// franz-go 是懒连接的，kgo.NewClient 不会发起网络请求，
// 因此生命周期相关的竞态可以在没有 Kafka 的环境下验证。
func newRaceTestGroup(t *testing.T) *Group {
	t.Helper()

	c, err := Open(Config{Brokers: []string{"127.0.0.1:9092"}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	g, err := c.NewGroup(GroupConfig{
		Group:          "race-group",
		Topics:         []string{"race-topic"},
		Handler:        func(context.Context, *kgo.Record) error { return nil },
		DrainTimeout:   200 * time.Millisecond,
		CommitInterval: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	return g
}

// TestGroupRunCloseConcurrent 回归测试：Run 与 Close 并发不得有数据竞争。
//
// 旧实现里 cancel 是裸字段（Run 写、Close 读，且 Close 的设计用途就是
// 从另一个 goroutine 停止 Run），-race 下必然上报。
func TestGroupRunCloseConcurrent(t *testing.T) {
	const rounds = 10
	for i := 0; i < rounds; i++ {
		g := newRaceTestGroup(t)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = g.Run(context.Background())
		}()
		go func() {
			defer wg.Done()
			g.Close()
		}()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d: Run/Close 未在预期时间内结束", i)
		}
	}
}

// TestGroupCloseBeforeRun 回归测试：Close 先于 Run 调用时，Run 必须立即返回，
// 且底层客户端必须被关闭 —— 旧实现下 Close 是空操作（cancel 为 nil），
// kgo.Client 连同其后台协程永久泄漏。
func TestGroupCloseBeforeRun(t *testing.T) {
	g := newRaceTestGroup(t)
	g.Close()

	done := make(chan error, 1)
	go func() { done <- g.Run(context.Background()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close 之后的 Run 应立即成功返回, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close 先于 Run 调用后，Run 应立即返回而不是开始消费")
	}

	if !g.done.Load() {
		t.Fatal("Run 返回后 done 应置位")
	}
}

// TestGroupCloseIdempotent Close 可重复调用（含 Run 结束之后）。
func TestGroupCloseIdempotent(t *testing.T) {
	g := newRaceTestGroup(t)

	done := make(chan error, 1)
	go func() { done <- g.Run(context.Background()) }()

	g.Close()
	g.Close() // 重复调用不应 panic（底层客户端关闭由 Once 去重）
	g.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close 未让 Run 返回")
	}

	g.Close() // Run 结束之后再关一次
}
