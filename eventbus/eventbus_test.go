package eventbus_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zavierswong/go-infra/eventbus"
)

// TestPublishSync 同步发布：多订阅者按注册顺序收到事件。
func TestPublishSync(t *testing.T) {
	b := eventbus.New(eventbus.Config{})
	defer func() { _ = b.Close(context.Background()) }()

	var got []string
	mu := sync.Mutex{}
	record := func(name string) eventbus.Handler {
		return func(_ context.Context, e eventbus.Event) error {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, fmt.Sprintf("%s:%v", name, e.Payload))
			return nil
		}
	}

	if _, err := b.Subscribe("order.paid", record("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Subscribe("order.paid", record("b")); err != nil {
		t.Fatal(err)
	}

	if err := b.Publish(context.Background(), "order.paid", 42); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(got) != 2 || got[0] != "a:42" || got[1] != "b:42" {
		t.Fatalf("订阅者应按注册顺序收到事件, got %v", got)
	}
}

// TestPublishErrorJoin 订阅者错误被收集，且不阻断其他订阅者。
func TestPublishErrorJoin(t *testing.T) {
	b := eventbus.New(eventbus.Config{})
	defer func() { _ = b.Close(context.Background()) }()

	var called atomic.Bool
	boom := errors.New("boom")
	_, _ = b.Subscribe("t", func(context.Context, eventbus.Event) error { return boom })
	_, _ = b.Subscribe("t", func(context.Context, eventbus.Event) error {
		called.Store(true)
		return nil
	})

	err := b.Publish(context.Background(), "t", nil)
	if !errors.Is(err, boom) {
		t.Fatalf("错误应被汇总返回, got %v", err)
	}
	if !called.Load() {
		t.Fatal("一个订阅者报错不应阻断其他订阅者")
	}
}

// TestPanicRecovery 订阅者 panic 被转成错误，不炸发布方。
func TestPanicRecovery(t *testing.T) {
	b := eventbus.New(eventbus.Config{})
	defer func() { _ = b.Close(context.Background()) }()

	_, _ = b.Subscribe("t", func(context.Context, eventbus.Event) error {
		panic("handler bug")
	})

	err := b.Publish(context.Background(), "t", nil)
	if err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("panic 应转为错误返回, got %v", err)
	}
}

// TestUnsubscribe 退订后不再收到事件。
func TestUnsubscribe(t *testing.T) {
	b := eventbus.New(eventbus.Config{})
	defer func() { _ = b.Close(context.Background()) }()

	var n atomic.Int32
	cancel, err := b.Subscribe("t", func(context.Context, eventbus.Event) error {
		n.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), "t", nil); err != nil {
		t.Fatal(err)
	}
	cancel()
	cancel() // 幂等
	if err := b.Publish(context.Background(), "t", nil); err != nil {
		t.Fatal(err)
	}
	if got := n.Load(); got != 1 {
		t.Fatalf("退订后不应再收到事件, n=%d", got)
	}
	if topics := b.Topics(); len(topics) != 0 {
		t.Fatalf("无订阅者的 topic 应被清理, got %v", topics)
	}
}

// TestPublishAsync 异步事件最终被消费。
func TestPublishAsync(t *testing.T) {
	b := eventbus.New(eventbus.Config{})
	defer func() { _ = b.Close(context.Background()) }()

	ch := make(chan eventbus.Event, 1)
	_, err := b.Subscribe("t", func(_ context.Context, e eventbus.Event) error {
		ch <- e
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if !b.PublishAsync("t", "hello") {
		t.Fatal("入队应成功")
	}
	select {
	case e := <-ch:
		if e.Payload != "hello" {
			t.Fatalf("payload = %v", e.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("异步事件未被消费")
	}
}

// TestAsyncDropOnFullQueue 队列满时丢弃并计数，不阻塞发布方。
func TestAsyncDropOnFullQueue(t *testing.T) {
	b := eventbus.New(eventbus.Config{AsyncQueue: 1, AsyncWorkers: 1})
	defer func() { _ = b.Close(context.Background()) }()

	release := make(chan struct{})
	var processed atomic.Int32
	_, err := b.Subscribe("t", func(context.Context, eventbus.Event) error {
		processed.Add(1)
		<-release // 卡住唯一 worker
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	b.PublishAsync("t", 1) // 占用 worker
	time.Sleep(50 * time.Millisecond)
	b.PublishAsync("t", 2) // 占满队列（cap=1）
	time.Sleep(50 * time.Millisecond)
	if b.PublishAsync("t", 3) {
		t.Fatal("队列满应返回 false")
	}
	if b.Dropped() == 0 {
		t.Fatal("丢弃应被计数")
	}
	close(release)
}

// TestClose 排空已入队事件；关闭后发布报 ErrClosed。
func TestClose(t *testing.T) {
	b := eventbus.New(eventbus.Config{})

	var processed atomic.Int32
	_, err := b.Subscribe("t", func(context.Context, eventbus.Event) error {
		time.Sleep(20 * time.Millisecond) // 模拟慢消费
		processed.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	b.PublishAsync("t", 1)
	b.PublishAsync("t", 2)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := processed.Load(); got != 2 {
		t.Fatalf("Close 应排空已入队事件, processed=%d", got)
	}

	if err := b.Publish(context.Background(), "t", nil); !errors.Is(err, eventbus.ErrClosed) {
		t.Fatalf("关闭后 Publish 应返回 ErrClosed, got %v", err)
	}
	if b.PublishAsync("t", nil) {
		t.Fatal("关闭后 PublishAsync 应返回 false")
	}
	if _, err := b.Subscribe("t", func(context.Context, eventbus.Event) error { return nil }); !errors.Is(err, eventbus.ErrClosed) {
		t.Fatalf("关闭后 Subscribe 应返回 ErrClosed, got %v", err)
	}
}

// TestPublishNoSubscriber 无订阅者发布是 no-op。
func TestPublishNoSubscriber(t *testing.T) {
	b := eventbus.New(eventbus.Config{})
	defer func() { _ = b.Close(context.Background()) }()

	if err := b.Publish(context.Background(), "nobody", 1); err != nil {
		t.Fatalf("无订阅者发布不应报错, got %v", err)
	}
	if !b.PublishAsync("nobody", 1) {
		t.Fatal("无订阅者异步发布仍应成功入队")
	}
}

// TestCloseIdempotent Close 幂等。
func TestCloseIdempotent(t *testing.T) {
	b := eventbus.New(eventbus.Config{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close 应幂等, got %v", err)
	}
}
