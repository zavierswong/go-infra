package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/zavierswong/go-infra/metrics"
)

// 集成测试需要真实 Kafka。
//
//	# 一键起单机 Kafka（KRaft，无需 ZooKeeper）：
//	docker run -d --name kafka -p 9092:9092 \
//	  -e KAFKA_NODE_ID=1 \
//	  -e KAFKA_PROCESS_ROLES=broker,controller \
//	  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
//	  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 \
//	  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
//	  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 \
//	  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
//	  apache/kafka:latest
//
// 然后：KAFKA_TEST_BROKERS=127.0.0.1:9092 go test ./access/kafka/ -race -run Integration
//
// 未设置 KAFKA_TEST_BROKERS 时全部跳过，不污染 CI。

func integrationBrokers(t *testing.T) []string {
	t.Helper()
	addr := os.Getenv("KAFKA_TEST_BROKERS")
	if addr == "" {
		t.Skip("未设置 KAFKA_TEST_BROKERS，跳过集成测试")
	}
	return []string{addr}
}

func openTestClient(t *testing.T) *Client {
	t.Helper()
	c, err := Open(Config{
		Brokers: integrationBrokers(t),
		Name:    "kafka-it",
	})
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func uniqueTopic(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("go-infra-it-%d", time.Now().UnixNano())
}

// IntegrationProduceConsumeRoundTrip：声明主题 → 同步投递 → 消费组收齐 → 优雅退出。
func TestIntegrationProduceConsumeRoundTrip(t *testing.T) {
	c := openTestClient(t)
	topic := uniqueTopic(t)
	if err := c.EnsureTopic(context.Background(), TopicSpec{Topic: topic, Partitions: 3}); err != nil {
		t.Fatalf("EnsureTopic() = %v", err)
	}

	const n = 30
	for i := 0; i < n; i++ {
		if err := c.Produce(context.Background(), topic,
			[]byte(fmt.Sprintf("msg-%d", i)), WithKey([]byte(fmt.Sprintf("k%d", i%3)))); err != nil {
			t.Fatalf("Produce(%d) = %v", i, err)
		}
	}

	var (
		mu  sync.Mutex
		got = make(map[string]int)
	)

	gctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.RunGroup(gctx, GroupConfig{
			Group:      "it-" + topic,
			Topics:     []string{topic},
			FromOldest: true,
			Handler: func(ctx context.Context, rec *kgo.Record) error {
				mu.Lock()
				got[string(rec.Value)]++
				done := len(got) >= n
				mu.Unlock()
				if done {
					cancel()
				}
				return nil
			},
		})
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("RunGroup() = %v", err)
		}
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatalf("30s 内未收齐 %d 条（实收 %d）", n, len(got))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != n {
		t.Errorf("实收 %d 种消息, 期望 %d", len(got), n)
	}
}

// IntegrationProduceBatch：批量投递 + 事件计数。
func TestIntegrationProduceBatch(t *testing.T) {
	var publish, consume atomic.Int64
	c, err := Open(Config{
		Brokers: integrationBrokers(t),
		Name:    "kafka-it-batch",
		Observer: metrics.ObserverFunc(func(e metrics.Event) {
			switch e.Op {
			case metrics.OpPublish:
				publish.Add(1)
			case metrics.OpConsume:
				consume.Add(1)
			}
		}),
	})
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	topic := uniqueTopic(t)
	if err := c.EnsureTopic(context.Background(), TopicSpec{Topic: topic}); err != nil {
		t.Fatalf("EnsureTopic() = %v", err)
	}

	bodies := make([][]byte, 100)
	for i := range bodies {
		bodies[i] = []byte(fmt.Sprintf("batch-%d", i))
	}
	if err := c.ProduceBatch(context.Background(), topic, bodies); err != nil {
		t.Fatalf("ProduceBatch() = %v", err)
	}

	// ProduceBatch 整批只产生 1 个 publish 事件 —— 这是它的口径约定。
	if publish.Load() != 1 {
		t.Errorf("publish 事件 = %d, 期望 1", publish.Load())
	}

	st := c.Status()
	if st.ProduceBufferedRecords != 0 {
		t.Errorf("批量确认后缓冲应为 0, got %d", st.ProduceBufferedRecords)
	}
}

// IntegrationHandlerError：handler 失败的记录不标记位移，最终提交不上移。
// 这里只验证语义闭环（错误被上报、退出干净），重投行为由消费组协议保证。
func TestIntegrationHandlerError(t *testing.T) {
	c := openTestClient(t)
	topic := uniqueTopic(t)
	if err := c.EnsureTopic(context.Background(), TopicSpec{Topic: topic}); err != nil {
		t.Fatalf("EnsureTopic() = %v", err)
	}

	var errs []error
	if err := c.Produce(context.Background(), topic, []byte("x")); err != nil {
		t.Fatalf("Produce() = %v", err)
	}

	gctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var reported atomic.Int64
	_ = c.RunGroup(gctx, GroupConfig{
		Group:      "it-err-" + topic,
		Topics:     []string{topic},
		FromOldest: true,
		OnError: func(err error) {
			reported.Add(1)
			errs = append(errs, err)
		},
		Handler: func(ctx context.Context, rec *kgo.Record) error {
			cancel() // 处理一次即退出
			return errors.New("boom")
		},
	})

	if reported.Load() == 0 {
		t.Error("handler 失败应触发 OnError")
	}
}

// IntegrationConcurrentConsume：Concurrency > 1 时收齐全部消息。
func TestIntegrationConcurrentConsume(t *testing.T) {
	c := openTestClient(t)
	topic := uniqueTopic(t)
	if err := c.EnsureTopic(context.Background(), TopicSpec{Topic: topic, Partitions: 6}); err != nil {
		t.Fatalf("EnsureTopic() = %v", err)
	}

	const n = 200
	bodies := make([][]byte, n)
	for i := range bodies {
		bodies[i] = []byte(fmt.Sprintf("c-%d", i))
	}
	if err := c.ProduceBatch(context.Background(), topic, bodies); err != nil {
		t.Fatalf("ProduceBatch() = %v", err)
	}

	var seen atomic.Int64
	gctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.RunGroup(gctx, GroupConfig{
			Group:       "it-cc-" + topic,
			Topics:      []string{topic},
			FromOldest:  true,
			Concurrency: 8,
			Handler: func(ctx context.Context, rec *kgo.Record) error {
				if seen.Add(1) == n {
					cancel()
				}
				return nil
			},
		})
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("RunGroup() = %v", err)
		}
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatalf("30s 内未并发收齐 %d 条（实收 %d）", n, seen.Load())
	}
	if got := seen.Load(); got < n {
		t.Errorf("实收 %d 条, 期望 %d", got, n)
	}
}
