package kafka_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	infrakafka "github.com/zavierswong/go-infra/access/kafka"
	"github.com/zavierswong/go-infra/metrics"
)

// 最小可用：连接、声明主题、投递一条消息。
//
// 生产约定在默认值里都已是生产级：acks=all、幂等、批压缩（snappy）、
// 30s 送达超时。	ctx 只约束"入队前"，确认由 DeliveryTimeout 兜底。
func Example_client() {
	cli, err := infrakafka.Open(infrakafka.Config{
		Brokers: []string{"127.0.0.1:9092"},
		Name:    "order", // 实例名，会成为指标标签
		Topics: []infrakafka.TopicSpec{
			{Topic: "order.created", Partitions: 3, ReplicationFactor: 3},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// 同步投递：broker 确认（acks=all）后才返回。
	// 同一键的消息进同一分区，分区内有序 —— 需要按业务实体保序就设键。
	err = cli.Produce(context.Background(), "order.created",
		[]byte(`{"order_id":"o-1","amount":99}`),
		infrakafka.WithKey([]byte("o-1")),
	)
	if err != nil {
		log.Fatal(err)
	}
}

// 消费组：处理成功自动提交位移（at-least-once，handler 需幂等）。
//
// Concurrency 把 handler 撑到任意并发度；代价是分区内顺序不再保证，
// 保序需求请保持 1 并用 WithKey 分区。
func Example_consumerGroup() {
	cli, err := infrakafka.Open(infrakafka.Config{
		Brokers: []string{"127.0.0.1:9092"},
		Name:    "notify",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	err = cli.RunGroup(context.Background(), infrakafka.GroupConfig{
		Group:       "notify-sender",
		Topics:      []string{"order.created"},
		Concurrency: 8, // 8 个协程并发处理
		Handler: func(ctx context.Context, rec *kgo.Record) error {
			fmt.Printf("order %s\n", rec.Value)
			return nil
		},
		OnError: func(err error) { log.Printf("consume: %v", err) },
	})
	if err != nil {
		log.Fatal(err)
	}
}

// 高吞吐路径：ProduceAsync 不等确认，真正的并发由客户端的
// 缓冲 + 攒批 + 每 broker 并发请求撑起；ProduceBatch 批内并行送达。
func Example_highThroughput() {
	cli, err := infrakafka.Open(infrakafka.Config{
		Brokers: []string{"127.0.0.1:9092"},
		Name:    "tracker",
		Linger:  10 * time.Millisecond, // 微批：攒 10ms 再发，请求数与压缩率双收益
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	ctx := context.Background()

	// 异步投递：立即返回，结果在 promise 里回调（IO 协程，勿阻塞）。
	cli.ProduceAsync(ctx, "page.view", []byte("pv-1"), func(r *kgo.Record, err error) {
		if err != nil {
			log.Printf("投递失败 %s: %v", r.Topic, err)
		}
	}, infrakafka.WithKey([]byte("u-42")))

	// 批量投递：全部确认后才返回，整批只产生 1 个 publish 事件。
	bodies := make([][]byte, 500)
	for i := range bodies {
		bodies[i] = []byte(fmt.Sprintf("pv-%d", i))
	}
	if err := cli.ProduceBatch(ctx, "page.view", bodies); err != nil {
		log.Printf("批量投递部分失败: %v", err)
	}
}

// 手动提交位移：与外部存储的事务对齐（DB 提交成功后再提交位移）。
func Example_manualCommit() {
	cli, err := infrakafka.Open(infrakafka.Config{
		Brokers: []string{"127.0.0.1:9092"},
		Name:    "billing",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	gctx := context.Background()
	err = cli.RunGroup(gctx, infrakafka.GroupConfig{
		Group:             "billing-settle",
		Topics:            []string{"order.created"},
		DisableAutoCommit: true, // 关闭自动提交
		Handler: func(ctx context.Context, rec *kgo.Record) error {
			// settleInDB 写库成功后位移才提交，避免"位移走了、账没落"。
			// settleInDB(ctx, rec.Value)
			return nil
		},
	})
	_ = err // 完整用法见 README「快速开始」
}

// 接入可观测性：Observer 收到事件流，翻译成 Prometheus 是适配层的职责。
// 状态快照（Status）适合做 Gauge，缓冲水位持续逼近上限就是背压信号。
func Example_metrics() {
	publishDuration := promHistogramPlaceholder()

	cli, err := infrakafka.Open(infrakafka.Config{
		Brokers: []string{"127.0.0.1:9092"},
		Name:    "order",
		Observer: metrics.ObserverFunc(func(e metrics.Event) {
			publishDuration(e)
		}),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	st := cli.Status()
	_ = st.ProduceBufferedRecords // Gauge: kafka_produce_buffered_records
	_ = st.ProduceBufferedBytes   // Gauge: kafka_produce_buffered_bytes
}

func promHistogramPlaceholder() func(metrics.Event) {
	return func(metrics.Event) {}
}

// SASL + TLS：机制支持 plain / scram-sha-256 / scram-sha-512。
// 口令从环境变量读取，不要写进配置文件。
func Example_sasl() {
	cli, err := infrakafka.Open(infrakafka.Config{
		Brokers:  []string{"kafka.example.com:9093"},
		Name:     "prod",
		ClientID: "order-service",
		TLS: infrakafka.TLSConfig{
			Enable:     true,
			ServerName: "kafka.example.com",
		},
		SASL: infrakafka.SASLConfig{
			Mechanism: "scram-sha-512",
			Username:  os.Getenv("KAFKA_USER"),
			Password:  os.Getenv("KAFKA_PASS"),
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
}
