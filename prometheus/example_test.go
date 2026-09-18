package prometheus_test

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/zavierswong/go-infra/access/kafka"
	"github.com/zavierswong/go-infra/access/mysql"
	"github.com/zavierswong/go-infra/access/rabbitmq"
	"github.com/zavierswong/go-infra/access/redis"
	"github.com/zavierswong/go-infra/metrics"
	infraprom "github.com/zavierswong/go-infra/prometheus"
)

// 把 Exporter 接进 mysql / redis / rabbitmq 三个组件：
// 事件通道用 Observer，池水位与探活用注册函数，
// 最后挂到 /metrics 上被 Prometheus 抓取。
func ExampleExporter() {
	pw := infraprom.New()
	defer pw.Close()

	// pw 自身实现 prometheus.Collector，注册进自己的 Registry。
	reg := prometheus.NewRegistry()
	reg.MustRegister(pw)

	db, err := mysql.Open(mysql.Config{
		Dsn:      "user:pass@tcp(127.0.0.1:3306)/order?parseTime=true",
		Name:     "order",
		Observer: pw, // 事件通道：直方图 + 错误计数
	})
	if err != nil {
		return
	}
	defer db.Close()

	pw.RegisterPool(db.PoolStats) // 池水位：scrape 时现取
	// 探活：后台协程周期执行，结果落在 component_up。
	pw.RegisterHealth(metrics.ComponentMySQL, "order", db.HealthCheck)

	cache, err := redis.Open(redis.Config{
		Addr:     "127.0.0.1:6379",
		Name:     "cache",
		Observer: pw,
	})
	if err != nil {
		return
	}
	defer cache.Close()

	pw.RegisterPool(cache.PoolStats)
	pw.RegisterHealth(metrics.ComponentRedis, "cache", cache.HealthCheck)

	mq, err := rabbitmq.Open(rabbitmq.Config{
		Host:     "127.0.0.1",
		Port:     5672,
		Username: "app",
		Password: "pass",
		VHost:    "app_vhost",
		Name:     "notify",
		Observer: pw,
	})
	if err != nil {
		return
	}
	defer mq.Close()

	// rabbitmq 没有 PoolStats，用 Status 补上连接与发布通道的状态。
	pw.RegisterHealth(metrics.ComponentRabbitMQ, "notify", mq.HealthCheck)
	pw.RegisterStatus(func() infraprom.MQStatus {
		s := mq.Status()
		return infraprom.MQStatus{
			Component:           metrics.ComponentRabbitMQ,
			Instance:            s.Instance,
			Closed:              s.Closed,
			Connected:           s.Connected,
			PublishChannelReady: s.PublishChannelReady,
		}
	})

	// kafka：事件走 Observer，缓冲水位走 RegisterKafkaStatus。
	// 生产端与消费组是两个客户端，各自注册一次 ——
	// Instance 标签建议带组名后缀，避免序列混叠。
	producer, err := kafka.Open(kafka.Config{
		Brokers:  []string{"127.0.0.1:9092"},
		ClientID: "demo",
		Name:     "events",
		Observer: pw,
	})
	if err != nil {
		return
	}
	defer producer.Close()

	pw.RegisterHealth(metrics.ComponentKafka, "events", producer.HealthCheck)
	pw.RegisterKafkaStatus(func() infraprom.KafkaStatus {
		s := producer.Status()
		return infraprom.KafkaStatus{
			Component:              metrics.ComponentKafka,
			Instance:               s.Instance,
			Closed:                 s.Closed,
			ProduceBufferedRecords: s.ProduceBufferedRecords,
			ProduceBufferedBytes:   s.ProduceBufferedBytes,
		}
	})

	group, err := producer.NewGroup(kafka.GroupConfig{
		Group:       "demo-consumer",
		Topics:      []string{"events"},
		Handler:     func(context.Context, *kgo.Record) error { return nil },
		Concurrency: 8,
	})
	if err != nil {
		return
	}
	pw.RegisterKafkaStatus(func() infraprom.KafkaStatus {
		s := group.Status()
		return infraprom.KafkaStatus{
			Component:            metrics.ComponentKafka,
			Instance:             s.Instance + "-consumer",
			Closed:               s.Closed,
			FetchBufferedRecords: s.FetchBufferedRecords,
			FetchBufferedBytes:   s.FetchBufferedBytes,
		}
	})
	go group.Run(context.Background())

	// 暴露给 Prometheus 抓取。
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	_ = mux
	_ = context.Background()
	_ = time.Second
}
