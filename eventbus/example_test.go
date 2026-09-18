package eventbus_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/zavierswong/go-infra/eventbus"
	"github.com/zavierswong/go-infra/logger"
)

// topic 与 payload 类型成对定义在使用方包里，发布/订阅双方共享。
type orderPaid struct {
	OrderID string
	Amount  int64
}

// ExampleNew 同步发布：订阅者错误回传给发布方。
func ExampleNew() {
	b := eventbus.New(eventbus.Config{Logger: logger.Default()})
	defer func() { _ = b.Close(context.Background()) }()

	cancel, _ := b.Subscribe("order.paid", func(_ context.Context, e eventbus.Event) error {
		o := e.Payload.(orderPaid)
		fmt.Println("send sms for order", o.OrderID)
		return nil
	})
	defer cancel()

	err := b.Publish(context.Background(), "order.paid", orderPaid{OrderID: "A1", Amount: 9900})
	if err != nil {
		// errors.Join 汇总了全部订阅者的错误。
		fmt.Println("handlers failed:", err)
	}
}

// ExampleBus_PublishAsync 异步发布：发完就走，错误只写日志。
func ExampleBus_PublishAsync() {
	b := eventbus.New(eventbus.Config{Logger: logger.Default()})
	defer func() { _ = b.Close(context.Background()) }()

	_, _ = b.Subscribe("audit.log", func(context.Context, eventbus.Event) error {
		return errors.New("audit sink down") // 只会进日志，不影响发布方
	})

	ok := b.PublishAsync("audit.log", orderPaid{OrderID: "A2"})
	// ok == false 表示队列满被丢弃（可用 b.Dropped() 观测丢失量）。
	_ = ok
	time.Sleep(10 * time.Millisecond) // 示例里等一下 worker
}
