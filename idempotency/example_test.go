package idempotency_test

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"

	"github.com/zavierswong/go-infra/idempotency"
)

// 仅编译不执行（依赖真实 Redis）：MQ 消费去重标准用法。
func ExampleDo() {
	cli := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer cli.Close() //nolint:errcheck // 示例

	p, err := idempotency.New(cli, idempotency.Config{
		// KeyPrefix:   "myapp:idem:", // 默认 "idem:"
		// ResultTTL:   24 * time.Hour, // 幂等窗口：结果保留多久
	})
	if err != nil {
		return
	}

	// RabbitMQ at-least-once 重投 / 重复消费时，同一 msgID 的处理
	// 在结果窗口内只会真正执行一次，其余调用拿到缓存结果（Replayed=true）。
	res, err := idempotency.Do(context.Background(), p, "order-paid:"+msgID(), func(ctx context.Context) (bool, error) {
		return handleOrderPaid(ctx) // 真正的业务副作用，只应发生一次
	})
	if err != nil {
		// 失败不缓存：nack 重投后会重新执行。
		return
	}
	_ = res.Replayed // true 表示此前已成功处理过，直接复用结果
}

func msgID() string { return "example" }
func handleOrderPaid(ctx context.Context) (bool, error) {
	return true, errors.New("not implemented")
}
