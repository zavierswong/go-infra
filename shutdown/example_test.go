package shutdown_test

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/zavierswong/go-infra/logger"
	"github.com/zavierswong/go-infra/shutdown"
)

// ExampleNew 典型 main：HTTP + 消息消费 + 数据库连接池的统一收口。
// 关闭顺序与注册相反：http → mq → db（依赖上层资源的先停）。
func ExampleNew() {
	r := shutdown.New(shutdown.Config{
		Logger:  logger.Default(),
		Timeout: 30 * time.Second, // 总预算
	})

	var srv *http.Server
	var consumer mqConsumer
	var db dbPool

	// 注册顺序 = 依赖顺序：连接池最先注册 → 最后关闭。
	// 零参 Close() 用闭包适配 func(ctx) error 签名。
	r.Add("db-pool", func(context.Context) error { return db.Close() })
	r.Add("mq-consumer", func(context.Context) error { return consumer.Close() })
	r.AddTimeout("http-server", 15*time.Second, func(ctx context.Context) error {
		return srv.Shutdown(ctx) // 独立覆盖钩子超时
	})

	// 业务主循环：收到信号时 ctx 被取消，应尽快返回。
	if err := r.Run(func(ctx context.Context) error {
		return consumer.Run(ctx)
	}); err != nil {
		slog.Error("exit with error", "error", err)
	}
}

// 业务与资源的占位接口，示例可编译。
type mqConsumer struct{}

func (c mqConsumer) Run(ctx context.Context) error { <-ctx.Done(); return nil }
func (c mqConsumer) Close() error                  { return nil }

type dbPool struct{}

func (p dbPool) Close() error { return nil }
