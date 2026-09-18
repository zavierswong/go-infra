package tracing_test

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/zavierswong/go-infra/logger"
	infratrace "github.com/zavierswong/go-infra/tracing"
)

// 初始化：进程启动时调用一次，shutdown 注册进退出钩子。
func ExampleInit() {
	ctx := context.Background()

	shutdown, err := infratrace.Init(ctx, infratrace.Config{
		ServiceName:    "order-service",
		ServiceVersion: "1.2.0",
		Environment:    "prod",
		Endpoint:       "otel-collector:4317",
		Protocol:       "grpc", // 或 "http"（对应 Collector :4318）
		Insecure:       true,   // Collector 未挂 TLS 时必须
		SamplingRatio:  0.1,    // 根 span 按 10% 采样，上游已采样则全链路跟随
	})
	if err != nil {
		panic(err)
	}

	go func() {
		defer shutdown(context.Background())
	}()
	// 真实接法：把 shutdown 注册进 shutdown/ 包的钩子链
	//   shutdown.OnShutdown(shutdownFn)
	// 或在收到退出信号后调用：
	//   _ = shutdown(context.Background())

	// shutdown 已绑定上方 goroutine，仅为编译演示。
	_ = time.Second
}

// 日志-链路自动关联：包装 slog Handler 后，所有日志在 span 内
// 自动携带 trace_id / span_id，无需手工 logger.WithTraceID。
func ExampleNewLogHandler() {
	if err := logger.Init(logger.Config{}); err != nil {
		panic(err)
	}
	base := logger.Default().Handler()

	// 未初始化 tracing 时包装是纯透传 —— 可以在接线前就先上这层。
	var handler slog.Handler = infratrace.NewLogHandler(base)
	slog.New(handler).Info("所有日志自动带 trace_id（若当前存在 span）")
}

// HTTP 进出口：服务端中间件 + 客户端 RoundTripper 组成完整链路。
func ExampleHTTPMiddleware() {
	mux := http.NewServeMux()
	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		// 任意位置从 ctx 取当前 span 记录业务事件
		span := infratrace.SpanFromContext(r.Context())
		span.AddEvent("order-listed")
		w.WriteHeader(http.StatusOK)
	})

	mw := infratrace.HTTPMiddleware("") // span 名取 r.Pattern 路由模板
	server := &http.Server{
		Addr:    ":8080",
		Handler: mw(mux),
	}
	_ = server

	client := &http.Client{Transport: infratrace.NewRoundTripper(http.DefaultTransport)}
	_ = client
}

// MQ 等自定义载体：手工注入 / 提取上下文。
// amqp091 的 Delivery.Headers 是 map[string]any，
// franz-go 的 RecordHeaders 是 []kgo.RecordHeader{Key string, Value []byte}，
// 都先转成 map[string]string 即可复用同一对函数。
func ExampleInjectMap() {
	// 生产端（publish 前）：
	//   ctx, span := tracer.Start(ctx, "produce order.created")
	//   defer span.End()
	//   carrier := map[string]string{}
	//   tracing.InjectMap(ctx, carrier)
	//   headers := amqp.Table{}
	//   for k, v := range carrier { headers[k] = v }

	// 消费端（处理前）：
	//   carrier := map[string]string{}
	//   for k, v := range delivery.Headers {
	//       if s, ok := v.(string); ok { carrier[k] = s }
	//   }
	//   ctx := tracing.ExtractMap(context.Background(), carrier)
	//   _, span := tracer.Start(ctx, "consume order.created")
	//   defer span.End()
	_ = infratrace.InjectMap
	_ = infratrace.ExtractMap
}
