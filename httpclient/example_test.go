package httpclient_test

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/zavierswong/go-infra/breaker"
	"github.com/zavierswong/go-infra/httpclient"
	"github.com/zavierswong/go-infra/logger"
)

// ExampleNew 最小用法：超时 + 重试 + 日志注入。
func ExampleNew() {
	// 与 go-infra/logger 的全局底座打通：注入 logger.Default()。
	c := httpclient.New(httpclient.Config{
		Name:       "payment",
		Timeout:    5 * time.Second,
		MaxRetries: 2,
		Logger:     logger.Default(),
	})

	_ = c
}

// ExampleNew_withBreaker 带熔断的完整用法。
func ExampleNew_withBreaker() {
	br := breaker.New(breaker.Config{
		Name:             "payment-api",
		FailureThreshold: 10,               // 窗口内 10 次失败即熔断
		OpenTimeout:      30 * time.Second, // 冷却 30s 后半开探测
	})
	c := httpclient.New(httpclient.Config{
		Name:       "payment",
		Timeout:    5 * time.Second,
		MaxRetries: 2,
		Breaker:    br,
		Logger:     slog.Default(),
	})

	resp, err := c.Get("https://api.example.com/charge")
	if err != nil {
		// err 可能是 breaker.ErrOpen（熔断）——用 errors.Is 区分降级路径。
		fmt.Println("charge failed:", err)
		return
	}
	defer resp.Body.Close()
}

// ExampleNew_customTransport 注入自定义 Transport（加认证头）。
func ExampleNew_customTransport() {
	auth := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		req.Header.Set("Authorization", "Bearer "+tokenFromEnv())
		return http.DefaultTransport.RoundTrip(req)
	})

	c := httpclient.New(httpclient.Config{
		Name:      "github",
		Transport: auth, // 治理层（重试/日志）会包在它外面
	})
	_ = c
}

func tokenFromEnv() string { return "" }

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
