package breaker_test

import (
	"errors"
	"fmt"

	"github.com/zavierswong/go-infra/breaker"
)

// 用熔断器保护一次下游调用：熔断中快速失败走降级，
// 正常时结果自动上报给熔断器。
func Example() {
	b := breaker.New(breaker.Config{
		Name:              "payment",
		FailureThreshold:  3, // 窗口内 3 次失败即熔断
		Window:            0, // 零值取默认 1 分钟
		OpenTimeout:       0, // 零值取默认 30 秒
		HalfOpenMaxProbes: 1, // 半开只放 1 个探测
	})

	call := func() error { return errors.New("payment upstream timeout") }

	// 连续失败 3 次 → 熔断。
	for i := 0; i < 3; i++ {
		_ = b.Do(call)
	}

	// 熔断期间快速失败，fn 不再执行，调用方走降级。
	err := b.Do(func() error {
		fmt.Println("this line never runs")
		return nil
	})
	fmt.Println(errors.Is(err, breaker.ErrOpen), b.State())

	// Output:
	// true open
}
