package ratelimit_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zavierswong/go-infra/ratelimit"
)

// 本地令牌桶：单实例保护某个内部接口。
func ExampleLocalLimiter() {
	rl := ratelimit.NewLocal(ratelimit.LocalConfig{
		Rate:  100, // 平均每秒 100 次
		Burst: 200, // 允许 200 的瞬时突发
	})

	ok, _ := rl.Allow(context.Background(), "search-api")
	fmt.Println(ok)
	// Output:
	// true
}

// Redis 滑动窗口：跨实例的每用户配额。
// 无 Output：示例含网络依赖，只编译不执行（见 redis_test 的 miniredis 单测）。
func ExampleRedisLimiter() {
	var rdb *redis.Client

	rl := ratelimit.NewRedis(rdb, ratelimit.RedisConfig{
		Window: time.Minute,
		Limit:  60, // 每用户每分钟 60 次
		Prefix: "myapp:rl:",
	})

	ok, err := rl.Allow(context.Background(), "user:42")
	switch {
	case err != nil:
		// Redis 挂了：这里选择放行（fail-open），也可以选择拒绝（fail-close）。
		ok = true
	case !ok:
		// 配额满：拒绝并提示。
		_ = errors.New("quota exceeded")
	}
	_ = ok
}
