package cache_test

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zavierswong/go-infra/cache"
)

// 仅编译不执行（依赖真实 Redis）：两级缓存标准用法。
func ExampleGetOrLoad() {
	// 复用 access/redis 的连接亦可：cli := rdb.Client()
	cli := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer cli.Close() //nolint:errcheck // 示例

	m := cache.NewMulti(cli, cache.MultiConfig{
		Local:      cache.LocalConfig{Size: 10000, TTL: 30 * time.Second},
		Redis:      cache.RedisConfig{KeyPrefix: "myapp:cache:"},
		FailClosed: false, // Redis 故障时降级为未命中，可用性优先
		Logger:     slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	defer m.Close()

	ctx := context.Background()

	// 泛型读取：L1 → L2 → loader，loader 至多被一个调用方执行。
	user, err := cache.GetOrLoad(ctx, m, "user:42", 5*time.Minute,
		func(ctx context.Context) (*userProfile, error) {
			return loadUserProfileFromDB(ctx, 42)
		})
	if err != nil {
		return
	}
	_ = user

	// 数据更新后主动失效两级。
	_ = m.Invalidate(ctx, "user:42")
}

type userProfile struct {
	ID   int64
	Name string
}

func loadUserProfileFromDB(ctx context.Context, id int64) (*userProfile, error) {
	return &userProfile{ID: id, Name: "example"}, nil
}
