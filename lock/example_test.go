package lock_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	infraredis "github.com/zavierswong/go-infra/access/redis"
	"github.com/zavierswong/go-infra/lock"
)

// 典型用法：互斥执行某段关键逻辑（如防止定时任务多实例并发跑）。
// 开启看门狗后不必为「业务可能很慢」而把 TTL 调得很大。
func Example() {
	// 直接复用 access/redis 的连接；也可以自己持有一个
	// go-redis 的 UniversalClient。
	var rdb goredis.UniversalClient
	if rds, err := infraredis.Open(infraredis.Config{
		Addr: "127.0.0.1:6379",
		Name: "cache",
	}); err == nil {
		rdb = rds.Client()
	}

	clk := lock.New(rdb, lock.WithKeyPrefix("go-infra:lock:"))

	run := func() error {
		// ttl 10s：崩溃后锁最长残留 10s；
		// 看门狗按 ttl/3 自动续期，业务再慢也不会中途丢锁。
		l, err := clk.Acquire(context.Background(), "nightly-job", 10*time.Second)
		switch {
		case errors.Is(err, lock.ErrLocked):
			return errors.New("another instance is running")
		case err != nil:
			return err
		}
		// 解锁用独立 context：请求 ctx 此刻可能已取消。
		defer l.Unlock(context.Background())

		select {
		case <-l.Lost():
			// 可选：业务中途感知锁丢失（续期失败），尽快中止。
			return errors.New("lock lost, aborting")
		default:
		}

		fmt.Println(l.Key(), l.Token() != "")
		return nil
	}
	_ = run
}
