package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConfig 二级缓存（Redis）配置。
type RedisConfig struct {
	// KeyPrefix 是所有缓存 key 的统一前缀，用于多应用共享 Redis 时隔离命名空间。
	KeyPrefix string

	// TTL 是 Set 未显式指定过期时间时的默认 TTL。
	TTL time.Duration
}

// Redis 是二级缓存适配器：原始字节值 + TTL。
//
// 仅做薄封装（前缀拼接、nil 值跳过），错误原样上抛，
// fail-open / fail-close 的取舍由上层（Multi 或调用方）决定。
type Redis struct {
	cli    redis.UniversalClient
	prefix string
	ttl    time.Duration
}

// NewRedis 创建二级缓存。cli 可复用 access/redis 的底层连接
// （其 Client() 返回值即 *redis.Client，满足 UniversalClient）。
//
// cli 为 nil 时 panic：这是构造期就能确定的编程错误，与其等到
// 第一次 Get/Set 在 nil 接口上解引用（错误信息晦涩），不如立刻失败。
func NewRedis(cli redis.UniversalClient, cfg RedisConfig) *Redis {
	if cli == nil {
		panic("cache: NewRedis 需要非 nil 的 redis.UniversalClient")
	}
	return &Redis{cli: cli, prefix: cfg.KeyPrefix, ttl: cfg.TTL}
}

// Get 读取原始字节。未命中返回 (nil, false, nil)；
// Redis 故障返回 err，是否当作未命中由调用方决定。
func (r *Redis) Get(ctx context.Context, key string) ([]byte, bool, error) {
	b, err := r.cli.Get(ctx, r.prefix+key).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// Set 写入原始字节。ttl == 0 时使用配置的默认 TTL（仍为 0 则永不过期）；
// ttl < 0 是调用方算错了（例如 time.Until 一个已过去的时间点），
// 直接报错 —— 交给 Redis 会得到 "invalid expire time" 且上层只记一行日志，
// 缓存会静默失效。
func (r *Redis) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if ttl == 0 {
		ttl = r.ttl
	}
	if ttl < 0 {
		return fmt.Errorf("cache: 非法 ttl %v（key=%q）：负值通常是 time.Until 传了过去的时间点", ttl, key)
	}
	return r.cli.Set(ctx, r.prefix+key, val, ttl).Err()
}

// Delete 删除条目（不存在时静默）。
func (r *Redis) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = r.prefix + k
	}
	return r.cli.Del(ctx, full...).Err()
}
