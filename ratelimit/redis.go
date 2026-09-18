package ratelimit

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// 窗口脚本一次往返完成「清理 + 判定 + 记账」三步：
//
//   - ZREMRANGEBYSCORE：清掉窗口外的旧请求（滑动窗口的关键，
//     只靠 key TTL 过期会有边界突刺）；
//   - ZCARD < limit 才 ZADD 计入并放行；
//   - PEXPIRE 兜底：key 长期无写入时由 Redis 自动回收。
//
// 时间取自 **Redis 服务端**（TIME 命令）：多实例的机器时钟各不相同，
// 用客户端时间打分会出现"某台机器快 2 秒 → 它的记录提前出窗"的漏洞。
var windowScript = redis.NewScript(`
local t = redis.call("TIME")
local now = t[1] * 1000000 + t[2]
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now - tonumber(ARGV[1]))
if redis.call("ZCARD", KEYS[1]) < tonumber(ARGV[2]) then
	redis.call("ZADD", KEYS[1], now, ARGV[3])
	redis.call("PEXPIRE", KEYS[1], ARGV[4])
	return 1
end
return 0`)

// RedisConfig 是 Redis 滑动窗口限流器配置。
type RedisConfig struct {
	// Window 是窗口长度，默认 1m。
	Window time.Duration
	// Limit 是单个窗口内最多放行的请求数，必填（>0）。
	Limit int
	// Prefix 是键前缀，默认 "go-infra:rl:"。多套限流共用一个
	// Redis 时建议区分，避免互相踩配额。
	Prefix string
}

// RedisLimiter 是跨实例共享配额的滑动窗口限流器。
//
// 每次判定是一次 Lua 脚本往返（约 1 次 RTT）；对超高 QPS 的接口，
// 本地桶挡在前、Redis 窗口管总量是更常见的组合。
type RedisLimiter struct {
	cli redis.UniversalClient
	cfg RedisConfig

	// seq 保证同一微秒内多次写入的 member 仍唯一
	// （ZADD 的 member 撞车会互相覆盖，导致计数偏小）。
	seq atomic.Uint64
}

// NewRedis 创建 Redis 滑动窗口限流器。
func NewRedis(cli redis.UniversalClient, cfg RedisConfig) *RedisLimiter {
	if cfg.Window <= 0 {
		cfg.Window = time.Minute
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "go-infra:rl:"
	}
	return &RedisLimiter{cli: cli, cfg: cfg}
}

// Allow 实现 Limiter。
//
// 返回 true 表示放行（本次请求已计入窗口）；false 表示配额已满。
// Redis 网络错误原样返回 —— 是否「故障放行」由调用方决定。
func (l *RedisLimiter) Allow(ctx context.Context, key string) (bool, error) {
	now := time.Now()
	member := strconv.FormatInt(now.UnixNano(), 36) + "-" +
		strconv.FormatUint(l.seq.Add(1), 36)

	n, err := windowScript.Run(ctx, l.cli, []string{l.cfg.Prefix + key},
		l.cfg.Window.Microseconds(),
		l.cfg.Limit,
		member,
		l.cfg.Window.Milliseconds(),
	).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}
