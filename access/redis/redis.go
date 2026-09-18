// Package redis 封装 Redis 的连接与**连接池**管理。
//
// 在 github.com/redis/go-redis/v9 之上，补齐了四件容易写错的事：
//
//  1. **连接池显式可调**：暴露 PoolSize / MinIdleConns / MaxActiveConns / PoolTimeout /
//     ConnMaxIdleTime / ConnMaxLifetime 等参数，并给出合理默认值。
//     旧版把池大小写死，其中 MinIdleConns 缺失意味着流量低谷后连接被回收，
//     下一波请求要重新支付建连成本（跨可用区 / 带 TLS 时尤其明显）。
//  2. **修正重试语义**：旧版写的是 `MaxRetries: -1`，而 go-redis 里 -1
//     表示**禁用重试**（注释却写"最大重试次数"），连带退避参数一起空转。
//     现在默认 3 次，并把 -1 的含义写清楚。
//  3. **可恢复的关闭**：Close 幂等，且关闭后单例会被重置，可以重新 Open ——
//     旧版 Close 不重置全局变量，之后 Get 会返回一个已关闭的客户端。
//  4. **真实可用的日志钩子**：旧版三个 hook 的函数体全被注释掉，属于死代码；
//     这里实现了错误日志 + 慢命令告警，并正确地把 redis.Nil（key 不存在）
//     当作正常结果而不是错误。
//
// 与旧版 API 的关系：New / Get / Close 仅为平滑迁移保留并已标记 Deprecated。
// 新代码请使用 Open，返回的 *RDS 由调用方持有，支持多实例。
//
// 命名提醒：本包与 go-redis 的包名都叫 redis。同时需要引用 go-redis 的类型或
// 哨兵错误时请起别名：
//
//	infraredis "github.com/zavierswong/go-infra/access/redis"
//	goredis    "github.com/redis/go-redis/v9"
package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/zavierswong/go-infra/logger"
	"github.com/zavierswong/go-infra/metrics"
)

// RDS 是一个 Redis 客户端。
//
// 所有导出方法都可并发调用。它只是**生命周期与连接池的持有者**，
// 业务命令请通过 Client 拿到原生 go-redis 客户端后调用 ——
// 这样本包不需要跟着 go-redis 的版本逐个转发命令，也不会限制使用者的能力。
type RDS struct {
	cfg Config
	log *logger.Plog

	mu     sync.RWMutex
	client *goredis.Client
	closed bool
}

// Open 创建客户端并验证连通性。
//
// 返回的 *RDS 必须由调用方负责 Close。
func Open(cfg Config) (*RDS, error) {
	cfg, err := cfg.ready()
	if err != nil {
		return nil, err
	}
	opts, err := cfg.options()
	if err != nil {
		return nil, err
	}

	r := &RDS{cfg: cfg, log: logger.NewPlog("Redis")}

	client := goredis.NewClient(opts)
	client.AddHook(&loggingHook{
		log:  r.log,
		slow: cfg.LogSlowThreshold,
		all:  cfg.LogCommands,
	})

	// 没配 Observer 就不注册 metrics 钩子。钩子即使什么都不做，
	// 每条命令仍要多一次闭包调用与一次计时 —— 热路径上的常数越小越好。
	//
	// 注册顺序是有讲究的：go-redis 的 hooksState.rebuild 按倒序遍历 hooks 逐个包裹，
	// 所以**先注册的在最外层**。日志钩子先注册 → 它在最外层，metrics 钩子在内层，
	// 于是 metrics 量到的是不含日志开销的真实命令耗时。
	// 反过来的话，"慢命令日志"本身的格式化成本会计进指标里，那是自欺欺人。
	if cfg.Observer != nil {
		client.AddHook(&metricsHook{
			obs:  metrics.OrNop(cfg.Observer),
			name: cfg.Name,
			slow: cfg.LogSlowThreshold,
		})
	}

	// 启动即验证：连不上直接返回错误，不做"假装成功"。
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout+cfg.ReadTimeout)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("%w: 连通性检查失败: %v", ErrConnect, err)
	}

	r.client = client
	r.log.Infof(ctx,
		"Redis 已连接: addr=%s db=%d 池大小=%d 最小空闲=%d 最大连接=%d 重试=%d",
		cfg.Addr, cfg.DB, cfg.PoolSize, cfg.MinIdleConns, cfg.MaxActiveConns, cfg.MaxRetries)
	return r, nil
}

// options 把 Config 翻译成 go-redis 的 Options。
//
// 单独抽出来是为了**可测**：这个映射一旦漏掉某个字段（比如忘了传 MaxRetries），
// 配置就会静默失效，而那种问题在集成测试里很难被发现。
// TestOptionsMapping 会逐字段断言这里的输出。
func (c Config) options() (*goredis.Options, error) {
	tlsCfg, err := c.tlsConfig()
	if err != nil {
		return nil, err
	}

	return &goredis.Options{
		Addr:       c.Addr,
		Username:   c.Username,
		Password:   c.Password,
		DB:         c.DB,
		ClientName: c.ClientName,
		Protocol:   c.Protocol,

		// 连接池
		PoolSize:              c.PoolSize,
		MinIdleConns:          c.MinIdleConns,
		MaxIdleConns:          c.MaxIdleConns,
		MaxActiveConns:        c.MaxActiveConns,
		PoolTimeout:           c.PoolTimeout,
		PoolFIFO:              c.PoolFIFO,
		ConnMaxIdleTime:       c.ConnMaxIdleTime,
		ConnMaxLifetime:       c.ConnMaxLifetime,
		ConnMaxLifetimeJitter: c.ConnMaxLifetimeJitter,

		// 超时
		DialTimeout:           c.DialTimeout,
		ReadTimeout:           c.ReadTimeout,
		WriteTimeout:          c.WriteTimeout,
		ContextTimeoutEnabled: !c.DisableContextTimeout,

		// 重试
		MaxRetries:      c.MaxRetries,
		MinRetryBackoff: c.MinRetryBackoff,
		MaxRetryBackoff: c.MaxRetryBackoff,

		TLSConfig: tlsCfg,
	}, nil
}

// Client 返回原生 go-redis 客户端，业务命令都通过它执行。
// 客户端已关闭时返回 nil。
func (r *RDS) Client() *goredis.Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.client
}

// Config 返回生效后的配置（已补齐默认值）。
func (r *RDS) Config() Config { return r.cfg }

// Closed 报告客户端是否已关闭。
func (r *RDS) Closed() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.closed
}

// HealthCheck 探活，可用于健康检查接口。
func (r *RDS) HealthCheck(ctx context.Context) error {
	client, closed := r.snapshot()
	if closed {
		return ErrClosed
	}
	if client == nil {
		return ErrNotConnected
	}
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("%w: %v", ErrConnect, err)
	}
	return nil
}

// Stats 返回连接池统计，可直接接入 Prometheus 做监控。
//
// 常看的几个指标：
//   - Hits / Misses：命中池 vs 需要新建连接。**Misses 持续增长说明 MinIdleConns/PoolSize 偏小**；
//   - Timeouts：因池满而等待超时的次数，大于 0 说明 MaxActiveConns/PoolTimeout 需要调整；
//   - TotalConns / IdleConns / StaleConns：总量、空闲量、被回收量。
func (r *RDS) Stats() *goredis.PoolStats {
	client, _ := r.snapshot()
	if client == nil {
		return nil
	}
	return client.PoolStats()
}

// Close 关闭客户端。可重复调用，第二次起直接返回 nil。
//
// 修复了旧版的问题：旧版只关闭底层客户端，不重置全局变量，
// 于是 Close 之后 Get 会返回一个已关闭的客户端，且 New 因为"已存在"而直接返回 nil，
// 整个进程无法再恢复 Redis 能力。
func (r *RDS) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	client := r.client
	r.client = nil
	r.mu.Unlock()

	if client == nil {
		return nil
	}
	err := client.Close()
	r.log.Infof(context.Background(), "Redis 连接已关闭")
	if err != nil {
		return fmt.Errorf("关闭 Redis 客户端失败: %w", err)
	}
	return nil
}

// snapshot 一次性取出 client 与 closed，避免调用方分两次读锁拿到不一致的状态。
func (r *RDS) snapshot() (*goredis.Client, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.client, r.closed
}

// loggingHook 把 Redis 的连接与命令事件接到 go-infra/logger。
//
// 旧版的三个 hook 全是空壳（逻辑被注释掉），其中 ProcessPipelineHook 还会
// 每条 pipeline 无条件分配一个 []string 并逐条调 Name()，却从不使用 ——
// 纯粹的性能损耗。这里按需输出：默认只记失败的命令，成本接近零。
type loggingHook struct {
	log  *logger.Plog
	slow time.Duration // >0 时把超过该阈值的命令记为慢命令
	all  bool          // true 时记录每一条命令（Debug 级别）
}

var _ goredis.Hook = (*loggingHook)(nil)

// isRealErr 判断是否为"值得记录"的错误。
//
// goredis.Nil 表示 key 不存在，是正常的业务结果（缓存未命中），
// 把它当错误打日志会让告警被噪音淹没 —— 这是很多 Redis 封装都会犯的错。
func isRealErr(err error) bool {
	return err != nil && !errors.Is(err, goredis.Nil)
}

func (h *loggingHook) DialHook(next goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := next(ctx, network, addr)
		// 这里刻意用 Debug 而不是 Warn：go-redis 的拨号器内部本身就有重试，
		// 一次失败的 Open 会触发十余次 DialHook —— 每次都打 Warn 只会淹没日志
		// （go-redis 自己也会为拨号失败输出一条警告）。
		// 建连失败的真正原因会通过上层命令的错误暴露出来，由 report 记成 Warn。
		if err != nil {
			h.log.Debugf(ctx, "Redis 建连失败 [%s %s]: %v", network, addr, err)
		} else {
			h.log.Debugf(ctx, "Redis 建连成功 [%s %s]", network, addr)
		}
		return conn, err
	}
}

func (h *loggingHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	timing := h.slow > 0 || h.all
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if !timing {
			// 快路径：只关心失败。省掉热路径上的 time.Now()。
			err := next(ctx, cmd)
			if isRealErr(err) {
				h.log.Warnf(ctx, "Redis 命令失败: %s, 错误: %v", cmd.Name(), err)
			}
			return err
		}

		start := time.Now()
		err := next(ctx, cmd)
		h.report(ctx, time.Since(start), cmd.Name(), err)
		return err
	}
}

func (h *loggingHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmds)
		dur := time.Since(start)

		// 只有当确实要输出日志时才去拼命令名 —— 否则连切片都不分配。
		isSlow := h.slow > 0 && dur >= h.slow
		if !isRealErr(err) && !h.all && !isSlow {
			return err
		}

		names := make([]string, len(cmds))
		for i, c := range cmds {
			names[i] = c.Name()
		}
		h.report(ctx, dur,
			fmt.Sprintf("pipeline(%d 条): %s", len(cmds), strings.Join(names, ",")), err)
		return err
	}
}

// report 按"失败 > 全量 > 慢"的优先级输出，同一次调用只记一条。
func (h *loggingHook) report(ctx context.Context, d time.Duration, detail string, err error) {
	switch {
	case isRealErr(err):
		h.log.Warnf(ctx, "Redis 命令失败: %s, 错误: %v, 耗时 %s", detail, err, d)
	case h.all:
		h.log.Debugf(ctx, "Redis 命令成功: %s, 耗时 %s", detail, d)
	case h.slow > 0 && d >= h.slow:
		h.log.Warnf(ctx, "Redis 慢命令: %s, 耗时 %s（阈值 %s）", detail, d, h.slow)
	}
}
