// Package cache 提供进程内本地缓存与「本地 L1 + Redis L2」两级缓存。
//
// 核心能力：
//   - Local：零依赖的 LRU + TTL 进程内缓存；
//   - Redis：原始字节值的一级薄封装，可复用 access/redis 连接；
//   - Multi：两级组合，GetOrLoad 泛型加载（JSON 编解码 L2）+
//     singleflight 合并并发加载，防缓存击穿。
//
// 两级缓存的典型读路径：L1 命中 → 返回；L1 未命中 → L2 命中 →
// 回填 L1；L2 也未命中 → loader 加载 → 回写两级。
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// MultiConfig 两级缓存配置。
type MultiConfig struct {
	// Local 是 L1 配置。建议 Size > 0（否则 L1 只靠 TTL 约束，可能吃内存）。
	Local LocalConfig

	// Redis 是 L2 配置（KeyPrefix 建议必填，避免与其他业务 key 冲突）。
	Redis RedisConfig

	// FailClosed 决定 L2 读故障的处理策略：
	//   false（默认，fail-open）：当作未命中继续走 loader，可用性优先；
	//   true（fail-close）：直接返回错误，一致性优先。
	// 两种策略下 L2 写失败都仅记日志，不阻断读流程。
	FailClosed bool

	// Logger 用于记录 L2 降级/写失败等非致命事件，nil 则静默。
	Logger *slog.Logger
}

// Multi 是「本地 L1 + Redis L2」两级缓存。
type Multi struct {
	l1         *Local
	l2         *Redis
	g          singleflight.Group
	failClosed bool
	logger     *slog.Logger

	l1Hits   atomic.Int64
	l2Hits   atomic.Int64
	loads    atomic.Int64
	l2Errs   atomic.Int64
	l2Writes atomic.Int64

	closeOnce sync.Once
}

// NewMulti 创建两级缓存。Close 时会顺带停掉 L1 的后台清理协程。
func NewMulti(cli redis.UniversalClient, cfg MultiConfig) *Multi {
	return &Multi{
		l1:         NewLocal(cfg.Local),
		l2:         NewRedis(cli, cfg.Redis),
		failClosed: cfg.FailClosed,
		logger:     cfg.Logger,
	}
}

// Stats 缓存运行统计。
type Stats struct {
	L1Hits   int64 // L1 命中
	L2Hits   int64 // L2 命中（已回填 L1）
	Loads    int64 // 真实加载（loader 调用次数）
	L2Errors int64 // L2 读故障（fail-open 时已降级为未命中）
	L2Writes int64 // L2 回写次数
}

// Stats 返回累计统计快照。
func (m *Multi) Stats() Stats {
	return Stats{
		L1Hits:   m.l1Hits.Load(),
		L2Hits:   m.l2Hits.Load(),
		Loads:    m.loads.Load(),
		L2Errors: m.l2Errs.Load(),
		L2Writes: m.l2Writes.Load(),
	}
}

// l2WriteTimeout 是 L2 回写的兜底超时（仅在请求 ctx 已取消时启用）。
const l2WriteTimeout = 2 * time.Second

// typedValue 给加载结果套一层具体类型，使 singleflight 返回的 any
// 无论如何都能被安全地断言回 T。
//
// 直接传裸值有两个坑：
//  1. T 是接口类型且 loader 返回 nil 时，any 是 nil interface，
//     v.(T) 必然失败 —— 即便 nil 是这个 T 的合法值；
//  2. 不同 T 的调用若共享同一 singleflight key，leader 的动态类型
//     可能不是 follower 期望的类型，v.(T) 直接 panic。
//
// 套一层后 v 的动态类型恒为 typedValue[T]，断言永不失败。
type typedValue[T any] struct{ val T }

// typeKeys 缓存「reflect.Type → 类型名」，避免每次调用都拼字符串。
var typeKeys sync.Map

// typeKey 返回 T 的类型名，用于把不同 T 的加载隔离到不同的 singleflight key，
// 防止 A 类型的 leader 结果被 B 类型的 follower 拿到。
func typeKey[T any]() string {
	t := reflect.TypeOf((*T)(nil)).Elem()
	if v, ok := typeKeys.Load(t); ok {
		return v.(string)
	}
	s := t.String()
	typeKeys.Store(t, s)
	return s
}

// GetOrLoad 读取缓存，未命中时调用 loader 加载并回写两级。
//
//   - 并发防击穿：同一 key 的并发请求被 singleflight 合并，
//     loader 在同一时刻至多执行一次（其余共享结果）；
//   - 类型安全：L1 直接存 T 原值（零序列化开销），L2 经 JSON 编解码；
//   - ttl 是 L2 的过期时间（L1 用 MultiConfig.Local.TTL）；
//   - loader 的错误不缓存（下次请求会重试加载）；
//   - L2 读故障：fail-open 时降级为未命中（计入 L2Errors），fail-close 时上抛。
//
// 注意：L2 回填 L1 后，L1 中的副本不再与 L2 同步过期
// （L1 用自身 TTL）；跨实例一致性要求高的 key 请用较短 L1 TTL 或直接 Invalidate。
func GetOrLoad[T any](ctx context.Context, m *Multi, key string, ttl time.Duration, loader func(context.Context) (T, error)) (T, error) {
	if v, ok := m.l1.Get(key); ok {
		if t, typeOK := v.(T); typeOK {
			m.l1Hits.Add(1)
			return t, nil
		}
		// 类型不符：视为未命中（理论上仅发生在改类型后未重启的极端场景）。
		m.l1.Delete(key)
	}

	// singleflight key 带类型：不同 T 的加载互不共享结果，
	// 否则「谁先到谁当 leader」会让 follower 拿到错误动态类型的值。
	v, err, _ := m.g.Do(m.l2.prefix+key+"|"+typeKey[T](), func() (any, error) {
		// double-check：等锁期间可能已被兄弟请求填进 L1/L2。
		t, ok, l2err := getL2[T](ctx, m, key)
		if l2err != nil {
			if m.failClosed {
				return nil, l2err
			}
			// fail-open：已计数，降级为未命中继续走 loader。
			m.logWarn("cache: L2 读故障，降级为未命中", "key", key, "err", l2err)
		}
		if ok {
			return typedValue[T]{val: t}, nil
		}

		m.loads.Add(1)
		loaded, err := loader(ctx)
		if err != nil {
			return nil, err
		}
		m.l1.Set(key, loaded)
		m.writeL2(ctx, key, loaded, ttl)
		return typedValue[T]{val: loaded}, nil
	})
	if err != nil {
		var zero T
		return zero, err
	}
	// 断言必然成功（singleflight key 已按类型隔离）；留 comma-ok
	// 是因为「panic 整个进程」的代价远高于「返回一条明确错误」。
	tv, ok := v.(typedValue[T])
	if !ok {
		var zero T
		return zero, fmt.Errorf("cache: key %q 的加载结果类型不符（期望 %T，实际 %T）", key, zero, v)
	}
	return tv.val, nil
}

// getL2 尝试读 L2 并反序列化 + 回填 L1。
// 返回 (值, 是否命中, 原始错误)；fail-open / fail-close 由 GetOrLoad 决定。
func getL2[T any](ctx context.Context, m *Multi, key string) (T, bool, error) {
	var zero T
	b, ok, err := m.l2.Get(ctx, key)
	if err != nil {
		m.l2Errs.Add(1)
		return zero, false, err
	}
	if !ok {
		return zero, false, nil
	}
	var t T
	if jsonErr := json.Unmarshal(b, &t); jsonErr != nil {
		// 脏数据（如改过结构体字段）：当作未命中，让 loader 重新加载覆盖。
		m.logWarn("cache: L2 反序列化失败，当作未命中", "key", key, "err", jsonErr)
		return zero, false, nil
	}
	m.l2Hits.Add(1)
	m.l1.Set(key, t)
	return t, true, nil
}

// Invalidate 使指定 key 在两级同时失效。
//
// 顺序必须是「先 L2 再 L1」。反序会留下一个真实窗口：并发的读在 L2 删除
// **之前** 取到旧值、却在 L1 删除 **之后** 才回填（GetOrLoad 的 L2 命中
// 路径会回填 L1），于是刚删掉的陈旧值在 L1 里复活，并存活整个 L1 TTL；
// Local.TTL 为 0 时更是永久。先删 L2 可以把这个窗口压掉。
//
// 残留窗口：删除之前就已在飞的加载仍可能把旧值写回 L1。要彻底消除需给
// key 引入版本号（代际）机制，本包不提供——对一致性要求极高的 key，
// 请在删除后用 InvalidateLocal 做一次延迟二次删除。
func (m *Multi) Invalidate(ctx context.Context, key string) error {
	if err := m.l2.Delete(ctx, key); err != nil {
		return err
	}
	m.l1.Delete(key)
	return nil
}

// InvalidateLocal 仅失效本实例的 L1（其他实例的本地缓存不受影响）。
// 用于 L2 数据被外部更新后只刷新本机副本的场景。
//
// 也用作 Invalidate 的补充：在 L2 删除后延迟一小段时间再调一次，
// 可清掉「删除前已在飞、删除后才回填」的陈旧值。
func (m *Multi) InvalidateLocal(key string) {
	m.l1.Delete(key)
}

// Close 停止 L1 后台清理协程（若有）。可安全多次调用。
func (m *Multi) Close() {
	m.closeOnce.Do(func() { m.l1.Close() })
}

func (m *Multi) writeL2(ctx context.Context, key string, val any, ttl time.Duration) {
	b, err := json.Marshal(val)
	if err != nil {
		m.logWarn("cache: L2 序列化失败，跳过回写", "key", key, "err", err)
		return
	}
	// 回写与请求 ctx 解耦：loader 返回时调用方可能已经取消（上游超时、
	// 客户端断开），沿用请求 ctx 会把刚花代价加载出来的结果直接丢掉，
	// 下一个请求还得再加载一次。但仍要保留超时上限，避免 Redis 不可达
	// 时把这个 goroutine 挂死。
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), l2WriteTimeout)
		defer cancel()
	}
	if err := m.l2.Set(ctx, key, b, ttl); err != nil {
		m.logWarn("cache: L2 回写失败", "key", key, "err", err)
		return
	}
	m.l2Writes.Add(1)
}

func (m *Multi) logWarn(msg string, args ...any) {
	if m.logger != nil {
		m.logger.Warn(msg, args...)
	}
}
