package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/zavierswong/go-infra/metrics"
)

// 本文件把 go-redis 的 Hook 机制接到 metrics.Observer 上。
//
// 与 mysql / postgres 的差别：那边要自己用 GORM 的 callback 拼出开始与结束两个锚点，
// 这边 go-redis 已经提供了 Hook，直接按命令包装即可，不需要额外状态。
//
// 另一点不同：这里**没有**把指标塞进现有的 loggingHook，而是单独一个钩子。
// 两个理由 —— 日志与指标是两件独立的开关（有人只想打慢查询日志、有人只想打指标）；
// 而且没配 Observer 时干脆不注册钩子，热路径上连一次闭包调用都不多花。

// Detail 的截断上限。只在慢命令或出错时才拼接，所以这点开销可以接受。
const (
	// maxDetailArgs 只取命令的**第一个参数**。
	//
	// 这是刻意的隐私取舍：Redis 的参数里第 1 个几乎总是键名（GET k、HSET k f v、
	// EXPIRE k t 都是），而键名正是归因需要的；再往后就可能是**写入的值** ——
	// `SET session:abc <jwt>` 的第 2 个参数就是令牌。
	// Detail 会被写进日志，宁可少给一点上下文，也不要把业务数据铺出去。
	//
	// 需要更完整上下文时，请用 go-redis 原生的 CLIENT LIST / SLOWLOG，
	// 那些是运维主动开启的、边界清晰的通道。
	maxDetailArgs = 1

	maxDetailArgLen = 32
)

// sensitiveCommands 是"参数里可能含口令"的命令，Detail 一律不拼参数。
//
// 起因是一个实测出来的泄漏：go-redis 建连时会发
// `HELLO 3 AUTH default <password>`，而 AUTH 命令的第一个参数就是口令本身。
// Detail 会进日志，把数据库口令写进日志是绝对不能接受的。
//
// 键必须是大写 —— 调用处已经用 commandOp 规范化过了。
var sensitiveCommands = map[string]struct{}{
	"AUTH":    {}, // AUTH <password>
	"HELLO":   {}, // HELLO 3 AUTH <user> <password>
	"ACL":     {}, // ACL SETUSER <user> ><password>
	"CONFIG":  {}, // CONFIG SET requirepass <password>
	"MIGRATE": {}, // MIGRATE ... AUTH <password>
}

// omittedArgs 替换敏感命令的参数，让日志里能看出"这里本来有参数，被有意省略了"。
const omittedArgs = " [参数已省略：可能含口令]"

// PoolStats 返回归一化后的连接池快照。
//
// 与 Stats 的区别：Stats 返回 go-redis 的原生结构（字段名是 Hits/StaleConns 这套），
// PoolStats 返回 metrics.PoolStats，与 mysql / postgres 是同一个形状，
// 因此上层只需要一个导出器就能同时给三种组件打点。
//
// MaxOpen 取的是 MaxActiveConns，也就是**硬上限**；它为 0 时表示不限制，
// 此时 Saturated() 恒为 false —— 这是正确的语义，不是缺陷。
// 想观察"池是不是不够用"，在 redis 上看 Pending 与 Timeouts 更有意义。
func (r *RDS) PoolStats() metrics.PoolStats {
	return poolStatsFrom(r.cfg.Name, r.cfg.MaxActiveConns, r.Stats())
}

// poolStatsFrom 是归一化映射的实体，单独抽出来是为了**可测**。
//
// 与 Config.options() 同样的理由：这种"字段搬来搬去"的代码一旦漏掉某一项，
// 配置就会静默失效，而集成测试很难发现 —— 你只会看到某条曲线永远是 0。
// 抽成纯函数后，单测可以逐字段断言，不需要真实 Redis。
func poolStatsFrom(instance string, maxActiveConns int, s *goredis.PoolStats) metrics.PoolStats {
	out := metrics.PoolStats{
		Component: metrics.ComponentRedis,
		Instance:  instance,
		// MaxActiveConns 才是硬上限；为 0 表示不限制，此时 Saturated() 恒为 false，
		// 这是正确的语义（go-redis 的 PoolSize 只是软目标，不是上限）。
		MaxOpen: maxActiveConns,
	}

	// 连接池已关闭时 Stats 返回 nil，此时留一个只有标识的快照，
	// 而不是让调用方拿到 nil 去猜。
	if s == nil {
		return out
	}

	total := int(s.TotalConns)
	idle := int(s.IdleConns)
	inUse := total - idle
	if inUse < 0 {
		// 几个计数是分开读取的，并发下 total 可能小于 idle。
		// 夹到 0 而不是让负数流进指标里 —— 负的 Gauge 会让图表和告警阈值全部失真。
		inUse = 0
	}

	out.Open = total
	out.Idle = idle
	out.InUse = inUse
	out.Pending = int(s.PendingRequests)
	out.WaitCount = int64(s.WaitCount)
	// go-redis 内部存的是纳秒整数，这里转成 time.Duration 与数据库侧对齐。
	out.WaitDuration = time.Duration(s.WaitDurationNs)
	out.Hits = int64(s.Hits)
	out.Misses = int64(s.Misses)
	out.Timeouts = int64(s.Timeouts)
	out.Unusable = int64(s.Unusable)
	out.Stale = int64(s.StaleConns)
	return out
}

// classifyErr 把错误细化成 metrics 的低基数原因。
//
// metrics.ClassifyErr 只认通用原因，这里补上 redis 专有的几类。
// 其中最关键的是 goredis.Nil → not_found：它让缓存未命中不会污染错误率
// （配合 metrics.Event.IsError 使用）。
func classifyErr(err error) metrics.Reason {
	if err == nil {
		return metrics.ReasonNone
	}

	switch {
	case errors.Is(err, goredis.Nil):
		// 键不存在。缓存未命中、SETNX 抢锁失败都会走到这里。
		return metrics.ReasonNotFound

	case errors.Is(err, goredis.TxFailedErr):
		// WATCH 监视的键被改动，EXEC 放弃执行。
		// 语义上等价于"乐观锁冲突"，归 conflict 才能和真正的故障区分开。
		return metrics.ReasonConflict

	case errors.Is(err, goredis.ErrPoolTimeout), errors.Is(err, goredis.ErrPoolExhausted):
		// 池满导致拿不到连接。这不是网络问题而是**容量**问题，
		// 但它在调用方看到的就是一次超时，所以归 timeout ——
		// 并且它应当直接触发"调大 MinIdleConns / MaxActiveConns"的动作。
		return metrics.ReasonTimeout

	case errors.Is(err, goredis.ErrClosed), errors.Is(err, ErrClosed):
		return metrics.ReasonClosed

	case errors.Is(err, ErrConnect), errors.Is(err, ErrNotConnected):
		return metrics.ReasonConnect

	case errors.Is(err, ErrInvalidConfig):
		return metrics.ReasonInvalid
	}

	// 服务端返回的错误（-ERR / -WRONGTYPE / -NOSCRIPT …）。
	// 这些靠错误前缀区分，用 HasErrorPrefix 而不是字符串包含 —— 后者会
	// 把 value 里恰好出现的 "NOSCRIPT" 也误判成协议错误。
	if isServerError(err) {
		switch {
		case goredis.HasErrorPrefix(err, "NOSCRIPT"), goredis.HasErrorPrefix(err, "NOGROUP"):
			// 脚本不在缓存里 / 消费者组不存在。都是"去创建一下就好"的可恢复状态。
			return metrics.ReasonNotFound
		case goredis.HasErrorPrefix(err, "LOADING"):
			// 服务端正在把数据集载入内存，此刻不服务请求。稍后重试即可。
			return metrics.ReasonTimeout
		case goredis.HasErrorPrefix(err, "BUSYGROUP"):
			// 消费者组已存在：并发创建时的良性冲突。
			return metrics.ReasonConflict
		}
		// WRONGTYPE / 参数错误 / 未知子命令等：命令本身有问题。
		return metrics.ReasonInvalid
	}

	return metrics.ClassifyErr(err)
}

// isServerError 判断错误是否来自 Redis 服务端（协议层错误）而非网络层。
//
// goredis.Error 是个带 RedisError() 标记方法的接口，比字符串匹配可靠：
// 网络超时、连接断开走的是另一条路，不会误判。
func isServerError(err error) bool {
	var rErr goredis.Error
	return errors.As(err, &rErr)
}

// commandOp 把 go-redis 的命令名规范成指标标签用的形式。
//
// go-redis 内部用小写（"get" / "hgetall"），而 Redis 官方文档、redis_exporter
// 以及现成的 Grafana 面板全用大写。这里统一成大写，于是本库产出的指标
// 可以直接套用已有面板与告警规则，不必再写一层大小写转换。
//
// 代价是每次命令多一次小字符串分配。相对一次网络往返（百微秒级）可以忽略，
// 换来的是标签口径与整个生态一致。另外 go-redis 在建连时会发
// CLIENT MAINT_NOTIFICATIONS 这类内部命令，统一大写后它们在指标里
// 表现为 cmd="CLIENT"，需要过滤时能一眼认出来（见 README 注意事项）。
func commandOp(name string) metrics.Op {
	return metrics.Op(strings.ToUpper(name))
}

// metricsHook 把命令事件翻译成 metrics.Event。
//
// 实现的是 go-redis 的 Hook 接口。三个方法的取舍：
//   - DialHook 直接透传，不上报。go-redis 的拨号器内部自带重试，
//     一次建连失败会触发十余次 DialHook —— 按它计数会把一个事件放大成十几条
//     时间序列，而建连失败的影响本来就已经体现在命令的耗时与错误里了。
//   - ProcessHook 逐条命令上报，这是指标的主力。
//   - ProcessPipelineHook 整条 pipeline 上报一个事件，理由见 metrics.OpPipeline。
type metricsHook struct {
	obs  metrics.Observer
	name string
	slow time.Duration // >0 时，超过该阈值的命令会填充 Detail
}

var _ goredis.Hook = (*metricsHook)(nil)

// DialHook 原样透传，见类型注释。
func (h *metricsHook) DialHook(next goredis.DialHook) goredis.DialHook {
	return next
}

// ProcessHook 逐条命令上报。
func (h *metricsHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmd)
		elapsed := time.Since(start)

		h.obs.ObserveOp(metrics.Event{
			Component: metrics.ComponentRedis,
			Instance:  h.name,
			// Op 直接取命令名（GET / SET / HGETALL …），已统一大写。
			// Redis 命令总数约 200 个，作为标签基数可控，而且比笼统的 "cmd" 有用得多。
			Op:       commandOp(cmd.Name()),
			Duration: elapsed,
			Err:      err,
			Reason:   classifyErr(err),
			Detail:   h.detail(cmd, elapsed, err),
		})
		return err
	}
}

// ProcessPipelineHook 整条 pipeline 上报一个事件。
func (h *metricsHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmds)
		elapsed := time.Since(start)

		h.obs.ObserveOp(metrics.Event{
			Component: metrics.ComponentRedis,
			Instance:  h.name,
			Op:        metrics.OpPipeline,
			Duration:  elapsed,
			Err:       err,
			Reason:    classifyErr(err),
			Detail:    h.pipelineDetail(cmds, elapsed, err),
		})
		return err
	}
}

// detail 按"出错或慢"的策略决定是否填充命令详情。
//
// 为什么不每条都填：把参数格式化成字符串有真实开销（反射式的 %v），
// 而绝大多数命令都是几十微秒的正常调用，为它们付出这笔成本不值得。
// 慢命令与失败命令才是需要归因的对象，它们的出现频率天然很低。
//
// 参数只取前若干个并逐段截断：Redis 里存的常常是用户数据，
// 而 Detail 会被写进日志，不该把整条 value 铺出来。
func (h *metricsHook) detail(cmd goredis.Cmder, elapsed time.Duration, err error) string {
	if !h.needDetail(elapsed, err) {
		return ""
	}
	return formatCommand(string(commandOp(cmd.Name())), cmd.Args(), 1)
}

// pipelineDetail 与 detail 同样的策略，另外补上命令名列表。
func (h *metricsHook) pipelineDetail(cmds []goredis.Cmder, elapsed time.Duration, err error) string {
	if !h.needDetail(elapsed, err) {
		return ""
	}

	names := make([]string, 0, len(cmds))
	for _, c := range cmds {
		names = append(names, string(commandOp(c.Name())))
	}
	return fmt.Sprintf("pipeline(%d 条): %s", len(cmds), strings.Join(names, ","))
}

// needDetail 统一两条路径的判定，避免将来两处判断跑偏。
func (h *metricsHook) needDetail(elapsed time.Duration, err error) bool {
	if err != nil {
		return true
	}
	return h.slow > 0 && elapsed >= h.slow
}

// formatCommand 拼出"命令名 + 首个参数"的短描述。
//
// skip 是要跳过的 Args 前缀长度：go-redis 的 Args() 第 0 项就是命令名本身，
// 所以传 1。
//
// name 必须是**已大写**的命令名，sensitiveCommands 是按大写匹配的。
func formatCommand(name string, args []interface{}, skip int) string {
	if _, sensitive := sensitiveCommands[name]; sensitive {
		return name + omittedArgs
	}

	var b strings.Builder
	b.WriteString(name)

	limit := len(args)
	if limit > skip+maxDetailArgs {
		limit = skip + maxDetailArgs
	}
	for i := skip; i < limit; i++ {
		b.WriteByte(' ')
		b.WriteString(truncateRunes(fmt.Sprint(args[i]), maxDetailArgLen))
	}
	if total := len(args) - skip; total > maxDetailArgs {
		fmt.Fprintf(&b, " …(共 %d 个参数)", total)
	}
	return b.String()
}

// truncateRunes 按**字符**而非字节截断，避免把一个多字节字符切成乱码。
// 字节数已经不超过上限时直接返回，省掉一次转换。
func truncateRunes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}
