package metrics

import "time"

// Event 描述一次**已经结束**的操作。
//
// 设计成"完成后"的单个事件，而不是"开始 + 结束"两个事件：
// 直方图只需要一个观测值，而需要配对的接口会逼适配器维护一份"进行中"的表，
// 一旦某条路径忘了发结束事件（panic、提前 return）就会泄漏，且很难发现。
//
// Event 由值传递，不持有任何指针，可以在多个观察者之间安全地转发。
type Event struct {
	// Component 是产生事件的组件。
	Component Component

	// Instance 是实例名，来自各包的 Config.Name。
	//
	// 多实例场景（多库、读写分离、多套 MQ）靠它区分。为空时适配器应当
	// 使用一个固定占位值（如 "-"），不要让它变成空字符串标签。
	Instance string

	// Op 是操作类别的低基数名称。
	Op Op

	// Duration 是操作耗时，含连接池排队等待与网络往返。
	Duration time.Duration

	// Err 是失败原因；成功时为 nil。
	Err error

	// Reason 是 Err 的低基数归类，通常就是 ClassifyErr 的结果。
	//
	// 直接用 Err.Error() 或 Go 类型名做标签同样会带来基数问题与高基数文案，
	// 所以错误计数建议用 `..._errors_total{reason="timeout"}` 这种形式。
	Reason Reason

	// Detail 是操作上下文：SQL 语句、Redis 命令与键、MQ 路由键。
	//
	// **绝不要**把它作为指标 label —— 每条语句都是不同的值。
	// 它的用途是慢操作归因、按表统计的采样、以及 tracing span 的补充信息。
	//
	// 填充策略按各包成本不同：
	//   - mysql / postgres：始终填充。GORM 在构造阶段已经建好了 SQL 字符串，
	//     取用几乎零成本（内部是 strings.Builder，String() 不产生拷贝）。
	//   - redis：**仅在慢操作或出错时**填充。把命令参数拼成字符串有真实开销，
	//     在高 QPS 下每次都拼并不划算。
	//   - rabbitmq：填路由键或队列名，成本可忽略。
	//
	// 数据库侧的这个字符串可能指向 GORM 复用的缓冲区。取值后缓冲区只会被丢弃、
	// 不会被覆写，所以保留它是安全的；但若你的观察者会把 Detail 长期存起来，
	// 建议自己 clone 一份以免依赖这个实现细节。
	Detail string
}

// Failed 报告该操作是否失败。
func (e Event) Failed() bool { return e.Err != nil }

// IsError 报告该事件是否应计入**错误率**。
//
// 与 Failed 的区别在于它排除了 not_found：Redis 的键不存在（goredis.Nil）
// 和 SQL 查出零行都会以 error 的形式返回，但它们在业务上是正常结果 ——
// 缓存未命中本来就是缓存该有的样子。
//
// 错误率指标请一律用 IsError 而不是 Failed。用 Failed 的后果很具体：
// 缓存命中率下降会表现为"Redis 错误率飙升"，把人引向完全错误的方向。
// 需要观察未命中时，单独用 ReasonNotFound 做一条曲线。
func (e Event) IsError() bool {
	return e.Err != nil && e.Reason != ReasonNotFound
}

// Observer 接收操作级事件。
//
// 实现必须满足两个约束：
//
//  1. **并发安全**。同一个 Observer 会被多个 goroutine 同时调用。
//  2. **绝不阻塞**。它跑在业务调用的关键路径上，在这里做网络 IO（比如直接
//     推送到远端）会把耗时算到业务操作头上。
//
// 需要 IO 的实现应当把事件投入带缓冲的 channel，由后台协程消费；
// 缓冲满时**丢弃并计数**，而不是阻塞等待。
type Observer interface {
	ObserveOp(Event)
}

// ObserverFunc 让普通函数满足 Observer，省掉一个类型声明。
//
// 注意：nil 的 ObserverFunc 调用是安全的（直接返回），
// 但一个值为 nil 的 ObserverFunc 装进 Observer 接口后不等于 nil 接口，
// 所以传入各包 Config 前请确保它不是 nil，或直接用 OrNop 包装。
type ObserverFunc func(Event)

// ObserveOp 实现 Observer。
func (f ObserverFunc) ObserveOp(e Event) {
	if f == nil {
		return
	}
	f(e)
}

// NopObserver 丢弃全部事件，是"不观测"的显式表达。
type NopObserver struct{}

// ObserveOp 实现 Observer，什么也不做。
func (NopObserver) ObserveOp(Event) {}

// OrNop 在 o 为 nil 时返回 NopObserver，让调用点不必写 nil 判断。
//
// 它只能识别**真正的 nil 接口**。如果传入的是值为 nil 的具体类型
// （例如 `var f ObserverFunc; OrNop(f)`），返回的仍是那个非 nil 接口 ——
// ObserverFunc 自身的 nil 检查会兜住这种情况。
func OrNop(o Observer) Observer {
	if o == nil {
		return NopObserver{}
	}
	return o
}

// MultiObserver 把事件广播给多个观察者，用于同时接日志、指标与 tracing。
//
// nil 元素会被忽略；全部为 nil 时返回 NopObserver。
// 只有一个有效观察者时直接返回它，避免多余的间接调用。
func MultiObserver(observers ...Observer) Observer {
	live := make([]Observer, 0, len(observers))
	for _, o := range observers {
		if o != nil {
			live = append(live, o)
		}
	}

	switch len(live) {
	case 0:
		return NopObserver{}
	case 1:
		return live[0]
	default:
		return multiObserver(live)
	}
}

// multiObserver 顺序广播。用值类型切片，复制成本只在构造时付一次。
type multiObserver []Observer

func (m multiObserver) ObserveOp(e Event) {
	for _, o := range m {
		o.ObserveOp(e)
	}
}
