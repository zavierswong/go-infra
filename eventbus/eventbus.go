// Package eventbus 提供进程内事件总线（pub/sub），风格与 metrics.Event
// 的观察者模式一致：发布方只管广播，订阅方按 topic 消费。
//
// 两种派发模式：
//   - Publish（同步）：在调用方 goroutine 依次执行全部订阅者，
//     收集错误与 panic，适合"事件处理结果影响主流程"的场景；
//   - PublishAsync（异步）：投递到共享队列，由固定 worker 池消费，
//     非阻塞，适合"发完就走"的通知类事件；队列满时丢弃并计数。
//
// 生命周期：Close 停止接收新事件并排空异步队列；向已关闭的总线
// 发布返回 ErrClosed。
package eventbus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

// ErrClosed 表示总线已关闭，不能再发布或订阅。
var ErrClosed = errors.New("eventbus: bus is closed")

// Event 是总线传递的事件。Payload 用 any 而非泛型，换取"一个总线
// 承载全站事件"的简单性；订阅方自行断言类型（建议 topic 常量与
// payload 类型成对定义在使用方包里）。
type Event struct {
	Topic     string    // 事件主题，如 "order.paid"
	Payload   any       // 事件载荷
	Timestamp time.Time // 事件创建时间（发布时刻）
}

// Handler 事件处理器。返回 error 会被 Publish 汇总（errors.Join）；
// 异步派发时错误只写日志，无法回传发布方。
type Handler func(ctx context.Context, e Event) error

// Config 总线配置。零值字段取默认值，可直接 New(Config{})。
type Config struct {
	// Logger 用于异步派发错误、panic、丢弃事件的记录；
	// nil 时用 slog.Default()。
	Logger *slog.Logger

	// AsyncWorkers 异步派发 worker 数，<=0 用 DefaultAsyncWorkers。
	AsyncWorkers int

	// AsyncQueue 异步队列长度，<=0 用 DefaultAsyncQueue。
	// 满了以后 PublishAsync 丢弃事件并递增 Dropped 计数——
	// 宁可丢通知，不让发布方被慢消费者拖死。
	AsyncQueue int
}

// 默认值。
const (
	DefaultAsyncWorkers = 4
	DefaultAsyncQueue   = 1024
)

// subscriber 是一条订阅记录。用指针身份（*subscriber）做退订，
// 避免闭包比较。
type subscriber struct {
	handler Handler
}

// Bus 进程内事件总线。零值不可用，请用 New。
type Bus struct {
	cfg Config
	log *slog.Logger

	mu      sync.RWMutex
	subs    map[string][]*subscriber
	dropped uint64
	queued  uint64

	queue chan Event
	quit  chan struct{} // Close 时关闭，通知 worker 退出排空流程

	closeOnce sync.Once
	workerWG  sync.WaitGroup

	closedMu sync.Mutex
	closed   bool
}

// New 构造事件总线并启动异步 worker。
func New(cfg Config) *Bus {
	if cfg.AsyncWorkers <= 0 {
		cfg.AsyncWorkers = DefaultAsyncWorkers
	}
	if cfg.AsyncQueue <= 0 {
		cfg.AsyncQueue = DefaultAsyncQueue
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	b := &Bus{
		cfg:   cfg,
		log:   log,
		subs:  make(map[string][]*subscriber),
		queue: make(chan Event, cfg.AsyncQueue),
		quit:  make(chan struct{}),
	}
	for i := 0; i < cfg.AsyncWorkers; i++ {
		b.workerWG.Add(1)
		go b.worker()
	}
	return b
}

// Subscribe 注册订阅者，返回退订函数（幂等，可安全多次调用）。
// 总线关闭后 Subscribe 返回 ErrClosed。
func (b *Bus) Subscribe(topic string, h Handler) (func(), error) {
	if h == nil {
		return nil, errors.New("eventbus: nil handler")
	}
	// 先查关闭状态再拿 mu：closed 用独立锁保护，固定
	// closedMu → mu 的加锁顺序，避免与 tryEnqueue 死锁。
	// （极小窗口内"刚订阅完就 Close"是合法结果。）
	if b.isClosed() {
		return nil, ErrClosed
	}
	b.mu.Lock()
	sub := &subscriber{handler: h}
	b.subs[topic] = append(b.subs[topic], sub)
	b.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			list := b.subs[topic]
			for i, s := range list {
				if s == sub {
					b.subs[topic] = append(list[:i], list[i+1:]...)
					break
				}
			}
			if len(b.subs[topic]) == 0 {
				delete(b.subs, topic)
			}
		})
	}, nil
}

// Publish 同步发布：在调用方 goroutine 依次执行该 topic 的全部订阅者
// （注册顺序），逐个 recover panic 并收集错误，最后 errors.Join 返回。
// 单个订阅者失败不影响其他订阅者收到事件。
// 无订阅者时返回 nil（发布即遗忘，不报错）。
func (b *Bus) Publish(ctx context.Context, topic string, payload any) error {
	if b.isClosed() {
		return ErrClosed
	}
	b.mu.RLock()
	subs := b.subs[topic]
	b.mu.RUnlock()
	if len(subs) == 0 {
		return nil
	}

	e := Event{Topic: topic, Payload: payload, Timestamp: time.Now()}
	var errs []error
	for _, sub := range subs {
		if err := safeCall(ctx, sub.handler, e, b.log); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// PublishAsync 异步发布：事件进共享队列，由 worker 池消费。
// 非阻塞：队列满时丢弃事件、递增 Dropped 计数并返回 false；
// 总线已关闭同样返回 false。多 worker 并发消费，**不保证处理顺序**，
// 有序需求请在 Payload 里带序号自行重排。
func (b *Bus) PublishAsync(topic string, payload any) bool {
	e := Event{Topic: topic, Payload: payload, Timestamp: time.Now()}
	if !b.tryEnqueue(e) {
		return false
	}
	return true
}

func (b *Bus) tryEnqueue(e Event) bool {
	b.closedMu.Lock()
	if b.closed {
		b.closedMu.Unlock()
		return false
	}
	select {
	case b.queue <- e:
		b.closedMu.Unlock()
		return true
	default:
		// 队列满：先解除 closed 检查锁再计数，避免持锁做日志。
		b.closedMu.Unlock()
		b.mu.Lock()
		b.dropped++
		b.mu.Unlock()
		b.log.Warn("eventbus: async queue full, event dropped",
			"topic", e.Topic)
		return false
	}
}

// worker 消费异步队列。Close 关闭 quit 后先 drain 完 queue 中
// 已入队的事件再退出（range 语义保证）。
func (b *Bus) worker() {
	defer b.workerWG.Done()
	for {
		select {
		case e := <-b.queue:
			b.dispatchAsync(e)
		case <-b.quit:
			// 排空剩余事件后退出。
			for {
				select {
				case e := <-b.queue:
					b.dispatchAsync(e)
				default:
					return
				}
			}
		}
	}
}

func (b *Bus) dispatchAsync(e Event) {
	b.mu.RLock()
	subs := append([]*subscriber(nil), b.subs[e.Topic]...)
	b.mu.RUnlock()
	for _, sub := range subs {
		if err := safeCall(context.Background(), sub.handler, e, b.log); err != nil {
			b.log.Warn("eventbus: async handler error",
				"topic", e.Topic, "error", err)
		}
	}
}

// safeCall 执行 handler 并 recover panic —— 一个订阅者的 bug
// 不允许炸掉发布方或 worker。
func safeCall(ctx context.Context, h Handler, e Event, log *slog.Logger) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("eventbus: handler panicked: %v", p)
			log.Error("eventbus: handler panic recovered",
				"topic", e.Topic, "panic", fmt.Sprint(p),
				"stack", string(debug.Stack()))
		}
	}()
	return h(ctx, e)
}

// Dropped 返回异步队列满被丢弃的事件总数（诊断用）。
func (b *Bus) Dropped() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropped
}

// Topics 返回当前有订阅者的 topic 列表（诊断用）。
func (b *Bus) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	topics := make([]string, 0, len(b.subs))
	for t := range b.subs {
		topics = append(topics, t)
	}
	return topics
}

// Close 关闭总线：停止接收新事件，排空异步队列中已入队的事件，
// 等待 worker 退出。ctx 超时/取消时返回 ctx.Err()（worker 会继续
// 排空，但 Close 不再等待）。
// Close 幂等。
func (b *Bus) Close(ctx context.Context) error {
	var err error
	b.closeOnce.Do(func() {
		b.closedMu.Lock()
		b.closed = true
		b.closedMu.Unlock()
		close(b.quit)

		done := make(chan struct{})
		go func() {
			b.workerWG.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-ctx.Done():
			err = ctx.Err()
		}
	})
	return err
}

func (b *Bus) isClosed() bool {
	b.closedMu.Lock()
	defer b.closedMu.Unlock()
	return b.closed
}
