package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// LocalConfig 本地缓存配置。
type LocalConfig struct {
	// Size 是 LRU 容量（最大条目数）。<=0 表示不限制条数，仅受 TTL 约束。
	Size int

	// TTL 是条目存活时间。<=0 表示永不过期（仅受容量淘汰约束）。
	TTL time.Duration

	// CleanupInterval 是后台过期清理的运行周期。
	// <=0 不启动后台协程，过期条目仅靠读取时惰性删除
	// （长期不读的冷条目会滞留占用内存，见 README 注意事项）。
	CleanupInterval time.Duration
}

// localEntry 是 LRU 链表节点承载的条目。
type localEntry struct {
	key      string
	val      any
	expireAt int64 // unix 纳秒；0 表示不过期
}

// Local 是进程内 LRU + TTL 缓存。
//
// 零外部依赖、并发安全；Get 命中会移动到链表头部（最近使用）。
// 适用于一级缓存（L1）或独立使用的进程内缓存。
type Local struct {
	mu    sync.Mutex
	size  int
	ttl   time.Duration
	items map[string]*list.Element // key -> order 中的节点
	order *list.List               // front = 最近使用，back = 最久未用

	hits   atomic.Int64
	misses atomic.Int64

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
	janitor  bool
}

// NewLocal 创建本地缓存。cleanupInterval > 0 时启动后台清理协程，
// 调用 Close 停止。
func NewLocal(cfg LocalConfig) *Local {
	l := &Local{
		size:  cfg.Size,
		ttl:   cfg.TTL,
		items: make(map[string]*list.Element),
		order: list.New(),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	if cfg.CleanupInterval > 0 {
		l.janitor = true
		go l.cleanupLoop(cfg.CleanupInterval)
	}
	return l
}

// Get 读取条目。过期条目等价于不存在（惰性删除）。
func (l *Local) Get(key string) (any, bool) {
	l.mu.Lock()
	el, ok := l.items[key]
	if !ok {
		l.mu.Unlock()
		l.misses.Add(1)
		return nil, false
	}
	e := el.Value.(*localEntry)
	if e.expireAt != 0 && e.expireAt <= nowNano() {
		l.order.Remove(el)
		delete(l.items, key)
		l.mu.Unlock()
		l.misses.Add(1)
		return nil, false
	}
	l.order.MoveToFront(el)
	val := e.val
	l.mu.Unlock()
	l.hits.Add(1)
	return val, true
}

// Set 写入条目；容量超限时从链表尾部淘汰最久未使用的条目。
func (l *Local) Set(key string, val any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	el, ok := l.items[key]
	if ok {
		e := el.Value.(*localEntry)
		e.val = val
		e.expireAt = l.expireAt()
		l.order.MoveToFront(el)
		return
	}
	l.items[key] = l.order.PushFront(&localEntry{
		key:      key,
		val:      val,
		expireAt: l.expireAt(),
	})
	if l.size > 0 {
		for l.order.Len() > l.size {
			back := l.order.Back()
			if back == nil {
				break
			}
			l.order.Remove(back)
			delete(l.items, back.Value.(*localEntry).key)
		}
	}
}

// Delete 删除条目（不存在时静默）。
func (l *Local) Delete(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.items[key]; ok {
		l.order.Remove(el)
		delete(l.items, key)
	}
}

// Len 返回当前条目数（含已过期但未被清理的）。
func (l *Local) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.order.Len()
}

// Hits 返回累计命中次数。
func (l *Local) Hits() int64 { return l.hits.Load() }

// Misses 返回累计未命中次数（含过期）。
func (l *Local) Misses() int64 { return l.misses.Load() }

// Close 停止后台清理协程（若有）。可安全多次调用。
func (l *Local) Close() {
	l.stopOnce.Do(func() { close(l.stop) })
	if l.janitor {
		<-l.done
	}
}

func (l *Local) expireAt() int64 {
	if l.ttl <= 0 {
		return 0
	}
	return nowNano() + int64(l.ttl)
}

func (l *Local) cleanupLoop(interval time.Duration) {
	defer close(l.done)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			l.removeExpired()
		}
	}
}

func (l *Local) removeExpired() {
	now := nowNano()
	l.mu.Lock()
	defer l.mu.Unlock()
	for el := l.order.Back(); el != nil; {
		prev := el.Prev()
		e := el.Value.(*localEntry)
		if e.expireAt != 0 && e.expireAt <= now {
			l.order.Remove(el)
			delete(l.items, e.key)
		}
		el = prev
	}
}

// nowNano 独立成变量便于测试注入假时钟。
var nowNano = func() int64 { return time.Now().UnixNano() }
