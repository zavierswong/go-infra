package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zavierswong/go-infra/logger"
	"github.com/zavierswong/go-infra/metrics"
	"github.com/zavierswong/go-infra/utils"
)

// Client 是一个 RabbitMQ 客户端。
//
// 与旧版单例实现的三个关键区别：
//
//  1. **支持多实例**：不同 vhost / 集群可以共存，测试之间也不会互相污染。
//  2. **通道隔离**：生产者共用一条发布通道，每个消费者持有自己的独占通道。
//     这样 `Qos(prefetch)` 不会互相覆盖，某条通道被 broker 关闭也只影响一个消费者。
//  3. **懒自愈**：每个操作前都会检查连接与通道是否失效，失效就重建并重放拓扑。
//
// 第 3 点解决的是旧版最严重的缺陷：旧版只监听 connection 的 NotifyClose，
// 但 404（队列不存在）、406（属性不一致）这类错误只会让 **channel** 被 broker 关闭，
// connection 依然健康，于是重连流程永远不触发，客户端此后每次调用都失败——
// 相当于永久瘫痪，只能重启进程。
//
// Client 的所有导出方法都可并发调用。
type Client struct {
	cfg Config
	log *logger.Plog

	// obs 是预解析好的观察者：Config.Observer 为 nil 时是 NopObserver。
	//
	// 预解析而不是每次判空，是因为发布路径是热路径，一次接口判空虽小也没必要
	// 每条消息都做一遍。
	obs metrics.Observer

	// rebuildMu 让重建过程串行化：并发调用只会触发一次真实重建，其余等它做完。
	// 这也是"懒自愈"能保证不惊群的原因。
	rebuildMu sync.Mutex

	mu     sync.RWMutex
	conn   *amqp.Connection
	pub    *amqp.Channel
	closed bool

	// connected 记录"是否成功建立过连接"。仅用于区分首次建连与断线重连 ——
	// 两者都会走 dial，但只有后者该上报 reconnect 事件。
	// 启动时把首次建连记成 reconnect 会让"重连次数"这个指标从一开始就是错的。
	connected bool

	// waiters 保存"正等待退回结果"的发布方，键为消息的 MessageId。
	waiters sync.Map // map[string]chan amqp.Return

	// returnMu/onReturn 是可选的异步退回回调，见 SetReturnHandler。
	returnMu sync.RWMutex
	onReturn func(amqp.Return)

	drainWG sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

// Open 建立连接并声明拓扑。
//
// 返回的 Client 必须由调用方负责 Close。初始化失败时会按 Config.DialAttempts
// 做有限次重试，因此不会像旧版那样无限阻塞在初始化里。
func Open(cfg Config) (*Client, error) {
	cfg = cfg.normalize()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		cfg:    cfg,
		log:    logger.NewPlog("RabbitMQ"),
		obs:    metrics.OrNop(cfg.Observer),
		ctx:    ctx,
		cancel: cancel,
	}
	if _, err := c.ensure(ctx); err != nil {
		cancel()
		return nil, err
	}
	return c, nil
}

// Close 关闭客户端。可重复调用，第二次起直接返回 nil。
//
// 修复了旧版的两个问题：旧版在持锁状态下 `return r.channel.Close()`，
// 导致 defer 的 Unlock 不执行、后续所有操作抢锁永久阻塞（死锁）；
// 且通道非 nil 时直接 return，连接从未被关闭（泄漏）。
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn, pub := c.conn, c.pub
	c.conn, c.pub = nil, nil
	c.mu.Unlock()

	c.log.Infof(context.Background(), "开始关闭 RabbitMQ 客户端...")

	var errs []error
	if pub != nil && !pub.IsClosed() {
		if err := pub.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			errs = append(errs, fmt.Errorf("关闭发布通道: %w", err))
		}
	}
	if conn != nil && !conn.IsClosed() {
		if err := conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			errs = append(errs, fmt.Errorf("关闭连接: %w", err))
		}
	}

	c.cancel()

	// 等 basic.return 派发协程退出（通道关闭后它会自行结束）。
	// rebuildMu 保证此时没有重建在进行，Add 不会与 Wait 并发。
	c.rebuildMu.Lock()
	c.rebuildMu.Unlock()
	waitGroupTimeout(&c.drainWG, 3*time.Second)

	c.log.Infof(context.Background(), "RabbitMQ 客户端已关闭")
	return errors.Join(errs...)
}

// Closed 报告客户端是否已关闭。
func (c *Client) Closed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

// Config 返回生效后的配置（已补齐默认值、已展开延迟拓扑）。
func (c *Client) Config() Config { return c.cfg }

// HealthCheck 验证当前能否取得可用连接，可用于探活接口。
func (c *Client) HealthCheck(ctx context.Context) error {
	conn, err := c.ensureConn(ctx)
	if err != nil {
		return err
	}
	if conn.IsClosed() {
		return fmt.Errorf("%w: 连接已关闭", ErrNotConnected)
	}
	return nil
}

// ---------------------------------------------------------------------------
// 连接与通道的懒自愈
// ---------------------------------------------------------------------------

// ensure 返回可用的发布通道，必要时重建连接、重放拓扑、重建通道。
//
// 采用"先无锁快路径、再加锁重建、加锁后双检"的写法：
// 正常情况下只读一次 RWMutex 就返回，重建只在真的失效时发生一次。
func (c *Client) ensure(ctx context.Context) (*amqp.Channel, error) {
	if ch := c.livePub(); ch != nil {
		return ch, nil
	}

	c.rebuildMu.Lock()
	defer c.rebuildMu.Unlock()

	// 双检：等锁期间可能已被其他 goroutine 修好
	if ch := c.livePub(); ch != nil {
		return ch, nil
	}
	if c.Closed() {
		return nil, ErrClosed
	}

	conn := c.liveConn()
	fresh := false
	if conn == nil {
		var err error
		conn, fresh, err = c.reconnect(ctx, "publish")
		if err != nil {
			return nil, err
		}
	}

	// 走到这里有两种情况：刚建了一条新连接，或在仍然活着的连接上重建通道。
	//
	// 后者同样值得观测：它意味着通道被 broker 关闭而连接没断 ——
	// 典型原因是队列被删（404）或声明属性不一致（406）。
	// 这正是旧版最严重的盲区（只监听连接关闭，通道关了永远不触发重连），
	// 现在把它的发生次数暴露出来。
	chStart := time.Now()

	ch, chErr := c.buildPubChannel(conn)
	if !fresh {
		c.observe(metrics.OpChannelRebuild, chStart, chErr, "publish")
	}
	if chErr != nil {
		if fresh {
			// 新建的连接没能用上，关掉以免泄漏
			_ = conn.Close()
		}
		return nil, chErr
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = ch.Close()
		if fresh {
			_ = conn.Close()
		}
		return nil, ErrClosed
	}
	c.conn, c.pub = conn, ch
	return ch, nil
}

// ensureConn 返回可用连接，必要时重连并重放拓扑。
func (c *Client) ensureConn(ctx context.Context) (*amqp.Connection, error) {
	if conn := c.liveConn(); conn != nil {
		return conn, nil
	}

	c.rebuildMu.Lock()
	defer c.rebuildMu.Unlock()

	if conn := c.liveConn(); conn != nil {
		return conn, nil
	}
	if c.Closed() {
		return nil, ErrClosed
	}

	conn, _, err := c.reconnect(ctx, "connection")
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = conn.Close()
		return nil, ErrClosed
	}
	c.conn = conn
	return conn, nil
}

// reconnect 建一条新连接并在其上重放拓扑，同时上报 reconnect 事件。
//
// 抽取出来是为了让 ensure 与 ensureConn 共用同一份"建连 + 重放拓扑 + 上报"
// 的逻辑 —— 两处各写一遍很容易在其中一处漏掉事件或漏掉拓扑重放。
//
// 返回的 fresh 恒为 true（成功时），保留它只是让调用点读起来更清楚。
func (c *Client) reconnect(ctx context.Context, purpose string) (conn *amqp.Connection, fresh bool, err error) {
	start := time.Now()

	conn, err = c.dialWithRetry(ctx)
	if err == nil {
		// 新连接上必须重放拓扑：broker 重启、vhost 重建、
		// auto-delete 队列被回收，都会让之前的声明消失。
		if terr := applyTopology(conn, c.cfg); terr != nil {
			_ = conn.Close()
			conn, err = nil, terr
		}
	}

	// 首次建连不报 reconnect：它也走这条路径，但把启动时的第一次连接
	// 记成"重连过一次"，会让这个指标从一开始就偏大，且掩盖真实的重连。
	if c.markConnected() {
		c.observe(metrics.OpReconnect, start, err, purpose)
	}

	if err != nil {
		return nil, false, err
	}
	return conn, true, nil
}

// markConnected 把状态标记为已连接，并返回此前是否已经连过。
//
// 返回 true 表示这是一次真正的重连。
func (c *Client) markConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	already := c.connected
	c.connected = true
	return already
}

// channel 为消费者创建一条独占通道。
//
// 独占的意义：amqp 的 Qos 作用于整条通道，旧版让所有消费者共用一条通道，
// 于是后启动的消费者会覆盖先启动者的 prefetch。独占后各消费者互不干扰。
// 调用方负责在消费结束时 Close 该通道。
func (c *Client) channel(ctx context.Context) (*amqp.Channel, error) {
	conn, err := c.ensureConn(ctx)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("%w: 打开通道失败: %v", ErrNotConnected, err)
	}
	return ch, nil
}

// livePub 返回未失效的发布通道；nil 表示需要重建。
//
// 注意 amqp 的 IsClosed 不是 nil 安全的（内部直接解引用 atomic 字段），
// 所以必须先判空再调用。
func (c *Client) livePub() *amqp.Channel {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed || c.pub == nil || c.pub.IsClosed() {
		return nil
	}
	return c.pub
}

// liveConn 返回未失效的连接；nil 表示需要重连。
func (c *Client) liveConn() *amqp.Connection {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed || c.conn == nil || c.conn.IsClosed() {
		return nil
	}
	return c.conn
}

// buildPubChannel 在给定连接上建立发布通道：声明拓扑 → 开启 confirm → 接收 basic.return。
func (c *Client) buildPubChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("%w: 打开发布通道失败: %v", ErrNotConnected, err)
	}

	if c.cfg.Confirm {
		// Confirm 必须开启：否则 Publish 返回 nil 只代表"帧写进了 socket"，
		// 不代表 broker 收下了消息，也就没有任何持久化保证可言。
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			return nil, fmt.Errorf("%w: 开启 publisher confirm 失败: %v", ErrTopology, err)
		}
	}

	// NotifyReturn 会在通道关闭时被库关闭，因此这里的 range 会自动退出。
	returns := ch.NotifyReturn(make(chan amqp.Return, 64))
	c.drainWG.Add(1)
	go func() {
		defer c.drainWG.Done()
		for r := range returns {
			// 退回消息是"消息没进任何队列"的**唯一可靠**信号：
			// Publish 的同步判定受 ReturnWindow 限制可能漏，
			// 而异步回调需要调用方自己注册 SetReturnHandler。
			// 上报成事件后，"不可路由"从一个只能靠日志发现的问题变成可告警的指标。
			c.observe(metrics.OpReturn, time.Now(), ErrUnroutable, fmt.Sprintf(
				"%s / %s (code=%d %s)", r.Exchange, r.RoutingKey, r.ReplyCode, r.ReplyText))

			// 先给异步回调，保证不因同步窗口错过而漏掉退回消息。
			if h := c.returnHandler(); h != nil {
				h(r)
			}
			v, ok := c.waiters.Load(r.MessageId)
			if !ok {
				continue
			}
			select {
			case v.(chan amqp.Return) <- r:
			default: // 发布方已放弃等待，丢弃即可
			}
		}
	}()

	return ch, nil
}

// dialWithRetry 按 Config.DialAttempts 建连，失败按指数退避重试。
func (c *Client) dialWithRetry(ctx context.Context) (*amqp.Connection, error) {
	dialCfg, err := c.cfg.dialConfig()
	if err != nil {
		return nil, err
	}
	uri := c.cfg.amqpURI()
	safeURI := utils.SanitizeDSN(uri)

	backoff := c.cfg.DialBackoff
	for attempt := 1; ; attempt++ {
		conn, err := amqp.DialConfig(uri, dialCfg)
		if err == nil {
			if attempt > 1 {
				c.log.Infof(ctx, "连接 %s 成功（第 %d 次尝试）", safeURI, attempt)
			}
			return conn, nil
		}

		// 达到尝试上限（DialAttempts 为负表示无限重试）
		if c.cfg.DialAttempts > 0 && attempt >= c.cfg.DialAttempts {
			return nil, fmt.Errorf("%w: 连接 %s 失败（已尝试 %d 次）: %v",
				ErrNotConnected, safeURI, attempt, err)
		}

		c.log.Warnf(ctx, "连接 %s 失败（第 %d 次），%s 后重试: %v", safeURI, attempt, backoff, err)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("%w: %v", ErrNotConnected, ctx.Err())
		case <-c.ctx.Done():
			timer.Stop()
			return nil, ErrClosed
		case <-timer.C:
		}

		if backoff < c.cfg.DialMaxBackoff {
			backoff *= 2
			if backoff > c.cfg.DialMaxBackoff {
				backoff = c.cfg.DialMaxBackoff
			}
		}
	}
}

// applyTopology 幂等地声明交换机、队列与绑定。
//
// 顺序很重要：先交换机、再队列、最后绑定。任一步失败都返回错误——
// 此时通道已被 broker 关闭，因此本函数在内部用临时通道完成声明，
// 失败不会污染连接上其他正在使用的通道。
//
// 声明失败常见原因是"同名但属性不同"（如 durable 由 false 改成 true），
// broker 会回 406 PRECONDITION_FAILED 并说明到底哪个属性不一致。
// RabbitMQ 不允许修改已存在实体的属性，需要先删除重建或改名。
func applyTopology(conn *amqp.Connection, cfg Config) error {
	if len(cfg.Exchanges) == 0 && len(cfg.Queues) == 0 && len(cfg.Bindings) == 0 {
		return nil
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("%w: 创建拓扑声明通道失败: %v", ErrTopology, err)
	}
	defer func() { _ = ch.Close() }()

	for _, ex := range cfg.Exchanges {
		args := ex.Args
		if ex.AlternateExchange != "" {
			args = mergeTable(args, amqp.Table{"x-alternate-exchange": ex.AlternateExchange})
		}
		if err := ch.ExchangeDeclare(
			ex.Name, string(ex.Kind), ex.Durable, ex.AutoDelete, ex.Internal, false, args,
		); err != nil {
			return fmt.Errorf("%w: 声明交换机 %s(%s) 失败: %v", ErrTopology, ex.Name, ex.Kind, err)
		}
	}

	for _, q := range cfg.Queues {
		if _, err := ch.QueueDeclare(q.Name, q.Durable, q.AutoDelete, q.Exclusive, false, q.Args); err != nil {
			return fmt.Errorf("%w: 声明队列 %s 失败: %v", ErrTopology, q.Name, err)
		}
	}

	for _, b := range cfg.Bindings {
		if err := ch.QueueBind(b.Queue, b.RoutingKey, b.Exchange, false, b.Args); err != nil {
			return fmt.Errorf("%w: 绑定 %s → %s (routing_key=%q) 失败: %v",
				ErrTopology, b.Exchange, b.Queue, b.RoutingKey, err)
		}
	}
	return nil
}

// mergeTable 返回 base 的副本并覆盖 extra，避免修改调用方传入的 map。
func mergeTable(base, extra amqp.Table) amqp.Table {
	out := make(amqp.Table, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// waitGroupTimeout 等待 wg 归零，超时后放弃等待并返回 false。
// 用于收尾阶段避免无限阻塞。
func waitGroupTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}
