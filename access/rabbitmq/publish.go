package rabbitmq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/zavierswong/go-infra/metrics"
)

// PublishOption 定制单次发布。所有选项都是可选叠加的。
type PublishOption func(*publishOptions)

type publishOptions struct {
	exchange      string
	routingKey    string
	mandatory     bool
	persistent    bool
	contentType   string
	priority      uint8
	expiration    time.Duration
	headers       amqp.Table
	messageID     string
	correlationID string
	appID         string
	msgType       string
	replyTo       string
	delay         time.Duration
}

func newPublishOptions() publishOptions {
	return publishOptions{
		persistent:  true, // 默认持久化：消息可靠性优先，需要牺牲时用 Transient 显式关掉
		contentType: "application/json",
	}
}

func (o *publishOptions) validate() error {
	if o.exchange == "" && o.routingKey == "" {
		return fmt.Errorf("%w: 必须用 ToQueue 或 ToExchange 指定投递目标", ErrInvalidConfig)
	}
	if o.delay < 0 {
		return fmt.Errorf("%w: delay 不能为负", ErrInvalidConfig)
	}
	return nil
}

// ToExchange 投递到指定交换机的指定 routing key。
//
// 这是使用路由模式的入口：
//   - direct：routing key 需与绑定完全一致；
//   - topic：routing key 用 "." 分段，可被 "*" / "#" 模式匹配；
//   - fanout：routing key 被忽略，消息广播到所有绑定队列；
//   - headers：routing key 被忽略，按 headers 匹配。
func ToExchange(exchange, routingKey string) PublishOption {
	return func(o *publishOptions) {
		o.exchange = exchange
		o.routingKey = routingKey
	}
}

// ToQueue 直接投递到队列（走默认交换机，routing key 即队列名）。
// 这是最简单的工作队列用法，不涉及任何自定义交换机。
func ToQueue(queue string) PublishOption {
	return func(o *publishOptions) {
		o.exchange = ""
		o.routingKey = queue
	}
}

// WithMandatory 声明"消息不可路由时必须报错"。
//
// 不开这个开关时，投递到一个不存在的队列/无匹配绑定的 exchange，
// broker 会把消息直接丢弃，而 Publish 返回 nil —— 调用方完全无从感知。
// 开启后若消息被退回，Publish 返回 ErrUnroutable。
func WithMandatory() PublishOption {
	return func(o *publishOptions) { o.mandatory = true }
}

// Transient 把消息标记为非持久化。
// 默认是持久化（配合 durable 队列，broker 重启后消息仍在）；
// 只有能容忍重启丢消息时才关掉它。
func Transient() PublishOption {
	return func(o *publishOptions) { o.persistent = false }
}

// WithContentType 设置 Content-Type，默认 application/json。
func WithContentType(ct string) PublishOption {
	return func(o *publishOptions) { o.contentType = ct }
}

// WithPriority 设置消息优先级，需要目标队列声明了 x-max-priority（见 PriorityArgs）。
func WithPriority(p uint8) PublishOption {
	return func(o *publishOptions) { o.priority = p }
}

// WithExpiration 设置消息级 TTL。到期未被消费的消息会被丢弃或走死信。
//
// 注意：消息级 TTL 只在"到达队头"时才被检查，因此队列里混用不同 TTL 会出现
// 队头阻塞（长 TTL 的消息挡住后面短 TTL 的消息）。需要精确延迟请用 PublishDelay。
func WithExpiration(d time.Duration) PublishOption {
	return func(o *publishOptions) { o.expiration = d }
}

// WithHeaders 附加自定义 headers；headers 交换机的路由依据就是它。
func WithHeaders(h amqp.Table) PublishOption {
	return func(o *publishOptions) { o.headers = mergeTable(o.headers, h) }
}

// WithMessageID 设置消息 ID。消费方可用它做幂等去重。
// mandatory 发布且未显式设置时，本包会自动生成一个用于关联退回消息。
func WithMessageID(id string) PublishOption {
	return func(o *publishOptions) { o.messageID = id }
}

// WithCorrelationID 设置关联 ID，RPC 风格调用用它串起请求与响应。
func WithCorrelationID(id string) PublishOption {
	return func(o *publishOptions) { o.correlationID = id }
}

// WithType 设置业务消息类型，便于消费方按类型分发。
func WithType(t string) PublishOption {
	return func(o *publishOptions) { o.msgType = t }
}

// WithReplyTo 设置回复地址，RPC 风格调用用它告诉对方结果发到哪。
func WithReplyTo(q string) PublishOption {
	return func(o *publishOptions) { o.replyTo = q }
}

// WithAppID 设置来源应用标识。
func WithAppID(id string) PublishOption {
	return func(o *publishOptions) { o.appID = id }
}

// WithDelay 把消息改为延迟投递，延迟结束后才会进入业务队列。
//
// 这依赖 Config.Delay 声明的延迟拓扑（DLX + 队列级 TTL），
// 因此延迟档位必须是预先声明好的离散值：请求的档位若未声明，
// 消息会被延迟交换机丢弃（配合 WithMandatory 可以拿到明确报错）。
//
// 旧版用一个 per-message TTL 假装延迟：实测有消费者时消息 4ms 就到，
// 无消费者时消息到期被直接丢弃 —— 既不延迟也会丢消息。
func WithDelay(d time.Duration) PublishOption {
	return func(o *publishOptions) { o.delay = d }
}

// Publish 发布一条消息。
//
// 默认语义（可被选项改写）：
//   - 持久化投递（DeliveryMode=persistent）；
//   - 开启 publisher confirm，等待 broker 确认后才返回；
//   - 目标由 ToQueue / ToExchange 指定。
//
// 返回值：
//   - nil：broker 已确认收下（且开了 mandatory 时未被退回）；
//   - ErrNotConfirmed：broker nack 或确认超时，消息可能已丢失；
//   - ErrUnroutable：消息不可路由，已被退回，未进入任何队列；
//   - ErrClosed / ErrNotConnected / ErrPublish：客户端状态或链路问题。
//
// 重试语义是 at-least-once：若通道在投递过程中被关闭，本方法会在新通道上重投一次，
// 极端情况下同一条消息可能被投递两次，消费方应基于 MessageId 做幂等。
func (c *Client) Publish(ctx context.Context, body []byte, opts ...PublishOption) error {
	o := newPublishOptions()
	for _, fn := range opts {
		fn(&o)
	}
	if err := o.validate(); err != nil {
		// 参数错误在这里就返回，**不上报事件**：它连一次投递尝试都不算，
		// 计进去只会让"投递次数"这个分母虚高。
		return err
	}

	// 记下调用方本意的目标用于事件归因，必须赶在下面延迟改写之前取 ——
	// 排查时想知道的是"我想投到 sms.send"，而不是"投到了延迟交换机"。
	intent := publishDetail(&o)

	if o.delay > 0 {
		if c.cfg.Delay == nil {
			return fmt.Errorf("%w: 配置里没有 Config.Delay，无法投递延迟消息（delay=%s）",
				ErrInvalidConfig, o.delay)
		}
		o.exchange = c.cfg.Delay.ExchangeName()
		o.routingKey = DelayRoutingKey(o.delay)
	}

	start := time.Now()
	err := c.publish(ctx, body, &o)
	c.observe(metrics.OpPublish, start, err, intent)
	return err
}

// publish 是投递主体，由 Publish 包裹以便统一上报耗时。
//
// 耗时口径是整个 Publish 调用：含 publisher confirm 的等待，
// 也含通道失效后"重建 + 重投"的那一次。也就是调用方真实被阻塞的时长。
func (c *Client) publish(ctx context.Context, body []byte, o *publishOptions) error {
	msg := amqp.Publishing{
		ContentType:   o.contentType,
		Body:          body,
		Timestamp:     time.Now(),
		Headers:       o.headers,
		Priority:      o.priority,
		CorrelationId: o.correlationID,
		MessageId:     o.messageID,
		Type:          o.msgType,
		ReplyTo:       o.replyTo,
		AppId:         o.appID,
	}
	if o.persistent {
		msg.DeliveryMode = amqp.Persistent
	} else {
		msg.DeliveryMode = amqp.Transient
	}
	if o.expiration > 0 {
		msg.Expiration = strconv.FormatInt(o.expiration.Milliseconds(), 10)
	}

	// mandatory 需要按 MessageId 关联被退回的消息，缺省时自动生成一个。
	var retCh chan amqp.Return
	if o.mandatory {
		if msg.MessageId == "" {
			msg.MessageId = newMessageID()
		}
		retCh = make(chan amqp.Return, 1)
		// 业务自定义 MessageId 在并发下可能重复：后注册者会覆盖先注册者的
		// 等待通道，先者的退回消息就再无人接收（甚至被后者误领）。
		// 冲突时给 id 加唯一后缀，保证 MessageId 与等待通道一一对应。
		id := msg.MessageId
		for {
			if _, loaded := c.waiters.LoadOrStore(id, retCh); !loaded {
				break
			}
			id = msg.MessageId + "-" + newMessageID()
		}
		msg.MessageId = id
		defer c.waiters.Delete(id)
	}

	err := c.publishOnce(ctx, msg, *o, retCh)
	// 通道在投递途中被关闭（broker 关闭通道、网络抖动等）时重建并重投一次。
	// 只对"链路已失效"这一类错误重试，业务语义错误（如 nack）不重试。
	if err != nil && errors.Is(err, ErrPublish) {
		if ch := c.livePub(); ch == nil {
			c.log.Warnf(ctx, "发布通道已失效，重建后重投一次: %v", err)
			if retryErr := c.publishOnce(ctx, msg, *o, retCh); retryErr == nil {
				return nil
			}
		}
	}
	return err
}

// publishOnce 执行一次投递并等待确认。
func (c *Client) publishOnce(ctx context.Context, msg amqp.Publishing, o publishOptions, retCh chan amqp.Return) error {
	ch, err := c.ensure(ctx)
	if err != nil {
		return err
	}

	if !c.cfg.Confirm {
		if err := ch.PublishWithContext(ctx, o.exchange, o.routingKey, o.mandatory, false, msg); err != nil {
			return fmt.Errorf("%w: %v", ErrPublish, err)
		}
		return c.settleReturn(ctx, retCh, o)
	}

	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, o.exchange, o.routingKey, o.mandatory, false, msg)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPublish, err)
	}
	if dc == nil {
		// 未开启 confirm 模式时库会返回 nil，此时拿不到确认，只能视为已发出。
		return c.settleReturn(ctx, retCh, o)
	}

	waitCtx, cancel := context.WithTimeout(ctx, c.cfg.ConfirmTimeout)
	defer cancel()

	acked, err := dc.WaitContext(waitCtx)
	if err != nil {
		return fmt.Errorf("%w: 等待 broker 确认超时（%s）: %v", ErrNotConfirmed, c.cfg.ConfirmTimeout, err)
	}
	if !acked {
		return fmt.Errorf("%w: broker 返回 nack", ErrNotConfirmed)
	}
	return c.settleReturn(ctx, retCh, o)
}

// settleReturn 判定一次 mandatory 发布是否被退回。
//
// 时序约束：broker 对不可路由的消息**先发 basic.return、再回 confirm ack**，
// 但库把 return 写进本进程的缓冲通道后，还需要一次 goroutine 调度才能派发到这里的
// retCh，所以无法在 ack 返回的瞬间零窗口地判定，只能给一个很短的窗口。
// 该窗口只在调用方显式开启 mandatory 时付出，默认路径（未开 mandatory）零开销。
//
// 需要绝对不漏地观察退回消息时，请改用 SetReturnHandler 注册异步回调。
func (c *Client) settleReturn(ctx context.Context, retCh chan amqp.Return, o publishOptions) error {
	if !o.mandatory || retCh == nil {
		return nil
	}
	if c.cfg.ReturnWindow < 0 {
		return nil // 显式不等待：结果只走 SetReturnHandler
	}
	window := c.cfg.ReturnWindow
	if window == 0 {
		window = defaultReturnWindow
	}

	timer := time.NewTimer(window)
	defer timer.Stop()

	select {
	case r := <-retCh:
		return fmt.Errorf("%w: broker 退回 %d %s（exchange=%q routing_key=%q）",
			ErrUnroutable, r.ReplyCode, r.ReplyText, r.Exchange, r.RoutingKey)
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PublishDelay 投递一条延迟消息，delay 之后才会进入目标队列。
//
// 目标仍由 ToQueue / ToExchange 指定：延迟到期后消息会被转投到那里。
//
//	err := cli.PublishDelay(ctx, body, 30*time.Second, rabbitmq.ToQueue("sms.send"))
func (c *Client) PublishDelay(ctx context.Context, body []byte, delay time.Duration, opts ...PublishOption) error {
	return c.Publish(ctx, body, append(opts, WithDelay(delay))...)
}

// PublishToQueue 投递到队列（默认交换机），是最常用的点对点写法。
func (c *Client) PublishToQueue(ctx context.Context, queue string, body []byte, opts ...PublishOption) error {
	return c.Publish(ctx, body, append(opts, ToQueue(queue))...)
}

// SetReturnHandler 注册一个异步回调，broker 每退回一条消息就调用一次。
//
// 与 Publish 的同步判定互补：同步判定受 ReturnWindow 限制，回调则不会漏，
// 但回调在线程池里执行，不能阻塞太久。传 nil 取消注册。
func (c *Client) SetReturnHandler(fn func(amqp.Return)) {
	c.returnMu.Lock()
	c.onReturn = fn
	c.returnMu.Unlock()
}

func (c *Client) returnHandler() func(amqp.Return) {
	c.returnMu.RLock()
	defer c.returnMu.RUnlock()
	return c.onReturn
}

// newMessageID 生成唯一消息 ID（8 字节随机数的十六进制）。
func newMessageID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 随机源失效时退化为时间戳，仅用于关联，不承担安全职责。
		return fmt.Sprintf("m-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
