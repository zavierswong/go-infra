package kafka

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/zavierswong/go-infra/metrics"
)

// Handler 处理一条 Kafka 记录。
//
// 返回 nil → 本包标记"该记录已处理"，位移提交后不会再收到它；
// 返回 error → 本包**不标记**该记录。由于位移提交按"连续已标记"推进，
// 失败记录会被跳过提交，重启 / 再均衡后会从它开始重新消费 ——
// 即默认语义是 at-least-once，处理逻辑必须幂等。
//
// 参数是完整的 *kgo.Record：Key（保序依据）、Headers（追踪透传）、
// TopicPartitionOffset（定位重投）全部可取。
//
// ctx 在消费组停止时被取消，耗时的处理应尊重它以便优雅退出。
type Handler func(ctx context.Context, rec *kgo.Record) error

// GroupConfig 描述一个消费组。
type GroupConfig struct {
	// Group 消费组名（必填）。同组的成员分摊全部分区，
	// 每个分区内最多只有一个成员在消费。
	Group string

	// Topics 要订阅的主题列表（必填）。支持正则（需 broker 端允许 auto.create）。
	Topics []string

	// Handler 消息处理函数（必填）。
	Handler Handler

	// Concurrency 是并发处理协程数。0 或 1 表示串行处理，严格保序。
	//
	// 大于 1 时**不再保证分区内顺序**：Kafka 的顺序保证止于"分区内"，
	// 而并发处理打乱了这一点。保序需求请保持 1 并用 WithKey 把同类消息
	// 哈希进同一分区；吞吐需求才调大它（典型值 4 × CPU 核数起步）。
	//
	// 位移提交不受并发影响：提交按"每分区连续已处理的最大位移"推进，
	// 乱序完成时慢的那条会把提交点钉在原地，不会跳过未处理的消息。
	Concurrency int

	// InstanceID 是静态成员 ID。设置后滚动重启不会触发全组再均衡
	// （broker 会把旧成员的分区直接还回来），适合部署实例数固定的服务。
	// 为空表示动态成员（默认）。注意：静态成员离线超过 SessionTimeout
	// 仍会被踢出并触发再均衡。
	InstanceID string

	// FromOldest 在消费组**没有已提交位移**时从头开始消费。
	// 默认 false：从各分区最新位置开始（新组上线不会重放历史）。
	// 已有位移的消费组不受该选项影响。
	FromOldest bool

	// DisableAutoCommit 关闭自动提交。默认不关闭：
	// handler 处理成功的位移按 CommitInterval 自动提交（Marks 模式，
	// 提交的是"处理完的进度"而不是"拉取的进度"，语义与 AMQP 手动 ack 对齐）。
	// 需要与外部事务对齐（DB 提交成功后再提交位移）时才打开它，
	// 配合 Group.CommitSync 手动提交。
	DisableAutoCommit bool

	// CommitInterval 自动提交间隔，0 表示默认 5s。
	// 间隔越长，重复消费窗口越大（宕机后从上次提交处重放）。
	CommitInterval time.Duration

	// MaxPollRecords 是单次拉取派发给 handler 的记录数上限。0 表示默认 100。
	// 该值 × Concurrency 决定了在途记录的内存占用，不要无脑调大。
	MaxPollRecords int

	// StopOnHandlerError 为 true 时，任一 handler 失败都会让 Run 返回该错误。
	// 默认 false：失败只回调 OnError / 记日志，消息按上文语义等待重投。
	StopOnHandlerError bool

	// OnError 接收消费运行自身的错误（拉取失败、handler 失败、提交失败）。
	// 为 nil 时只记日志。回调会被多个协程并发调用，需自行保证线程安全。
	OnError func(err error)

	// DrainTimeout 是停止时等待在途处理结束的上限，0 表示默认 30s。
	// 超时后本包放弃等待：未处理完的记录没被标记，重启后会重投。
	DrainTimeout time.Duration
}

// 默认值集中在此，便于测试与文档对齐。
const (
	defaultCommitInterval = 5 * time.Second
	defaultMaxPollRecords = 100
	defaultDrainTimeout   = 30 * time.Second
)

// Group 是一个消费组实例。
//
// franz-go 要求一个消费组独占一个底层客户端（生产与多个消费组
// 混用同一客户端会让再均衡语义变模糊），因此 NewGroup 会按
// 父 Client 的配置**另建**一条连接。该连接由 Group 自行管理，
// Run 结束或 Close 时关闭，不影响父 Client 的生产路径。
type Group struct {
	client *Client // 父客户端：只借用其 cfg / log / obs，不碰其 kgo.Client
	cfg    GroupConfig

	cli *kgo.Client

	jobs chan *kgo.Record
	wg   sync.WaitGroup
	stop atomic.Bool

	// cancel 由 Run 写入、Close 读取 —— 两者通常在不同 goroutine，
	// 因此必须用 atomic 承载；早先是裸字段，Run+Close 组合必然构成
	// 数据竞争（Close 的存在意义恰恰是"从外部停止正在跑的 Run"）。
	cancel atomic.Pointer[context.CancelFunc]

	// closed 记录"调用方已请求停止"。有了它，Close 早于 Run 时也能
	// 生效：Run 启动后立刻退出，不会因为无人取消而一直消费。
	closed atomic.Bool

	// cliOnce 保证底层客户端只关闭一次：Close（Run 未启动时）与
	// Run 收尾两条路径都会尝试关闭。
	cliOnce sync.Once

	handlerErr atomic.Pointer[error]

	// done 在 Run 退出后置位：此后底层客户端已关闭，Status 不再读水位。
	done atomic.Bool

	// track 维护"每分区连续成功水位"，nil 表示 DisableAutoCommit。
	// 见 groupTracker 的说明 —— 它是把"失败不标记"变成真正可提交
	// 语义的关键，不能直接用 MarkCommitRecords（marks 取最大 offset，
	// 会让提交点越过失败记录）。
	track *groupTracker
}

// NewGroup 校验配置并创建一个消费组（此时还未开始消费、未建连）。
func (c *Client) NewGroup(cfg GroupConfig) (*Group, error) {
	if cfg.Group == "" {
		return nil, fmt.Errorf("%w: GroupConfig.Group 不能为空", ErrInvalidConfig)
	}
	if len(cfg.Topics) == 0 {
		return nil, fmt.Errorf("%w: GroupConfig.Topics 不能为空", ErrInvalidConfig)
	}
	if cfg.Handler == nil {
		return nil, fmt.Errorf("%w: GroupConfig.Handler 不能为空", ErrInvalidConfig)
	}
	if cfg.Concurrency < 0 {
		return nil, fmt.Errorf("%w: GroupConfig.Concurrency 不能为负", ErrInvalidConfig)
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}
	if cfg.CommitInterval <= 0 {
		cfg.CommitInterval = defaultCommitInterval
	}
	if cfg.MaxPollRecords <= 0 {
		cfg.MaxPollRecords = defaultMaxPollRecords
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = defaultDrainTimeout
	}
	autoCommit := !cfg.DisableAutoCommit

	pc := c.cfg // 生产端配置复用：brokers / tls / sasl / client_id / dial_timeout
	opts := []kgo.Opt{
		kgo.SeedBrokers(pc.Brokers...),
		kgo.ClientID(pc.ClientID),
		kgo.DialTimeout(pc.DialTimeout),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.WithHooks(hookConnectErrors(c)),
	}
	if cfg.InstanceID != "" {
		opts = append(opts, kgo.InstanceID(cfg.InstanceID))
	}
	if cfg.FromOldest {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	} else {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}
	if autoCommit {
		// Marks 模式：提交的不是"拉取进度"，而是"handler 处理完的进度"。
		// 这是"处理完才提交"的关键，语义与 AMQP 的手动 ack 对齐。
		opts = append(opts, kgo.AutoCommitMarks(), kgo.AutoCommitInterval(cfg.CommitInterval))
	} else {
		opts = append(opts, kgo.DisableAutoCommit())
	}
	if pc.TLS.Enable {
		tlsCfg, err := buildTLS(pc.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	if pc.SASL.enabled() {
		mech, err := pc.SASL.mechanism()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mech))
	}

	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: 创建消费客户端失败: %v", ErrInvalidConfig, err)
	}

	g := &Group{
		client: c,
		cfg:    cfg,
		cli:    cli,
		jobs:   make(chan *kgo.Record, cfg.MaxPollRecords),
	}
	if autoCommit {
		g.track = newGroupTracker(cli)
	}
	return g, nil
}

// RunGroup 是"创建并运行消费组"的便捷写法（阻塞）。
// 需要独立控制生命周期时请用 NewGroup + Run。
func (c *Client) RunGroup(ctx context.Context, cfg GroupConfig) error {
	g, err := c.NewGroup(cfg)
	if err != nil {
		return err
	}
	return g.Run(ctx)
}

// Run 开始消费并阻塞，直到 ctx 被取消、调用了 Close、
// 或 StopOnHandlerError 下 handler 失败。
//
// broker 断连、再均衡都在内部自动恢复，调用方无需重试；
// Run 返回非 nil 的情况只有：StopOnHandlerError 触发、
// 消费客户端无法创建（配置错误，在 NewGroup 就会暴露）。
//
// Run 每个实例只能调用一次；需要再次消费请新建 Group。
//
// 若 Close 先于 Run 被调用，Run 会立即返回（底层客户端已由 Close 关闭）。
func (g *Group) Run(ctx context.Context) error {
	if g.stop.Swap(true) {
		return fmt.Errorf("%w: Run 只能调用一次", ErrConsume)
	}

	ctx, cancel := context.WithCancel(ctx)
	g.cancel.Store(&cancel)
	defer func() {
		g.cancel.Store(nil)
		cancel()
	}()
	defer g.done.Store(true)

	// Close 早于 Run：已请求停止，不再启动消费。
	if g.closed.Load() {
		return nil
	}

	for i := 0; i < g.cfg.Concurrency; i++ {
		g.wg.Add(1)
		go g.worker(ctx)
	}

	g.client.log.Infof(ctx, "消费组已启动：group=%s topics=%v concurrency=%d autocommit=%v",
		g.cfg.Group, g.cfg.Topics, g.cfg.Concurrency, !g.cfg.DisableAutoCommit)

	var runErr error
	for runErr == nil {
		fetches := g.cli.PollRecords(ctx, g.cfg.MaxPollRecords)
		if err := ctx.Err(); err != nil {
			break
		}
		// 拉取级错误（主题不存在、权限等）与单条记录无关，只上报不中断：
		// 它们大多会被 broker 恢复或 metadata 刷新自愈。
		fetches.EachError(func(topic string, part int32, err error) {
			g.report(fmt.Errorf("group=%s 拉取 %s[%d] 失败: %w", g.cfg.Group, topic, part, err))
		})

		// 逐条派发给 worker。注意 RecordIter 的官方用法是 post 语句留空
		//（Next() 自带"读取并前进"语义）：旧实现在 post 和 body 里各调
		// 一次 Next()，每轮前进两格 —— 一半消息被静默丢弃，且记录数为
		// 奇数时会在已排空的迭代器上越界 panic（Next() 内部是裸下标）。
		for it := fetches.RecordIter(); !it.Done(); {
			rec := it.Next()
			select {
			case g.jobs <- rec:
				g.trackDispatch(rec)
			case <-ctx.Done():
			}
			if ctx.Err() != nil {
				break
			}
		}

		if e := g.handlerErr.Load(); e != nil {
			runErr = *e // StopOnHandlerError：带着原始错误退出
		}
	}

	g.drain()
	g.commitFinal(ctx)
	g.closeClient()
	return runErr
}

// closeClient 关闭底层消费客户端（幂等）。
//
// 正常路径由 Run 收尾调用；但 Close 先于 Run 调用时 Run 根本不会执行，
// 此时必须由 Close 亲自关闭，否则该客户端（含 franz-go 的后台协程与
// 连接）会永久泄漏。两条路径都走这里，用 Once 去重。
func (g *Group) closeClient() {
	g.cliOnce.Do(func() { g.cli.Close() })
}

// Close 停止消费组：取消 Run 的循环、等待在途处理结束、做最终位移提交。
//
// 可在 Run 期间调用（从任意 goroutine），也可在 Run 之前调用 ——
// 后者会让随后的 Run 立即返回，不会出现"Close 无效、消费照跑"的情况。
// 可重复调用。
func (g *Group) Close() {
	g.closed.Store(true)

	if c := g.cancel.Load(); c != nil {
		(*c)() // Run 正在跑：由它收尾并关闭底层客户端。
		return
	}
	// Run 未启动或已结束：底层客户端没有别人会关，这里必须关掉。
	g.closeClient()
}

// Status 返回消费组的状态快照，形状与 Client.Status 相同（复用 Status 类型）。
//
// 与生产端 Status 的两点差异：
//
//  1. 水位只看 Fetch 侧 —— 消费组客户端不生产，Produce 侧恒为 0；
//     FetchBuffered* 是"已拉取但 handler 还没处理完"的积压，
//     持续增长说明 handler 或下游成了瓶颈；
//  2. Closed 表示 Run 是否已退出（而非应用层 Close 调用）——
//     退出后底层客户端已关闭，水位恒为 0。
//
// Group 字段填消费组名，映射到监控的 Instance 标签时建议带上它
// （如 name+"/"+group），避免与父 Client 的生产端序列混叠。
func (g *Group) Status() Status {
	done := g.done.Load()
	st := Status{
		Instance:    g.client.cfg.Name,
		Group:       g.cfg.Group,
		ClientID:    g.client.cfg.ClientID,
		SeedBrokers: g.client.cfg.Brokers,
		Closed:      done,
	}
	if done || g.cli == nil {
		return st
	}
	st.FetchBufferedRecords = g.cli.BufferedFetchRecords()
	st.FetchBufferedBytes = g.cli.BufferedFetchBytes()
	return st
}

// worker 是处理协程主体：从 jobs 取记录 → handler → 成功则标记提交。
func (g *Group) worker(ctx context.Context) {
	defer g.wg.Done()
	for rec := range g.jobs {
		if err := g.handle(ctx, rec); err != nil {
			if g.cfg.StopOnHandlerError {
				// 只保留第一个失败作为 Run 的返回值。
				// cancel 是 atomic 承载的（Close 会从别的 goroutine 读它）；
				// 此时为 nil 只可能是 Run 已收尾，无需取消。
				if g.handlerErr.CompareAndSwap(nil, &err) {
					if c := g.cancel.Load(); c != nil {
						(*c)()
					}
				}
			} else {
				g.report(fmt.Errorf("group=%s 处理失败: %w", g.cfg.Group, err))
			}
		}
	}
}

// handle 执行一次处理并按结果维护连续成功水位。耗时口径是整个 handler 执行。
func (g *Group) handle(ctx context.Context, rec *kgo.Record) error {
	start := time.Now()
	err := g.safeHandle(ctx, rec)
	g.client.observeEvent(metrics.Event{
		Op:       metrics.OpConsume,
		Duration: time.Since(start),
		Err:      err,
		Detail:   recordDetail(rec),
	})
	if err != nil {
		// 失败记录是"不可提交的栅栏"：连续成功水位被钉在它身上，
		// 之后同分区的记录即使全部成功也不会提交 —— 重启/再均衡后
		// 从失败者开始重投（at-least-once）。
		if g.track != nil {
			g.track.settle(rec, false)
		}
		return err
	}
	// 成功：尝试把连续成功水位向前推进并标记。只有水位真正前进时
	// 才会产生一次 mark（见 groupTracker.settle）。
	if g.track != nil {
		g.track.settle(rec, true)
	}
	return nil
}

// safeHandle 调用 handler 并把 panic 转成普通错误。
// 否则一个越界的下标就会让整个进程崩掉，且该记录永远等不到重投。
func (g *Group) safeHandle(ctx context.Context, rec *kgo.Record) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	return g.cfg.Handler(ctx, rec)
}

// drain 停止派发并等待在途处理结束，超时则记录告警后放弃。
func (g *Group) drain() {
	close(g.jobs)
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(g.cfg.DrainTimeout):
		g.client.log.Warnf(context.Background(),
			"消费组 %s 停止时仍有处理协程未结束（已等待 %s）",
			g.cfg.Group, g.cfg.DrainTimeout)
	}
}

// commitFinal 做停止前的最终位移提交，并上报 commit 事件。
// 无论 DisableAutoCommit 与否都尝试一次：关闭自动提交且从未手动提交时，
// 这里给了调用方一次"至少把处理完的提交掉"的机会。
func (g *Group) commitFinal(ctx context.Context) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.cfg.CommitInterval)
	defer cancel()

	start := time.Now()
	err := g.cli.CommitMarkedOffsets(cctx)
	if errors.Is(err, kgo.ErrClientClosed) {
		return
	}
	var obErr error
	if err != nil {
		obErr = fmt.Errorf("%w: %w", ErrCommit, err)
	}
	g.client.observe(metrics.OpCommit, start, obErr, g.cfg.Group)
	if err != nil {
		g.report(fmt.Errorf("group=%s 最终位移提交失败: %w", g.cfg.Group, err))
	}
}

// CommitSync 手动提交当前所有已标记的位移（DisableAutoCommit 时使用）。
//
// 典型用法是与外部存储的事务对齐：DB 事务提交成功后再提交位移，
// 避免"位移走了、数据没落库"（消息丢失）或反过来（消息重复）。
func (g *Group) CommitSync(ctx context.Context) error {
	start := time.Now()
	err := g.cli.CommitMarkedOffsets(ctx)
	if err != nil {
		err = fmt.Errorf("%w: %w", ErrCommit, err)
	}
	g.client.observe(metrics.OpCommit, start, err, g.cfg.Group)
	return err
}

// trackDispatch / trackSettle 是 tracker 的 nil 安全包装
// （DisableAutoCommit 时 track 为 nil，不维护水位）。
func (g *Group) trackDispatch(rec *kgo.Record) {
	if g.track != nil {
		g.track.dispatch(rec)
	}
}

func (g *Group) trackSettle(rec *kgo.Record, ok bool) {
	if g.track != nil {
		g.track.settle(rec, ok)
	}
}

// report 输出消费侧错误：优先回调 OnError，否则记日志。
func (g *Group) report(err error) {
	if err == nil {
		return
	}
	if g.cfg.OnError != nil {
		g.cfg.OnError(err)
		return
	}
	g.client.log.Errorf(context.Background(), "%v", err)
}

// recordDetail 描述一条被处理的记录，用于归因。
// 只放定位信息（主题/分区/位移），**不放消息体** —— 消息体是业务数据。
func recordDetail(rec *kgo.Record) string {
	return fmt.Sprintf("%s[%d]@%d", rec.Topic, rec.Partition, rec.Offset)
}

// ---- 连续成功水位：把"失败不标记"变成真正可提交的语义 ----

// tpKey 标识一个 topic 分区。
type tpKey struct {
	topic string
	part  int32
}

// groupTracker 维护"每分区连续成功水位"，只在水位真正前进时调用
// MarkCommitOffsets。
//
// 为什么不能用 MarkCommitRecords：franz-go 的 marks 语义是"已标记记录
// 中的最大 offset"，**没有任何连续性检查**（consumer_group.go 中 head
// 直接取 max，且官方文档明确 "does not allow rewinds"）。于是默认配置
// 下（StopOnHandlerError=false，失败后继续消费后续记录）：
//
//	rec0 失败(未 mark) → rec1..rec9 成功(mark) → 提交点 = 10
//
// 提交点一越过 rec0 就再也无法回退 —— rec0 既不会被当前进程重投，
// 重启后也不会重放，at-least-once 被静默打破。Concurrency > 1 时
// 乱序完成让这个窗口更大。
//
// 这里改为：每个分区维护"最小未成功 offset"，它就是可提交的水位 ——
// 失败记录成为不可提交的栅栏，水位之后的记录即使全部成功也不会提交，
// 重启/再均衡后从失败者开始重投。这正是 Handler 文档承诺的语义。
//
// 内存：每个分区 O(1)（inflight 只含在途记录，失败只记最小的一个 ——
// 失败不重试，最小失败者本身就是永久栅栏，更大 offset 的失败不影响
// 水位，无需逐一记录）。
type groupTracker struct {
	cli *kgo.Client

	mu    sync.Mutex
	parts map[tpKey]*partProgress
}

// partProgress 是一个分区的连续成功水位。
type partProgress struct {
	// next 是下一个期望派发的 offset（即"last dispatched + 1"）。
	// 派发时若 offset 出现空洞（如事务控制记录被驱动过滤），空洞
	// 视为已消费，next 直接跳过去。
	next int64
	// inflight 记录已派发、尚未结算（handler 未返回）的 offset 及其
	// 次数（同 offset 重复派发只会在 rewind 后出现）。
	inflight map[int64]int
	// failedMin 是最早失败的 offset。失败不会被本进程重试，它是
	// 永久栅栏；更大的失败 offset 无需记录（水位钉在 failedMin）。
	hasFailed bool
	failedMin int64
	// lastEpoch 是最近一次派发记录的 leader epoch，mark 时随 head 上报。
	lastEpoch int32
	// marked 是已向 client mark 过的水位，-1 表示从未（marks 不允许
	// 回退，head 没有前进时不重复 mark）。
	marked int64
}

func newGroupTracker(cli *kgo.Client) *groupTracker {
	return &groupTracker{cli: cli, parts: make(map[tpKey]*partProgress)}
}

// dispatch 在记录派发给 worker 之前登记在途。未派发成功的记录
// （ctx 取消分支）不会走到这里，因此不会污染水位。
func (t *groupTracker) dispatch(rec *kgo.Record) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := tpKey{rec.Topic, rec.Partition}
	p := t.parts[key]
	if p == nil {
		p = &partProgress{next: rec.Offset + 1, marked: -1, inflight: make(map[int64]int)}
		t.parts[key] = p
	}
	p.inflight[rec.Offset]++
	if rec.Offset >= p.next {
		p.next = rec.Offset + 1 // offset 空洞（过滤掉的控制记录）：视为已消费
	}
	p.lastEpoch = rec.LeaderEpoch
}

// settle 在 handler 返回后结算：成功则尝试推进水位并 mark，失败则
// 把水位钉在自己身上。
func (t *groupTracker) settle(rec *kgo.Record, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	p := t.parts[tpKey{rec.Topic, rec.Partition}]
	if p == nil {
		return // 不可达：settle 之前必有 dispatch；防御而已
	}
	if n := p.inflight[rec.Offset]; n > 1 {
		p.inflight[rec.Offset] = n - 1
	} else {
		delete(p.inflight, rec.Offset)
	}
	if ok {
		p.lastEpoch = rec.LeaderEpoch
	} else if !p.hasFailed || rec.Offset < p.failedMin {
		p.hasFailed = true
		p.failedMin = rec.Offset
	}

	head := t.headLocked(p)
	if head <= p.marked {
		return // 水位没有前进（或 rewind），marks 不允许回退
	}
	p.marked = head
	t.cli.MarkCommitOffsets(map[string]map[int32]kgo.EpochOffset{
		rec.Topic: {rec.Partition: {Epoch: p.lastEpoch, Offset: head}},
	})
}

// headLocked 计算可提交水位：全部结算到底时是 next，否则是最小的
// 未成功 offset（在途或失败）。
func (t *groupTracker) headLocked(p *partProgress) int64 {
	head := p.next
	if p.hasFailed && p.failedMin < head {
		head = p.failedMin
	}
	for off := range p.inflight {
		if off < head {
			head = off
		}
	}
	return head
}
