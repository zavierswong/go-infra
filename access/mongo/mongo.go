// Package mongo 封装 MongoDB 的连接与**连接池**管理。
//
// 在 go.mongo.org/mongo-driver/v2 之上，补齐了六件容易写错的事：
//
//  1. **可信的初始化结果**。`mongo.Connect` **不验证**目标可达
//     （驱动文档原话：does not validate that the MongoDB deployment is reachable），
//     配置写对了它就返回成功。本包补一次带超时的真实 Ping，
//     于是"URI 拼错、账号不对、副本集名不符"全部提前到启动期暴露。
//  2. **可控的启动耗时**。驱动默认 ServerSelectionTimeout=30s，
//     意味着连不上时要挂 30 秒；再乘上重试次数就是几分钟的启动等待。
//     本包用独立于 URI 的 DialProbeTimeout 给每次尝试封顶，
//     最坏耗时 = DialAttempts × DialProbeTimeout，可算可控。
//  3. **连接池调优**。MaxPoolSize / MinPoolSize / MaxConnecting / MaxConnIdleTime
//     四项全暴露。**注意 MaxPoolSize 是"每台服务器"的上限**，
//     副本集下真实上限要乘节点数 —— 这是最容易算错的一个数。
//  4. **连接池可观测**。驱动不提供任何池统计 API，本包订阅 CMAP 事件流自己计数，
//     产出与 mysql / postgres / redis 同形状的 metrics.PoolStats。
//  5. **操作级指标与慢查询日志**。通过命令监听器上报；
//     日志里的 Detail 只含**结构**（库.集合 + 字段名），不含任何取值。
//  6. **可恢复的关闭**。Close 幂等、不死锁、真正断开全部连接；重建时先建新客户端再换。
//
// # 与 mysql / postgres 包的关系
//
// API 形状与它们保持一致（Open / Client / HealthCheck / Reconnect / Close /
// Closed / Healthy / Config / PoolStats），从那边迁过来只需换包名。
// 差异集中在三处：
//
//   - 没有连接池对象可返回 —— MongoDB 的池在驱动内部，只有事件流；
//   - 没有 `Stats() sql.DBStats`，改成 `PoolStats()` 与 `Status()`；
//   - 驱动是异步自愈的（后台维护拓扑），所以 Reconnect 的必要性比 SQL 侧低得多。
//
// # 关于 driver v2
//
// 本包基于 mongo-driver **v2**。v1 升 v2 有几个影响配置的破坏性变更，
// 已在本包内处理或拦下（详见 README 的「从 v1 迁移」章节）：
// socketTimeoutMS 被删除、OperationTimeout（timeoutMS）取代了各处 maxTime。
package mongo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zavierswong/go-infra/logger"
	"github.com/zavierswong/go-infra/metrics"
)

// closeDrainTimeout 是 Close 时等待"在途操作归还连接"的上限。
//
// 驱动的 Disconnect 会等在用连接归还；超时后就**强制关闭**它们，
// 于是正在进行中的读写会失败。给它一个上界，是为了让 Close 不无限期挂住 ——
// 停机流程里"关不掉"比"有几个请求失败"要严重得多。
const closeDrainTimeout = 5 * time.Second

// MongoDB 是一个 MongoDB 客户端，持有一个 *mongo.Client 与其连接池的计数器。
//
// 所有导出方法都可并发调用。Close 之后调用 Client / Database 会返回 nil，
// 请先用 Closed 判断，或用 HealthCheck 探活。
type MongoDB struct {
	cfg  Config
	info *uriInfo
	log  *logger.Plog

	// mu 是**唯一**保护 client / closed / healthy 的锁。
	mu      sync.RWMutex
	client  *mongo.Client
	closed  bool
	healthy bool

	// rebuildMu 串行化"重建客户端"，避免监控协程与调用方同时重建。
	rebuildMu sync.Mutex

	// pool 是 CMAP 事件累计的计数器。它不随重建清零 ——
	// 它是"这个实例生命周期内发生过什么"的累计量，语义上就该是单调增的。
	pool poolCounters

	// ctx 在 Close 时取消，用于让监控循环与正在进行的建连立刻退出。
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Open 建立连接并启动健康监控。
//
// 返回的 *MongoDB 必须由调用方负责 Close。初始化失败会按 Config.DialAttempts
// 做有限次重试，每次尝试的上限是 Config.DialProbeTimeout，因此
// 最坏阻塞时间是 `DialAttempts × DialProbeTimeout`，不会无限等待。
func Open(cfg Config) (*MongoDB, error) {
	cfg, err := cfg.ready()
	if err != nil {
		return nil, err
	}

	// 构造驱动选项（含 ApplyURI 与驱动的权威校验）。
	//
	// 只构造这一次：ApplyURI 对 TLS 相关的 URI 参数会**读取本地证书文件**，
	// 重复构造不仅有开销，还可能因为文件被改动而得到不一致的结果。
	opts, info, err := cfg.clientOptions()
	if err != nil {
		return nil, err
	}

	// 把 URI 覆盖后的**生效**池上限写回 cfg。
	//
	// 时机很关键：此刻客户端还没发布给任何人（monitor 协程尚未启动、
	// 函数还没返回），因此这次写入是无竞争的。之后再改 cfg 就会与
	// PoolStats / Status / ping 的读取构成数据竞争，所以只在这里做一次。
	cfg.MaxPoolSize = effectiveMaxPoolSize(opts)

	ctx, cancel := context.WithCancel(context.Background())
	m := &MongoDB{
		cfg:    cfg,
		info:   info,
		log:    logger.NewPlog("MongoDB"),
		ctx:    ctx,
		cancel: cancel,
	}

	if err := m.dial(ctx, opts); err != nil {
		cancel()
		return nil, err
	}

	m.wg.Add(1)
	go m.monitor()

	m.log.Infof(ctx,
		"MongoDB 已连接: 主机=%s 库=%s 最大连接/节点=%d 最小连接=%d 空闲回收=%s",
		info.hostsString(), info.dbName, cfg.MaxPoolSize, cfg.MinPoolSize,
		describeIdleTimeout(cfg.MaxConnIdleTime))
	return m, nil
}

// describeIdleTimeout 把"0 = 不回收"这个反直觉的取值说清楚，
// 免得日志里出现一个含义不明的 `空闲回收=0s`。
func describeIdleTimeout(d time.Duration) string {
	if d <= 0 {
		return "不回收（驱动默认）"
	}
	return d.String()
}

// dial 按 Config.DialAttempts 建连，退避从 DialBackoff 指数增长到 DialMaxBackoff。
//
// DialAttempts 为负表示无限重试，但每次尝试前都会检查 ctx，
// 因此 Close 能立刻中断这个过程。
func (m *MongoDB) dial(ctx context.Context, opts *options.ClientOptions) error {
	attempts := m.cfg.DialAttempts
	infinite := attempts < 0
	backoff := m.cfg.DialBackoff

	var lastErr error
	for attempt := 1; infinite || attempt <= attempts; attempt++ {
		if m.Closed() {
			return ErrClosed
		}
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return lastErr
			}
			return err
		}

		client, err := m.open(ctx, opts)
		if err == nil {
			m.adopt(client)
			if attempt > 1 {
				m.log.Infof(ctx, "第[%d]次尝试连接 MongoDB 成功", attempt)
			}
			return nil
		}

		lastErr = err
		m.log.Warnf(ctx, "第[%d]次连接 MongoDB 失败: %v", attempt, err)

		if !infinite && attempt >= attempts {
			break
		}

		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(backoff):
		}

		if backoff *= 2; backoff > m.cfg.DialMaxBackoff {
			backoff = m.cfg.DialMaxBackoff
		}
	}
	return lastErr
}

// open 建立一个客户端并验证连通性。
//
// opts 由配置构造一次后复用：驱动不会修改传入的 options（ApplyURI 在
// clientOptions 里已经调过），因此多次 Connect 复用同一个 *ClientOptions 是安全的。
//
// ⚠️ 注意每次都会**新挂一份监听器**：监听器里带着 metrics.Observer，
// 而重建后 Observer 应当仍然生效，所以必须重新挂。
func (m *MongoDB) open(ctx context.Context, opts *options.ClientOptions) (*mongo.Client, error) {
	clientOpts := opts

	// 完整复制一份选项再挂监听器：避免把监听器写到调用方共用/复用的 options 上。
	// MergeClientOptions 是驱动提供的复制入口（v2 里其它 Merge* 已删除。
	clientOpts = options.MergeClientOptions(clientOpts)

	// 连接池事件 → 自己维护的计数器。**必须始终注册**：
	// 即使没有 Observer，PoolStats() 也要有数据。
	clientOpts.SetPoolMonitor(&event.PoolMonitor{Event: m.pool.observe})

	// 命令事件 → 指标 + 日志。
	// 两者都不需要时不注册，热路径上连一次闭包调用都不多花。
	if cm := m.commandMonitor(); cm != nil {
		clientOpts.SetMonitor(&event.CommandMonitor{
			Started:   cm.Started,
			Succeeded: cm.Succeeded,
			Failed:    cm.Failed,
		})
	}

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("%w: 创建客户端失败: %v", ErrConnect, err)
	}

	// 真实探活。
	//
	// 这一步不可省略：mongo.Connect **只校验选项，不验证可达性**，
	// 而它会**启动后台拓扑监控协程**，所以失败时必须显式 Disconnect，
	// 否则每重试一次就泄漏一组协程。
	if err := m.verify(ctx, client); err != nil {
		drainCtx, cancel := context.WithTimeout(context.Background(), closeDrainTimeout)
		_ = client.Disconnect(drainCtx)
		cancel()
		return nil, err
	}
	return client, nil
}

// verify 用带超时的上下文探活。
//
// 超时用 DialProbeTimeout 而**不是** ServerSelectionTimeout：
// 后者默认 30s，会让启动耗时失控；而且它是给业务操作用的旋钮，
// 拿它来卡启动期会把两个不同的语义混在一起。
func (m *MongoDB) verify(ctx context.Context, client *mongo.Client) error {
	probeCtx, cancel := context.WithTimeout(ctx, m.cfg.DialProbeTimeout)
	defer cancel()

	if err := client.Ping(probeCtx, nil); err != nil {
		return fmt.Errorf("%w: 连通性检查失败: %v", ErrConnect, err)
	}
	return nil
}

// commandMonitor 按需构造命令监听器；不需要时返回 nil。
//
// 两个都不需要的情形是"Observer 为 nil 且 LogLevel=silent"——
// 此时连 Started 回调都不注册，于是驱动不会再为每条命令复制一份命令文档。
func (m *MongoDB) commandMonitor() *commandMonitor {
	level := m.cfg.effectiveLogLevel()
	hasObserver := m.cfg.Observer != nil

	if !hasObserver && level == logLevelSilent {
		return nil
	}
	return &commandMonitor{
		obs:      metrics.OrNop(m.cfg.Observer),
		log:      m.log,
		instance: m.cfg.Name,
		level:    level,
		slow:     m.cfg.SlowThreshold,
	}
}

// adopt 用新客户端替换旧的，并断开旧客户端。
//
// 必须在持锁状态下复查 closed：Close 不抢 rebuildMu，因此"重建已建出新
// 客户端、尚未挂载"与"Close"可以交错发生。少了这一步复查，新客户端会被
// 挂到已关闭的实例上、此后再无人断开 —— mongo.Client 背后是一整组 SDAM
// 拓扑监控与连接池维护协程，泄漏代价远大于一个 SQL 池。
func (m *MongoDB) adopt(client *mongo.Client) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		// 实例已关闭：新客户端就地断开。
		if client != nil {
			drainCtx, cancel := context.WithTimeout(context.Background(), closeDrainTimeout)
			_ = client.Disconnect(drainCtx)
			cancel()
		}
		return
	}
	old := m.client
	m.client = client
	m.healthy = true
	m.mu.Unlock()

	// 在锁外断开旧客户端：Disconnect 会等待在途操作归还连接（可能耗时），
	// 持锁做会拖住所有读。
	if old != nil && old != client {
		drainCtx, cancel := context.WithTimeout(context.Background(), closeDrainTimeout)
		_ = old.Disconnect(drainCtx)
		cancel()
	}
}

// Client 返回底层驱动客户端。客户端已关闭时返回 nil。
//
// 它是本包的**逃生舱**：需要驱动的高级能力（会话、事务、Change Stream、
// GridFS、聚合游标、自定义 BSON 编解码）时直接用它，
// 本包只负责连接生命周期与可观测性，不试图把驱动 API 再包一遍。
func (m *MongoDB) Client() *mongo.Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.client
}

// Database 返回指定库的句柄。客户端已关闭时返回 nil。
func (m *MongoDB) Database(name string) *mongo.Database {
	client := m.Client()
	if client == nil {
		return nil
	}
	return client.Database(name)
}

// DefaultDatabase 返回 URI 路径里指定的默认库。
//
// URI 没写库名（`mongo://host:27017`）时返回 ErrInvalidConfig ——
// 这不是"运行时错误"，而是"配置里少写了东西"，所以用同一个哨兵。
// 显式报错而不是悄悄退化成 "test" 或 "admin"：
// 连错库是要靠人工发现的，不应该被库的默认值掩盖。
func (m *MongoDB) DefaultDatabase() (*mongo.Database, error) {
	if m.info.dbName == "" {
		return nil, fmt.Errorf(
			"%w: URI 未指定默认库名，请用 Database(name) 显式指定，"+
				"或把库名写进连接串路径（mongo://host:27017/<dbname>）",
			ErrInvalidConfig)
	}
	db := m.Database(m.info.dbName)
	if db == nil {
		return nil, ErrClosed
	}
	return db, nil
}

// HealthCheck 探活，可用于健康检查接口。
func (m *MongoDB) HealthCheck(ctx context.Context) error {
	client, closed := m.snapshotClient()
	if closed {
		return ErrClosed
	}
	if client == nil {
		return ErrNotConnected
	}
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("%w: %v", ErrConnect, err)
	}
	return nil
}

// Closed 报告客户端是否已关闭。
func (m *MongoDB) Closed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.closed
}

// Healthy 报告最近一次后台探活的结果。
func (m *MongoDB) Healthy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.healthy && !m.closed
}

// Config 返回**生效后**的配置：默认值已补齐，且已计入 URI 的覆盖。
//
// 所以 Config().MaxPoolSize 与用户写进配置的 MaxPoolSize **可能不同** ——
// 当 URI 里带了 maxPoolSize 时，它才是驱动真正使用的那个数。
// 想要"用户原本填了什么"，请自己保留原始 Config。
func (m *MongoDB) Config() Config { return m.cfg }

// URI 返回**抹掉密码**后的连接串，便于排查"到底连的是哪个集群"。
//
// 脱敏覆盖两处口令：userinfo 里的密码，以及
// `tlsCertificateKeyFilePassword` 这类查询参数。详见 uri.go。
func (m *MongoDB) URI() string { return m.info.sanitized() }

// PoolStats 返回归一化后的连接池快照。
//
// 它是**推算**出来的，不是驱动直接给的 —— 驱动没有池统计 API，
// 这个快照来自本包订阅的 CMAP 事件计数器。两个必须知道的语义：
//
//   - MaxOpen 是**每台服务器**的上限，而其它字段是所有服务器聚合的
//     （多节点部署下 Open 可能大于 MaxOpen，这是正确的）；
//   - WaitCount/WaitDuration 与 database/sql 的口径不同，见 metrics.PoolStats 的说明。
//
// 字段恒为 0 的项（MaxIdleClosed / MaxLifetimeClosed / Hits / Misses）
// 不要拿去建指标 —— 驱动没有这些概念。
func (m *MongoDB) PoolStats() metrics.PoolStats {
	return m.pool.snapshot(m.cfg.Name, m.cfg.MaxPoolSize)
}

// Status 是客户端的瞬时状态快照，适合映射成 Gauge。
//
// 它回答"这个客户端现在能不能用、连的是哪儿"，与事件流互补：
// 事件告诉你发生了什么，快照告诉你此刻处于什么状态。
type Status struct {
	// Instance 是实例名，来自 Config.Name。
	Instance string
	// Closed 表示客户端是否已关闭。
	Closed bool
	// Healthy 表示最近一次后台探活是否成功。
	Healthy bool
	// Hosts 是连接串里的主机列表（逗号分隔，不含口令）。
	Hosts string
	// Database 是 URI 路径里的默认库名，可能为空。
	Database string
	// SessionsInProgress 是"已开启但未结束的会话数"。
	//
	// 驱动对每次操作默认使用隐式会话、操作结束即归还，
	// 所以它约等于**在途操作数**。它是判断"是不是有一批慢操作把连接占住了"
	// 最直接的信号 —— 比看 Open 更有指向性。
	SessionsInProgress int
	// PoolCleared 是连接池被清空的累计次数（主节点变更、maxPoolSize 变更、
	// 网络错误后的"池清理"都会触发）。
	//
	// 它陡增通常对应一次主从切换或网络抖动，是解释"刚才为什么有一批超时"
	// 的关键证据 —— 这也是它没进 metrics.PoolStats 而放在这里的原因：
	// 它是**事件性**的，不是水位指标。
	PoolCleared int64
}

// Status 返回当前状态快照。
func (m *MongoDB) Status() Status {
	client, closed := m.snapshotClient()

	st := Status{
		Instance:    m.cfg.Name,
		Closed:      closed,
		Healthy:     m.Healthy(),
		Hosts:       m.info.hostsString(),
		Database:    m.info.dbName,
		PoolCleared: m.pool.poolCleared.Load(),
	}
	if client != nil {
		st.SessionsInProgress = client.NumberSessionsInProgress()
	}
	return st
}

// Reconnect 主动重建客户端：先建新客户端、就绪后再断开旧的，期间旧客户端仍可服务。
//
// 一般**不需要**调用。与 SQL 侧不同，MongoDB 驱动本来就在后台维护拓扑：
// 节点恢复、主从切换、连接被回收它都会自己处理，业务代码什么都不用做。
// 需要它的场景只有两类：
//
//   - URI 变了（例如切到另一个集群）；
//   - 客户端状态确实异常（极少见，通常伴随驱动自身的 bug）。
//
// ⚠️ **重连会让之前取到的句柄失效。** `Database()` / `Collection` 句柄是挂在
// 某个具体的 *mongo.Client 上的，而 Reconnect 换掉了那个客户端并断开了它 ——
// 旧句柄再用会报 `client is disconnected`。
//
// 这一点与 mysql / postgres 包**不同**：SQL 侧换的是连接池，*gorm.DB 本身稳定，
// 句柄一直有效。所以从那边迁过来时，最容易踩的就是"缓存了 Collection 句柄"。
// 结论：**不要缓存 Collection，每次用时现取**（取句柄本身没有网络开销）。
func (m *MongoDB) Reconnect() error {
	m.rebuildMu.Lock()
	defer m.rebuildMu.Unlock()

	if m.Closed() {
		return ErrClosed
	}

	start := time.Now()
	opts, _, err := m.cfg.clientOptions()
	if err != nil {
		return err
	}
	if err := m.dial(m.ctx, opts); err != nil {
		m.observeReconnect(start, err)
		return err
	}
	m.observeReconnect(start, nil)
	return nil
}

// observeReconnect 上报一次重建事件。
//
// 重建本身也可能失败，所以这里用"重建前是否需要观测"来判定，
// 而不是看有没有 commandMonitor（重建失败时它仍在）。
func (m *MongoDB) observeReconnect(start time.Time, err error) {
	obs := metrics.OrNop(m.cfg.Observer)
	obs.ObserveOp(metrics.Event{
		Component: metrics.ComponentMongoDB,
		Instance:  m.cfg.Name,
		Op:        metrics.OpReconnect,
		Duration:  time.Since(start),
		Err:       err,
		Reason:    classifyErr(err),
	})
}

// Close 断开客户端并停止监控。可重复调用，第二次起直接返回 nil。
func (m *MongoDB) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	client := m.client
	m.client = nil
	m.healthy = false
	m.mu.Unlock()

	// 顺序很关键：先取消 ctx 让监控循环与在建连接退出，再断开客户端，
	// 最后等协程收敛。全程不持锁，避免与读操作互相等待。
	//
	// cancel 与 log 都要判空：Close 被承诺是幂等且安全的，
	// 而"Open 失败后仍 defer c.Close()"、"对零值调用"都是真实存在的路径，
	// 那时这两个字段还是 nil。用空指针换一个 panic 不划算。
	if m.cancel != nil {
		m.cancel()
	}

	var err error
	if client != nil {
		drainCtx, cancel := context.WithTimeout(context.Background(), closeDrainTimeout)
		if cerr := client.Disconnect(drainCtx); cerr != nil {
			err = fmt.Errorf("断开 MongoDB 连接失败: %w", cerr)
		}
		cancel()
	}
	m.wg.Wait()

	if m.log != nil {
		m.log.Infof(context.Background(), "MongoDB 连接已断开")
	}
	return err
}

// monitor 周期性探活。
//
// 只做**观测**（更新 Healthy、打日志）；需要重建时由 RebuildAfterFailures 显式开启。
func (m *MongoDB) monitor() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cfg.HealthCheckInterval)
	defer ticker.Stop()

	failures := 0
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
		}

		err := m.ping()
		switch {
		case err == nil:
			if failures > 0 {
				m.log.Infof(m.ctx, "MongoDB 连接已恢复（此前连续失败 %d 次）", failures)
			}
			failures = 0
			m.setHealthy(true)

		case errors.Is(err, ErrClosed):
			return

		default:
			failures++
			m.setHealthy(false)
			m.log.Warnf(m.ctx, "MongoDB 健康检查失败（连续 %d 次）: %v", failures, err)

			if need := m.cfg.RebuildAfterFailures; need > 0 && failures >= need {
				m.log.Warnf(m.ctx, "连续失败 %d 次，主动重建客户端", failures)
				if rerr := m.Reconnect(); rerr != nil {
					m.log.Errorf(m.ctx, "重建客户端失败: %v", rerr)
				} else {
					failures = 0
				}
			}
		}
	}
}

// ping 用带超时的上下文探活。快速路径只读一次锁，不阻塞调用方。
//
// 这里**主动上报**一个 Op=ping 的事件：后台探活产生的往返也是真实流量，
// 让它可见比让它隐形更好（metric 上过滤掉即可）。健康检查的失败率
// 本身就是最重要的信号之一。
func (m *MongoDB) ping() error {
	client, closed := m.snapshotClient()
	if closed {
		return ErrClosed
	}
	if client == nil {
		return ErrNotConnected
	}

	probeCtx, cancel := context.WithTimeout(m.ctx, m.cfg.DialProbeTimeout)
	defer cancel()

	start := time.Now()
	err := client.Ping(probeCtx, nil)

	metrics.OrNop(m.cfg.Observer).ObserveOp(metrics.Event{
		Component: metrics.ComponentMongoDB,
		Instance:  m.cfg.Name,
		Op:        metrics.OpPing,
		Duration:  time.Since(start),
		Err:       err,
		Reason:    classifyErr(err),
		// Detail 走与命令监听器**同一个**构造函数，而不是手工拼字符串：
		// 探活产生的 ping 与业务自己发的 ping 会落进同一张图，
		// 两处格式不一致的话，"按 Detail 聚合"就得不到一个完整的 ping 视图。
		Detail: commandDetail("admin", "ping", nil),
	})
	return err
}

// snapshotClient 一次性读出 client 与 closed，避免调用方拿到不一致的组合。
func (m *MongoDB) snapshotClient() (*mongo.Client, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.client, m.closed
}

func (m *MongoDB) setHealthy(v bool) {
	m.mu.Lock()
	m.healthy = v
	m.mu.Unlock()
}
