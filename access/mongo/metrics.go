package mongo

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zavierswong/go-infra/logger"
	"github.com/zavierswong/go-infra/metrics"
)

// 本文件把 mongo-driver 的两条事件流接到 metrics.Observer 上：
//
//  1. **命令事件**（event.CommandMonitor）→ 操作级事件，用来做延迟直方图与错误计数。
//     与 mysql/postgres 的 GORM callback、redis 的 Hook 是同一个位置。
//  2. **连接池事件**（event.PoolMonitor）→ 自己维护计数器，产出 metrics.PoolStats。
//
// 第 2 条是本包比其它包多出来的一块工作，原因是**驱动根本不提供连接池统计 API**：
// database/sql 有 sql.DBStats、go-redis 有 PoolStats，而 mongo-driver 只有
// CMAP 事件流（ConnectionPoolCreated / ConnectionCreated / ConnectionCheckedOut …）。
// 所以想在 Prometheus 里看 MongoDB 的连接池水位，只能自己订事件、自己计数。
//
// # 关于命令监听的开销
//
// 注册了 Started 回调时，驱动必须把命令文档**复制并序列化成 bson.Raw**
// 才能交给回调（见驱动的 redactStartedInformationCmd），这是一次
// 与命令大小成正比的分配。所以：
//
//   - 不需要任何观测与日志时（Observer 为 nil 且 LogLevel=silent），
//     本包**一个监听器都不注册**，热路径上零额外开销；
//   - 其余情况下这次复制无法避免 —— 它是换取"哪个集合慢"这类归因信息的代价。
//
// 对比一下：Detail 本身的构造成本可以忽略（只遍历命令文档的顶层键，
// 不做反射、不打印取值），所以本包**每条命令都填充 Detail**，
// 这一点与 redis 包"仅在慢/错时填充"的策略不同 ——
// 那边要把命令参数格式化成字符串（反射式 %v），确实有开销。

// poolCounters 是自己维护的连接池计数器。
//
// 全部用原子量：CMAP 事件由驱动自己的 goroutine 触发，
// 且**必须非阻塞** —— 在回调里加锁会把驱动的池操作拖慢。
type poolCounters struct {
	// created / closed 是连接的生与死，用来推算"当前存活连接数"。
	created atomic.Int64
	closed  atomic.Int64

	// checkOutStarted / checkedOut / checkOutFailed* 是取连接的三条出口：
	// 排队、拿到、失败。三者之差就是"还在排队"的数量。
	checkOutStarted       atomic.Int64
	checkedOut            atomic.Int64
	checkOutFailedTimeout atomic.Int64
	checkOutFailedClosed  atomic.Int64
	checkOutFailedConnErr atomic.Int64

	// waitNanos 是**全部成功检出**的累计耗时。
	//
	// ⚠️ 口径与 database/sql 的 WaitDuration 不同：那边只统计"被阻塞的请求"，
	// 这边包含命中空闲连接、耗时近乎为 0 的快路径。见 metrics.PoolStats 的说明。
	waitNanos atomic.Int64

	// 按"为什么被关"分类的连接回收次数。
	idleClosed  atomic.Int64 // MaxConnIdleTime 到期
	staleClosed atomic.Int64 // 拓扑变化导致失效（主从切换、SDAM 判死）
	errorClosed atomic.Int64 // 操作中遇到网络错误，连接自行关闭

	// poolCleared 是"池因故被清空"的次数（如 maxPoolSize 变更、主节变更）。
	// 它不在 metrics.PoolStats 里，但排障时很有用，所以只留作内部诊断。
	poolCleared atomic.Int64
}

// observe 消费一个 CMAP 事件。
//
// 这里**不做任何可能阻塞的操作**：只做原子加减与一次字符串比较。
func (c *poolCounters) observe(evt *event.PoolEvent) {
	if evt == nil {
		return
	}

	switch evt.Type {
	case event.ConnectionCreated:
		c.created.Add(1)

	case event.ConnectionReady:
		// 连接可用，不改变计数（created 已经算过了）。

	case event.ConnectionClosed:
		c.closed.Add(1)
		switch evt.Reason {
		case event.ReasonIdle:
			c.idleClosed.Add(1)
		case event.ReasonStale:
			c.staleClosed.Add(1)
		case event.ReasonError:
			c.errorClosed.Add(1)
		}

	case event.ConnectionCheckOutStarted:
		c.checkOutStarted.Add(1)

	case event.ConnectionCheckedOut:
		c.checkedOut.Add(1)
		c.waitNanos.Add(int64(evt.Duration))

	case event.ConnectionCheckOutFailed:
		switch evt.Reason {
		case event.ReasonTimedOut:
			c.checkOutFailedTimeout.Add(1)
		case event.ReasonPoolClosed:
			c.checkOutFailedClosed.Add(1)
		case event.ReasonConnectionErrored:
			c.checkOutFailedConnErr.Add(1)
		}

	case event.ConnectionPoolCleared:
		c.poolCleared.Add(1)
	}
}

// snapshot 把计数器换算成 metrics.PoolStats。
//
// 换算规则集中在这里，且是一个**纯函数**（只读计数器、不改），
// 于是可以脱离真实 MongoDB 做逐字段单测 —— 这类"字段搬来搬去"的代码
// 一旦算错，表现是"某条曲线一直是 0"或"负的 Gauge"，靠集成测试很难发现。
//
// maxPoolSize 是**每台服务器**的上限（见 Config.MaxPoolSize 的说明），
// 而这里的计数是**所有服务器聚合**的 —— 多节点部署下 Open 可能大于 MaxOpen，
// 这是正确的，不是 bug。
func (c *poolCounters) snapshot(instance string, maxPoolSize int) metrics.PoolStats {
	created := c.created.Load()
	closed := c.closed.Load()
	live := created - closed
	if live < 0 {
		// 计数是分开读的，并发下可能读到"新连接刚建好但还没算进 created"的一瞬，
		// 夹到 0 而不是让负数流进指标里 —— 负的 Gauge 会让图表与告警阈值全部失真。
		live = 0
	}

	inUse := c.checkedOut.Load()
	idle := live - inUse
	if idle < 0 {
		idle = 0
	}

	// 排队深度 = 发起的检出 - 已经拿到 - 已经失败。
	// 三个计数同样不是原子地一起读的，所以要夹到非负。
	checkedOut := inUse
	failed := c.checkOutFailedTimeout.Load() +
		c.checkOutFailedClosed.Load() + c.checkOutFailedConnErr.Load()
	pending := c.checkOutStarted.Load() - checkedOut - failed
	if pending < 0 {
		pending = 0
	}

	return metrics.PoolStats{
		Component: metrics.ComponentMongoDB,
		Instance:  instance,
		MaxOpen:   maxPoolSize,
		Open:      int(live),
		InUse:     int(inUse),
		Idle:      int(idle),
		Pending:   int(pending),

		WaitCount:    checkedOut,
		WaitDuration: time.Duration(c.waitNanos.Load()),

		// MaxIdleClosed 与 MaxLifetimeClosed 恒为 0：
		// 驱动没有"空闲连接数上限"也没有"连接最长寿命"这两个概念。
		MaxIdleTimeClosed: c.idleClosed.Load(),

		// Hits / Misses 恒为 0：驱动不暴露"是否命中空闲连接"。
		Timeouts: c.checkOutFailedTimeout.Load(),
		Unusable: c.checkOutFailedConnErr.Load() + c.errorClosed.Load(),
		Stale:    c.staleClosed.Load(),
	}
}

// commandMonitor 实现 event.CommandMonitor 的三个回调。
//
// 三个回调都由**业务调用的那个 goroutine** 同步执行（驱动在操作收尾时直接调用），
// 因此这里的工作会算进业务操作的耗时里。所以：
//   - 不分配（除了 Detail 那个短字符串）；
//   - 不做 IO；日志走 logger 的同步写（与 GORM 慢日志同样处理）。
type commandMonitor struct {
	obs      metrics.Observer
	log      *logger.Plog
	instance string
	level    string
	slow     time.Duration

	// pending 保存 Started 阶段拿到的命令文档，供 Succeeded/Failed 生成 Detail。
	//
	// 为什么必须有它：CommandFinishedEvent 里**只有命令名与耗时**，
	// 没有命令文档；要生成"哪个集合、哪些字段"的摘要只能在这里先存下来。
	//
	// 用 sync.Map 而不是普通 map：回调来自任意业务 goroutine，
	// 而 RequestID 是全局单调递增的 int64，各 goroutine 天然操作不同键 ——
	// 这正是 sync.Map 擅长的"键不相交"场景，比单把互斥锁少一层竞争。
	//
	// **不会泄漏**：本包的 commandMonitor 一定同时注册 Succeeded 与 Failed，
	// 而驱动的 canPublishFinishedEvent 在这两个回调都非 nil 时
	// 对每次成功/失败（含未确认写）都会发结束事件，
	// 所以每个 Started 都必然对应一次 take。
	started sync.Map // map[int64]bson.Raw
}

// Started 记录命令文档，供结束回调生成 Detail。
//
// evt.Command 是驱动**新分配**的一份副本（见 redactStartedInformationCmd），
// 保留它不会与驱动的内部缓冲区产生别名问题。
// 对敏感命令驱动会把它置空，于是这里存进去的就是 nil —— commandDetail 会处理。
func (m *commandMonitor) Started(_ context.Context, evt *event.CommandStartedEvent) {
	if evt == nil {
		return
	}
	m.started.Store(evt.RequestID, evt.Command)
}

// take 取出并删除某个请求的命令文档。
func (m *commandMonitor) take(requestID int64) bson.Raw {
	raw, ok := m.started.LoadAndDelete(requestID)
	if !ok {
		return nil
	}
	doc, ok := raw.(bson.Raw)
	if !ok {
		return nil
	}
	return doc
}

// Succeeded 上报成功命令。
func (m *commandMonitor) Succeeded(_ context.Context, evt *event.CommandSucceededEvent) {
	if evt == nil {
		return
	}
	m.finish(evt.CommandName, evt.DatabaseName, evt.RequestID, evt.Duration, nil)
}

// Failed 上报失败命令。
func (m *commandMonitor) Failed(_ context.Context, evt *event.CommandFailedEvent) {
	if evt == nil {
		return
	}
	m.finish(evt.CommandName, evt.DatabaseName, evt.RequestID, evt.Duration, evt.Failure)
}

// finish 是两条结束路径的公共部分。
//
// op 取命令名**原文**（find / insert / aggregate / getMore …），
// 大小写原样保留，与 mongodb_exporter、db.currentOp() 的输出对齐。
func (m *commandMonitor) finish(cmd, db string, requestID int64, elapsed time.Duration, err error) {
	detail := commandDetail(db, cmd, m.take(requestID))

	m.obs.ObserveOp(metrics.Event{
		Component: metrics.ComponentMongoDB,
		Instance:  m.instance,
		Op:        metrics.Op(cmd),
		Duration:  elapsed,
		Err:       err,
		Reason:    classifyErr(err),
		Detail:    detail,
	})

	m.logCommand(cmd, detail, elapsed, err)
}

// logCommand 按 LogLevel 输出命令日志。
//
// 级别语义（与 GORM 的那套一致）：
//
//	silent  不打（指标照常上报）
//	error   只打失败
//	warn   失败 + 慢操作
//	info   全部
func (m *commandMonitor) logCommand(cmd, detail string, elapsed time.Duration, err error) {
	switch m.level {
	case logLevelSilent:
		return
	case logLevelError:
		if err == nil {
			return
		}
	case logLevelWarn:
		if err == nil && !m.isSlow(elapsed) {
			return
		}
	}

	ctx := context.Background()
	if err != nil {
		// 失败命令用 Error 级别：它总是需要人看一眼。
		m.log.Errorf(ctx, "MongoDB 命令失败 [%s] %s 耗时=%s: %v",
			cmd, detail, elapsed, err)
		return
	}
	if m.isSlow(elapsed) {
		m.log.Warnf(ctx, "MongoDB 慢操作 [%s] %s 耗时=%s（阈值 %s）",
			cmd, detail, elapsed, m.slow)
		return
	}
	m.log.Infof(ctx, "MongoDB 命令 [%s] %s 耗时=%s", cmd, detail, elapsed)
}

// isSlow 判定是否超过慢操作阈值。负阈值表示不判定。
func (m *commandMonitor) isSlow(elapsed time.Duration) bool {
	return m.slow > 0 && elapsed >= m.slow
}

// ---------------------------------------------------------------------------
// Detail 的构造
// ---------------------------------------------------------------------------

// 摘要的规模上限。命令文档里的字段数量是业务可控的（有人会往 filter 里塞几十个条件），
// 所以必须有界 —— 否则一条慢查询的日志可能有几 KB。
const (
	maxDetailNames = 12
	// maxDetailDepth 限制下钻层数。两层足够覆盖
	// `updates[].u.$set{...}` 这种最常见的形状。
	maxDetailDepth = 2
	// maxDetailStringLen 限制标量取值的长度。
	maxDetailStringLen = 24
)

// detailKeys 是"值得提取结构"的命令字段白名单。
//
// 只列 CRUD 与聚合里信息量最大的那些。其余字段（$db、lsid、txnNumber、
// readConcern、writeConcern、$clusterTime、apiVersion…）一律不进 Detail ——
// 它们是协议胶水，对归因没有帮助，却会把摘要撑得没法读。
var detailKeys = map[string]struct{}{
	"filter":      {},
	"query":       {},
	"projection":  {},
	"sort":        {},
	"hint":        {},
	"key":         {},
	"pipeline":    {},
	"documents":   {},
	"updates":     {},
	"deletes":     {},
	"update":      {},
	"q":           {},
	"u":           {},
	"aggregation": {},
}

// scalarDetailKeys 是允许输出**取值**的字段。
//
// 只有这两个字段的取值是"字段名/索引名"这种**结构信息**而不是业务数据，
// 所以可以安全地打出来（distinct 的 key 是字段名，hint 是索引名）。
// filter 之类的取值绝对不能打 —— 那里就是业务数据本身。
var scalarDetailKeys = map[string]struct{}{
	"key":  {},
	"hint": {},
}

// sensitiveCommands 是"参数里可能含凭据"的命令，Detail 一律不展开。
//
// 驱动本身已经对同一批命令做了脱敏（redactCommand 会把 Command 置空），
// 所以这里其实是**第二层保险**：万一将来驱动的行为变了，
// 也不至于把认证握手的内容写进日志。
//
// 键必须小写 —— 比较前会做 ToLower。
var sensitiveCommands = map[string]struct{}{
	"authenticate":    {},
	"saslstart":       {},
	"saslcontinue":    {},
	"getnonce":        {},
	"createuser":      {},
	"updateuser":      {},
	"copydbgetnonce":  {},
	"copydbsaslstart": {},
	"copydb":          {},
}

// commandDetail 把一条命令摘要成"可安全写入日志"的一行。
//
// 输出的形状是：
//
//	<db>.<集合> <命令名> <字段>{<子字段名>} …
//
// 例如：
//
//	shop.orders find filter{user_id,status} projection{_id,amount} sort{created_at}
//	shop.orders insert doc{_id,user_id,amount,created_at}
//	shop.orders update updates{q{_id} u{$set{status,updated_at}}}
//	shop.orders aggregate pipeline[$match,$group,$sort]
//	admin.ping
//
// 库级命令（ping / listCollections / dropDatabase …）没有集合名，
// 于是库名与命令名之间直接用 `.` 相连，形如 `admin.ping`。
//
// # 设计原则：只给**结构**，不给**取值**
//
// 这是刻意的隐私取舍，比 redis 包的策略更严：
//
//   - filter / documents / update 的取值就是业务数据，可能含身份证号、手机号、
//     甚至明文口令（用户自己存的），而 Detail 会被写进日志与 tracing span。
//   - 但"是哪个集合、按哪些字段查、更新了哪些字段"恰好是归因需要的全部信息 ——
//     字段名是 schema 的一部分，不是数据。
//
// 唯一的例外是 scalarDetailKeys 里的字段（字段名/索引名本身），
// 以及命令名后的库名与集合名 —— 后者在排查"哪个集合慢"时不可或缺。
func commandDetail(db, cmd string, doc bson.Raw) string {
	var b strings.Builder

	if db != "" {
		b.WriteString(db)
		b.WriteByte('.')
	}

	// 命令文档的第一个元素约定就是命令名本身（形如 {find: "orders", ...}），
	// 且它的取值如果是字符串，那就是**集合名** —— 这是 MongoDB 命令格式的约定，
	// 不需要按命令名维护一张表。
	collection, rest := splitTarget(cmd, doc)
	if collection != "" {
		b.WriteString(collection)
		b.WriteByte(' ')
	}
	b.WriteString(cmd)

	if _, sensitive := sensitiveCommands[strings.ToLower(cmd)]; sensitive {
		b.WriteString(" [参数已省略：可能含凭据]")
		return b.String()
	}

	budget := maxDetailNames
	summarizeFields(&b, rest, 0, &budget)
	return b.String()
}

// splitTarget 拆出集合名与"剩下的字段"。
//
// 两种情况要特殊处理：
//   - **getMore**：第一个元素是游标 ID（数字），集合名在后面的 `collection` 字段里。
//     游标是长查询最典型的形态，所以这个特例必须处理，否则最该看的命令反而没上下文。
//   - 命令文档为空（驱动对敏感命令做了脱敏，会把它置空）：原样返回。
func splitTarget(cmd string, doc bson.Raw) (string, bson.Raw) {
	if len(doc) == 0 {
		return "", nil
	}

	if strings.EqualFold(cmd, "getMore") {
		if v := doc.Lookup("collection"); v.Type == bson.TypeString {
			return v.StringValue(), nil
		}
		return "", nil
	}

	elems, err := doc.Elements()
	if err != nil || len(elems) == 0 {
		return "", nil
	}
	// 第一个元素的取值是字符串 → 它是集合名。
	// 库级命令（ping、listCollections、dropDatabase…）的取值是数字或文档，走到这里为空。
	if elems[0].Value().Type != bson.TypeString {
		return "", doc
	}
	return elems[0].Value().StringValue(), doc
}

// summarizeFields 把文档里"白名单字段"的结构追加到 b。
//
// budget 是全局的字段名配额（跨层共享），depth 是当前下钻层数。
// 两者共同保证输出有界 —— 这是本文件里唯一需要小心的正确性问题。
func summarizeFields(b *strings.Builder, doc bson.Raw, depth int, budget *int) {
	if len(doc) == 0 || depth > maxDetailDepth {
		return
	}
	elems, err := doc.Elements()
	if err != nil {
		return
	}

	for _, elem := range elems {
		if *budget <= 0 {
			b.WriteString(" …")
			return
		}

		key := elem.Key()
		val := elem.Value()

		// 命中的条件：在白名单里，或是 `$set` / `$inc` 这类更新操作符。
		// 后者单独放行，是为了让 `u{$set{status}}` 能展开成字段名 ——
		// 更新语句里最该知道的就是"改了哪些字段"。
		_, whitelisted := detailKeys[key]
		isOperator := strings.HasPrefix(key, "$") && val.Type == bson.TypeEmbeddedDocument
		if !whitelisted && !isOperator {
			continue
		}

		switch val.Type {
		case bson.TypeEmbeddedDocument:
			b.WriteByte(' ')
			b.WriteString(key)
			*budget--
			sub := *budget
			writeDocNames(b, val.Document(), depth+1, &sub)
			*budget = sub

		case bson.TypeArray:
			arr, ok := val.ArrayOK()
			if !ok {
				continue
			}
			writeArrayNames(b, key, arr, depth, budget)

		case bson.TypeString:
			if _, ok := scalarDetailKeys[key]; !ok {
				continue
			}
			b.WriteByte(' ')
			b.WriteString(key)
			b.WriteByte('=')
			b.WriteString(truncateRunes(val.StringValue(), maxDetailStringLen))
			*budget--
		}
	}
}

// writeDocNames 输出一个子文档的字段名，形如 `{_id,user_id,amount}`。
//
// 与 summarizeFields 的区别：这里是"输出键名"而不是"按白名单递归"，
// 所以它还会对键名里嵌套的 `$xxx` 操作符再下钻一层（`$set{status,updated_at}`）。
func writeDocNames(b *strings.Builder, doc bson.Raw, depth int, budget *int) {
	if len(doc) == 0 {
		b.WriteString("{}")
		return
	}
	elems, err := doc.Elements()
	if err != nil {
		return
	}

	b.WriteByte('{')
	first := true
	for _, elem := range elems {
		if *budget <= 0 {
			b.WriteString(",…")
			break
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		*budget--

		b.WriteString(elem.Key())
		// 只对"看起来像操作符的子文档"再钻一层，避免把业务文档整棵树铺出来。
		if depth+1 <= maxDetailDepth+1 &&
			strings.HasPrefix(elem.Key(), "$") &&
			elem.Value().Type == bson.TypeEmbeddedDocument {
			sub := *budget
			writeDocNames(b, elem.Value().Document(), depth+1, &sub)
			*budget = sub
		}
	}
	b.WriteByte('}')
}

// writeArrayNames 输出数组类字段的摘要。
//
// 三种数组的语义完全不同，所以分开处理：
//
//   - **pipeline**：只列阶段名（`[$match,$group]`）。阶段的**参数**就是业务数据，
//     而且一眼看阶段构成已经足够判断"这条聚合在干什么"。
//   - **documents / updates / deletes / aggregation**：只取**第一个元素**。
//     批量写可能有上千个文档，全铺出来会把日志撑爆，而第一个元素的字段结构
//     已经能代表这一批（它们通常同构）。
//   - 其余数组：整段跳过。例如 `cursors`（killCursors 的 ID 列表）没有归因价值。
func writeArrayNames(b *strings.Builder, key string, arr bson.RawArray, depth int, budget *int) {
	values, err := arr.Values()
	if err != nil || len(values) == 0 {
		return
	}

	switch key {
	case "pipeline":
		b.WriteByte(' ')
		b.WriteString(key)
		b.WriteByte('[')
		for i, v := range values {
			if *budget <= 0 {
				b.WriteString(",…")
				break
			}
			if v.Type != bson.TypeEmbeddedDocument {
				continue
			}
			stage, ok := firstKey(v.Document())
			if !ok {
				continue
			}
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(stage)
			*budget--
		}
		b.WriteByte(']')

	case "documents":
		// 批量插入的文档数组：只取第一个，标签用 doc 而不是 documents，
		// 让"这里只展开了一个"这件事在日志里一眼可见。
		if values[0].Type != bson.TypeEmbeddedDocument {
			return
		}
		summarizeAsDoc(b, "doc", values[0].Document(), depth+1, budget)

	case "updates", "deletes", "aggregation":
		if values[0].Type != bson.TypeEmbeddedDocument {
			return
		}
		summarizeAsDoc(b, key, values[0].Document(), depth+1, budget)
	}
}

// summarizeAsDoc 按 `label{...}` 的形式展开一个子文档，并对其内部
// 继续套用白名单规则（于是 `updates` 里的 `q`/`u` 会被展开）。
func summarizeAsDoc(b *strings.Builder, label string, doc bson.Raw, depth int, budget *int) {
	b.WriteByte(' ')
	b.WriteString(label)
	if depth > maxDetailDepth+1 {
		b.WriteString("{…}")
		return
	}

	sub := *budget
	var inner strings.Builder
	summarizeFields(&inner, doc, depth, &sub)
	*budget = sub

	if inner.Len() == 0 {
		// 内部没有任何白名单字段（例如 updates 的元素只有 q/u 之外的东西），
		// 就退化成"列字段名"，至少能看出结构。
		writeDocNames(b, doc, depth, budget)
		return
	}
	// 把内层产生的 " q{_id} u{$set{x}}" 收进 `label{...}` 里。
	b.WriteByte('{')
	b.WriteString(strings.TrimPrefix(inner.String(), " "))
	b.WriteByte('}')
}

// firstKey 返回文档的第一个键名。
func firstKey(doc bson.Raw) (string, bool) {
	elems, err := doc.Elements()
	if err != nil || len(elems) == 0 {
		return "", false
	}
	return elems[0].Key(), true
}

// truncateRunes 按**字符**而非字节截断，避免把一个多字节字符切成乱码。
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

// ---------------------------------------------------------------------------
// 错误归类
// ---------------------------------------------------------------------------

// classifyErr 把错误细化成 metrics 的低基数原因。
//
// metrics.ClassifyErr 只认通用原因，这里补上 MongoDB 专有的几类。
// 其中最关键的是 mongo.ErrNoDocuments → not_found：
// FindOne 查不到是**正常业务结果**，它让这类"查了但没有"不会污染错误率
// （配合 metrics.Event.IsError 使用）。
func classifyErr(err error) metrics.Reason {
	if err == nil {
		return metrics.ReasonNone
	}

	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		// 查不到文档。FindOne / FindOneAndUpdate 未命中都会走到这里。
		return metrics.ReasonNotFound

	case errors.Is(err, mongo.ErrClientDisconnected), errors.Is(err, ErrClosed):
		return metrics.ReasonClosed

	case errors.Is(err, ErrNotConnected), errors.Is(err, ErrConnect):
		return metrics.ReasonConnect

	case errors.Is(err, ErrInvalidConfig):
		return metrics.ReasonInvalid
	}

	// 唯一索引冲突：业务冲突而非瞬时故障，归 conflict 才能和真正的故障区分开。
	if IsDuplicateKey(err) {
		return metrics.ReasonConflict
	}

	// 事务类标签。TransientTransactionError 的语义是"整个事务重来"，
	// 与乐观锁冲突是同一类问题（并发争用），所以同样归 conflict。
	if IsTransactionRetryable(err) {
		return metrics.ReasonConflict
	}

	// 鉴权/授权失败：几乎总是配置问题，不是"数据库坏了"。
	if IsUnauthorized(err) {
		return metrics.ReasonInvalid
	}

	// 游标失效、集合不存在：目标不在那儿，属于 not_found 这一族。
	// 它们都值得单独看曲线 —— 游标失效陡增通常意味着有长遍历在超时。
	if IsCursorNotFound(err) || IsNamespaceNotFound(err) {
		return metrics.ReasonNotFound
	}

	// 写确认级别没被满足：命令执行了、数据可能写进去了，但没写"稳"。
	// 归 rejected（"这次写入没能被正常确认"）比归 conflict 更贴近事实。
	if IsWriteConcernError(err) {
		return metrics.ReasonRejected
	}

	if IsTimeout(err) {
		return metrics.ReasonTimeout
	}
	if IsNetworkError(err) {
		return metrics.ReasonConnect
	}

	// 参数/文档类错误：错误在调用方，重试没有意义。
	var invalid mongo.InvalidArgumentError
	if errors.As(err, &invalid) {
		return metrics.ReasonInvalid
	}
	var marshalErr mongo.MarshalError
	if errors.As(err, &marshalErr) {
		return metrics.ReasonInvalid
	}

	return metrics.ClassifyErr(err)
}
