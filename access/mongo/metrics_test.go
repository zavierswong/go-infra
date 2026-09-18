package mongo

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zavierswong/go-infra/logger"
	"github.com/zavierswong/go-infra/metrics"
)

// 本文件测的是 metrics.go 里那两块**能脱离真实数据库**的逻辑：
//
//  1. poolCounters：CMAP 事件流 → metrics.PoolStats 的换算；
//  2. commandDetail：命令文档 → 日志摘要的构造。
//
// 之所以必须单独测，是因为这两块都是"把字段从一个结构搬进另一个结构"。
// 这类代码写错了不会报错，表现是"某条曲线一直是 0"或者"日志里少了一截"，
// 靠集成测试几乎发现不了 —— 只有逐字段断言才能钉住。

// ---------------------------------------------------------------------------
// 连接池计数器 → PoolStats
// ---------------------------------------------------------------------------

func TestPoolCountersSnapshot(t *testing.T) {
	// 构造一串事件，然后**手工算出**每个字段该是多少。
	// 期望值是算出来的，不是从实现里抄的。
	var c poolCounters
	for _, evt := range []*event.PoolEvent{
		{Type: event.ConnectionCreated},                                               // created=1
		{Type: event.ConnectionCreated},                                               // created=2
		{Type: event.ConnectionCreated},                                               // created=3
		{Type: event.ConnectionCreated},                                               // created=4
		{Type: event.ConnectionCreated},                                               // created=5
		{Type: event.ConnectionReady},                                                 // 不改变任何计数
		{Type: event.ConnectionCheckOutStarted},                                       // started=1
		{Type: event.ConnectionCheckOutStarted},                                       // started=2
		{Type: event.ConnectionCheckOutStarted},                                       // started=3
		{Type: event.ConnectionCheckedOut, Duration: 5 * time.Millisecond},            // checkedOut=1
		{Type: event.ConnectionCheckedOut, Duration: 7 * time.Millisecond},            // checkedOut=2
		{Type: event.ConnectionCheckOutFailed, Reason: event.ReasonTimedOut},          // failedTimeout=1
		{Type: event.ConnectionClosed, Reason: event.ReasonIdle},                      // closed=1 idleClosed=1
		{Type: event.ConnectionClosed, Reason: event.ReasonStale},                     // closed=2 staleClosed=1
		{Type: event.ConnectionClosed, Reason: event.ReasonError},                     // closed=3 errorClosed=1
		{Type: event.ConnectionCheckOutFailed, Reason: event.ReasonPoolClosed},        // failedClosed=1
		{Type: event.ConnectionCheckOutFailed, Reason: event.ReasonConnectionErrored}, // failedConnErr=1
		{Type: event.ConnectionPoolCleared},                                           // poolCleared=1
	} {
		c.observe(evt)
	}

	got := c.snapshot("order", 100)

	// created=5, closed=3 → 存活 2
	// checkedOut=2 → InUse=2；Idle = 2-2 = 0
	// started=3, failed=1+1+1=3 → Pending = 3-2-3 = -2 → 夹到 0
	want := metrics.PoolStats{
		Component: metrics.ComponentMongoDB,
		Instance:  "order",
		MaxOpen:   100,
		Open:      2,
		InUse:     2,
		Idle:      0,
		Pending:   0,

		WaitCount:    2,
		WaitDuration: 12 * time.Millisecond,

		MaxIdleTimeClosed: 1,
		Timeouts:          1,
		Unusable:          2, // failedConnErr(1) + errorClosed(1)
		Stale:             1,
	}

	if got != want {
		t.Errorf("PoolStats 不符。\n实际: %+v\n期望: %+v", got, want)
	}

	// 驱动没有的概念必须保持 0 —— 给它们建指标只会得到一堆永远为 0 的曲线。
	if got.MaxIdleClosed != 0 {
		t.Errorf("MaxIdleClosed 恒为 0（驱动没有空闲连接数上限），实际 %d", got.MaxIdleClosed)
	}
	if got.MaxLifetimeClosed != 0 {
		t.Errorf("MaxLifetimeClosed 恒为 0（驱动没有连接最长寿命），实际 %d", got.MaxLifetimeClosed)
	}
	if got.Hits != 0 || got.Misses != 0 {
		t.Errorf("Hits/Misses 恒为 0（驱动不暴露是否命中空闲连接），实际 %d/%d",
			got.Hits, got.Misses)
	}

	// PoolCleared 不在 PoolStats 里，但内部计数器要留着。
	if c.poolCleared.Load() != 1 {
		t.Errorf("poolCleared 期望 1，实际 %d", c.poolCleared.Load())
	}
}

func TestPoolCountersSnapshotClampsNegative(t *testing.T) {
	// 计数器是**分开读**的（各自 Load），并发下可能读到中间态：
	// 例如"连接已建好并检出，但 ConnectionCreated 还没算进 created"。
	// 这时 live - inUse 会是负数。负的 Gauge 会让图表与告警阈值全部失真，
	// 所以 snapshot 必须把它们夹到 0。
	var c poolCounters

	// 只记"检出"，不记"创建"：live=0 而 inUse=1。
	c.observe(&event.PoolEvent{Type: event.ConnectionCheckedOut})
	// 大量失败的检出，使 started - checkedOut - failed 为负。
	c.observe(&event.PoolEvent{Type: event.ConnectionCheckOutStarted})
	c.observe(&event.PoolEvent{Type: event.ConnectionCheckOutFailed, Reason: event.ReasonTimedOut})
	c.observe(&event.PoolEvent{Type: event.ConnectionCheckOutFailed, Reason: event.ReasonTimedOut})
	c.observe(&event.PoolEvent{Type: event.ConnectionCheckOutFailed, Reason: event.ReasonPoolClosed})

	got := c.snapshot("x", 0)

	if got.Open < 0 {
		t.Errorf("Open 不应为负，实际 %d", got.Open)
	}
	if got.Idle < 0 {
		t.Errorf("Idle 不应为负，实际 %d", got.Idle)
	}
	if got.Pending < 0 {
		t.Errorf("Pending 不应为负，实际 %d", got.Pending)
	}
	if got.InUse != 1 {
		t.Errorf("InUse 期望 1，实际 %d", got.InUse)
	}
	if got.Idle != 0 {
		t.Errorf("Idle 应被夹到 0，实际 %d", got.Idle)
	}
}

func TestPoolCountersIgnoresNilEvent(t *testing.T) {
	// 驱动的回调理论上不会给 nil，但"防御一个 nil"比"线上 panic"便宜太多。
	var c poolCounters
	c.observe(nil)

	got := c.snapshot("x", 10)
	if got.Open != 0 || got.InUse != 0 || got.Pending != 0 {
		t.Errorf("nil 事件不应改变任何计数，实际 %+v", got)
	}
}

func TestPoolCountersUnknownReasonIgnored(t *testing.T) {
	// 驱动将来新增 reason 时，不能把计数算到已有的桶里 ——
	// 那会让既有曲线出现无法解释的跳变。未知 reason 只记"关了一条连接"。
	var c poolCounters
	c.observe(&event.PoolEvent{Type: event.ConnectionClosed, Reason: "someFutureReason"})

	got := c.snapshot("x", 1)
	if got.Open != 0 {
		// created=0, closed=1 → live=-1 → 夹到 0
		t.Errorf("Open 期望 0，实际 %d", got.Open)
	}
	if got.MaxIdleTimeClosed != 0 || got.Stale != 0 || got.Unusable != 0 {
		t.Errorf("未知 reason 不应落进任何分类桶，实际 %+v", got)
	}
}

func TestPoolCountersMonotonicAcrossReconnect(t *testing.T) {
	// 计数器**不随重建清零**：它的语义是"这个实例生命周期内发生过什么"，
	// 本来就该单调增。清零会让 Prometheus 的 rate() 出现反向尖刺。
	var c poolCounters
	c.observe(&event.PoolEvent{Type: event.ConnectionCreated})
	c.observe(&event.PoolEvent{Type: event.ConnectionCheckOutStarted})
	c.observe(&event.PoolEvent{Type: event.ConnectionCheckedOut})

	first := c.snapshot("x", 10)

	c.observe(&event.PoolEvent{Type: event.ConnectionCreated})
	c.observe(&event.PoolEvent{Type: event.ConnectionCheckOutStarted})
	c.observe(&event.PoolEvent{Type: event.ConnectionCheckedOut})
	second := c.snapshot("x", 10)

	if second.WaitCount <= first.WaitCount {
		t.Errorf("WaitCount 应单调增：first=%d second=%d", first.WaitCount, second.WaitCount)
	}
	if second.Open <= first.Open {
		t.Errorf("Open 应单调增（无连接关闭时）：first=%d second=%d", first.Open, second.Open)
	}
}

// ---------------------------------------------------------------------------
// 命令摘要（Detail）
// ---------------------------------------------------------------------------

// raw 把 bson.D 编成 bson.Raw，模拟驱动交给监听器的命令文档。
func raw(t *testing.T, doc bson.D) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal 失败: %v", err)
	}
	return b
}

func TestCommandDetail(t *testing.T) {
	tests := []struct {
		name string
		db   string
		cmd  string
		doc  bson.D
		want string
	}{
		{
			// 最典型的读：库.集合 + 命令 + 命中的白名单字段 + 子文档的字段名。
			name: "find",
			db:   "shop",
			cmd:  "find",
			doc: bson.D{
				{Key: "find", Value: "orders"},
				{Key: "filter", Value: bson.D{
					{Key: "user_id", Value: 1001},
					{Key: "status", Value: "paid"},
				}},
				{Key: "projection", Value: bson.D{
					{Key: "_id", Value: 1},
					{Key: "amount", Value: 1},
				}},
				{Key: "sort", Value: bson.D{{Key: "created_at", Value: -1}}},
				// 协议胶水字段：必须被丢掉，否则摘要会没法读。
				{Key: "$db", Value: "shop"},
				{Key: "lsid", Value: bson.D{{Key: "id", Value: "x"}}},
			},
			want: "shop.orders find filter{user_id,status} projection{_id,amount} sort{created_at}",
		},
		{
			// documents 是数组：只展开**第一个**元素。批量插入可能有上千个文档，
			// 全铺出来会把日志撑爆，而第一个的字段结构已能代表这一批。
			// 标签用 doc 而不是 documents，让"只展开了一个"这件事一眼可见。
			name: "insert",
			db:   "shop",
			cmd:  "insert",
			doc: bson.D{
				{Key: "insert", Value: "orders"},
				{Key: "documents", Value: bson.A{
					bson.D{
						{Key: "_id", Value: 1},
						{Key: "user_id", Value: 1001},
						{Key: "amount", Value: 99},
					},
					bson.D{{Key: "_id", Value: 2}},
				}},
			},
			want: "shop.orders insert doc{_id,user_id,amount}",
		},
		{
			// 更新语句里最该知道的是"改了哪些字段"，所以 $set 这类更新操作符
			// 即使不在白名单里也要展开 —— 否则 `u{$set{...}}` 只剩一个 u。
			name: "update",
			db:   "shop",
			cmd:  "update",
			doc: bson.D{
				{Key: "update", Value: "orders"},
				{Key: "updates", Value: bson.A{
					bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: 1}}},
						{Key: "u", Value: bson.D{
							{Key: "$set", Value: bson.D{
								{Key: "status", Value: "done"},
								{Key: "updated_at", Value: 1},
							}},
						}},
						{Key: "multi", Value: false},
					},
				}},
			},
			want: "shop.orders update updates{q{_id} u{$set{status,updated_at}}}",
		},
		{
			// pipeline 只列阶段名。阶段的**参数**就是业务数据，
			// 而"由哪几个阶段组成"已经足够判断这条聚合在干什么。
			name: "aggregate",
			db:   "shop",
			cmd:  "aggregate",
			doc: bson.D{
				{Key: "aggregate", Value: "orders"},
				{Key: "pipeline", Value: bson.A{
					bson.D{{Key: "$match", Value: bson.D{{Key: "user_id", Value: 1}}}},
					bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$status"}}}},
					bson.D{{Key: "$sort", Value: bson.D{{Key: "n", Value: -1}}}},
				}},
				{Key: "cursor", Value: bson.D{}},
			},
			want: "shop.orders aggregate pipeline[$match,$group,$sort]",
		},
		{
			// getMore 是个必须处理的特例：命令文档的第一个元素是**游标 ID（数字）**，
			// 集合名藏在后面的 collection 字段里。不处理的话，
			// 最该看的长查询反而没有任何上下文。
			name: "getMore",
			db:   "shop",
			cmd:  "getMore",
			doc: bson.D{
				{Key: "getMore", Value: int64(7387263492)},
				{Key: "collection", Value: "orders"},
			},
			want: "shop.orders getMore",
		},
		{
			// 库级命令没有集合名，于是库名与命令名直接相连。
			// 注意结尾是 `.` 而不是空格 —— 与上面几条保持同一种分隔规则。
			name: "ping",
			db:   "admin",
			cmd:  "ping",
			doc:  bson.D{{Key: "ping", Value: 1}},
			want: "admin.ping",
		},
		{
			// 敏感命令的文档可能含凭据。驱动本身已经把这些命令的 Command 置空，
			// 这里是**第二层保险**：即使驱动行为变了，也不会把认证握手写进日志。
			name: "敏感命令",
			db:   "admin",
			cmd:  "saslStart",
			doc:  bson.D{{Key: "saslStart", Value: 1}},
			want: "admin.saslStart [参数已省略：可能含凭据]",
		},
		{
			// 大小写不敏感：驱动对这几个命令名的大小写并不统一。
			name: "敏感命令大小写混写",
			db:   "admin",
			cmd:  "SASLContinue",
			doc:  bson.D{{Key: "SASLContinue", Value: 1}},
			want: "admin.SASLContinue [参数已省略：可能含凭据]",
		},
		{
			// 驱动对敏感命令会把 Command 置空；此时不能 panic，也不能丢命令名。
			name: "文档为空（被驱动脱敏）",
			db:   "admin",
			cmd:  "authenticate",
			doc:  bson.D{},
			want: "admin.authenticate [参数已省略：可能含凭据]",
		},
		{
			// 没有库名时不能凭空补一个点。
			name: "库名为空",
			db:   "",
			cmd:  "ping",
			doc:  bson.D{{Key: "ping", Value: 1}},
			want: "ping",
		},
		{
			// 白名单里没有任何字段时，摘要就是"库.集合 + 命令名"，
			// 至少能回答"哪个集合上跑了什么"。
			name: "只有集合名与命令名",
			db:   "shop",
			cmd:  "count",
			doc:  bson.D{{Key: "count", Value: "orders"}},
			want: "shop.orders count",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := commandDetail(tc.db, tc.cmd, raw(t, tc.doc))
			if got != tc.want {
				t.Errorf("commandDetail 不符。\n实际: %q\n期望: %q", got, tc.want)
			}
		})
	}
}

func TestCommandDetailNeverLeaksValues(t *testing.T) {
	// 这是本包最重要的隐私约定，比 redis 包的策略更严：
	// **只给结构，不给取值**。
	//
	// filter / documents / update 的取值就是业务数据 —— 可能含身份证号、手机号，
	// 甚至用户自己存的明文口令 —— 而 Detail 会被写进日志与 tracing span。
	// 字段名是 schema 的一部分，不是数据，所以可以打。
	secret := "13800138000"
	doc := raw(t, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{
			{Key: "phone", Value: secret},
			{Key: "id_card", Value: "110101199001011234"},
			{Key: "password", Value: "hunter2"},
		}},
	})

	got := commandDetail("app", "find", doc)

	for _, leak := range []string{secret, "110101199001011234", "hunter2"} {
		if strings.Contains(got, leak) {
			t.Errorf("Detail 泄漏了取值 %q，实际: %s", leak, got)
		}
	}
	// 但字段名必须打出来 —— 那是归因需要的全部信息。
	for _, field := range []string{"phone", "id_card", "password"} {
		if !strings.Contains(got, field) {
			t.Errorf("Detail 应包含字段名 %q，实际: %s", field, got)
		}
	}
}

func TestCommandDetailScalarAllowlist(t *testing.T) {
	// 唯一的例外是 key / hint：它们是**字段名与索引名**，
	// 属于结构信息而不是业务数据，所以允许输出取值。
	doc := raw(t, bson.D{
		{Key: "distinct", Value: "orders"},
		{Key: "key", Value: "user_id"},
	})
	got := commandDetail("shop", "distinct", doc)
	if !strings.Contains(got, "key=user_id") {
		t.Errorf("distinct 的 key 是字段名，应当输出取值，实际: %s", got)
	}

	// hint 是索引名，同理。
	doc = raw(t, bson.D{
		{Key: "find", Value: "orders"},
		{Key: "hint", Value: "idx_user_status"},
	})
	got = commandDetail("shop", "find", doc)
	if !strings.Contains(got, "hint=idx_user_status") {
		t.Errorf("hint 是索引名，应当输出取值，实际: %s", got)
	}
}

func TestCommandDetailIsBounded(t *testing.T) {
	// 命令文档的大小由业务控制 —— 有人会往 filter 里塞几十个条件。
	// 摘要必须有界，否则一条慢查询的日志可能有几 KB，
	// 而慢查询日志恰恰是量最大的那一类。
	filter := bson.D{}
	for i := 0; i < 200; i++ {
		filter = append(filter, bson.E{Key: fieldName(i), Value: i})
	}
	doc := raw(t, bson.D{
		{Key: "find", Value: "orders"},
		{Key: "filter", Value: filter},
	})

	got := commandDetail("shop", "find", doc)

	if fields := strings.Count(got, ",") + 1; fields > maxDetailNames+2 {
		t.Errorf("字段数 %d 超出上限（预算 %d）: %s", fields, maxDetailNames, got)
	}
	if len(got) > 512 {
		t.Errorf("Detail 长度 %d 过长: %s", len(got), got)
	}
	// 截断必须留痕，否则会让人以为"这个 filter 就只有这些条件"。
	if !strings.Contains(got, "…") {
		t.Errorf("超出预算时应以 … 标记截断，实际: %s", got)
	}
}

func TestCommandDetailDeepNestingIsBounded(t *testing.T) {
	// 嵌套深度同样有界：否则一个刻意构造的深层文档能把日志打爆。
	deep := bson.D{{Key: "$set", Value: bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "$set", Value: bson.D{
					{Key: "$set", Value: bson.D{{Key: "deep", Value: 1}}},
				}},
			}},
		}},
	}}}
	doc := raw(t, bson.D{
		{Key: "update", Value: "orders"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "u", Value: deep},
		}}},
	})

	// 不 panic、不无限递归，就是这里要保证的事。
	got := commandDetail("shop", "update", doc)
	if len(got) > 512 {
		t.Errorf("深度嵌套时 Detail 长度 %d 过长: %s", len(got), got)
	}
}

func TestCommandDetailHandlesEmptyAndCorruptInput(t *testing.T) {
	// nil 文档（驱动对敏感命令会置空）与空文档都不能 panic。
	if got := commandDetail("admin", "ping", nil); got != "admin.ping" {
		t.Errorf("nil 文档时期望 admin.ping，实际 %q", got)
	}
	if got := commandDetail("", "", nil); got != "" {
		t.Errorf("库名与命令名都为空时期望空串，实际 %q", got)
	}
}

func fieldName(i int) string {
	// 生成定长字段名，便于断言"字段数有界"而不受名字长度影响。
	const letters = "abcdefghijklmnopqrstuvwxyz"
	return string([]byte{letters[i%26], letters[(i/26)%26], letters[(i/676)%26]})
}

// ---------------------------------------------------------------------------
// 命令监听器
// ---------------------------------------------------------------------------

func TestCommandMonitorStartedTakePair(t *testing.T) {
	// Started 存文档、take 取走并删除。两者必须配对 ——
	// 不配对就是内存泄漏，而泄漏的是每条命令的 bson.Raw 副本。
	m := &commandMonitor{}
	doc := raw(t, bson.D{{Key: "find", Value: "orders"}})

	m.Started(context.Background(), &event.CommandStartedEvent{
		RequestID: 7,
		Command:   doc,
	})

	got := m.take(7)
	if len(got) == 0 {
		t.Fatal("take 应取回 Started 存的文档")
	}
	// 第二次取必须为空（LoadAndDelete 语义）。
	if again := m.take(7); len(again) != 0 {
		t.Errorf("同一个 RequestID 被取了两次，第二次应返回 nil，实际 %d 字节", len(again))
	}
	// 未知的 RequestID 也要安全返回。
	if missing := m.take(999); len(missing) != 0 {
		t.Errorf("未知 RequestID 应返回 nil，实际 %d 字节", len(missing))
	}
}

func TestCommandMonitorNilEvents(t *testing.T) {
	// 三个回调都要能扛住 nil 事件：驱动在异常路径下有可能传 nil，
	// 而在回调里 panic 会把业务 goroutine 一起带走。
	m := &commandMonitor{
		obs:   metrics.NopObserver{},
		log:   logger.NewPlog("MongoDB-Test"),
		level: logLevelSilent,
	}
	m.Started(context.Background(), nil)
	m.Succeeded(context.Background(), nil)
	m.Failed(context.Background(), nil)
}

func TestCommandMonitorObservesEvent(t *testing.T) {
	// 成功命令与失败命令都要上报，且 Reason 要按 MongoDB 的语义归类。
	var events []metrics.Event
	obs := metrics.ObserverFunc(func(e metrics.Event) { events = append(events, e) })

	m := &commandMonitor{
		obs:      obs,
		log:      logger.NewPlog("MongoDB-Test"),
		instance: "order",
		level:    logLevelSilent,
		slow:     100 * time.Millisecond,
	}

	doc := raw(t, bson.D{{Key: "find", Value: "orders"}})
	m.Started(context.Background(), &event.CommandStartedEvent{RequestID: 1, Command: doc})
	m.Succeeded(context.Background(), &event.CommandSucceededEvent{
		CommandFinishedEvent: event.CommandFinishedEvent{
			CommandName:  "find",
			DatabaseName: "shop",
			RequestID:    1,
			Duration:     3 * time.Millisecond,
		},
	})

	if len(events) != 1 {
		t.Fatalf("成功命令应上报 1 个事件，实际 %d 个", len(events))
	}
	got := events[0]
	if got.Component != metrics.ComponentMongoDB {
		t.Errorf("Component 期望 %q，实际 %q", metrics.ComponentMongoDB, got.Component)
	}
	if got.Instance != "order" {
		t.Errorf("Instance 期望 order，实际 %q", got.Instance)
	}
	// Op 取命令名原文，与 mongodb_exporter、db.currentOp() 的输出对齐。
	if got.Op != metrics.Op("find") {
		t.Errorf("Op 期望 find，实际 %q", got.Op)
	}
	if got.Detail != "shop.orders find filter{}" && !strings.Contains(got.Detail, "shop.orders find") {
		t.Errorf("Detail 应含 shop.orders find，实际 %q", got.Detail)
	}
	if got.Failed() {
		t.Error("成功命令不应标记为失败")
	}

	// 失败命令。
	m.Failed(context.Background(), &event.CommandFailedEvent{
		CommandFinishedEvent: event.CommandFinishedEvent{
			CommandName:  "insert",
			DatabaseName: "shop",
			RequestID:    2,
			Duration:     5 * time.Millisecond,
		},
		Failure: mongo.ErrNoDocuments,
	})

	if len(events) != 2 {
		t.Fatalf("失败命令应再上报 1 个事件，实际 %d 个", len(events))
	}
	failed := events[1]
	if !failed.Failed() {
		t.Error("失败命令应标记为失败")
	}
	if failed.Reason != metrics.ReasonNotFound {
		t.Errorf("ErrNoDocuments 应归为 %q，实际 %q", metrics.ReasonNotFound, failed.Reason)
	}
	// 关键：查不到文档是**正常业务结果**，不能污染错误率。
	if failed.IsError() {
		t.Error("ErrNoDocuments 属于正常结果，IsError() 应为 false")
	}
}

func TestCommandMonitorSlowDetection(t *testing.T) {
	m := &commandMonitor{slow: 100 * time.Millisecond}
	if m.isSlow(50 * time.Millisecond) {
		t.Error("50ms 不应判定为慢操作（阈值 100ms）")
	}
	if !m.isSlow(100 * time.Millisecond) {
		t.Error("正好等于阈值应判定为慢操作")
	}
	if !m.isSlow(200 * time.Millisecond) {
		t.Error("200ms 应判定为慢操作")
	}

	// 负阈值 = 不判定慢操作（warn 级别下只打失败）。
	off := &commandMonitor{slow: -1}
	if off.isSlow(time.Hour) {
		t.Error("负阈值表示不判定慢操作")
	}
}

// 确认 logger 与 metrics 在本文件里都被真正用到（避免 import 被误删）。
var (
	_ = logger.NewPlog
	_ = metrics.NopObserver{}
)
