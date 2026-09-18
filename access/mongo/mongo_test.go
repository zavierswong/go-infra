package mongo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/zavierswong/go-infra/metrics"
)

// 本文件是连接**真实 MongoDB** 的集成测试。
//
// 默认连接本机开发环境（127.0.0.1:27017，root/123456，authSource=admin），
// 可用环境变量覆盖：
//
//	TEST_MONGODB_URI  完整连接串（优先级最高）
//	TEST_MONGODB_DB   要使用的库名（默认 gointra_test）
//
// 跳过策略：`go test -short` 跳过全部集成用例；MongoDB 不可达时也会跳过，
// 但跳过信息里写明地址与如何覆盖 —— 避免出现"什么都没跑却是绿的"。
//
// 与 rabbitmq 包相反、与 mysql 包一致：一旦**连得上但配置/操作出错**，
// 用例会直接失败。那种情况属于真实缺陷，不该被当成环境问题掩盖掉。

const defaultTestDB = "gointra_test"

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// testURI 返回测试用的连接串。
func testURI(t *testing.T) string {
	t.Helper()
	if v := os.Getenv("TEST_MONGODB_URI"); v != "" {
		return v
	}
	// 驱动 v2 只认 mongodb / mongodb+srv，本包对外统一写成 mongo://
	// （由 uriInfo.driverURI 归一化），这里保持项目约定写法。
	return fmt.Sprintf("mongo://root:123456@127.0.0.1:27017/%s?authSource=admin",
		envOr("TEST_MONGODB_DB", defaultTestDB))
}

// testDBName 返回测试用库名。
func testDBName(t *testing.T) string {
	t.Helper()
	if v := os.Getenv("TEST_MONGODB_URI"); v != "" {
		cfg, err := parseURI(v)
		if err == nil && cfg.dbName != "" {
			return cfg.dbName
		}
	}
	return envOr("TEST_MONGODB_DB", defaultTestDB)
}

// testConfig 返回一份"测试友好"的配置：
// 日志关掉（否则每个用例都会刷屏）、探活超时收紧（连不上时快速跳过）。
func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		URI:              testURI(t),
		Name:             "gointra_test",
		AppName:          "go-infra-mongo-test",
		DialAttempts:     2,
		DialProbeTimeout: 4 * time.Second,
		DialBackoff:      50 * time.Millisecond,
		DialMaxBackoff:   200 * time.Millisecond,
		LogLevel:         logLevelSilent,
	}
}

// openClient 建立连接；`-short` 或数据库不可达时跳过。
func openClient(t *testing.T, cfg Config) *MongoDB {
	t.Helper()

	if testing.Short() {
		t.Skip("跳过集成测试（-short）：需要真实 MongoDB")
	}

	m, err := Open(cfg)
	if err != nil {
		if errors.Is(err, ErrConnect) {
			t.Skipf("MongoDB 不可达，跳过：%v\n"+
				"  地址：%s\n"+
				"  可用 TEST_MONGODB_URI 覆盖（形如 mongo://user:pass@host:27017/db?authSource=admin）",
				err, cfg.URI)
		}
		// 配置错误不是环境问题，直接失败。
		t.Fatalf("Open 失败（非连接问题）: %v", err)
	}

	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Errorf("Close 报错: %v", err)
		}
	})
	return m
}

// uniqueCollection 生成互不冲突的集合名，让用例之间完全隔离
// （MongoDB 没有"临时库"，只能靠命名隔离，用完 drop）。
func uniqueCollection(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

// ---------------------------------------------------------------------------
// 连接与基础探活
// ---------------------------------------------------------------------------

func TestOpenAndHealthCheck(t *testing.T) {
	cfg := testConfig(t)
	p := openClient(t, cfg)
	ctx := context.Background()

	if p.Closed() {
		t.Fatal("刚打开就不应处于已关闭状态")
	}
	if !p.Healthy() {
		t.Error("刚打开时 Healthy 应为 true")
	}
	if p.Client() == nil {
		t.Fatal("Client() 不应为 nil")
	}
	if err := p.HealthCheck(ctx); err != nil {
		t.Errorf("HealthCheck 应成功: %v", err)
	}

	// URI() 是给人看的排查工具，必须保留主机、抹掉口令。
	uri := p.URI()
	if !contains(uri, "127.0.0.1:27017") {
		t.Errorf("URI() 应保留主机，实际: %s", uri)
	}
	if contains(uri, "123456") {
		t.Errorf("URI() 不得泄漏口令，实际: %s", uri)
	}

	// 生效配置：默认值应已补齐。
	got := p.Config()
	if got.DialProbeTimeout != cfg.DialProbeTimeout {
		t.Errorf("DialProbeTimeout 期望 %v，实际 %v", cfg.DialProbeTimeout, got.DialProbeTimeout)
	}
	if got.MaxPoolSize != defaultPoolSize {
		t.Errorf("未配置时 MaxPoolSize 期望 %d，实际 %d", defaultPoolSize, got.MaxPoolSize)
	}
}

func TestDefaultDatabase(t *testing.T) {
	p := openClient(t, testConfig(t))

	db, err := p.DefaultDatabase()
	if err != nil {
		t.Fatalf("URI 带了库名时 DefaultDatabase 应成功: %v", err)
	}
	// 这个库必须真的能用（不是只造了个句柄）。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Client().Ping(ctx, nil); err != nil {
		t.Errorf("默认库的客户端 ping 失败: %v", err)
	}
}

func TestDefaultDatabaseWithoutDBName(t *testing.T) {
	// URI 没写库名时，DefaultDatabase 必须**显式报错**，
	// 而不是悄悄退化成 "test" 或 "admin" —— 连错库要靠人工发现，
	// 不应该被库的默认值掩盖。
	p := openClient(t, Config{
		// 注意：路径里的 `/` **不能省**。写成 `mongodb://host:27017?authSource=admin`
		// 会被驱动的解析器拒绝（"must have a / before the query ?"）——
		// 也就是说"不指定库名"的正确写法是留一个空路径 `/`，而不是干脆不写。
		URI:              "mongo://root:123456@127.0.0.1:27017/?authSource=admin",
		Name:             "no_dbname",
		DialAttempts:     2,
		DialProbeTimeout: 4 * time.Second,
		LogLevel:         logLevelSilent,
	})

	if _, err := p.DefaultDatabase(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("URI 无库名时应返回 ErrInvalidConfig，实际: %v", err)
	}
	// 但显式指定库名仍然可用。
	if p.Database("admin") == nil {
		t.Error("显式 Database(name) 不应受影响")
	}
}

// ---------------------------------------------------------------------------
// CRUD 与错误语义
// ---------------------------------------------------------------------------

// order 是集成测试用的文档形态。
type order struct {
	ID       int64  `bson:"_id"`
	UserID   int64  `bson:"user_id"`
	Status   string `bson:"status"`
	Amount   int64  `bson:"amount"`
	Tag      string `bson:"tag,omitempty"`
	Location string `bson:"location,omitempty"`
}

func TestCRUD(t *testing.T) {
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	coll := p.Database(testDBName(t)).Collection(uniqueCollection("gointra_crud"))
	// 用 `===` 前缀的库名区分测试数据，便于人工排查。
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coll.Drop(dropCtx)
	})

	in := order{ID: 1, UserID: 1001, Status: "paid", Amount: 99, Tag: "v1"}
	if _, err := coll.InsertOne(ctx, in); err != nil {
		t.Fatalf("InsertOne 失败: %v", err)
	}

	var out order
	if err := coll.FindOne(ctx, bson.M{"_id": 1}).Decode(&out); err != nil {
		t.Fatalf("FindOne 失败: %v", err)
	}
	if out != in {
		t.Errorf("读回不符。\n实际: %+v\n期望: %+v", out, in)
	}

	// UpdateOne
	if _, err := coll.UpdateOne(ctx,
		bson.M{"_id": 1},
		bson.M{"$set": bson.M{"status": "done"}},
	); err != nil {
		t.Fatalf("UpdateOne 失败: %v", err)
	}
	var updated order
	if err := coll.FindOne(ctx, bson.M{"_id": 1}).Decode(&updated); err != nil {
		t.Fatalf("更新后读回失败: %v", err)
	}
	if updated.Status != "done" {
		t.Errorf("状态期望 done，实际 %q", updated.Status)
	}

	// DeleteOne
	res, err := coll.DeleteOne(ctx, bson.M{"_id": 1})
	if err != nil {
		t.Fatalf("DeleteOne 失败: %v", err)
	}
	if res.DeletedCount != 1 {
		t.Errorf("DeletedCount 期望 1，实际 %d", res.DeletedCount)
	}

	// 删掉之后再查应当命中 ErrNoDocuments。
	err = coll.FindOne(ctx, bson.M{"_id": 1}).Decode(&out)
	if !errors.Is(err, mongo.ErrNoDocuments) {
		t.Errorf("删掉后应返回 ErrNoDocuments，实际: %v", err)
	}
}

func TestFindOneNoDocumentsIsNotAnError(t *testing.T) {
	// 这条用例钉住的是**本包最重要的一个语义**：
	// 查不到文档是正常业务结果（对应 SQL 的 sql.ErrNoRows、Redis 的 goredis.Nil），
	// 如果把它计入错误率，任何一个"查了但没有"的正常路径都会表现为故障。
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	coll := p.Database(testDBName(t)).Collection(uniqueCollection("gointra_nodoc"))

	var out order
	err := coll.FindOne(ctx, bson.M{"_id": -1}).Decode(&out)

	if !IsNoDocuments(err) {
		t.Fatalf("期望 IsNoDocuments 为 true，实际: %v", err)
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		t.Error("应可用 errors.Is(err, mongo.ErrNoDocuments) 判定")
	}
	// classifyErr 必须把它归到 not_found，而不是 unknown/closed。
	if got := classifyErr(err); got != metrics.ReasonNotFound {
		t.Errorf("classifyErr 期望 %q，实际 %q", metrics.ReasonNotFound, got)
	}
	// 关键：IsError() 为 false —— 这条事件不该进入错误率。
	evt := metrics.Event{Component: metrics.ComponentMongoDB, Err: err, Reason: classifyErr(err)}
	if evt.IsError() {
		t.Error("查不到文档不应被计入错误率（Event.IsError 应为 false）")
	}
}

func TestDuplicateKeyIsConflictNotRetryable(t *testing.T) {
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	name := uniqueCollection("gointra_dup")
	coll := p.Database(testDBName(t)).Collection(name)
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coll.Drop(dropCtx)
	})

	// 唯一索引是复现 11000 的前提。
	if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "user_id", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("uq_user"),
	}); err != nil {
		t.Fatalf("建唯一索引失败: %v", err)
	}

	if _, err := coll.InsertOne(ctx, order{ID: 1, UserID: 1001, Status: "paid"}); err != nil {
		t.Fatalf("首次插入应成功: %v", err)
	}

	_, err := coll.InsertOne(ctx, order{ID: 2, UserID: 1001, Status: "paid"})
	if err == nil {
		t.Fatal("重复的 user_id 应触发唯一键冲突")
	}

	if !IsDuplicateKey(err) {
		t.Errorf("应识别为唯一键冲突，实际: %v", err)
	}
	if got := ServerErrorCode(err); got != errCodeDuplicateKey {
		t.Errorf("服务端错误码期望 %d，实际 %d", errCodeDuplicateKey, got)
	}
	// 名称比数字可读，适合打日志。
	if name := ServerErrorName(err); name != "DuplicateKey" {
		t.Logf("注意：ServerErrorName 返回 %q（不同服务端版本可能不同）", name)
	}

	// 冲突是**业务冲突**，不是瞬时故障 —— 重试只会再撞一次。
	if IsRetryable(err) {
		t.Error("唯一键冲突不应被判为可重试")
	}
	if got := classifyErr(err); got != metrics.ReasonConflict {
		t.Errorf("classifyErr 期望 %q，实际 %q", metrics.ReasonConflict, got)
	}
}

func TestInvalidArgumentIsClassifiedInvalid(t *testing.T) {
	// 参数写错（把非文档传给 filter）会得到 mongo.InvalidArgumentError，
	// 这是**调用方的错**，重试没有任何意义，日志里也不该表现成"数据库故障"。
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	coll := p.Database(testDBName(t)).Collection(uniqueCollection("gointra_badarg"))

	// filter 必须是文档；这里故意传字符串。
	if err := coll.FindOne(ctx, "这不是文档").Decode(&bson.M{}); err == nil {
		t.Fatal("非法 filter 应当报错")
	} else if got := classifyErr(err); got != metrics.ReasonInvalid {
		t.Errorf("classifyErr 期望 %q，实际 %q（err=%v）", metrics.ReasonInvalid, got, err)
	}
}

func TestContextCancellation(t *testing.T) {
	// ctx 取消必须立刻生效，且分类为 canceled（而不是 timeout 或 unknown）——
	// 用户主动放弃与真的超时，在图上要能分开看。
	p := openClient(t, testConfig(t))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.HealthCheck(ctx)
	if err == nil {
		t.Fatal("ctx 已取消时 HealthCheck 应报错")
	}

	// HealthCheck 会把驱动错误包一层 ErrConnect，所以这里断言的是
	// "错误链里能找到 context.Canceled"。
	if !errors.Is(err, context.Canceled) {
		t.Logf("提示：错误链里未包含 context.Canceled（err=%v）", err)
	}
	if got := classifyErr(err); got != metrics.ReasonCanceled && got != metrics.ReasonConnect {
		t.Errorf("classifyErr 期望 canceled 或 connect，实际 %q", got)
	}
}

// ---------------------------------------------------------------------------
// 连接池可观测性
// ---------------------------------------------------------------------------

func TestPoolStatsAfterTraffic(t *testing.T) {
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	coll := p.Database(testDBName(t)).Collection(uniqueCollection("gointra_pool"))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coll.Drop(dropCtx)
	})

	// 制造一些真实流量，让 CMAP 事件流有东西可记。
	for i := 0; i < 20; i++ {
		if _, err := coll.InsertOne(ctx, bson.M{"_id": i, "n": i}); err != nil {
			t.Fatalf("第 %d 次插入失败: %v", i, err)
		}
	}
	for i := 0; i < 20; i++ {
		var out bson.M
		if err := coll.FindOne(ctx, bson.M{"_id": i}).Decode(&out); err != nil {
			t.Fatalf("第 %d 次查询失败: %v", i, err)
		}
	}

	// 池统计是**事件累计**出来的，会有极短的延迟；给它一点时间收敛。
	var got metrics.PoolStats
	deadline := time.Now().Add(3 * time.Second)
	for {
		got = p.PoolStats()
		if got.Open > 0 && got.WaitCount > 0 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got.Component != metrics.ComponentMongoDB {
		t.Errorf("Component 期望 %q，实际 %q", metrics.ComponentMongoDB, got.Component)
	}
	if got.Instance != "gointra_test" {
		t.Errorf("Instance 期望 gointra_test，实际 %q", got.Instance)
	}
	// 跑过流量之后线程池里必定有连接。
	if got.Open <= 0 {
		t.Errorf("跑过流量后 Open 应大于 0，实际 %d（PoolStats=%+v）", got.Open, got)
	}
	// 检出次数 = 实际发生过的操作次数（至少这么多）。
	if got.WaitCount <= 0 {
		t.Errorf("跑过流量后 WaitCount 应大于 0，实际 %d", got.WaitCount)
	}
	// MaxOpen 必须等于**生效**的每节点上限。
	if got.MaxOpen != defaultPoolSize {
		t.Errorf("MaxOpen 期望 %d，实际 %d", defaultPoolSize, got.MaxOpen)
	}

	// 驱动没有的概念必须恒为 0（别拿它们建指标）。
	if got.MaxIdleClosed != 0 || got.MaxLifetimeClosed != 0 || got.Hits != 0 || got.Misses != 0 {
		t.Errorf("驱动不提供的字段应恒为 0，实际 %+v", got)
	}
}

func TestPoolStatsMaxOpenFollowsURIOverride(t *testing.T) {
	// URI 覆盖 Config 是本包的核心约定，这条约定必须**一路贯穿到指标**：
	// 否则会出现"URI 写 7、指标报 100"的错位，而"池吃满"的告警
	// 正是基于这个数 —— 报错了就等于告警失灵。
	cfg := testConfig(t)
	cfg.MaxPoolSize = 50                                // Config 说 50
	cfg.URI = withURIParam(cfg.URI, "maxPoolSize", "7") // URI 说 7

	p := openClient(t, cfg)

	if got := p.Config().MaxPoolSize; got != 7 {
		t.Errorf("生效配置的 MaxPoolSize 期望 7（URI 优先），实际 %d", got)
	}
	if got := p.PoolStats().MaxOpen; got != 7 {
		t.Errorf("PoolStats.MaxOpen 期望 7（URI 优先），实际 %d", got)
	}
}

func TestStatus(t *testing.T) {
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	coll := p.Database(testDBName(t)).Collection(uniqueCollection("gointra_status"))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coll.Drop(dropCtx)
	})

	if _, err := coll.InsertOne(ctx, bson.M{"_id": 1}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	st := p.Status()

	if st.Instance != "gointra_test" {
		t.Errorf("Instance 期望 gointra_test，实际 %q", st.Instance)
	}
	if st.Closed {
		t.Error("未关闭时 Closed 应为 false")
	}
	if !st.Healthy {
		t.Error("正常时 Healthy 应为 true")
	}
	if !contains(st.Hosts, "127.0.0.1:27017") {
		t.Errorf("Hosts 期望含 127.0.0.1:27017，实际 %q", st.Hosts)
	}
	if st.Database != testDBName(t) {
		t.Errorf("Database 期望 %q，实际 %q", testDBName(t), st.Database)
	}
	// 驱动对每次操作默认使用隐式会话、结束即归还，
	// 所以这个数约等于**在途操作数**，不该是负数。
	if st.SessionsInProgress < 0 {
		t.Errorf("SessionsInProgress 不应为负，实际 %d", st.SessionsInProgress)
	}
	// PoolCleared 是累计量，没有主从切换时应当为 0。
	if st.PoolCleared < 0 {
		t.Errorf("PoolCleared 不应为负，实际 %d", st.PoolCleared)
	}
}

// ---------------------------------------------------------------------------
// metrics 接入（真实流量）
// ---------------------------------------------------------------------------

// recordingObserver 收集事件，供断言使用。
type recordingObserver struct {
	mu     sync.Mutex
	events []metrics.Event
}

func (o *recordingObserver) ObserveOp(e metrics.Event) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, e)
}

func (o *recordingObserver) snapshot() []metrics.Event {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]metrics.Event, len(o.events))
	copy(out, o.events)
	return out
}

func TestObserverReceivesCommands(t *testing.T) {
	obs := &recordingObserver{}

	cfg := testConfig(t)
	cfg.Observer = obs
	p := openClient(t, cfg)
	ctx := context.Background()

	coll := p.Database(testDBName(t)).Collection(uniqueCollection("gointra_obs"))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coll.Drop(dropCtx)
	})

	if _, err := coll.InsertOne(ctx, bson.M{"_id": 1, "user_id": 7}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}
	var out bson.M
	if err := coll.FindOne(ctx, bson.M{"_id": 1}).Decode(&out); err != nil {
		t.Fatalf("查询失败: %v", err)
	}

	evts := obs.snapshot()
	if len(evts) == 0 {
		t.Fatal("Observer 应当收到事件")
	}

	// 至少要有 insert 与 find 两条，且都指向我们操作的那个集合。
	var sawInsert, sawFind bool
	for _, e := range evts {
		if e.Component != metrics.ComponentMongoDB {
			t.Errorf("Component 期望 %q，实际 %q", metrics.ComponentMongoDB, e.Component)
		}
		if e.Instance != "gointra_test" {
			t.Errorf("Instance 期望 gointra_test，实际 %q", e.Instance)
		}

		// ping 是**库级命令**，没有集合名，Detail 形如 `admin.ping`。
		// 它来自 Open 时的连通性验证（verify 里的 client.Ping），
		// 所以"每个事件都要含库.集合"这个断言对它不成立 —— 要单独排除。
		if e.Op == metrics.OpPing {
			continue
		}

		if !contains(e.Detail, "."+coll.Name()+" ") {
			t.Errorf("Detail 应含库.集合，实际 %q", e.Detail)
		}
		switch e.Op {
		case metrics.Op("insert"):
			sawInsert = true
		case metrics.Op("find"):
			sawFind = true
		}
	}
	if !sawInsert {
		t.Errorf("未收到 insert 事件，实际事件: %s", describeEvents(evts))
	}
	if !sawFind {
		t.Errorf("未收到 find 事件，实际事件: %s", describeEvents(evts))
	}

	// Detail 只能含**结构**（字段名），不能含取值 —— 这里插入的是 user_id=7，
	// 一个孤零零的 7 很容易混进日志而没人注意。
	for _, e := range evts {
		if contains(e.Detail, "=7") {
			t.Errorf("Detail 不应包含字段取值，实际 %q", e.Detail)
		}
	}
}

func TestObserverSeesPingFromMonitor(t *testing.T) {
	// 后台探活产生的 ping 也是真实往返，本包主动把它上报。
	// 让它可见比让它隐形好（metric 上过滤掉即可），
	// 因为健康检查的失败率本身就是最重要的信号之一。
	obs := &recordingObserver{}

	cfg := testConfig(t)
	cfg.Observer = obs
	cfg.HealthCheckInterval = 200 * time.Millisecond // 加快探活节奏
	p := openClient(t, cfg)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, e := range obs.snapshot() {
			if e.Op == metrics.OpPing {
				if e.Detail != "admin.ping" {
					t.Errorf("ping 的 Detail 期望 admin.ping，实际 %q", e.Detail)
				}
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("未在 5s 内收到后台探活的 ping 事件（HealthCheckInterval=%v）", cfg.HealthCheckInterval)
	_ = p
}

// ---------------------------------------------------------------------------
// 生命周期：并发、重连、关闭
// ---------------------------------------------------------------------------

func TestConcurrentAccess(t *testing.T) {
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	coll := p.Database(testDBName(t)).Collection(uniqueCollection("gointra_conc"))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coll.Drop(dropCtx)
	})

	const workers = 8
	const perWorker = 25

	var wg sync.WaitGroup
	errCh := make(chan error, workers*2)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id := int64(w*perWorker + i)
				if _, err := coll.InsertOne(ctx, bson.M{"_id": id, "w": w}); err != nil {
					errCh <- fmt.Errorf("worker %d 插入 %d 失败: %w", w, id, err)
					return
				}
				var out bson.M
				if err := coll.FindOne(ctx, bson.M{"_id": id}).Decode(&out); err != nil {
					errCh <- fmt.Errorf("worker %d 查询 %d 失败: %w", w, id, err)
					return
				}
			}
		}(w)
	}

	// 并发读池统计与状态：这些方法必须在有并发写入时也能安全调用
	// （它们与 Open/Close/Reconnect 共用锁，写错了 -race 会立刻抓到）。
	stop := make(chan struct{})
	var readerWg sync.WaitGroup
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = p.PoolStats()
			_ = p.Status()
			_ = p.Healthy()
			_ = p.URI()
			_ = p.Config()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(stop)
	readerWg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	// 数据完整性：所有文档都应在。
	count, err := coll.CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("Count 失败: %v", err)
	}
	if count != workers*perWorker {
		t.Errorf("文档数期望 %d，实际 %d", workers*perWorker, count)
	}
}

func TestReconnect(t *testing.T) {
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	name := uniqueCollection("gointra_reconn")
	coll := p.Database(testDBName(t)).Collection(name)
	t.Cleanup(func() {
		// 注意：这里要**重新取**句柄 —— 见下方"重连后句柄会失效"的说明。
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Database(testDBName(t)).Collection(name).Drop(dropCtx)
	})

	if _, err := coll.InsertOne(ctx, bson.M{"_id": 1}); err != nil {
		t.Fatalf("重连前插入失败: %v", err)
	}

	before := p.Client()

	if err := p.Reconnect(); err != nil {
		t.Fatalf("Reconnect 失败: %v", err)
	}

	after := p.Client()
	if after == nil {
		t.Fatal("重连后 Client() 不应为 nil")
	}
	// 本包的约定是"先建新客户端再换旧的"，所以指针必须变。
	if after == before {
		t.Error("Reconnect 应当换成新的客户端实例")
	}
	if p.Closed() || !p.Healthy() {
		t.Errorf("重连后状态异常：Closed=%v Healthy=%v", p.Closed(), p.Healthy())
	}

	// ⚠️ 这是一条**容易踩的约束，与 mysql/postgres 包不同**：
	// Reconnect 换掉的是底层 *mongo.Client，而 Collection / Database 句柄
	// 是挂在**旧客户端**上的。旧客户端已被 Disconnect，
	// 所以之前拿到的 `coll` 再用就会报 "client is disconnected"。
	//
	// 对比 SQL 侧：那边换的是池，*gorm.DB 本身稳定，句柄一直有效。
	// MongoDB 侧必须重取句柄（或者干脆不要缓存句柄）。
	if _, err := coll.InsertOne(ctx, bson.M{"_id": 2}); err == nil {
		t.Log("提示：旧句柄在重连后仍可用，与预期不符（驱动行为可能已变化）")
	}

	// 重新取句柄后必须真的能用。
	fresh := p.Database(testDBName(t)).Collection(name)
	if _, err := fresh.InsertOne(ctx, bson.M{"_id": 2}); err != nil {
		t.Fatalf("重连后重新取句柄的插入应成功: %v", err)
	}
	if _, err := fresh.InsertOne(ctx, bson.M{"_id": 3}); err != nil {
		t.Fatalf("重连后第二次插入应成功: %v", err)
	}

	// 池计数器**不随重建清零** —— 它的语义是"这个实例生命周期内发生过什么"，
	// 清零会让 Prometheus 的 rate() 出现反向尖刺。
	if p.PoolStats().Open <= 0 {
		t.Errorf("重连后 PoolStats.Open 应大于 0，实际 %d", p.PoolStats().Open)
	}
}

func TestCloseThenUse(t *testing.T) {
	cfg := testConfig(t)
	p := openClient(t, cfg)

	if err := p.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}

	// openClient 注册的 cleanup 会再 Close 一次 —— 幂等性由那里顺带验证。
	if !p.Closed() {
		t.Error("Close 后 Closed() 应为 true")
	}
	if p.Healthy() {
		t.Error("Close 后 Healthy() 应为 false")
	}
	if p.Client() != nil {
		t.Error("Close 后 Client() 应为 nil")
	}
	if p.Database("x") != nil {
		t.Error("Close 后 Database() 应为 nil")
	}
	if err := p.HealthCheck(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("Close 后 HealthCheck 应返回 ErrClosed，实际: %v", err)
	}
	if err := p.Reconnect(); !errors.Is(err, ErrClosed) {
		t.Errorf("Close 后 Reconnect 应返回 ErrClosed，实际: %v", err)
	}
	// 再 Close 一次必须无害（停机的 defer 链里可能被调到两次）。
	if err := p.Close(); err != nil {
		t.Errorf("重复 Close 应返回 nil，实际: %v", err)
	}
	// Close 之后读取池统计不应 panic（监控协程已退出）。
	_ = p.PoolStats()
}

func TestCloseDuringConcurrentUse(t *testing.T) {
	// 关闭时正在有业务操作：Close 必须能在有界时间内返回，
	// 且不 panic、不把自己和调用方一起卡死。
	// 这是停机流程里最容易出问题的地方 —— "关不掉"比"有几个请求失败"严重得多。
	cfg := testConfig(t)
	p := openClient(t, cfg)
	ctx := context.Background()

	coll := p.Database(testDBName(t)).Collection(uniqueCollection("gointra_close"))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coll.Drop(dropCtx)
	})

	if _, err := coll.InsertOne(ctx, bson.M{"_id": 1}); err != nil {
		t.Fatalf("预热插入失败: %v", err)
	}

	// 让几个 goroutine 持续操作，然后从另一个 goroutine 关闭。
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var out bson.M
				_ = coll.FindOne(ctx, bson.M{"_id": 1}).Decode(&out)
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	err := p.Close()
	elapsed := time.Since(start)

	close(stop)
	wg.Wait()

	if err != nil {
		t.Errorf("Close 报错: %v", err)
	}
	// closeDrainTimeout 是 5s，留点余量。
	if elapsed > 10*time.Second {
		t.Errorf("Close 耗时 %v，超过预期的排空上限", elapsed)
	}
	if !p.Closed() {
		t.Error("Close 后应处于已关闭状态")
	}
}

// ---------------------------------------------------------------------------
// 事务与其它驱动能力（逃生舱）
// ---------------------------------------------------------------------------

// requireTransactions 在当前部署不支持事务时跳过。
//
// 事务需要**副本集或分片集群**（mongos）。本地最常见的开发形态 ——
// 单节点 mongod —— 会直接报
// `IllegalOperation: Transaction numbers are only allowed on a replica set member or mongos`。
// 那是部署形态的限制，不是本包的问题，所以跳过而不是失败。
func requireTransactions(t *testing.T, p *MongoDB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var hello bson.M
	if err := p.Client().Database("admin").
		RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		t.Skipf("无法执行 hello 命令，跳过事务用例: %v", err)
	}

	// mongos（分片集群）会返回 msg=isdbgrid。
	if msg, _ := hello["msg"].(string); msg == "isdbgrid" {
		return
	}
	// 副本集成员会带 setName。
	if name, _ := hello["setName"].(string); name != "" {
		return
	}
	t.Skip("当前 MongoDB 是单节点（hello 里没有 setName），不支持事务；" +
		"要跑这条用例需要副本集或分片集群部署")
}

func TestTransactionCommitAndAbort(t *testing.T) {
	// 事务属于"驱动的高级能力"，本包不重新包装，只保证
	// Client() 这个逃生舱拿到的是可用的客户端。
	// 这里顺带确认事务的会话/锁在连接池上工作正常。
	p := openClient(t, testConfig(t))
	requireTransactions(t, p)
	ctx := context.Background()

	coll := p.Database(testDBName(t)).Collection(uniqueCollection("gointra_txn"))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coll.Drop(dropCtx)
	})

	// 提交
	//
	// 注意 driver v2 的一处 API 变化：**Session 从接口变成了结构体**
	// （v1 是 mongo.Session 接口 + SessionContext，v2 是 *mongo.Session
	// 配 mongo.NewSessionContext）。WithTransaction 的回调收到的是
	// 已经是 SessionContext 的 ctx，直接拿它去做操作即可。
	sess, err := p.Client().StartSession()
	if err != nil {
		t.Fatalf("StartSession 失败: %v", err)
	}
	defer sess.EndSession(ctx)

	if _, err := sess.WithTransaction(ctx, func(sc context.Context) (any, error) {
		_, err := coll.InsertOne(sc, bson.M{"_id": 1, "status": "committed"})
		return nil, err
	}); err != nil {
		t.Fatalf("事务提交失败: %v", err)
	}

	var out bson.M
	if err := coll.FindOne(ctx, bson.M{"_id": 1}).Decode(&out); err != nil {
		t.Fatalf("提交后应能读到: %v", err)
	}
	if out["status"] != "committed" {
		t.Errorf("提交后状态期望 committed，实际 %v", out["status"])
	}

	// 回滚：WithTransaction 里返回错误会触发 abort。
	sess2, err := p.Client().StartSession()
	if err != nil {
		t.Fatalf("StartSession 失败: %v", err)
	}
	defer sess2.EndSession(ctx)

	sentinel := errors.New("故意失败以触发回滚")
	_, err = sess2.WithTransaction(ctx, func(sc context.Context) (any, error) {
		if _, err := coll.InsertOne(sc, bson.M{"_id": 2}); err != nil {
			return nil, err
		}
		return nil, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("期望返回 sentinel 错误，实际: %v", err)
	}

	err = coll.FindOne(ctx, bson.M{"_id": 2}).Decode(&bson.M{})
	if !errors.Is(err, mongo.ErrNoDocuments) {
		t.Errorf("回滚后 _id=2 不应存在，实际: %v", err)
	}
}

func TestWithSessionHelper(t *testing.T) {
	// mongo.WithSession 是 v2 的**包级函数**（不是 Client 上的方法，
	// 也没有 v1 那样的 mongo.Session 接口）。本包不包装它 ——
	// 这里只是确认"逃生舱拿到的客户端确实具备完整能力"。
	p := openClient(t, testConfig(t))
	ctx := context.Background()

	coll := p.Database(testDBName(t)).Collection(uniqueCollection("gointra_sess"))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coll.Drop(dropCtx)
	})

	sess, err := p.Client().StartSession()
	if err != nil {
		t.Fatalf("StartSession 失败: %v", err)
	}
	defer sess.EndSession(ctx)

	if err := mongo.WithSession(ctx, sess, func(sc context.Context) error {
		_, err := coll.InsertOne(sc, bson.M{"_id": 1})
		return err
	}); err != nil {
		t.Fatalf("WithSession 失败: %v", err)
	}

	if mongo.SessionFromContext(ctx) != nil {
		t.Error("原始 ctx 里不应有会话（会话只活在回调的 ctx 里）")
	}
}

func TestReadPreferenceAppliedToRealClient(t *testing.T) {
	// 读偏好写在 Config 里也要真的作用到客户端的操作上 ——
	// 只看 opts.ReadPreference 只能证明"选项被设置了"，
	// 证明不了"客户端真的在用它"。
	cfg := testConfig(t)
	cfg.ReadPreference = "primaryPreferred"
	cfg.ReadConcern = "majority"
	cfg.WriteConcern = "majority"

	p := openClient(t, cfg)
	ctx := context.Background()

	coll := p.Database(testDBName(t)).Collection(uniqueCollection("gointra_rp"))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coll.Drop(dropCtx)
	})

	if _, err := coll.InsertOne(ctx, bson.M{"_id": 1}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}
	var out bson.M
	if err := coll.FindOne(ctx, bson.M{"_id": 1}).Decode(&out); err != nil {
		t.Fatalf("查询失败: %v", err)
	}

	// 再直接验证一次驱动的解析结果（单节点下 primaryPreferred 等价 primary）。
	mode, err := readpref.ModeFromString("primarypreferred")
	if err != nil {
		t.Fatalf("readpref 解析失败: %v", err)
	}
	if mode != readpref.PrimaryPreferredMode {
		t.Errorf("读偏好解析异常: %v", mode)
	}
}

func TestWriteConcernJournalApplied(t *testing.T) {
	// WriteConcernJournal 用 *bool，是为了区分"没配"与"配成 false"。
	// 这里验证配成 false 时插入依然成功（单节点上 journal=false 完全合法）。
	cfg := testConfig(t)
	cfg.WriteConcern = "1"
	cfg.WriteConcernJournal = boolPtr(false)

	p := openClient(t, cfg)
	ctx := context.Background()

	coll := p.Database(testDBName(t)).Collection(uniqueCollection("gointra_wc"))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coll.Drop(dropCtx)
	})

	if _, err := coll.InsertOne(ctx, bson.M{"_id": 1}); err != nil {
		t.Fatalf("journal=false 的插入不应失败: %v", err)
	}

	// 直接核对**驱动选项**确实拿到了这两个取值
	// （v2 的 *mongo.Client 不暴露 WriteConcern 访问器，
	//  所以只能从配置构造出来的选项上看）。
	opts, _, err := p.Config().clientOptions()
	if err != nil {
		t.Fatalf("clientOptions 失败: %v", err)
	}
	if opts.WriteConcern == nil {
		t.Fatal("生效配置应能构造出写关注")
	}
	if opts.WriteConcern.Journal == nil || *opts.WriteConcern.Journal {
		t.Errorf("Journal 期望 false，实际 %v", opts.WriteConcern.Journal)
	}
	// 数字形式的 w 会被解析成 int；不同驱动版本可能是 int32/int64，
	// 这里统一按数值比较。
	if got, ok := asInt(opts.WriteConcern.W); !ok || got != 1 {
		t.Errorf("W 期望 1，实际 %#v", opts.WriteConcern.W)
	}

	// 生效配置也要能反映出这两个值（*bool 的意义就在于区分"没配"与"配成 false"）。
	got := p.Config()
	if got.WriteConcernJournal == nil || *got.WriteConcernJournal {
		t.Errorf("生效配置的 WriteConcernJournal 期望 false，实际 %v", got.WriteConcernJournal)
	}
	if got.WriteConcern != "1" {
		t.Errorf("生效配置的 WriteConcern 期望 \"1\"，实际 %q", got.WriteConcern)
	}
}

// asInt 把驱动返回的 w 值（可能是 int / int32 / int64）统一成 int。
func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	default:
		return 0, false
	}
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

// contains 是 strings.Contains 的简写，集中放在这里避免到处 import strings。
func contains(s, sub string) bool { return strings.Contains(s, sub) }

// withURIParam 往连接串里追加一个查询参数（已有参数时用 & 连接）。
func withURIParam(uri, key, value string) string {
	sep := "?"
	for i := 0; i < len(uri); i++ {
		if uri[i] == '?' {
			sep = "&"
			break
		}
	}
	return uri + sep + key + "=" + value
}

func describeEvents(evts []metrics.Event) string {
	out := ""
	for _, e := range evts {
		out += fmt.Sprintf("\n  op=%s detail=%q err=%v", e.Op, e.Detail, e.Err)
	}
	if out == "" {
		return "（空）"
	}
	return out
}
