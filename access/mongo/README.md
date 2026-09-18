# mongodb

`go-infra` 的 MongoDB 封装。基于 `go.mongodb.org/mongo-driver/v2`，把**连接生命周期**与**连接池可观测性**作为一等公民暴露出来。

本包**不重新包装驱动的数据访问 API** —— 会话、事务、Change Stream、GridFS、聚合游标都直接用驱动原生写法，通过 `Client()` 拿到客户端即可。本包只负责驱动没做好的那几件事。

## 特性

| 能力 | 说明 |
|---|---|
| 可信的初始化 | `mongo.Connect` **不验证可达性**（驱动原话：*does not validate that the MongoDB deployment is reachable*），配置写对了它就返回成功。本包补一次带超时的真实 `Ping`，"URI 拼错 / 账号不对 / 副本集名不符"全部提前到启动期 |
| 可控的启动耗时 | 驱动默认 `ServerSelectionTimeout=30s`，连不上要挂 30 秒。本包用独立的 `DialProbeTimeout` 给每次尝试封顶，**最坏耗时 = `DialAttempts` × `DialProbeTimeout`**，可算可控 |
| 连接池显式可调 | `MaxPoolSize` / `MinPoolSize` / `MaxConnecting` / `MaxConnIdleTime` 四项全暴露，每一项都写清了"该不该调" |
| 连接池可观测 | 驱动**不提供任何池统计 API**。本包订阅 CMAP 事件流自己计数，产出与 mysql / postgres / redis 同形状的 `metrics.PoolStats` |
| 操作级指标与慢查询日志 | 通过命令监听器上报；日志里的 `Detail` **只含结构**（库.集合 + 字段名），不含任何取值 |
| 零观测开销开关 | `Observer` 为 nil 且 `LogLevel=silent` 时**一个监听器都不注册**，热路径上零额外开销 |
| 配置单位守卫 | 把整数直接写进 `Duration` 字段（`5` → `5ns`）会在启动期报错，而不是让超时形同虚设 |
| URI 优先语义 | URI 里的参数**永远覆盖** `Config` 同名字段，与驱动语义一致，不存在"改了 URI 却不生效" |
| 拦截失效参数 | `socketTimeoutMS` 在 v2 已被删除但驱动仍会解析（**静默不生效**），本包启动期直接报错 |
| 可恢复的关闭 | `Close` 幂等、不死锁、对零值客户端也安全；重建时先建新客户端再断开旧的 |

## 架构

```mermaid
flowchart TB
    O["Open(cfg)"] --> V["validate<br/>先校验原始配置<br/>+ URI 结构校验"]
    V --> N["normalize<br/>再补默认值"]
    N --> CO["clientOptions<br/>先 Set*，最后 ApplyURI<br/>→ 得到 URI 优先语义"]
    CO --> E["回写生效的 MaxPoolSize<br/>让指标与实际一致"]
    E --> D["dial<br/>按 DialAttempts 重试<br/>每次尝试受 DialProbeTimeout 封顶"]
    D --> C["mongo.Connect<br/>只校验选项，不验证可达"]
    C --> P["真实 Ping 探活<br/>失败则 Disconnect 防协程泄漏"]
    P --> A["adopt<br/>换入新客户端，断开旧客户端"]
    A --> M["monitor 协程<br/>周期探活 → 更新 Healthy"]
    A --> CL["Client() *mongo.Client<br/>逃生舱"]
    A --> PS["PoolStats()<br/>来自 CMAP 事件计数器"]
    M -.->|"RebuildAfterFailures"| R["Reconnect"]
```

两条事件流是本包可观测性的全部来源：

```mermaid
flowchart LR
    subgraph driver["mongo-driver"]
        CM["CommandMonitor<br/>Started / Succeeded / Failed"]
        PM["PoolMonitor<br/>CMAP 事件流"]
    end
    CM --> OBS["metrics.Event<br/>Component / Instance / Op<br/>Duration / Err / Reason / Detail"]
    PM --> CNT["poolCounters<br/>原子计数器"]
    CNT --> PS["metrics.PoolStats"]
    OBS --> E1["prometheus.Exporter"]
    PS --> E1
    E1 --> PROM["Prometheus"]
```

`validate → normalize → dial` 的顺序是有意的：**先校验再补默认值**。反过来的话，`-1` 会被 `normalize` 当成"未设置"替换成默认值，非法输入就被静默吞掉了。

## 安装

```bash
go get github.com/zavierswong/go-infra/access/mongo
```

驱动版本：`go.mongodb.org/mongo-driver/v2 v2.9.1`。

> ⚠️ 本包基于 driver **v2**。v1 → v2 有几个影响配置的破坏性变更（`socketTimeoutMS` 被删除、`Session` 从接口变成结构体等），详见 [从 mongo-driver v1 迁移](#从-mongo-driver-v1-迁移)。

## 快速开始

```go
package main

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zavierswong/go-infra/access/mongo"
)

func main() {
	client, err := mongo.Open(mongo.Config{
		URI:     "mongo://app:secret@127.0.0.1:27017/shop?authSource=admin",
		Name:    "order",          // 只进指标标签；多集群时必须填
		AppName: "order-api@prod", // 会随握手上报给服务端，排查慢查询的性价比最高
	})
	if err != nil {
		log.Fatalf("连接 MongoDB 失败: %v", err)
	}
	defer client.Close() // 幂等，可重复调用

	ctx := context.Background()

	// DefaultDatabase 用 URI 路径里的库名。
	// 没写库名时返回 ErrInvalidConfig，而不是悄悄退化成 test / admin。
	db, err := client.DefaultDatabase()
	if err != nil {
		log.Fatal(err)
	}

	coll := db.Collection("orders")

	if _, err := coll.InsertOne(ctx, bson.M{
		"order_id": "A1001",
		"user_id":  1001,
		"amount":   9900,
	}); err != nil {
		log.Fatalf("写入失败: %v", err)
	}

	var out bson.M
	if err := coll.FindOne(ctx, bson.M{"order_id": "A1001"}).Decode(&out); err != nil {
		// 查不到文档是**正常业务结果**，别当成故障上报。
		if mongo.IsNoDocuments(err) {
			log.Println("订单不存在")
			return
		}
		log.Fatal(err)
	}
	log.Printf("订单: %v", out)

	// 连接池指标（建议定期上报，或交给 prometheus 适配包在 scrape 时现取）
	stats := client.PoolStats()
	log.Printf("池上限/节点=%d 已建=%d 使用中=%d 等待次数=%d",
		stats.MaxOpen, stats.Open, stats.InUse, stats.WaitCount)

	_ = time.Second
}
```

## 连接池调优

这是本包的核心价值之一。MongoDB 的池在驱动内部，**调错了不会报错，只会让 P99 悄悄变差**。

### 一次操作的取连接路径

```mermaid
flowchart LR
    OP["业务操作"] --> A{"池里有<br/>可用连接?"}
    A -->|"有"| RE["直接复用<br/>微秒级"]
    A -->|"无"| B{"已达<br/>MaxPoolSize?"}
    B -->|"未达"| NC{"在建数已达<br/>MaxConnecting?"}
    NC -->|"未达"| NEW["新建连接<br/>TCP + TLS + 握手 + 认证<br/>毫秒级"]
    NC -->|"已达"| Q1["排队等建连<br/>被 MaxConnecting 限流"]
    B -->|"已达"| W["排队等连接<br/>最多 ServerSelectionTimeout"]
    W -->|"超时"| ERR["报连接池等待超时<br/>Go 驱动里唯一会报出来的池超时"]
    NEW --> RUN["执行命令"]
    RE --> RUN
```

关键点：**新建连接比复用连接慢三个数量级**，而 `MaxConnecting` 默认只有 **2** —— 池空了以后最多只有 2 个连接在同时建，其余请求全部排队。这个默认值是为了保护服务端（几十个 goroutine 同时握手会把 mongod 打慢），通常不该动。

### 四个参数各自的职责

| 参数 | 驱动默认 | 本包默认 | 作用 | 调小 | 调大 |
|---|---|---|---|---|---|
| `MaxPoolSize` | `100`（**每节点**） | `100` | 每节点连接数上限 | 请求排队，`WaitCount` 上升 | 服务端 connection 数被吃光 |
| `MinPoolSize` | `0` | `0` | 池中常驻的最小连接数 | 首次请求有建连抖动 | 每个实例常驻连接，实例多时不划算 |
| `MaxConnecting` | `2` | `2` | **同时**在建的连接数上限 | 池恢复变慢 | 建连握手风暴打慢 mongod |
| `MaxConnIdleTime` | `0`（不回收） | `0` | 空闲连接的存活时间 | 波峰时重建连 | 波谷时段连接一直占着 |

> 本包显式设置 `MaxPoolSize=100` 与 `MaxConnecting=2`（**与驱动默认值相同，行为不变**）。目的是让这两个数**可见**：它们会出现在启动日志里，也会成为 `PoolStats.MaxOpen`，于是"到底允许多少连接"不用去翻驱动源码。

### 最容易算错的一个数：`MaxPoolSize` 是"每台服务器"

这是 MongoDB 与 MySQL / Redis 在池语义上最大的差异：

| | 池上限的口径 |
|---|---|
| MySQL / PostgreSQL | 整个客户端一共这么多连接 |
| Redis | 整个客户端一共这么多连接（`MaxActiveConns`） |
| **MongoDB** | **每台服务器**这么多连接 |

所以一个 3 节点副本集、`MaxPoolSize=100` 的客户端，最多会建立 **300** 条服务端连接。多实例部署时要按下面这个式子核对服务端的 `connection` 上限（mongod 默认 65536）：

```
实例数 × 节点数 × MaxPoolSize  ≤  mongod 的 connection 上限
```

一个 10 实例 × 3 节点 × 100 的部署会申请 3000 条连接 —— 单看"100"完全看不出问题。

对应地，`metrics.PoolStats` 里 **`MaxOpen` 是每节点上限，其余字段是全节点聚合**。所以多节点部署下 `Open` 可能大于 `MaxOpen`，**这是正确的，不是 bug**。

### 三种典型场景

| 场景 | 建议配置 | 理由 |
|---|---|---|
| 在线 API（QPS 高、单次操作快） | `MaxPoolSize: 服务端能承受的连接数 / (实例数 × 节点数)`、`MaxConnIdleTime: 5m` | 上限要按最坏部署形态倒推；空闲回收让波谷时把连接还回去 |
| 后台批处理（单实例、长任务） | `MaxPoolSize: 200~500`、`MaxConnecting: 8`、`MaxConnIdleTime: 0` | 实例少，可以多占；建连并发调大以加快池恢复；连接一直用着不必回收 |
| 多租户 / 多集群 | **必须填 `Name`**，如 `"order"` / `"order_analytics"` | 不填的话 `Event.Instance` 是空串，两个集群的指标会叠成一条曲线 |

### 用 `PoolStats()` 判断该往哪调

`PoolStats()` **不来自驱动** —— 驱动没有任何池统计 API，这是本包订阅 CMAP 事件自己算出来的。正因为是推算的，有几个字段的语义必须说清楚：

| 现象 | 通常意味着 | 动作 |
|---|---|---|
| `WaitCount` 持续增长 | 上限偏小，或单次操作太慢占着连接不放 | 先看 `Op` 维度的延迟分布；确认不是慢查询再调大 `MaxPoolSize` |
| `Pending` 长期 > 0 | 请求在排队等连接 | 同上，同时核对 `MaxConnecting` 是否偏小 |
| `Timeouts` 增长 | 等待连接超时（**用户可见的失败**） | 紧急：调大上限或缩短单次操作耗时 |
| `Stale` 增长 | 拓扑变化导致连接失效（主从切换、SDAM 判死） | 看 `Status().PoolCleared` 是否同时陡增，确认是否发生切换 |
| `Unusable` 增长 | 取到的连接已失效被丢弃 | 服务端有回收策略而客户端 `MaxConnIdleTime` 太长 |
| `Idle` 长期为 0 | 连接一直在用 | 正常；若同时 `Pending` > 0 则需要扩容 |

**以下字段恒为 0，不要拿去建指标**（驱动没有这些概念，建了只会得到一堆永远为 0 的曲线）：

| 字段 | 为什么恒为 0 |
|---|---|
| `MaxIdleClosed` | 驱动没有"空闲连接数上限"这个概念 |
| `MaxLifetimeClosed` | 驱动没有"连接最长寿命"这个概念（只有空闲超时） |
| `Hits` / `Misses` | 驱动不暴露"是否命中空闲连接" |

两个**与 `database/sql` 口径不同**的地方，跨组件看板时要留意：

- `MaxOpen` 是每节点上限（SQL 侧是全局上限，见上文）；
- `WaitCount` / `WaitDuration` 统计的是**全部成功检出**（含命中空闲连接、耗时近乎为 0 的快路径），而 `database/sql` 只统计被阻塞的请求。所以本包的 `WaitDuration` 天然比 SQL 侧"大"，两者不可直接比较。

### 多实例部署

```mermaid
flowchart TB
    subgraph app["10 个应用实例"]
        A1["实例 1<br/>Name=order-1"]
        A2["实例 2<br/>Name=order-2"]
        AX["..."]
    end
    A1 -->|"MaxPoolSize=100"| N1["节点 1"]
    A2 -->|"MaxPoolSize=100"| N1
    AX -->|"MaxPoolSize=100"| N1
    A1 -->|"MaxPoolSize=100"| N2["节点 2"]
    A2 -->|"MaxPoolSize=100"| N2
    AX -->|"MaxPoolSize=100"| N2
    A1 -->|"MaxPoolSize=100"| N3["节点 3"]
    A2 -->|"MaxPoolSize=100"| N3
    AX -->|"MaxPoolSize=100"| N3
    N1 -.->|"合计 10×3×100 = 3000 条"| MG["mongod<br/>connection 上限 65536"]
```

**多实例部署必须填 `Config.Name`。** 否则 `metrics.Event.Instance` 与 `PoolStats.Instance` 都是空字符串，Prometheus 上看就是所有实例的曲线叠在一起，`WaitCount` 会以 10 倍速增长而看不出是哪个实例的问题。

## 超时：四个字段的职责完全不同

这是本包最容易配错的地方。MongoDB 没有"一个超时管所有"的概念。

```mermaid
flowchart LR
    subgraph startup["启动期"]
        DPT["DialProbeTimeout<br/>每次尝试的探活上限"]
        CT["ConnectTimeout<br/>单次建连超时"]
    end
    subgraph runtime["运行期"]
        SST["ServerSelectionTimeout<br/>选不到可用节点等多久"]
        OT["OperationTimeout<br/>整个操作的超时 CSOT"]
        CTX["ctx deadline<br/>调用方给的超时"]
    end
    CT --> DPT
    DPT -->|"DialAttempts 次"| UP["启动完成"]
    OT -.->|"ctx 有 deadline 时<br/>完全失效"| CTX
```

| 字段 | 驱动默认 | 本包默认 | 管什么 |
|---|---|---|---|
| `ConnectTimeout` | `30s` | 用驱动默认 | **单次建连**：TCP + TLS + 握手 + 认证。**不是**操作超时 |
| `ServerSelectionTimeout` | `30s` | 建议 `3s~5s` | 选不到可用节点时等多久。决定"MongoDB 挂了"多久后业务才拿到错误 |
| `DialProbeTimeout` | —（本包自有） | `5s` | **启动期每次尝试**的探活上限 |
| `HeartbeatInterval` | `10s` | 用驱动默认 | 后台探测节点状态的间隔。决定"主从切换后多久被发现"，硬下限 500ms |
| `OperationTimeout` | `0`（不限） | 视场景 | 整个操作的超时（CSOT，对应 URI 的 `timeoutMS`）。⚠️ 见下文陷阱 |

### 为什么 `DialProbeTimeout` 要单独存在

`ServerSelectionTimeout` 默认 30s，**它是给业务操作用的旋钮**。如果拿它来卡启动期的探活，`DialAttempts=3` 时最坏要挂 **90 秒**才启动失败 —— 停机发布时这是灾难。

拆开之后两件事各有独立、语义清晰的旋钮：

```
启动期探活   →  DialProbeTimeout（默认 5s，属于"启动流程"）
运行期选节点 →  ServerSelectionTimeout（默认 30s，属于"业务请求"）
```

于是启动耗时有明确上界：

```
最坏启动耗时 ≈ DialAttempts × DialProbeTimeout = 3 × 5s = 15s
```

### ⚠️ `OperationTimeout` 会被 ctx deadline 完全屏蔽

**这是本包最容易误用的一个配置。** 驱动的实现是：

```go
if timeout == nil || IsTimeoutContext(parent) {
    return parent, cancel      // 什么都不做
}
```

而 `IsTimeoutContext` 判定的是"ctx 上**有没有** deadline"（**不看长短**）。也就是说：

> **只要调用方的 ctx 带了任意 deadline，`OperationTimeout` 就完全失效**，由那个 deadline 单独说了算。

```go
// 例子一：HTTP 请求的 ctx 常有很长的 deadline
ctx, _ := context.WithTimeout(r.Context(), 5*time.Minute)
coll.Find(ctx, ...)          // OperationTimeout=5s 不生效，这个操作可以跑 5 分钟 ❌

// 例子二：只有完全不设 deadline 时，它才起作用
coll.Find(context.Background(), ...)   // 受 5s 限制 ✅
```

所以它是**兜底**而不是**上限**：想让 5s 一定生效，必须自己给每次操作套 ctx。推荐的做法就是**永远自己套 ctx**，不要依赖 `OperationTimeout`。

### 关于 `socketTimeoutMS`（v1 有，v2 没有）

driver v1 有 `socketTimeoutMS`（单次网络读写超时），**v2 把它删除了**（见驱动仓库 `docs/migration-2.0.md` 的「MaxTime」一节）。

但 v2 的连接串解析器**仍然认识 `sockettimeoutms`** 并把它存进 `ConnString` —— 只是 `ApplyURI` 不再把它应用到选项上。

这是最坏的一类故障：**配置看起来完全正常、驱动也不报错、日志里什么异常都没有，而你以为存在的"操作超时"根本不存在**，慢查询会一直挂着。

所以本包选择**启动期直接报错**，而不是静默忽略：

```
mongodb: 配置非法: URI 参数 socketTimeoutMS=5000 在 mongo-driver v2 中**已失效**
（驱动仍会解析它，但不再应用到客户端选项上，属于静默不生效）；
请改用 Config.OperationTimeout（URI 的 timeoutMS）或给操作传入带 deadline 的 ctx
```

## 配置参考

### 标识与可观测性

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `URI` | `string` | 必填 | 连接串，`mongodb://` 或 `mongodb+srv://` |
| `Name` | `string` | `""` | 实例名，只进指标标签。**多集群 / 多实例必须填** |
| `Observer` | `metrics.Observer` | `nil` | 事件接收者。`nil` 表示不观测，此时不注册任何监听器 |

### 连接池

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `MaxPoolSize` | `int` | `100` | **每台服务器**的连接数上限。`0` = 用驱动默认值 100。⚠️ URI 里的 `0` 表示"不限制" |
| `MinPoolSize` | `int` | `0` | 池中常驻最小连接数，`0` = 懒建连 |
| `MaxConnecting` | `int` | `2` | 同时在建的连接数上限 |
| `MaxConnIdleTime` | `time.Duration` | `0` | 空闲连接存活时间。**`0` = 不因空闲而关闭** |

### 超时

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `ConnectTimeout` | `time.Duration` | `0`（驱动 30s） | 单次建连超时 |
| `ServerSelectionTimeout` | `time.Duration` | `0`（驱动 30s） | 选不到可用节点时等多久 |
| `OperationTimeout` | `time.Duration` | `0`（不限） | 全操作超时（CSOT）。⚠️ ctx 有 deadline 时失效 |
| `HeartbeatInterval` | `time.Duration` | `0`（驱动 10s） | 心跳间隔，驱动硬下限 500ms |

### 建连行为

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `DialAttempts` | `int` | `3` | 初始化尝试次数（含第一次）。`1` = 失败即返回，**负值 = 无限重试**（`Close` 可中断） |
| `DialProbeTimeout` | `time.Duration` | `5s` | 每次尝试的探活上限 |
| `DialBackoff` | `time.Duration` | `1s` | 重试退避起始值 |
| `DialMaxBackoff` | `time.Duration` | `30s` | 重试退避上限；小于 `DialBackoff` 时会被抬到 `DialBackoff` |

### 健康监控

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `HealthCheckInterval` | `time.Duration` | `30s` | 后台探活间隔 |
| `RebuildAfterFailures` | `int` | `0` | 连续失败多少次后主动重建客户端。**默认 0 = 不重建**（驱动本来就在后台自愈） |

### 覆盖项（仅当 URI 未指定时才生效）

| 字段 | 类型 | 对应 URI 参数 | 说明 |
|---|---|---|---|
| `AppName` | `string` | `appName` | 随握手上报给服务端，**排查问题时最有性价比的一项** |
| `ReplicaSet` | `string` | `replicaSet` | 设置后驱动会**校验**实际副本集名，能挡住"连错集群" |
| `ReadPreference` | `string` | `readPreference` | `primary` / `primaryPreferred` / `secondary` / `secondaryPreferred` / `nearest`，大小写与连字符不敏感 |
| `ReadConcern` | `string` | `readConcernLevel` | `local` / `available` / `majority` / `linearizable` / `snapshot` |
| `WriteConcern` | `string` | `w` | `"majority"` / `"1"` / `"0"` / tag set 名。⚠️ `"0"` = 不等待任何确认 |
| `WriteConcernJournal` | `*bool` | `journal` | `*bool` 用于区分"没配"与"配成 false" |
| `RetryWrites` | `*bool` | `retryWrites` | 驱动默认 `true`，**建议保持打开** |
| `RetryReads` | `*bool` | `retryReads` | 驱动默认 `true` |
| `DirectConnection` | `*bool` | `directConnection` | 单节点部署要它；副本集上用会**绕过故障转移**。与多主机/SRV 互斥 |
| `LoadBalanced` | `*bool` | `loadBalanced` | Atlas Serverless 等 LB 前置形态。与多主机、`replicaSet` 互斥 |

### 日志

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `LogLevel` | `string` | `""`（按 `info`） | `silent` / `error` / `warn` / `info` |
| `SlowThreshold` | `time.Duration` | `200ms` | 慢操作阈值。**负值 = 不判定慢操作** |

### URI 优先语义

**URI 里的参数永远覆盖 `Config` 里的同名字段。** 这不是本包自定的规则，而是驱动的语义（驱动文档原话：*later option setter calls overwrite the values from previous option setter calls, including the ApplyURI method*）。本包把 `ApplyURI` 放在所有 `Set*` **之后**调用，于是天然得到这个行为，不需要自己维护一张"URI 里有没有这个参数"的表。

好处很实际：URI 是运维手里临时改的那个旋钮（改连接串比重发配置快），覆盖语义与直觉一致。

```go
mongo.Config{
    URI:         "mongo://h:27017/shop?maxPoolSize=7",
    MaxPoolSize: 50,        // 被 URI 的 7 覆盖
}
```

⚠️ 注意 `0` 的语义差异：

| 位置 | `0` 的含义 |
|---|---|
| `Config.MaxPoolSize` | 用驱动默认值 `100` |
| URI 的 `maxPoolSize=0` | **不限制**（没有硬上限） |

要不限制只能写在 URI 里。

另外，`Open` 之后 `Config()` 返回的是**生效值** —— URI 带了 `maxPoolSize` 时，`Config().MaxPoolSize` 会被改写为 URI 的取值，`PoolStats().MaxOpen` 同理。这是必须的：否则会出现"URI 写 7、指标报 100"的错位，而"池吃满"的告警正是基于这个数。

### 配置格式说明

所有 `time.Duration` 字段都接受 `"300ms"` / `"30s"` / `"2m"` 这类字符串（viper 的默认解码器已包含 `StringToTimeDurationHookFunc`）。直接使用 `mapstructure` 时需要自行 `DecodeHook(mapstructure.StringToTimeDurationHookFunc())`。

### YAML 示例

```yaml
mongodb:
  order:
    uri: "mongo://app:${MONGO_PASSWORD}@h1:27017,h2:27017,h3:27017/shop?replicaSet=rs0&authSource=admin"
    name: "order"
    app_name: "order-api@prod"

    max_pool_size: 50              # 每节点；3 节点副本集 → 实际 150
    min_pool_size: 0
    max_connecting: 2
    max_conn_idle_time: 5m

    connect_timeout: 5s
    server_selection_timeout: 3s
    dial_probe_timeout: 5s
    dial_attempts: 3

    health_check_interval: 30s
    rebuild_after_failures: 0

    read_preference: "primaryPreferred"
    read_concern: "majority"
    write_concern: "majority"

    log_level: "warn"
    slow_threshold: 200ms
```

## 连接串（URI）参考

### 两种 scheme

```
mongodb://user:pass@h1:27017,h2:27017,h3:27017/shop?replicaSet=rs0   # 直连种子列表
mongodb+srv://user:pass@cluster0.example.com/shop?retryWrites=true   # 走 DNS SRV 发现节点
```

**SRV 形式不能写端口** —— 端口只能来自 DNS 记录。写了驱动会报错，本包在解析阶段就拦下并说明原因。

### 常见写法错误（本包启动期就会报错）

| 写法 | 报错 | 正确写法 |
|---|---|---|
| `mongodb://host:27017?authSource=admin` | `must have a / before the query ?` | `mongodb://host:27017/?authSource=admin`（**留一个空路径**） |
| `mongodb+srv://cluster0.example.com:27017/db` | SRV 不能指定端口 | 去掉端口 |
| `mongodb://h1:27017,,h2:27017/db` | 主机列表有空项 | 去掉多余逗号 |
| `mongodb://h1:/db` | 端口为空 | 补上端口或整个去掉 `:` |
| `mongodb://h1:70000/db` | 端口超出范围 | 1–65535 |
| `mongodb://h:27017/a/b` | 路径只能是一个库名 | 库名不能含 `/` |
| `mongodb://h:27017/db?maxPoolSize=1&MAXPOOLSIZE=2` | 参数大小写冲突 | MongoDB 的参数名大小写不敏感，去掉重复项 |
| `mongodb://h:27017/db?x=%zz` | 参数转义非法 | 修正百分号编码 |
| 密码含 `@ : / ? # %` 但未编码 | URI 解析失败 | 做百分号编码，例如 `p@ss` → `p%40ss` |

### 脱敏

`URI()` 返回抹掉口令的连接串，可安全写入日志。它覆盖**两处**口令：

```
userinfo 里的密码：    mongodb://user:pass@h1/...            → mongodb://user:***@h1/...
证书私钥口令：         ?tlsCertificateKeyFilePassword=pkpass → ?tlsCertificateKeyFilePassword=***
```

> 只抹 `userinfo` 的脱敏函数（例如 `utils.SanitizeDSN`）认不出 PostgreSQL / MongoDB 的其它口令入口，会把私钥口令原样写进日志。本包自己实现了一遍。

没有命中任何口令时**逐字节原样返回** —— 日志里看到的就是配置里写的那一行，方便直接 diff。

## API 参考

### 生命周期

| 方法 | 说明 |
|---|---|
| `Open(cfg Config) (*MongoDB, error)` | 建立连接并启动健康监控。失败会按 `DialAttempts` 重试，最坏阻塞 `DialAttempts × DialProbeTimeout` |
| `(*MongoDB) Close() error` | 断开并停止监控。幂等，对零值客户端也安全 |
| `(*MongoDB) Reconnect() error` | 主动重建客户端，**先建新再断旧**，期间旧客户端仍可服务。⚠️ 会让已有句柄失效 |
| `(*MongoDB) Closed() bool` | 是否已关闭 |
| `(*MongoDB) Healthy() bool` | 最近一次后台探活的结果 |
| `(*MongoDB) HealthCheck(ctx) error` | 立即探活，适合接进 `/healthz` |
| `(*MongoDB) Config() Config` | 返回**生效后**的配置（默认值已补齐，且已计入 URI 覆盖） |
| `(*MongoDB) URI() string` | 抹掉口令的连接串 |

### 数据访问（逃生舱）

| 方法 | 说明 |
|---|---|
| `(*MongoDB) Client() *mongo.Client` | 底层驱动客户端。`Close` 后返回 `nil` |
| `(*MongoDB) Database(name) *mongo.Database` | 指定库的句柄。`Close` 后返回 `nil` |
| `(*MongoDB) DefaultDatabase() (*mongo.Database, error)` | URI 路径里的默认库；没写库名时返回 `ErrInvalidConfig` |

本包刻意**不包装**驱动的数据访问 API。理由是那些 API 已经足够好用，而重新包一遍只会制造一层与驱动版本脱节的抽象。需要会话、事务、Change Stream、GridFS、聚合游标时直接用 `Client()`。

> ⚠️ **不要缓存 `Collection` 句柄。** `Reconnect()` 会换掉底层 `*mongo.Client` 并断开旧的，挂在旧客户端上的句柄再用会报 `client is disconnected`。这与 mysql / postgres 包不同（SQL 侧换的是连接池，`*gorm.DB` 本身稳定）。取句柄没有网络开销，每次现取即可。

### 可观测性

| 方法 | 说明 |
|---|---|
| `(*MongoDB) PoolStats() metrics.PoolStats` | 连接池快照（**推算值**，见下文注意事项） |
| `(*MongoDB) Status() Status` | 瞬时状态快照，适合映射成 Gauge |

`Status` 字段：

| 字段 | 说明 |
|---|---|
| `Instance` / `Hosts` / `Database` | 标识信息，不含口令 |
| `Closed` / `Healthy` | 状态 |
| `SessionsInProgress` | 已开启未结束的会话数。驱动对每次操作默认使用隐式会话、结束即归还，所以它约等于**在途操作数** |
| `PoolCleared` | 池被清空的累计次数。主节点变更、`maxPoolSize` 变更、网络错误后的池清理都会触发。**它陡增通常对应一次主从切换或网络抖动** |

### 错误处理

```go
var (
    ErrInvalidConfig = errors.New("mongo: 配置非法")       // URI、字段取值、单位写错
    ErrConnect       = errors.New("mongo: 无法连接数据库")  // 网络、认证、副本集名不符、选不到节点
    ErrClosed        = errors.New("mongo: 客户端已关闭")
    ErrNotConnected  = errors.New("mongo: 尚未建立连接")
)
```

全部用 `errors.Is` 判定，**不要匹配错误文本**。

服务端错误判定（依据错误码与标签，MongoDB 的公开契约）：

| 函数 | 含义 | 典型错误码 |
|---|---|---|
| `IsNoDocuments(err)` | 查不到文档（`mongo.ErrNoDocuments`） | — |
| `IsDuplicateKey(err)` | 唯一索引冲突 | `11000` / `11001` / `12582` / `16460` |
| `IsTimeout(err)` | 超时（客户端 ctx、`net.Error`、服务端 `maxTimeMS`、池等待超时） | `50` |
| `IsNetworkError(err)` | 链路层故障（`NetworkError` 标签） | — |
| `IsCursorNotFound(err)` | 游标已失效 | `43` |
| `IsNamespaceNotFound(err)` | 库或集合不存在（**只有 DDL 才会返回**） | `26` |
| `IsUnauthorized(err)` | 鉴权/授权失败 | `13` / `18` |
| `IsWriteConcernError(err)` | 写确认级别未被满足 | `64` / `79` / `100` |
| `IsTransactionRetryable(err)` | 事务可**整体重试**（`TransientTransactionError` 标签） | — |
| `IsCommitRetryable(err)` | 只能**重试 commit**（`UnknownTransactionCommitResult` 标签） | — |
| `IsRetryable(err)` | 原样重试是否有意义 | 见下表 |

`IsRetryable` 的白名单与驱动内部的 `retryableCodes` **保持一致**（不是凭经验列的）：

| 错误码 | 名称 |
|---|---|
| `6` / `7` | HostUnreachable / HostNotFound |
| `89` / `9001` | NetworkTimeout / SocketException |
| `91` / `11600` | ShutdownInProgress / InterruptedAtShutdown |
| `134` | ReadConcernMajorityNotAvailableYet |
| `189` / `10107` | PrimarySteppedDown / NotWritablePrimary |
| `262` | ExceededTimeLimit（服务端主动放弃，**可以安全重试**） |
| `11602` | InterruptedDueToReplStateChange |
| `13435` / `13436` | NotPrimaryNoSecondaryOk / NotPrimaryOrSecondary |

⚠️ **`context.DeadlineExceeded` 不在可重试之列。** ctx 超时的写操作可能已经落库，重试会造成重复写入 —— 这必须由业务侧用幂等键解决。注意它与上表的 `262` 区分：后者是**服务端**主动放弃，确实可以安全重试。

⚠️ **两种事务标签不能混用**，混用会造成重复写入：

| 标签 | 含义 | 正确做法 |
|---|---|---|
| `TransientTransactionError` | 事务快照失效 | **丢掉事务，重新 `StartTransaction` 并重跑全部语句** |
| `UnknownTransactionCommitResult` | COMMIT 的响应丢了，事务**可能已提交** | **只重试 commit**，绝不能重跑事务 |

`classifyErr` 把错误归入 `metrics.Reason` 的闭集（`none` / `timeout` / `canceled` / `closed` / `connect` / `not_found` / `invalid` / `conflict` / `rejected` / `unknown`）。其中两个映射值得单独记：

- `mongo.ErrNoDocuments` → `not_found`，且 `metrics.Event.IsError()` 返回 `false` —— **查不到文档不该污染错误率**；
- 唯一键冲突与 `TransientTransactionError` → `conflict`（业务冲突/并发争用，不是"数据库坏了"）。

## metrics 暴露

本包产出两种形状的数据，与 mysql / postgres / redis / rabbitmq 完全对称：

### 1. 事件流 → `metrics.Event`

每个命令结束后上报一条（`Observer` 非 nil 或 `LogLevel` 非 silent 时）：

| 字段 | 内容 |
|---|---|
| `Component` | 恒为 `metrics.ComponentMongoDB`（`"mongodb"`） |
| `Instance` | `Config.Name` |
| `Op` | **命令名原文**：`find` / `insert` / `update` / `delete` / `aggregate` / `getMore` / `ping` / `reconnect` …。与 `mongodb_exporter`、`db.currentOp()` 的输出对齐 |
| `Duration` | 命令耗时 |
| `Err` | 失败时非 nil |
| `Reason` | `metrics.Reason`，见上文 |
| `Detail` | 结构摘要，见下文 |

**只有 `Op` 适合做指标标签。** `Detail` 每条都不同，一旦作为标签就会造成基数爆炸，甚至把 Prometheus 打挂。

后台探活产生的 `ping` 也会上报（`Op=ping`，`Detail=admin.ping`）。让它可见比让它隐形好 —— 健康检查的失败率本身就是最重要的信号之一；不想要的话在上报侧过滤掉即可。

### 2. CMAP 事件 → `metrics.PoolStats`

驱动不提供池统计 API，这份快照来自本包订阅 `event.PoolMonitor` 自建的原子计数器。`PoolStats` 在**每次 scrape 时现取**，换算规则集中在一个纯函数里（`poolCounters.snapshot`），因此可以脱离真实 MongoDB 做逐字段单测。

### 接入 Prometheus

`prometheus`（`github.com/zavierswong/go-infra/prometheus`）是独立的适配包（`access/*` **不 import prometheus**，这是本仓库的可观测性分层铁律）。

> ⚠️ 它与官方 `github.com/prometheus/client_golang/prometheus` **包名相同**，二者通常要同时 import（适配包 + `MustRegister`），需要给其中一个起别名。仓库约定用 `infraprom` 指本仓库的适配包：

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    infraprom "github.com/zavierswong/go-infra/prometheus"
)

pw := infraprom.New(infraprom.WithNamespace("goinfra"))

// 池快照在 scrape 时现取，不做缓存
pw.RegisterPool(client.PoolStats)

// 探活结果反映在 component_up 上，后台执行、带独立超时，与 scrape 路径隔离
pw.RegisterHealth(metrics.ComponentMongoDB, "order", client.HealthCheck)

prometheus.MustRegister(pw)
defer pw.Close()
```

### `Detail` 的构造规则

```
shop.orders find filter{user_id,status} projection{_id,amount} sort{created_at}
shop.orders insert doc{_id,user_id,amount,created_at}
shop.orders update updates{q{_id} u{$set{status,updated_at}}}
shop.orders aggregate pipeline[$match,$group,$sort]
shop.orders getMore
admin.ping
```

设计原则是**只给结构，不给取值** —— 这比 redis 包的策略更严：

- `filter` / `documents` / `update` 的取值就是业务数据，可能含身份证号、手机号，甚至用户自己存的明文口令，而 `Detail` 会被写进日志与 tracing span；
- 但"是哪个集合、按哪些字段查、更新了哪些字段"恰好是归因需要的全部信息 —— 字段名是 schema 的一部分，不是数据。

唯一的例外是 `key` / `hint`（字段名与索引名，本身是结构信息）。另外驱动已对 9 个敏感命令（`authenticate`、`saslStart`、`createUser` …）做了脱敏，本包在其之上再加一层保险：这些命令的 `Detail` 一律不展开。

摘要有界：最多 12 个字段名、下钻 2 层、标量取值截断到 24 字符，超出以 `…` 标记。命令文档的大小是业务可控的（有人会往 `filter` 里塞几十个条件），不设界的话一条慢查询的日志可能有几 KB。

## 与 mysql / postgres 的差异

API 形状刻意保持一致（`Open` / `Client` / `HealthCheck` / `Reconnect` / `Close` / `Closed` / `Healthy` / `Config` / `PoolStats`），从那边迁过来主要是换包名。差异集中在四处：

| 维度 | mysql / postgres | mongodb |
|---|---|---|
| 数据访问句柄 | `DB() *gorm.DB`、`SQL() *sql.DB` | `Client() *mongo.Client`、`Database(name)` |
| 池统计来源 | `sql.DBStats`（`database/sql` 直接提供） | **本包自建计数器**（驱动没有 API） |
| `MaxOpen` 口径 | 整个客户端 | **每台服务器** |
| `Reconnect` 对句柄的影响 | 无（换的是池，`*gorm.DB` 稳定） | **已有句柄失效**，必须重取 |
| `Reconnect` 的必要性 | 较高，用于恢复连接池 | **较低**（驱动在后台维护拓扑、自动重连） |

`Open` 的签名也不同：MongoDB 没有 `*sql.DB` 这样的池对象可返回，所以 `Close` 是 `*MongoDB` 上的方法，而不是 `*sql.DB` 上的。

## 从 mongo-driver v1 迁移

本包基于 **v2**。v1 → v2 有几个影响写法的破坏性变更，本包已处理或拦下：

| 变更 | v1 | v2 | 本包的处理 |
|---|---|---|---|
| `socketTimeoutMS` | 支持（单次网络读写超时） | **已删除**，但解析器仍认识它 | **启动期报错**，避免静默不生效 |
| 超时控制 | `socketTimeoutMS` + 各处 `MaxTime` | `ClientOptions.Timeout`（CSOT）+ ctx deadline | 暴露为 `Config.OperationTimeout` |
| `Session` | `mongo.Session` **接口** + `mongo.SessionContext` | `*mongo.Session` **结构体** + `mongo.NewSessionContext` | 不包装，直接用驱动原生写法 |
| `WithSession` | `client.WithSession(ctx, fn)` | 包级函数 `mongo.WithSession(ctx, sess, fn)` | 同上 |
| `tlsCertificateKeyFilePassword` | 支持 | 支持 | 脱敏时**一并抹掉**（它也是口令） |

事务的正确写法（v2）：

```go
sess, err := client.Client().StartSession()
if err != nil {
	return err
}
defer sess.EndSession(ctx)

_, err = sess.WithTransaction(ctx, func(sc context.Context) (any, error) {
	// sc 已经是 SessionContext，直接拿它做操作
	if _, err := coll.InsertOne(sc, doc); err != nil {
		return nil, err
	}
	return nil, nil
})
```

> 事务需要**副本集或分片集群**。单节点 mongod 会报 `IllegalOperation: Transaction numbers are only allowed on a replica set member or mongos`。

## 已知缺陷与注意事项

这一节是刻意保留的。本包有几处**能力边界**与**设计取舍**，用之前必须知道。

### 1. `PoolStats()` 是推算值，字段语义与 `database/sql` 有差异

驱动没有任何池统计 API，这份快照来自本包自建的 CMAP 事件计数器。因此：

- `MaxOpen` 是**每节点**上限，其余字段是**全节点聚合** → 多节点下 `Open` 可能大于 `MaxOpen`（正确行为）；
- `WaitCount` / `WaitDuration` 包含命中空闲连接、耗时近乎为 0 的**快路径**，而 `database/sql` 只统计被阻塞的请求 → 两者不可直接比较；
- `MaxIdleClosed` / `MaxLifetimeClosed` / `Hits` / `Misses` **恒为 0**（驱动没有这些概念），不要建指标；
- 计数器是**分开读**的（各自 `Load`），并发下可能读到中间态。`snapshot` 已把可能为负的结果夹到 0 —— 负的 Gauge 会让图表与告警阈值全部失真。

### 2. `Reconnect()` 会让已有句柄失效

`Database()` / `Collection` 句柄挂在具体的 `*mongo.Client` 上，`Reconnect` 换掉并断开了它，旧句柄再用会报 `client is disconnected`。**不要缓存 `Collection`。**（这也意味着 `Reconnect` 的必要性比 SQL 侧低得多 —— 驱动本来就在后台维护拓扑。）

### 3. `OperationTimeout` 会被 ctx deadline 完全屏蔽

见上文「⚠️ `OperationTimeout` 会被 ctx deadline 完全屏蔽」。**结论：永远自己给操作套 ctx，不要依赖它。**

### 4. `Close` 会强制关闭在途连接

`Close` 等待在途操作归还连接的**上限是 5 秒**，超时后强制关闭，于是正在进行中的读写会失败。这个上界是有意为之 —— 停机流程里"关不掉"比"有几个请求失败"严重得多。

### 5. 池统计不随重建清零

`Reconnect` 之后 `created` / `closed` / `checkedOut` 等计数器**继续累加**，不清零。这是刻意的：它们的语义是"这个实例生命周期内发生过什么"，本来就该单调增；清零会让 Prometheus 的 `rate()` 出现反向尖刺。

### 6. 探测到的 `ping` 会进指标

后台健康检查产生的 `ping` 也会上报 `metrics.Event`。不想让它影响"业务操作数"的看板，请在上报侧按 `Op == "ping"` 过滤。

### 7. `IsNamespaceNotFound` 对 `find` 不适用

对不存在的集合执行 `find` **不会**返回错误码 `26`，只会返回零行（`IsNoDocuments`）。只有 `drop`、`createIndexes` 这类 DDL 才会返回 `26`。

### 8. `Config()` 返回的是生效值

`Open` 之后 `Config().MaxPoolSize` 可能与你写进配置的值不同（当 URI 带了 `maxPoolSize` 时）。想要"用户原本填了什么"，请在调用 `Open` 之前自己保留原始 `Config`。

### 9. 单节点部署不支持事务

这是 MongoDB 本身的限制，不是本包的。集成测试里的 `TestTransactionCommitAndAbort` 在单节点环境下会**跳过**并在输出里说明原因。

## 日志

本包通过 `go-infra/logger` 输出，日志级别由 `Config.LogLevel` 控制：

| 级别 | 行为 |
|---|---|
| `silent` | 一条都不打（指标仍然照常上报） |
| `error` | 只打失败的命令 |
| `warn` | 失败的命令 + 超过 `SlowThreshold` 的慢命令 |
| `info` | 全部命令 |

⚠️ `warn` / `info` 下命令日志的量与业务 QPS 成正比，生产环境建议用 `warn` 或 `error`。

日志样例：

```
MongoDB 已连接: 主机=h1:27017,h2:27017 库=shop 最大连接/节点=100 最小连接=0 空闲回收=不回收（驱动默认）
MongoDB 慢操作 [find] shop.orders find filter{user_id,status} sort{created_at} 耗时=312ms（阈值 200ms）
MongoDB 命令失败 [insert] shop.orders insert doc{_id,user_id,amount} 耗时=5ms: E11000 duplicate key error
MongoDB 健康检查失败（连续 2 次）: mongodb: 无法连接数据库: 连通性检查失败: server selection error
MongoDB 连接已恢复（此前连续失败 3 次）
```

`LogLevel=silent` **且** `Observer=nil` 时，本包**一个监听器都不注册**。这不是可有可无的优化 —— 只要注册了 `Started` 回调，驱动就必须把命令文档复制并序列化成 `bson.Raw` 才能交给回调，那是一次与命令大小成正比的分配。

调用本包前建议先初始化日志底座：

```go
if err := logger.Init(logger.Config{
	Level:   "info",
	Format:  "json",
	Service: "order-api",
}); err != nil {
	log.Fatalf("初始化日志失败: %v", err)
}
defer logger.Close()
```

## 测试

```bash
# 全部（需要本地 MongoDB）
go test ./access/mongo/ -race -count=1

# 只跑纯逻辑用例（不连数据库）
go test ./access/mongo/ -short -count=1
```

本地环境（与环境变量覆盖）：

| 变量 | 默认值 |
|---|---|
| `TEST_MONGODB_URI` | `mongodb://root:123456@127.0.0.1:27017/gointra_test?authSource=admin` |
| `TEST_MONGODB_DB` | `gointra_test` |

启动本地 MongoDB：

```bash
docker run -d --name mongo -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=root \
  -e MONGO_INITDB_ROOT_PASSWORD=123456 \
  mongo:7
```

跳过策略：

- `-short` 跳过全部集成用例；
- MongoDB 不可达时跳过，但**跳过信息里写明地址与如何用环境变量覆盖**，避免出现"什么都没跑却是绿的"；
- 与 mysql 包一致：一旦**连得上但配置/操作出错**，用例直接**失败** —— 那种情况属于真实缺陷，不该被当成环境问题掩盖。

### 用例覆盖

| 文件 | 顶层用例 | 内容 |
|---|---|---|
| `config_test.go` | 28 | 配置默认值与校验、URI 解析与脱敏、驱动选项装配、哨兵错误、快速失败 |
| `errors_test.go` | 18 | 服务端错误码/标签判定、重试白名单、`classifyErr` 闭集 |
| `metrics_test.go` | 15 | CMAP 事件 → `PoolStats` 逐字段换算、`Detail` 构造与有界性、命令监听器 |
| `mongodb_test.go` | 21 | **真实 MongoDB** 集成：CRUD、错误语义、池统计、并发、生命周期、事务 |
| `example_test.go` | 14 个 `Example` | 可编译的用法示例（无 `// Output:`，只做编译检查） |

合计 **82 个顶层用例 / 231 次运行**（含表驱动子用例），`-race` 全绿。

几条值得单独说的用例：

| 用例 | 钉住的是什么 |
|---|---|
| `TestOpenFailsFastOnUnreachableHost` | 连一个确定没人监听的端口，断言返回时间**远小于**驱动默认的 30s —— 证明 `DialProbeTimeout` 真的生效 |
| `TestOpenDialAttemptsRetries` | 比较 `DialAttempts=1` 与 `3` 的**相对**耗时，证明重试真的发生了（不比绝对值，避免 CI 上不稳定） |
| `TestDialRespectsCancelledContext` | 无限重试（`DialAttempts<0`）不会变成"永远卡住"，已取消的 ctx 能让它立刻返回 |
| `TestClientOptionsURIOverridesConfig` | URI 覆盖 Config；配套 `TestClientOptionsConfigAppliesWhenURISilent` 做反向验证，避免"Config 被完全忽略"的实现也能通过 |
| `TestPoolCountersSnapshot` | 手工算出 18 个 CMAP 事件后的每个字段期望值，逐字段断言 |
| `TestCommandDetailNeverLeaksValues` | 把手机号、身份证号塞进 filter，断言 `Detail` 里**只有字段名、没有取值** |
| `TestCommandDetailIsBounded` | 200 个字段的 filter，断言摘要字段数与总长度都有界 |
| `TestRetryableCodesMatchDriverList` | 重试白名单与驱动清单**逐项对齐**（少一个码会让主从切换不再自动重试，多一个码会反复重试不该重试的操作） |
| `TestClassifyErrReasonsAreLowCardinality` | 喂各种畸形错误，断言 `classifyErr` 的返回值不会越出闭集（否则指标标签基数爆炸） |
| `TestFindOneNoDocumentsIsNotAnError` | 查不到文档时 `Event.IsError()` 必须为 `false` —— 它不该污染错误率 |
| `TestPoolStatsMaxOpenFollowsURIOverride` | URI 的 `maxPoolSize=7` 必须一路贯穿到 `PoolStats().MaxOpen`，否则"池吃满"的告警会基于错误的上限 |
| `TestCloseDuringConcurrentUse` | 有并发操作时 `Close` 仍能在有界时间内返回，不 panic、不死锁 |
| `TestConcurrentAccess` | 8 个 goroutine 共 400 次读写，同时并发读取 `PoolStats` / `Status` / `Healthy`（配合 `-race` 验证锁的正确性） |

### 可编译的用法示例

`example_test.go` 里的 14 个 `Example` 覆盖：基础连接、完整配置、URI 覆盖语义、池统计、状态快照、健康检查、URI 脱敏、重连注意事项、metrics 接入、错误判定、事务标签、超时配置陷阱、逃生舱完整用法、优雅停机。

它们**没有** `// Output:` 注释，因此 `go test` 只做编译检查、不会执行 —— 保证文档里的代码不会随 API 演进而失效。

## 目录结构

```
access/mongo/
├── mongo.go       客户端主体：Open / dial / monitor / Close / Reconnect
│                    Client / Database / DefaultDatabase / HealthCheck
│                    PoolStats / Status
├── config.go        Config 定义、validate / normalize、驱动选项装配
│                    URI 优先语义、单位哨兵、写关注与读偏好解析
├── uri.go           URI 解析与结构校验、脱敏（含证书私钥口令）
├── errors.go        哨兵错误、服务端错误码与标签判定、重试白名单
├── metrics.go       CMAP 事件 → PoolStats 换算、命令监听器、Detail 构造
├── config_test.go   纯逻辑：配置 + URI + 驱动选项 + 快速失败
├── errors_test.go   纯逻辑：错误判定与归类
├── metrics_test.go  纯逻辑：池计数器 + Detail 构造 + 命令监听器
├── mongo_test.go  集成：连真实 MongoDB
├── example_test.go  可编译示例（14 个）
└── README.md
```
