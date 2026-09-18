# go-infra

[![Go Version](https://img.shields.io/badge/go-1.26-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

基于 Go 的公共基础设施库：数据库、缓存、消息队列、对象存储、日志、指标、链路追踪、分布式治理组件与应用骨架。每个包都可以独立引用，默认值即生产级。

```bash
go get github.com/zavierswong/go-infra/<package>
```

## 设计原则

- **可复用包目录直接放仓库最外层**，不设 `pkg/` / `internal/` 中间层。
- **可观测性分层铁律**：`access/*` 只暴露拉模式快照（PoolStats / QueueDepth / HealthCheck）与事件流（`metrics.Observer`），**不 import prometheus / otel**。翻译成指标是顶层 `prometheus/` 包的职责，翻译成 trace 是 `tracing/` 包的职责。不使用监控的项目不会被拖入额外依赖。
- **默认值即生产级**：连接池参数、重试、优雅关闭等按生产语义给出默认值，同时全部可通过 `Config` 显式覆盖。
- **非单例、多实例友好**：多实例场景必须填 `Config.Name`，实例名会成为指标标签。

## 组件总览

### 数据访问（`access/`）

| 组件 | 说明 |
|---|---|
| [access/mysql](./access/mysql/) | MySQL 封装（GORM），连接池一等公民，可控建连重试与可恢复关闭 |
| [access/postgres](./access/postgres/) | PostgreSQL 封装（GORM），与 mysql 同一套池化/重试语义 |
| [access/redis](./access/redis/) | Redis 封装（go-redis v9），修正重试语义、日志钩子与关闭语义 |
| [access/rabbitmq](./access/rabbitmq/) | RabbitMQ（AMQP 0-9-1）封装，声明式拓扑、四种路由模式、端到端不丢消息、通道级故障自愈 |
| [access/mongo](./access/mongo/) | MongoDB 封装（mongo-driver v2），连接生命周期与池可观测性一等公民，数据 API 直接用驱动原生写法 |
| [access/kafka](./access/kafka/) | Kafka 客户端（franz-go），默认即生产级，可观测性中立 |

### 缓存与幂等

| 组件 | 说明 |
|---|---|
| [cache](./cache/) | 本地 LRU+TTL（L1）+ Redis（L2）两级缓存，`GetOrLoad` 泛型加载内置 singleflight 防击穿 |
| [idempotency](./idempotency/) | 基于 Redis 的幂等执行器，同一业务 key 在结果保留窗口内至多成功执行一次 |
| [lock](./lock/) | Redis 分布式锁：SETNX + 唯一令牌 + Lua 原子释放/续期，可选看门狗自动续期 |
| [ratelimit](./ratelimit/) | 进程内令牌桶 + Redis 滑动窗口两种限流器，一个接口 |

### 可观测性

| 组件 | 说明 |
|---|---|
| [metrics](./metrics/) | 与监控后端无关的「可观测事实」契约（事件推 + 快照拉），刻意只依赖标准库 |
| [prometheus](./prometheus/) | 把 `metrics` 契约翻译成 Prometheus 指标的适配层，全仓库唯一 import `client_golang` 的位置 |
| [tracing](./tracing/) | 基于 OpenTelemetry 的分布式链路追踪，全仓库唯一 import otel 的位置 |
| [logger](./logger/) | 基于 `log/slog` 的进程级全局日志底座：JSON/text 双格式、终端彩色、按体积轮转 + gzip、ctx 透传 trace_id、GORM SQL 日志接入 |

### 治理组件

| 组件 | 说明 |
|---|---|
| [breaker](./breaker/) | 三态熔断器（Closed / Open / HalfOpen），滑动窗口失败数阈值，零外部依赖 |

### 应用骨架

| 组件 | 说明 |
|---|---|
| [httpclient](./httpclient/) | 统一超时 / 重试 / 熔断 / 日志注入的 HTTP Client 工厂，四层 Transport 洋葱结构 |
| [eventbus](./eventbus/) | 进程内事件总线（pub/sub），同步 / 异步两种派发 |
| [shutdown](./shutdown/) | 统一优雅退出：signal 收口 → 通知业务退出 → 按注册逆序执行清理钩子 |
| [supervisor](./supervisor/) | 进程管理库：Start / Stop / Restart / Status，pidfile + 优雅信号 → SIGKILL 兜底 |

### 存储与工具

| 组件 | 说明 |
|---|---|
| [storage](./storage/) | 统一对象存储：同一套 `Storage` 接口在腾讯云 COS / 阿里云 OSS / MinIO（S3 兼容）之间切换，内置重试包装器 |
| [avatar](./avatar/) | 随机头像生成（DiceBear 官方 Go 移植），参数映射、多格式输出、风格缓存与并发热点收敛 |
| [utils](./utils/) | 通用工具集：SanitizeDSN、可定制位宽的雪花 ID（53 位方案兼容 JS Number）等纯函数 |

## 目录结构

```text
go-infra/
├── access/            # 数据访问：mysql / postgres / redis / rabbitmq / mongo / kafka
├── cache/             # 本地 + 两级缓存
├── idempotency/       # 幂等执行器
├── lock/              # Redis 分布式锁
├── ratelimit/         # 限流器
├── breaker/           # 熔断器
├── metrics/           # 可观测事实契约（零观测依赖）
├── prometheus/        # Prometheus 适配层
├── tracing/           # OpenTelemetry 链路追踪
├── logger/            # 全局日志底座
├── httpclient/        # HTTP Client 工厂
├── eventbus/          # 进程内事件总线
├── shutdown/          # 优雅退出
├── supervisor/        # 进程管理
├── storage/           # 对象存储
├── avatar/            # 随机头像生成
└── utils/             # 通用工具
```

## 使用示例

各组件均可独立引用，以 Redis 锁为例：

```go
import "github.com/zavierswong/go-infra/lock"
```

每个包目录下都有完整 README（特性、架构图、配置表、API、注意事项）与可编译的 `example_test.go` 示例。

## 测试

```bash
go test ./... -race -count=1
```

- 单元测试零外部依赖，开箱即跑（`ratelimit`、`cache` 等使用 miniredis 模拟 Redis）。
- 集成测试按环境变量门控（如 `KAFKA_TEST_BROKERS`），本地无对应中间件时自动跳过。

## License

MIT
