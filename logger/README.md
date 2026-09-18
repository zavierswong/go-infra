# logger — 进程级全局日志底座

基于标准库 `log/slog` 的全局日志包：**一次 Init，全项目直接用**。零第三方依赖（仅标准库），支持 JSON/text 双格式、ANSI 终端彩色输出、按体积轮转 + gzip 压缩、ctx 透传 trace_id，以及 GORM SQL 日志接入。

## 特性一览

- **开箱即用**：不调用 `Init` 也能直接打日志（懒加载默认实例：stdout + info + json），不存在"忘记初始化就 panic"和包间初始化顺序问题；
- **热更新**：`Init` 可重复调用做热替换；`SetLevel` 运行期动态调级，立即生效、无需重建；
- **读路径无锁**：全局状态用 `atomic.Pointer` 整体替换，打日志不加锁；
- **彩色终端输出**：text 格式 + 终端输出时自动着色（尊重 `NO_COLOR`/`FORCE_COLOR`），JSON 与文件输出永不携带色码；
- **体积轮转**：文件输出时按大小轮转，历史文件按"数量 + 天数"双条件清理，可选 gzip 压缩；
- **trace_id 透传**：`WithTraceID(ctx, id)` 后经任何出口输出的日志自动携带 `trace_id` 字段；
- **GORM 适配**：`logger.Mysql` 实现 `gorm.io/gorm/logger.Interface`，SQL 日志统一汇入全局日志器；
- **caller 默认开启，输出统一为 `函数:行号`**：text 与 JSON 都是 `source=handler.GetUser:42` 形式的字符串（slog 原生 text 只给文件:行号、JSON 给对象，本包统一改写），所有出口都指向业务调用处、不会被包装函数污染；可用 `DisableCaller` 关闭；
- **Fatal 语义完整**：`Fatalf` 先刷盘再 `os.Exit(1)`，级别渲染为 `fatal` 且高于 error，退出路径可测（`exitFunc` 注入 + 子进程断言）。

## 快速开始

```go
import "github.com/zavierswong/go-infra/logger"

func main() {
	if err := logger.Init(logger.Config{
		Level:   "debug",
		Format:  "json",
		Service: "user-api",
		// DisableCaller: true,              // 关闭调用者信息(默认输出 函数:行号)
		// Output: "logs/app.log",           // 文件输出 + 自动轮转
		// Rotate: logger.RotateConfig{MaxSizeMB: 100, MaxBackups: 7, MaxAgeDays: 30, Compress: true},
	}); err != nil {
		return err
	}
	defer logger.Close()

	logger.Infof("服务已启动, 端口: %d", 8080)   // 包级 printf 风格
	logger.Ctx(ctx).Info("处理请求", "uid", 1001) // 带 ctx 字段的结构化风格

	lg := logger.NewPlog("MySQL")                  // 具名子日志器
	lg.Errorf(ctx, "查询失败: %v", err)
}
```

未初始化时也可直接用（适合写公共库、测试代码）：

```go
logger.Warnf("redis 连接重试, 第 %d 次", n)
```

## 配置项

```go
type Config struct {
	Level      string       // debug/info/warn/error, 默认 info（非法值回落 info）
	Format     string       // json/text, 默认 json
	Output     string       // stdout(默认)/stderr/文件路径
	Service    string       // 服务名, 写入每条日志的 service 字段
	DisableCaller bool         // 是否关闭调用者信息; 默认输出(函数:行号)
	TimeFormat string       // 时间格式, 默认 RFC3339 毫秒精度
	Color      string       // 色彩输出: auto(默认,自动探测终端)/on(强制)/off(关闭)
	Rotate     RotateConfig // 文件轮转, 仅 Output 为文件路径时生效
}

type RotateConfig struct {
	MaxSizeMB  int  // 单文件最大体积(MB), 默认 100
	MaxBackups int  // 最多保留的历史文件数, 默认 7
	MaxAgeDays int  // 历史文件最长保留天数, 默认 30, 负数表示不按时间清理
	Compress   bool // 是否 gzip 压缩历史文件
}
```

所有字段都有默认值，零值 `Config{}` 即可用。

### 色彩输出规则

| 场景 | 是否着色 |
|---|---|
| text + stdout/stderr 终端（auto 默认） | ✅ |
| `Color: "on"` 强制（仍限 text + 非文件输出） | ✅ |
| 管道/重定向/`NO_COLOR` 非空/`TERM=dumb` | ❌ |
| `FORCE_COLOR` 非空（CI 兜底） | ✅ |
| JSON 格式（任何情况） | ❌ 转义符会破坏结构化解析 |
| 文件/轮转输出（任何情况） | ❌ |

输出示例（text 彩色）：

```
time=2026-09-16T12:00:00.000+08:00 level=info source=handler.GetUser:42 msg="服务已启动, 端口: 8080" service=user-api
```

其中 `level` 按级别着色（debug 灰 / info 绿 / warn 黄 / error 红），`msg` 加粗。

### 调用者信息

调用者信息**默认开启**（`DisableCaller: true` 可关闭），text 与 JSON 呈现形式**一致**——都是 `函数:行号` 字符串：

| 格式 | 输出 |
|---|---|
| text | `source=handler.GetUser:42` |
| json | `"source":"handler.GetUser:42"` |

- 值来自 `runtime` 栈帧，函数名裁剪为 `包名.函数名`（去掉 import path 前缀，如 `logger.(*Plog).Infof` 保留方法形态），便于阅读与 grep；
- slog 不做统一：原生 text 只输出文件:行号、JSON 输出 `{function,file,line}` 对象，本包在 `ReplaceAttr` 中把两者都改写为同一个字符串形式；
- 需要文件路径时请自行从函数名追查，或开启 `DisableCaller` 后按业务需要补充字段；
- 关闭开销：`DisableCaller: true` 后不再解析调用栈帧，适合对性能敏感的高频路径；
- 输出的位置始终是**业务调用处**，不会因经过 `Plog` 等方法包装而偏移。

## API 一览

### 初始化与生命周期

| 函数 | 说明 |
|---|---|
| `Init(cfg Config) error` | 初始化全局日志器；可重复调用热替换，替换后关闭旧文件句柄 |
| `Close() error` | 刷新并关闭文件句柄，随后回落到默认实例（stdout），可重复调用 |
| `Sync() error` | 刷盘；文件输出时等价于 fsync |
| `SetLevel(level string)` | 运行期调级（debug/info/warn/error/fatal），热生效 |

### 包级日志函数（printf 风格）

```go
logger.Debugf(format, args...)
logger.Infof(format, args...)
logger.Warnf(format, args...)
logger.Errorf(format, args...)
logger.Fatalf(format, args...)   // 输出后退出进程，见下方「Fatal 与退出」
```

### Fatal 与退出

`Fatalf` 与标准库 `log.Fatalf` 语义对齐：**先同步写日志，再以状态码 1 退出进程**。

| 项目 | 实现 |
|---|---|
| 级别 | `LevelFatalValue = slog.LevelError + 4`（slog 无内置 fatal 级别），渲染为 `level=fatal`，**高于 error**，所以 `Level: "error"` 时依然输出 |
| 退出码 | 固定 `os.Exit(1)` |
| 刷盘 | 退出前自动 `Sync()`：`os.Exit` 不执行 defer，`defer logger.Close()`/`Sync()` 都不会跑 |
| 可测性 | 退出走包内变量 `exitFunc`，测试可替换；真实退出行为另有子进程用例断言 |
| 着色 | fatal 用加粗红（`\x1b[1;31m`），终端里一眼可见 |

```go
if err := app.Run(); err != nil {
	logger.Fatalf("服务启动失败: %v", err) // 打印后进程退出, 无需再写 os.Exit(1)
}
```

> ⚠️ **只应在 `main` 或启动阶段使用**。库、中间件、请求处理链路里调用 `Fatalf` 会直接终结宿主进程，绕过后者的优雅退出逻辑（连接池关闭、请求收尾、指标上报等），这类场景请改用 `Errorf` 并把错误返回给调用方。

### 标准库对接

| 函数 | 说明 |
|---|---|
| `Default() *slog.Logger` | 当前全局 slog.Logger |
| `Ctx(ctx) *slog.Logger` | 附带 ctx 上下文字段（如 trace_id）的 Logger，caller 仍指向调用处 |
| `With(args...) *slog.Logger` | 附带固定字段的 Logger |
| `WithName(name string) *slog.Logger` | 带 `logger=name` 字段的 Logger |

### Plog 具名子日志器

零值可用，`With`/`Named` 返回副本、不污染原实例，可安全并发：

```go
lg := logger.NewPlog("MySQL")
lg.Infof(ctx, "连接成功, 耗时: %s", elapsed)          // printf 风格, ctx 第一参数
lg.Info(ctx, "查询完成", "rows", 10)                  // 结构化风格
sub := lg.Named("pool").With("dsn", "root:***@tcp/db") // 名字拼接为 "MySQL.pool"
sub.Warn(ctx, "连接池已满", "inUse", 8, "max", 10)
```

方法齐全：`Debugf/Infof/Warnf/Errorf`（printf 风格）、`Debug/Info/Warn/Error`（结构化风格），
以及同样会退出进程的 `Fatalf/Fatal(ctx, ...)`（带 `logger` 字段，语义同包级 Fatalf）。

`Plog` 不缓存全局实例，每次输出都取当前全局日志器，因此 `Init` 热替换后自动跟随。

### trace_id 透传

```go
ctx := logger.WithTraceID(r.Context(), traceID) // 中间件里注入
logger.NewPlog("Order").Infof(ctx, "下单成功, 订单号: %s", orderNo)
// 输出自动携带 trace_id="..."
```

## GORM SQL 日志接入

```go
db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
	Logger: &logger.Mysql{
		Name:          "MySQL",              // logger 字段名, 默认 gorm
		SlowThreshold: 200 * time.Millisecond, // 慢 SQL 阈值, 0=默认 200ms, 负数=关闭告警
		LogLevel:      "warn",               // silent/error/warn/info, 默认 info
		IgnoreRecordNotFoundError: true,      // 忽略 ErrRecordNotFound
	},
})
```

单条 SQL 的输出级别：执行失败 → `error`，超过慢 SQL 阈值 → `warn`，其余 → `info`；字段包含 `sql`（超 4KB 截断）、`rows`、`elapsed`，并透传 ctx 中的 `trace_id`。`LogMode` 返回副本而非就地修改，可被 GORM 与业务方并发安全持有。

## 文件轮转

`Output` 为文件路径时自动启用：

- 写入前检查体积，超过 `MaxSizeMB` 即轮转，历史文件命名为 `<日志文件>.<毫秒时间戳>[.gz]`；
- 轮转后按 **数量（MaxBackups）+ 天数（MaxAgeDays）** 双条件清理；
- `Compress: true` 时历史文件 gzip 压缩（持锁内同步完成，保证不与写入交错，代价是轮转瞬间短暂阻塞）；
- 进程收尾请 `defer logger.Close()`，避免退出阶段继续往已关闭的文件写。

## 设计要点（改造前必读）

1. **级别过滤必须走 `Enabled`**：slog 只在 `Logger.Enabled` 里做级别判断，`Handler.Handle` 内部不判断。包内为携带 caller PC 手工构造 `slog.Record`，因此 `emit` 显式调用 `Enabled`——这同时保证 `SetLevel` 热调级生效。
2. **caller 固定 skip=4**：调用链 `Callers → callerPC → emit → 包装函数 → 调用方`。所有对外日志入口必须直接调用 `emit`，中间不许再加包装层，否则 caller 全部偏移。
3. **色彩经 writer 层还原**：`TextHandler` 渲染层会把 `ReplaceAttr` 注入的 ESC 字节转义成字面量 `\x1b`，终端无法显色；`colorUnescapeWriter` 负责还原真实 ESC 字节，仅在 text+stdout 彩色分支接入。
4. **printf 防误解析**：无参数时不做 `Sprintf`，避免消息中的 `%` 被误解析。
5. **统一 source 表示**：`slog` 对 source 的处理两种 handler 不一致——text 渲染 `文件:行号`（丢函数名）、JSON 渲染 `{function,file,line}` 对象；本包在 `ReplaceAttr` 中拦截 `slog.SourceKey`（此时值是 `*slog.Source`），统一返回 `slog.String(SourceKey, "包名.函数:行号")`。返回 `slog.String` 后 handler 的 `*Source` 特判分支不再命中，两种格式输出自然一致。

## 运行测试

```bash
cd <仓库根目录>
go test ./logger/ -race -count=1 -v
```

测试覆盖：未初始化可用、级别过滤与热调级、caller 定位（`函数:行号`）、printf 展开、trace_id、Plog 副本不可变性、并发不丢日志、轮转数量/压缩/按天清理、色彩开关（含 `NO_COLOR`/`FORCE_COLOR`）、GORM 级别映射与 Trace 分支、Fatal（退出码 1、`level=fatal`、Level=error 时仍输出、子进程验证真实退出）。
