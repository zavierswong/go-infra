# utils

与业务无关的通用工具集。约定：默认只放**纯函数**（无全局状态、无副作用、
可并发调用）；唯一例外是 `snowflake.go` 的雪花 ID 生成器（有状态但零依赖、
高频共用，见包注释说明）。

## ⚠️ 依赖代价（重要）

`phone.go` 引入了 `github.com/nyaruka/phonenumbers`。该库在 `init()` 中
加载全球号码元数据，**包的 init 函数无法被链接器裁剪**，因此：

| 场景 | 二进制增量 |
|---|---|
| `utils` 不含 `phone.go`（仅 SanitizeDSN + Snowflake） | ~0（几乎与纯 fmt 程序相同） |
| `utils` 含 `phone.go`（实测） | **+5.54 MB** |

也就是说，**只要 import 了本包，不管有没有用到手机号解析，都会多背 5.5MB**。

对体积敏感的服务，把 `phone.go` / `phone_test.go` 挪到一个独立顶层包
（如 `phone/`）即可规避，改动量只有 import 路径一行。是否值得拆分，
取决于「手机号解析的复用广度」与「二进制体积敏感度」的权衡。

## SanitizeDSN

抹掉 DSN 中的密码，便于安全写入日志：

```go
utils.SanitizeDSN("amqp://guest:secret@127.0.0.1:5672/")
// amqp://guest:***@127.0.0.1:5672/

utils.SanitizeDSN("root:secret@tcp(127.0.0.1:3306)/app")
// root:***@tcp(127.0.0.1:3306)/app
```

不含密码的 DSN 原样返回。同时覆盖 URL 形式（`scheme://user:pass@host`）
与 MySQL 形式（`user:pass@tcp(host)/db`）。

## 手机号解析（全球地区）

基于 Google libphonenumber 的 Go 移植，覆盖 240+ 个地区的号码长度、
前缀、运营商与号码类型规则。核心是**归一化**与**归属识别**：
把用户随便敲的一串号码变成可入库、可比对的标准形态。

```go
p, err := utils.ParsePhone("138 0013 8000", "CN")
// p.E164          +8613800138000
// p.National      138 0013 8000
// p.International +86 138 0013 8000
// p.RFC3966       tel:+86-138-0013-8000
// p.CountryCode   86
// p.Region        CN
// p.Type          mobile
// p.Valid         true
// p.Possible      true

// 只想要标准化结果
e164, err := utils.NormalizePhone("(415) 555-2671", "US") // +14155552671

// 表单校验：解析失败也算非法，不返回错误
ok := utils.IsValidPhone("13800138000", "CN") // true
ok = utils.IsValidPhone("13800138000", "")    // false（无默认地区且无 + 前缀）

// 归属地区
region, err := utils.PhoneRegion("+81-3-1234-5678", "") // JP

// 日志脱敏
utils.MaskPhone("13800138000", "CN")        // +86138****8000
utils.MaskPhone("+14155552671", "")         // +1415***2671
utils.MaskPhoneWith("13800138000", "CN", 0, 4) // +86*******8000
```

### defaultRegion 的语义

| 传值 | 含义 |
|---|---|
| `"CN"` / `"cn"` | 号码未带国际前缀时按该地区解析；大小写不敏感 |
| `""` 或 `"ZZ"` | 没有默认地区，**号码必须自带 `+国家码`**，否则报错 |
| 其他 | 返回 `ErrPhoneUnknownRegion` |

号码本身的书写很宽容：空格、连字符、括号、点号、IDD 前缀（`00`）都能识别。
超过 250 字符的输入会被底层拒绝（libphonenumber 的防 ReDoS 上限）。

### PhoneType 取值

`mobile`、`fixed_line`、`fixed_line_or_mobile`、`toll_free`、`premium_rate`、
`shared_cost`、`voip`、`personal_number`、`pager`、`uan`、`voicemail`、`unknown`。

> 注意 `fixed_line_or_mobile`：美国、加拿大等地区固话与手机号段重叠，
> 库只能给出这个值。所以**不要**用 `p.Type != PhoneTypeMobile` 来拒绝号码，
> 否则会把美加用户全部拦掉。宽松场景用 `p.Valid`，严格场景才用 `p.IsMobile()`。

### Valid 与 Possible 的区别

- `Possible`：长度符合地区规则，但前缀可能仍无效。
- `Valid`：号码在该地区的号码计划中确实存在。`Valid ⇒ Possible`，反之不成立。

解析得出国家码但号码非法时不返回 error，只把 `Valid` 置为 false——
这样调用方能区分"这不是电话号码"与"这是电话号码但号段不存在"。

### MaskPhone 的不变量

**只要提取到的号码至少含 1 位数字，输出必然至少有 1 位被打码。**
保留位数之和超过号码长度时会先压缩后缀、再压缩前缀：

```go
utils.MaskPhone("1", "CN")                 // +86*
utils.MaskPhoneWith("12345", "CN", 3, 4)   // +86*2345
```

无数字输入返回空串（没有可泄漏的内容）。这条不变量有专门的测试锁定。

### 并发安全

`utils` 中所有 phone 函数都是纯函数语义，可无锁并发调用。

底层 `phonenumbers` 运行期唯一的可变状态是一张受 `sync.RWMutex` 保护的
正则缓存（其 `regexCache`），元数据在 `init()` 中一次性加载后不再写入——
`useMetadata` 是未导出函数，应用代码无法在运行期替换元数据。

一处需要留意：`phonenumbers.GetSupportedRegions()` 等接口**直接返回内部
map**。本包一律返回副本（`SupportedPhoneRegions()`），避免调用方写入污染
全局元数据；本包也不对外暴露 `*PhoneMetadata`。

### 测试

```bash
go test ./utils/ -race -count=1
```

覆盖：16 个地区的解析与类型判定、默认地区语义（含 `ZZ`）、E.164 归一化、
非法但可解析的号码、脱敏不变量与自定义保留位、支持地区副本不可污染、
32 goroutine × 跨地区并发。

## Snowflake 雪花 ID

纯数字 int64 发号器，面向 **DB 主键**场景（BIGINT 直存 / 十进制字符串主键）。
位布局：`时间戳毫秒 | 节点 | 序列`，三段位宽可定制，总位宽 ≤ 63。

```go
s, err := utils.NewSnowflake(utils.SnowflakeConfig{Node: 1})
id, err := s.Next()          // int64，直接作 BIGINT 主键
str, err := s.NextString()   // 十进制字符串，作字符串主键/订单号
p := s.Parse(id)             // 反解：生成时间 / 节点 / 序列
```

### 位宽方案速查

| 方案（time+node+seq） | 总位宽 | 数字位数 | 特点 |
|---|---|---|---|
| 41+10+12（默认） | 63 | ≤19 | 经典切分，1024 节点 × 409万/ms，69 年 |
| 41+5+7 | 53 | ≤16 | **可安全过 JS Number**（前端/接口直出） |
| 45+2+6 | 53 | ≤16 | 单机或少量节点，每节点 64/ms |

### 配置参考

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `TimeBits` / `NodeBits` / `SeqBits` | `int` | 41/10/12 | 三段位宽；定制时建议三段都显式给出，总和 ≤63 |
| `Node` | `int64` | `0` | 节点 ID，多实例必须互不相同，范围 `[0, 2^NodeBits)` |
| `StartTime` | `time.Time` | 2024-01-01 UTC | 纪元基准；相对 1970 多 54 年余量 |
| `MaxRollbackWait` | `time.Duration` | `2s` | 时钟回拨容忍窗口 |

### 注意事项

- **单调性保证**：同进程内 ID 严格递增（互斥锁 + 毫秒内序列），
  可安全用作聚簇索引主键，无页分裂抖动。
- **时钟回拨**：回拨 ≤ `MaxRollbackWait` 时自旋等待时钟追上；
  超窗返回错误（**宁缺勿重**，不产出重复 ID）。NTP 阶跃较大的机器
  请调大窗口或改用不依赖墙钟的方案。
- **多实例部署**：`Node` 必须互不相同（建议从配置中心/环境变量分配），
  两个相同 Node 的实例在同一毫秒可能产出重复 ID。
- **吞吐上限**：每毫秒每节点 `2^SeqBits` 个；默认 409 万/ms，
  53 位方案（seq=7）为 128/ms，高频写表慎用短位宽。
- **Parse 与位宽绑定**：反解结果只在生成时使用的位宽配置下有意义。
- `NextString` 是十进制无符号字符串，无前缀无分隔符；需要带业务前缀
  （如 `ORD-...`）请自行拼接。

### 测试

```bash
go test ./utils/ -race -count=1
```

覆盖：并发唯一性 + 单调性、53 位定制、字符串输出、Parse 反解、
小幅回拨等待、大幅回拨拒绝、序列耗尽进位、多节点互异、非法配置。

## 目录结构

```
utils/
├── utils.go                  # SanitizeDSN
├── phone.go                  # 手机号解析 / 归一化 / 脱敏
├── snowflake.go              # 雪花 ID 生成器
├── utils_test.go
├── phone_test.go
├── snowflake_test.go
└── example_snowflake_test.go # 可编译示例
```
