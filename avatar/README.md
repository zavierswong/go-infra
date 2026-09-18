# avatar

随机头像生成。底层是 DiceBear 的**官方 Go 移植**
（`github.com/dicebear/dicebear-go/v10` + `github.com/dicebear/styles/v10`），
本包负责参数映射、多格式输出、风格缓存与并发热点收敛。

**为什么单独一个包**：它需要引入渲染核心 + 61 个风格定义 + SVG 光栅化器，
代价约 +9.5MB 二进制（明细见下）。放进 `utils` 会让所有工具调用方一起买单。

## 快速开始

```go
gen := avatar.New() // 可复用、并发安全

// 随机 seed -> 随机头像，输出 PNG
png, err := gen.Generate(avatar.Options{
    Style:  "lorelei",
    Size:   256,
    Format: avatar.FormatPNG,
})

// 用用户 ID 当 seed -> 可复现头像，输出 SVG
svg, err := gen.Generate(avatar.Options{
    Seed:   "user-42",
    Format: avatar.FormatSVG,
})
```

典型用法是"注册时分配默认头像"：

```go
// 存库只存 seed（或用用户 ID 当 seed），不用存图片
seed := uuid.NewString()

// 某处需要展示时现算
out, err := gen.Generate(avatar.Options{Seed: seed, Size: 128, Format: avatar.FormatPNG})
w.Header().Set("Content-Type", avatar.FormatPNG.ContentType())
w.Write(out)
```

## 输出格式

| Format | 输出 | Content-Type | 需要光栅化 | 风格支持 |
|---|---|---|---|---|
| `FormatSVG`（默认） | SVG 文本 | `image/svg+xml` | 否 | **全部 61 个** |
| `FormatDataURI` | `data:image/svg+xml;charset=utf-8,...` | `text/plain` | 否 | **全部 61 个** |
| `FormatPNG` | PNG，保留透明通道 | `image/png` | 是 | 52 个 |
| `FormatJPEG` | JPEG，无透明通道，空白处合成白底 | `image/jpeg` | 是 | 52 个 |

`Format` 的零值（`""`）等价于 `FormatSVG`，所以最简调用不用显式指定格式。
DataURI 可直接塞进 `<img src>` 或 CSS `background-image`，省一次请求。

## ⚠️ PNG / JPEG 的兼容性边界

DiceBear 只产出 SVG，位图需要本包自己做 SVG 光栅化（`oksvg` + `rasterx`，
纯 Go 无 cgo）。`oksvg` 支持 `path`/`g`/`use`/`clipPath`/渐变/变换，但
**不支持 `<mask>`**。

实测 61 个风格中 **52 个可光栅化，9 个不支持**：

```
bottts  bottts-neutral  cameo  disco  glyphs  personas  planets  slice  stack
```

对这 9 个风格，PNG/JPEG 会返回 `ErrRasterUnsupported` 并提示降级到 SVG：

```go
out, err := gen.Generate(avatar.Options{Style: "bottts", Format: avatar.FormatPNG})
if errors.Is(err, avatar.ErrRasterUnsupported) {
    // 降级：改用 SVG，或换一个不使用遮罩的风格
    svg, _ := gen.GenerateSVG(avatar.Options{Style: "bottts"})
}
```

### 为什么是"报错"而不是"尽力而为"

`oksvg` 默认的 `IgnoreErrorMode` 会**静默跳过**不认识的元素。遇到 `<mask>`
时，本该被遮罩裁掉的内容会整块丢失，输出一张空白图，而且**不返回任何错误**。
一张白图会被当成正常结果发到用户手上——这种静默数据错误比显式失败危险得多。

所以本包采用"宁可报错，不出白图"：

1. 光栅化前先剥离 `<metadata>`（DiceBear 的版权块，不参与渲染，
   但会让 `StrictErrorMode` 误报"无法处理 metadata"）；
2. 自行拦截 `<mask>`——`StrictErrorMode` 覆盖不到"mask 定义在 `<defs>`
   内、由外部 `<g>` 引用"的写法（`bottts` 正是这种），只靠它仍会漏出白图；
3. 用 `StrictErrorMode` 让其余不支持的特性直接报错。

`avatar_test.go` 里的 `TestRasterNeverSilentlyBlank` 遍历全部 61 个风格，
锁定"**每个风格要么产出有内容的位图，要么明确报 ErrRasterUnsupported**"
这条不变量，防止后续升级依赖时悄悄退化。

> 若那 9 个风格必须出位图，可改用 `resvg` 等支持遮罩的渲染器（需 cgo），
> 或调用 DiceBear 官方 HTTP API。本包不内置这两条路径。

## 确定性（seed）

| Seed | 行为 |
|---|---|
| 非空 | **同 seed + 同参数 = 字节级一致的输出**（`idRandomization` 默认关闭） |
| 空 | 自动生成随机 seed，即"随机头像" |

字节级可复现意味着可以直接拿输出做 ETag、CDN 缓存键、以及"内容有没有变"的比对。

`idRandomization` 是为数不多的例外开关：打开后 SVG 内的元素 id 会带随机后缀，
输出不再一致。**只在把多个同款头像内联进同一个 HTML 文档时**才需要打开
（否则 id 会撞车，浏览器只会渲染其中一个）。

随机 seed 来自 `crypto/rand`（16 字节 hex），不共享全局 `math/rand` 状态，
不同进程/实例不会因为种子相同而产出同一批头像。

## 并发安全

`Generator` 可被任意多个 goroutine 共享。风格定义是一份 150KB~450KB 的
JSON，解析 + schema 校验的开销远大于渲染本身，因此必须缓存：

```go
func (g *Generator) style(name string) (*dicebear.Style, error) {
    g.mu.RLock()                       // 命中缓存时只读锁
    entry, ok := g.cache[name]
    g.mu.RUnlock()
    if ok {
        return entry.style, entry.err
    }

    g.mu.Lock()                        // 未命中时全程持写锁
    defer g.mu.Unlock()
    if entry, ok := g.cache[name]; ok { // 双重检查
        return entry.style, entry.err
    }
    // ... 解析并写入缓存
}
```

这里的取舍值得说明：

- **为什么不用 `sync.Map`**：`sync.Map` 的 `LoadOrStore` 在冷缓存且并发
  首访同一风格时，会让 N 个 goroutine 各自解析一遍再丢掉 N-1 份结果。
  风格定义的解析不便宜，写锁 + 双重检查能保证**每个风格只解析一次**。
- **为什么只缓存不预热**：61 个风格全量解析要几百毫秒且大部分用不上，
  按需填充更合适。首次调用会付一次解析开销，之后只有一次 RLock。

依赖侧的并发情况（已逐个核对源码）：

| 依赖 | 全局可变状态 | 结论 |
|---|---|---|
| `dicebear-go/v10` | 无包级 var；PRNG 是每渲染实例的局部 mulberry32；`rand.Uint32()`（仅 `idRandomization` 路径）来自 goroutine-safe 的 `math/rand/v2` | 安全 |
| `dicebear/styles/v10` | 61 个 `//go:embed` 出来的只读 string | 安全 |
| `oksvg` / `rasterx` | 包级 var 均为只读的错误值、dispatch map、函数值 | 安全 |
| `nyaruka/phonenumbers` | 见 utils/README | 安全 |

验证：

```bash
go test ./avatar/ -race -count=1
```

`TestConcurrency`（32 goroutine × 4 种格式 × 6 个风格）与
`TestConcurrentColdCacheSameStyle`（专打冷缓存竞争热点）在 `-race` 下通过。

## 二进制体积代价

以纯 `fmt` 程序（2.38MB）为基线实测：

| 组件 | 增量 |
|---|---|
| `dicebear-go` + `styles`（61 个风格定义合计 4.6MB + 渲染核心） | +7.55 MB |
| `oksvg` + `rasterx`（光栅化） | +1.74 MB |
| **`avatar` 包合计** | **+9.55 MB** |

其中 `styles` 用 `//go:embed` 嵌入了全部 61 个风格，**无法按风格裁剪**——
即使只用 `lorelei`，4.6MB 的 JSON 也全部进二进制。

对体积敏感的服务有两个办法：

1. 自己 `//go:embed` 需要的风格 JSON，直接调 `dicebear.NewStyle` 绕开本包；
2. 把头像生成拆成独立服务/Serverless 函数，主服务只存 seed。

## 参数参考

结构体字段一一对应 DiceBear 的选项名，`StyleOptions` 是风格专属选项的透传口。

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `Style` | `string` | `"lorelei"` | 风格名，见 `Styles()` |
| `Seed` | `string` | 随机 | 决定长相；同值同外观 |
| `Size` | `int` | `256` | 边长，范围 `[16, 2048]`；`0` 表示用默认值 |
| `Format` | `Format` | `FormatSVG` | 输出格式 |
| `BackgroundColor` | `[]string` | 风格自带 | 形如 `["#b6e3f4"]`；传 `[]string{}` 显式去掉背景 |
| `Flip` | `[]string` | — | `"horizontal"` / `"vertical"` / `"both"` |
| `Rotate` | `[]float64` | — | 角度范围 `[min,max]`，`-360~360` |
| `Scale` | `[]float64` | — | 缩放范围，`0~10` |
| `BorderRadius` | `[]float64` | — | 圆角范围，`0~50` |
| `TranslateX/Y` | `[]float64` | — | 平移范围，`-1000~1000` |
| `FontFamily` / `FontWeight` | `[]string` / `[]float64` | — | 影响含文字的风格（如 `initials`） |
| `Title` | `string` | — | 写入 SVG `<title>`，利于可访问性 |
| `IDRandomization` | `bool` | `false` | 打开后元素 id 加随机后缀，输出不再可复现 |
| `StyleOptions` | `map[string]any` | — | 风格专属选项透传，如 `{"hairVariant": []string{"short01"}}` |
| `JPEGQuality` | `int` | `90` | 仅 JPEG 生效，`1~100` |

几点约定：

- **`Size` 上限是 2048**（DiceBear 自身允许 4096）。光栅化需要 `W×H×4` 字节的
  RGBA 缓冲，2048² ≈ 16MB/张、4096² ≈ 64MB/张；尺寸若来自外部输入，不设上限
  就是一条内存放大攻击路径。越界返回 `ErrSizeOutOfRange`。
- **显式字段优先于 `StyleOptions`**：同名键以结构体字段为准，避免透传口
  意外劫持 `seed`/`size`（有测试锁定）。
- 选项值不合法时由 DiceBear 返回错误，本包不做白名单校验（61 个风格各有几百个
  选项，维护白名单不现实）。需要确切的选项名时用 `Generator.StyleOptions()`：

```go
gen := avatar.New()
desc, err := gen.StyleOptions("adventurer")
// desc["seed"]        -> {"type":"string"}
// desc["backgroundColor"] -> {"type":"color","list":true}
// desc["eyesVariant"] -> {"type":"enum","values":[...],"list":true,"weighted":true}
// desc["eyesProbability"]     -> {"type":"number","min":0,"max":100}
```

这个描述符适合直接喂给管理后台生成表单。

## 错误

均可用 `errors.Is` 判定：

| 错误 | 含义 | 处理建议 |
|---|---|---|
| `ErrUnknownStyle` | 风格名不存在 | 用 `Styles()` 校验 |
| `ErrUnknownFormat` | 格式不支持 | 用四种 `Format` 常量之一 |
| `ErrSizeOutOfRange` | `Size` 越界 | 收到用户输入时按 `[16,2048]` 夹取 |
| `ErrRasterUnsupported` | 该风格无法光栅化 | 降级到 `FormatSVG` / `FormatDataURI` |

## 测试

```bash
go test ./avatar/ -race -count=1
```

覆盖：同 seed 字节级一致、空 seed 随机性、**全部 61 个风格的 SVG 生成**、
四种格式的魔数与尺寸、JPEG 质量对体积的影响、
**61 个风格的"不静默出白图"不变量**、降级路径、尺寸边界、风格/格式不存在、
默认风格、`idRandomization` 开关、风格选项透传与非法值拒绝、
显式字段优先级、背景色与翻转、选项描述符、并发（含冷缓存竞争）。

## 目录结构

```
avatar/
├── avatar.go       # Generator / Options / 选项映射 / 风格缓存
├── format.go       # Format 枚举 + SVG 光栅化（含兼容性拦截）
├── avatar_test.go
└── example_test.go # 可编译示例
```

## 许可

风格定义的著作权属于各自的作者，许可信息随每个风格的定义一起分发。
DiceBear 渲染的 SVG 会带 `<metadata>` 块，其中含 `dc:creator` 与
`dcterms:license`——**对外输出的 SVG 会保留该块**，请勿在展示/分发时剥离，
这是风格作者的署名要求。（光栅化前会临时剥掉它，因为它不参与渲染，
且会让光栅化器的严格模式误报；位图中不含该信息，商用前请自行核对
各风格的具体许可。）
