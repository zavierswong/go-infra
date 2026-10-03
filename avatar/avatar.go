// Package avatar 提供随机头像生成能力，底层是 DiceBear 的官方 Go 移植
// （github.com/dicebear/dicebear-go/v10 + github.com/dicebear/styles/v10）。
//
// 面向的场景：用户注册后自动分配一个默认头像——只存一个 seed 字符串，
// 不必让用户上传图片；同一个 seed 永远得到同一个头像。
//
// 基本用法：
//
//	gen := avatar.New()
//
//	// 随机 seed，输出 PNG
//	png, err := gen.Generate(avatar.Options{Style: "lorelei", Size: 256, Format: avatar.FormatPNG})
//
//	// 以用户 ID 为 seed，得到可复现的头像
//	svg, err := gen.Generate(avatar.Options{Seed: "user-42", Format: avatar.FormatSVG})
//
// 输出格式：SVG（所有风格都支持）、DataURI（可直接塞进 <img src>）、
// PNG、JPEG。后两者需要把 SVG 光栅化，少数使用了 SVG mask 等特性的风格
// 无法转换，此时返回 ErrRasterUnsupported 并提示改用 SVG——详见 format.go
// 与 README 的兼容性表。
//
// 并发安全：Generator 可被多个 goroutine 共享；其内部缓存已解析的风格
// 定义，同一风格只会被解析一次。
package avatar

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"

	dicebear "github.com/dicebear/dicebear-go/v10"
	"github.com/dicebear/styles/v10"
)

const (
	// DefaultStyle 是未指定风格时使用的风格。
	DefaultStyle = "lorelei"
	// DefaultSize 是未指定尺寸时的边长（像素）。
	DefaultSize = 256
	// MinSize 与 MaxSize 限定边长范围。
	//
	// 上限压到 2048（而非 DiceBear 自身的 4096）是因为光栅化需要
	// W×H×4 字节的 RGBA 缓冲：2048² ≈ 16MB/张，4096² ≈ 64MB/张。
	// 若尺寸来自外部输入，不设上限就是一条内存放大攻击路径。
	MinSize = 16
	MaxSize = 2048
)

// Options 描述一次头像生成请求。零值可用，等价于「默认风格的随机头像」。
type Options struct {
	// Style 是风格名，取值见 Styles()；为空时用 DefaultStyle。
	Style string
	// Seed 决定头像的具体长相。为空时自动生成一个随机 seed
	// （即"随机头像"）；非空时同 seed 必然得到相同外观，可用用户 ID
	// 之类的稳定值作为 seed。
	Seed string
	// Size 是输出边长（正方形），默认 DefaultSize，范围 [MinSize, MaxSize]。
	Size int
	// Format 是输出格式，默认 FormatSVG。
	Format Format

	// BackgroundColor 是画布背景色，取值形如 ["#b6e3f4"]，
	// 留空则用风格自带的背景。传空切片可显式去掉背景。
	BackgroundColor []string
	// Flip 控制翻转，可选 "horizontal" / "vertical" / "both"。
	Flip []string
	// Rotate 是旋转角度范围 [min, max]，如 []float64{-10, 10}；
	// 传单个值时写作 []float64{5}。
	Rotate []float64
	// Scale 是缩放范围 [min, max]，取值 0~10。
	Scale []float64
	// BorderRadius 是圆角范围 [min, max]，取值 0~50。
	BorderRadius []float64
	// TranslateX / TranslateY 是平移范围 [min, max]，取值 -1000~1000。
	TranslateX []float64
	TranslateY []float64
	// FontFamily / FontWeight 影响含文字的风格（如 initials）。
	FontFamily []string
	FontWeight []float64
	// Title 写入 SVG 的 <title>，提升可访问性。
	Title string
	// IDRandomization 为 true 时给 SVG 内的元素 id 加随机后缀。
	//
	// 默认 false：同 seed 同参数产出字节级一致的 SVG，便于做 ETag、
	// CDN 缓存与"改没改"的比对。仅当要把多个同款头像内联进同一个
	// HTML 文档（id 会撞车）时才需要打开。
	IDRandomization bool

	// StyleOptions 透传风格专属选项，如 map[string]any{"hairVariant": []string{"short01"}}。
	// 可取值见 Styles() 对应风格的选项描述符；本包不做白名单校验，
	// 非法选项由 DiceBear 返回错误。
	// 同名键以结构体上的显式字段为准。
	StyleOptions map[string]any

	// JPEGQuality 是 JPEG 编码质量，1~100，默认 DefaultJPEGQuality。
	// 仅 Format 为 FormatJPEG 时生效。
	JPEGQuality int
}

// Generator 是头像生成器，持有已解析风格定义的缓存，可并发使用。
//
// 风格定义是一份 150KB~450KB 的 JSON，解析并做 schema 校验的开销远大于
// 渲染本身，因此必须缓存。缓存只在首次用到某个风格时填充，不做全量预热。
type Generator struct {
	mu    sync.RWMutex
	cache map[string]*styleEntry
}

// styleEntry 是缓存项。构造完成后内容不再变化，可被多 goroutine 同时读取。
type styleEntry struct {
	style *dicebear.Style
	err   error
}

// New 创建一个 Generator。
func New() *Generator {
	return &Generator{cache: make(map[string]*styleEntry, 8)}
}

// Generate 生成头像，返回按 Options.Format 编码后的字节。
//
// 返回值语义随 Format 变化：
//   - FormatSVG：UTF-8 的 SVG 文本
//   - FormatDataURI：data:image/svg+xml;charset=utf-8,... 文本
//   - FormatPNG / FormatJPEG：二进制图片
//
// 服务端写响应时用 Options.Format.ContentType() 设置 Content-Type。
func (g *Generator) Generate(opts Options) ([]byte, error) {
	styleName, size, format, err := resolve(opts)
	if err != nil {
		return nil, err
	}

	style, err := g.style(styleName)
	if err != nil {
		return nil, err
	}

	seed := opts.Seed
	if seed == "" {
		if seed, err = randomSeed(); err != nil {
			return nil, err
		}
	}

	av, err := dicebear.NewAvatar(style, buildDiceBearOptions(opts, seed, size))
	if err != nil {
		return nil, fmt.Errorf("avatar: 生成 %s 风格头像失败: %w", styleName, err)
	}

	switch format {
	case FormatSVG:
		return []byte(av.SVG()), nil
	case FormatDataURI:
		return []byte(av.DataURI()), nil
	case FormatPNG, FormatJPEG:
		return rasterize(av.SVG(), rasterConfig{
			style:   styleName,
			seed:    seed,
			size:    size,
			format:  format,
			quality: opts.JPEGQuality,
		})
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownFormat, format)
	}
}

// GenerateSVG 是 Generate 的便捷形式，返回 SVG 文本。
//
// 它与 Generate 走同一条渲染路径，仅省去格式编码；所有风格都支持。
func (g *Generator) GenerateSVG(opts Options) (string, error) {
	opts.Format = FormatSVG
	data, err := g.Generate(opts)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// style 取出（必要时解析并缓存）指定风格。
//
// 白名单（styles.Get）未命中的名字**不缓存**：缓存 key 直接来自
// 调用方（Options.Style 很可能透传外部输入），未知风格也入缓存的话，
// 每个不同的非法名字都会留下一条永不淘汰的条目 —— 无界内存，可被
// 外部输入打爆。已知的解析失败（61 个风格内）才会缓存，数量有界。
func (g *Generator) style(name string) (*dicebear.Style, error) {
	g.mu.RLock()
	entry, ok := g.cache[name]
	g.mu.RUnlock()
	if ok {
		return entry.style, entry.err
	}

	def, found := styles.Get(name)
	if !found {
		return nil, fmt.Errorf("%w: %q（可用风格见 Styles()）", ErrUnknownStyle, name)
	}

	// 走到这里说明该风格合法但尚未缓存。全程持写锁，保证同一风格
	// 只解析一次；只有该风格的首次调用会付出这次锁开销。
	g.mu.Lock()
	defer g.mu.Unlock()
	if cached, ok := g.cache[name]; ok {
		return cached.style, cached.err
	}

	entry = &styleEntry{}
	if entry.style, entry.err = dicebear.NewStyle([]byte(def)); entry.err != nil {
		entry.err = fmt.Errorf("avatar: 解析 %s 风格定义失败: %w", name, entry.err)
	}
	g.cache[name] = entry

	return entry.style, entry.err
}

// Styles 返回所有可用风格名（已排序）。
//
// 需要注意：styles 包用 //go:embed 把全部 61 个风格定义（合计约 4.6MB）
// 编进了二进制，无法按风格裁剪。对体积敏感的服务可以自己 //go:embed
// 需要的 JSON，并直接调用 dicebear.NewStyle 绕开本包。
func Styles() []string {
	names := styles.All()
	sort.Strings(names)
	return names
}

// StyleOptions 返回指定风格支持的全部选项描述符（名字 → 类型/取值范围），
// 可用于给管理后台生成表单，或确认某个风格专属选项的确切名字。
//
// 依赖 Generator 的风格缓存；风格不存在时返回 ErrUnknownStyle。
func (g *Generator) StyleOptions(style string) (map[string]any, error) {
	s, err := g.style(style)
	if err != nil {
		return nil, err
	}
	return dicebear.NewOptionsDescriptor(s).ToJSON(), nil
}

// resolve 归一化并校验 Options 中与具体风格无关的字段。
//
// Format 的零值是空串，约定与 FormatSVG 等价，因此这里统一归一化，
// 避免调用方必须显式写 Format 才能拿到默认行为。
func resolve(opts Options) (styleName string, size int, format Format, err error) {
	styleName = opts.Style
	if styleName == "" {
		styleName = DefaultStyle
	}

	size = opts.Size
	if size == 0 {
		size = DefaultSize
	}
	if size < MinSize || size > MaxSize {
		return "", 0, "", fmt.Errorf("%w: %d，需在 [%d, %d] 内", ErrSizeOutOfRange, size, MinSize, MaxSize)
	}

	format = opts.Format
	if format == "" {
		format = FormatSVG
	}
	switch format {
	case FormatSVG, FormatDataURI, FormatPNG, FormatJPEG:
	default:
		return "", 0, "", fmt.Errorf("%w: %q", ErrUnknownFormat, format)
	}

	return styleName, size, format, nil
}

// buildDiceBearOptions 把 Options 映射成 DiceBear 的选项 map。
// 结构体上的显式字段优先于 StyleOptions 中的同名键。
func buildDiceBearOptions(opts Options, seed string, size int) map[string]any {
	out := make(map[string]any, len(opts.StyleOptions)+14)
	for k, v := range opts.StyleOptions {
		out[k] = v
	}

	out["seed"] = seed
	out["size"] = size

	setIfNotEmpty(out, "backgroundColor", opts.BackgroundColor)
	setIfNotEmpty(out, "flip", opts.Flip)
	setIfNotEmpty(out, "fontFamily", opts.FontFamily)
	setIfNotEmpty(out, "fontWeight", opts.FontWeight)
	setIfNotEmpty(out, "rotate", opts.Rotate)
	setIfNotEmpty(out, "scale", opts.Scale)
	setIfNotEmpty(out, "borderRadius", opts.BorderRadius)
	setIfNotEmpty(out, "translateX", opts.TranslateX)
	setIfNotEmpty(out, "translateY", opts.TranslateY)
	if opts.Title != "" {
		out["title"] = opts.Title
	}
	if opts.IDRandomization {
		out["idRandomization"] = true
	}

	return out
}

// setIfNotEmpty 只在切片非空时写入，避免把 nil 变成一个空数组——
// DiceBear 对空数组的处理与"未设置"不同（例如 flip: [] 会清空翻转配置）。
func setIfNotEmpty[T any](dst map[string]any, key string, value []T) {
	if len(value) > 0 {
		dst[key] = value
	}
}

// randomSeed 生成一个随机 seed。
//
// 用 crypto/rand 而非 math/rand：不共享全局状态，且不同实例/进程间不会
// 因为种子相同而产出同一批头像。
func randomSeed() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("avatar: 生成随机 seed 失败: %w", err)
	}
	return hex.EncodeToString(buf[:]), nil
}

// 本包可能返回的错误，均可用 errors.Is 判定。
var (
	// ErrUnknownStyle 表示风格名不存在。
	ErrUnknownStyle = errors.New("avatar: unknown style")
	// ErrUnknownFormat 表示输出格式不受支持。
	ErrUnknownFormat = errors.New("avatar: unknown format")
	// ErrSizeOutOfRange 表示 Size 超出 [MinSize, MaxSize]。
	ErrSizeOutOfRange = errors.New("avatar: size out of range")
	// ErrRasterUnsupported 表示该风格无法光栅化。
	// 调用方应降级到 FormatSVG 或 FormatDataURI。
	ErrRasterUnsupported = errors.New("avatar: style cannot be rasterized")
)
