package avatar

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"image/png"
	"regexp"
	"strings"

	"github.com/srwiley/oksvg"
	"github.com/srwiley/rasterx"
)

// Format 是头像的输出格式。
type Format string

const (
	// FormatSVG 输出 SVG 文本。所有风格都支持。
	FormatSVG Format = "svg"
	// FormatDataURI 输出 data:image/svg+xml;charset=utf-8,... 文本，
	// 可直接用作 <img src> 或 CSS background-image。所有风格都支持。
	FormatDataURI Format = "datauri"
	// FormatPNG 输出 PNG，保留透明通道。需要光栅化。
	FormatPNG Format = "png"
	// FormatJPEG 输出 JPEG。需要光栅化，且 JPEG 无透明通道，
	// 未绘制区域会合成到白色底。
	FormatJPEG Format = "jpeg"
)

// ContentType 返回该格式对应的 MIME 类型，便于直接写入 HTTP 响应头。
func (f Format) ContentType() string {
	switch f {
	case FormatSVG:
		return "image/svg+xml"
	case FormatDataURI:
		return "text/plain; charset=utf-8"
	case FormatPNG:
		return "image/png"
	case FormatJPEG:
		return "image/jpeg"
	default:
		return "application/octet-stream"
	}
}

// IsRaster 报告该格式是否需要光栅化。
// 需要光栅化的格式可能因风格使用了不支持的 SVG 特性而失败。
func (f Format) IsRaster() bool { return f == FormatPNG || f == FormatJPEG }

// DefaultJPEGQuality 是 JPEG 的默认编码质量。
const DefaultJPEGQuality = 90

// rasterConfig 收集光栅化所需的参数。
type rasterConfig struct {
	style   string
	seed    string
	size    int
	format  Format
	quality int
}

// reMetadata 匹配 SVG 里的 <metadata> 块（DiceBear 用它承载作者与许可信息）。
//
// 它只用于光栅化的输入：oksvg 在 StrictErrorMode 下会因无法处理 <metadata>
// 而报错，剥离后既不影响画面（<metadata> 不参与渲染），又能让 StrictErrorMode
// 把真正不支持的图形特性暴露成错误。
// 对外输出的 SVG 始终保留该块——里面的 dcterms:license 是风格作者的署名要求。
var reMetadata = regexp.MustCompile(`(?s)<metadata\b.*?</metadata>`)

// rasterize 把 DiceBear 渲染出的 SVG 转成位图。
//
// 这里刻意选择"宁可报错，也不出白图"的策略，原因见 ErrRasterUnsupported
// 与 README 的兼容性表：
//   - oksvg 默认的 IgnoreErrorMode 会静默跳过不认识的元素。遇到 <mask>
//     这类它不支持的特性时，被遮罩的内容会整块消失，输出一张空白图，
//     而且不返回任何错误——这种静默失败比显式报错危险得多。
//   - 因此这里改用 StrictErrorMode，并在其之前自行拦截 <mask>：
//     StrictErrorMode 覆盖不到"<mask> 定义在 <defs> 里却由外部引用"
//     的写法（如 bottts），只靠它仍会漏出白图。
func rasterize(svg string, cfg rasterConfig) ([]byte, error) {
	stripped := reMetadata.ReplaceAllString(svg, "")

	if strings.Contains(stripped, "<mask") {
		return nil, fmt.Errorf(
			"%w: 风格 %q 使用了 SVG <mask>，Go 版光栅化器（oksvg）不支持该特性；"+
				"请改用 FormatSVG 或 FormatDataURI，或换一个不使用遮罩的风格",
			ErrRasterUnsupported, cfg.style)
	}

	icon, err := oksvg.ReadIconStream(bytes.NewReader([]byte(stripped)), oksvg.StrictErrorMode)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: 风格 %q 的光栅化失败（%v）；请改用 FormatSVG 或 FormatDataURI",
			ErrRasterUnsupported, cfg.style, err)
	}

	size := cfg.size
	// SetTarget 把 SVG 的 viewBox 映射到目标尺寸。
	// 各风格的 viewBox 边长并不相同（lorelei 980、bottts 180、identicon 5…），
	// 不设置的话输出尺寸会随风格漂移，与 Options.Size 不符。
	icon.SetTarget(0, 0, float64(size), float64(size))

	img := image.NewRGBA(image.Rect(0, 0, size, size))
	if cfg.format == FormatJPEG {
		// JPEG 没有透明通道，不留白底的话未绘制区域会变成黑色。
		draw.Draw(img, img.Bounds(), image.NewUniform(color.White), image.Point{}, draw.Src)
	}
	scanner := rasterx.NewScannerGV(size, size, img, img.Bounds())
	icon.Draw(rasterx.NewDasher(size, size, scanner), 1.0)

	var buf bytes.Buffer
	// 预分配，避免编码阶段反复扩容。
	buf.Grow(size * size / 4)
	switch cfg.format {
	case FormatPNG:
		if err := png.Encode(&buf, img); err != nil {
			return nil, fmt.Errorf("avatar: PNG 编码失败: %w", err)
		}
	case FormatJPEG:
		quality := cfg.quality
		if quality <= 0 {
			quality = DefaultJPEGQuality
		}
		if quality > 100 {
			quality = 100
		}
		if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: quality}); err != nil {
			return nil, fmt.Errorf("avatar: JPEG 编码失败: %w", err)
		}
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownFormat, cfg.format)
	}

	return buf.Bytes(), nil
}
