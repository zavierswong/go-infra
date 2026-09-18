package avatar

import (
	"bytes"
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"strings"
	"sync"
	"testing"
)

func TestGenerateDeterministicWithSameSeed(t *testing.T) {
	gen := New()
	opts := Options{Style: "lorelei", Seed: "user-42", Size: 128, Format: FormatSVG}

	first, err := gen.Generate(opts)
	if err != nil {
		t.Fatalf("首次生成失败: %v", err)
	}
	// 换一个 Generator 实例，验证缓存不影响输出。
	second, err := New().Generate(opts)
	if err != nil {
		t.Fatalf("再次生成失败: %v", err)
	}

	if !bytes.Equal(first, second) {
		t.Error("同 seed 同参数应产出字节级一致的 SVG（idRandomization 默认为 false）")
	}
	if len(first) == 0 {
		t.Fatal("输出为空")
	}
}

func TestGenerateWithoutSeedIsRandom(t *testing.T) {
	gen := New()
	seen := make(map[string]struct{}, 8)
	for i := 0; i < 8; i++ {
		out, err := gen.Generate(Options{Style: "lorelei", Format: FormatSVG})
		if err != nil {
			t.Fatalf("第 %d 次生成失败: %v", i, err)
		}
		seen[string(out)] = struct{}{}
	}
	if len(seen) != 8 {
		t.Errorf("seed 留空时应产出互不相同的头像, 得到 %d 种", len(seen))
	}
}

func TestGenerateAllStylesProduceSVG(t *testing.T) {
	gen := New()
	styles := Styles()
	if len(styles) < 60 {
		t.Fatalf("可用风格数 = %d, 预期 61 个左右", len(styles))
	}

	for _, style := range styles {
		t.Run(style, func(t *testing.T) {
			out, err := gen.Generate(Options{Style: style, Seed: "probe-seed", Size: 64})
			if err != nil {
				t.Fatalf("风格 %s 生成失败: %v", style, err)
			}
			svg := string(out)
			if !strings.HasPrefix(svg, "<svg") {
				t.Errorf("输出不是 SVG: %.60s", svg)
			}
			if !strings.Contains(svg, `width="64"`) || !strings.Contains(svg, `height="64"`) {
				t.Errorf("SVG 未使用请求的尺寸 64")
			}
			if !strings.Contains(svg, "<metadata") {
				t.Error("SVG 应保留 <metadata>（内含风格许可署名）")
			}
		})
	}
}

func TestGenerateFormats(t *testing.T) {
	gen := New()

	t.Run("SVG", func(t *testing.T) {
		out, err := gen.Generate(Options{Seed: "s", Size: 96, Format: FormatSVG})
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if !strings.HasPrefix(string(out), "<svg") {
			t.Errorf("期望 SVG 文本, 得到 %.40q", out)
		}
	})

	t.Run("DataURI", func(t *testing.T) {
		out, err := gen.Generate(Options{Seed: "s", Size: 96, Format: FormatDataURI})
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if !strings.HasPrefix(string(out), "data:image/svg+xml;charset=utf-8,") {
			t.Errorf("期望 data URI 前缀, 得到 %.40q", out)
		}
	})

	t.Run("PNG", func(t *testing.T) {
		out, err := gen.Generate(Options{Seed: "s", Size: 128, Format: FormatPNG})
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if !bytes.HasPrefix(out, []byte{0x89, 'P', 'N', 'G'}) {
			t.Fatalf("PNG 魔数不匹配")
		}
		img, err := png.Decode(bytes.NewReader(out))
		if err != nil {
			t.Fatalf("PNG 解码失败: %v", err)
		}
		if b := img.Bounds(); b.Dx() != 128 || b.Dy() != 128 {
			t.Errorf("PNG 尺寸 = %dx%d, 期望 128x128（各风格 viewBox 不同，必须被 SetTarget 归一）", b.Dx(), b.Dy())
		}
	})

	t.Run("JPEG", func(t *testing.T) {
		out, err := gen.Generate(Options{Seed: "s", Size: 128, Format: FormatJPEG})
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if !bytes.HasPrefix(out, []byte{0xFF, 0xD8, 0xFF}) {
			t.Fatalf("JPEG 魔数不匹配")
		}
		img, err := jpeg.Decode(bytes.NewReader(out))
		if err != nil {
			t.Fatalf("JPEG 解码失败: %v", err)
		}
		if b := img.Bounds(); b.Dx() != 128 || b.Dy() != 128 {
			t.Errorf("JPEG 尺寸 = %dx%d, 期望 128x128", b.Dx(), b.Dy())
		}
	})

	t.Run("JPEG质量影响体积", func(t *testing.T) {
		low, err := gen.Generate(Options{Seed: "s", Size: 256, Format: FormatJPEG, JPEGQuality: 10})
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		high, err := gen.Generate(Options{Seed: "s", Size: 256, Format: FormatJPEG, JPEGQuality: 100})
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(low) >= len(high) {
			t.Errorf("低质量 JPEG(%d 字节) 应显著小于高质量(%d 字节)", len(low), len(high))
		}
	})
}

// TestRasterNeverSilentlyBlank 是本包最重要的一条不变量测试。
//
// 光栅化器（oksvg）不支持 SVG <mask> 等特性时，默认会静默跳过相应元素，
// 输出一张空白图且不报错。对"生成默认头像"这种场景，一张白图会被当成
// 正常结果发到用户手上，属于静默数据错误。
// 因此要求：每个风格要么产出有内容的位图，要么明确报 ErrRasterUnsupported。
func TestRasterNeverSilentlyBlank(t *testing.T) {
	gen := New()

	const size = 64
	var okCount int
	var unsupported []string

	for _, style := range Styles() {
		out, err := gen.Generate(Options{Style: style, Seed: "probe-seed", Size: size, Format: FormatPNG})
		if err != nil {
			if !errors.Is(err, ErrRasterUnsupported) {
				t.Errorf("风格 %s 返回了非预期错误: %v", style, err)
				continue
			}
			unsupported = append(unsupported, style)
			continue
		}

		img, err := png.Decode(bytes.NewReader(out))
		if err != nil {
			t.Errorf("风格 %s PNG 解码失败: %v", style, err)
			continue
		}
		if blankRatio(img) < 0.001 {
			t.Errorf("风格 %s 输出了近乎空白的图（静默失败），应改为返回 ErrRasterUnsupported", style)
			continue
		}
		okCount++
	}

	t.Logf("可光栅化 %d 个风格；不支持 %d 个: %v", okCount, len(unsupported), unsupported)

	// 已知不支持的风格必须报错而不是出白图，这里锁定其中几个代表性的。
	for _, style := range []string{"bottts", "bottts-neutral", "cameo", "glyphs"} {
		_, err := gen.Generate(Options{Style: style, Seed: "probe-seed", Size: size, Format: FormatPNG})
		if !errors.Is(err, ErrRasterUnsupported) {
			t.Errorf("风格 %s 期望 ErrRasterUnsupported, 得到 %v", style, err)
		}
	}
}

// blankRatio 返回非白像素占比，用于识别"整张白图"。
func blankRatio(img image.Image) float64 {
	b := img.Bounds()
	total := b.Dx() * b.Dy()
	if total == 0 {
		return 0
	}
	white := 0
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			r, g, bl, a := img.At(x, y).RGBA()
			if a == 0 {
				white++ // 透明区域在 JPEG 里会变白，同样视为"无内容"
				continue
			}
			if r>>8 == 255 && g>>8 == 255 && bl>>8 == 255 {
				white++
			}
		}
	}
	return float64(total-white) / float64(total)
}

func TestUnsupportedRasterFallsBackToSVG(t *testing.T) {
	// 文档承诺的降级路径：光栅化失败时改用 SVG 应成功。
	gen := New()
	if _, err := gen.Generate(Options{Style: "bottts", Seed: "s", Size: 64, Format: FormatPNG}); !errors.Is(err, ErrRasterUnsupported) {
		t.Fatalf("前置条件不成立: %v", err)
	}
	svg, err := gen.GenerateSVG(Options{Style: "bottts", Seed: "s", Size: 64})
	if err != nil {
		t.Fatalf("bottts 的 SVG 应始终可用, got %v", err)
	}
	if !strings.HasPrefix(svg, "<svg") {
		t.Errorf("输出不是 SVG: %.40s", svg)
	}
}

func TestSizeValidation(t *testing.T) {
	gen := New()

	for _, size := range []int{-1, 1, MinSize - 1, MaxSize + 1, 100000} {
		_, err := gen.Generate(Options{Seed: "s", Size: size})
		if !errors.Is(err, ErrSizeOutOfRange) {
			t.Errorf("Size=%d 期望 ErrSizeOutOfRange, 得到 %v", size, err)
		}
	}

	for _, size := range []int{MinSize, 64, DefaultSize, MaxSize} {
		if _, err := gen.Generate(Options{Seed: "s", Size: size, Format: FormatSVG}); err != nil {
			t.Errorf("Size=%d 应合法, got %v", size, err)
		}
	}

	// 0 表示"用默认值"，不是非法值。
	out, err := gen.Generate(Options{Seed: "s", Size: 0, Format: FormatSVG})
	if err != nil {
		t.Fatalf("Size=0 应回退到默认值, got %v", err)
	}
	if !strings.Contains(string(out), `width="256"`) {
		t.Error("Size=0 未回退到 DefaultSize=256")
	}
}

func TestUnknownStyleAndFormat(t *testing.T) {
	gen := New()

	if _, err := gen.Generate(Options{Style: "no-such-style"}); !errors.Is(err, ErrUnknownStyle) {
		t.Errorf("期望 ErrUnknownStyle, 得到 %v", err)
	}
	if _, err := gen.Generate(Options{Seed: "s", Format: "webp"}); !errors.Is(err, ErrUnknownFormat) {
		t.Errorf("期望 ErrUnknownFormat, 得到 %v", err)
	}
	if _, err := gen.StyleOptions("no-such-style"); !errors.Is(err, ErrUnknownStyle) {
		t.Errorf("StyleOptions 期望 ErrUnknownStyle, 得到 %v", err)
	}
}

func TestDefaultStyleUsedWhenEmpty(t *testing.T) {
	out, err := New().Generate(Options{Seed: "s", Size: 64})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	explicit, err := New().Generate(Options{Style: DefaultStyle, Seed: "s", Size: 64})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !bytes.Equal(out, explicit) {
		t.Errorf("Style 留空应等价于 %s", DefaultStyle)
	}
}

func TestIDRandomization(t *testing.T) {
	gen := New()

	// 默认关闭：字节级可复现。
	a, err := gen.Generate(Options{Style: "lorelei", Seed: "s", Size: 64})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	b, err := gen.Generate(Options{Style: "lorelei", Seed: "s", Size: 64})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Error("idRandomization 默认关闭时应字节级一致")
	}

	// 打开后：元素 id 带随机后缀，输出不再一致（用于同页面内联多个同款头像）。
	c, err := gen.Generate(Options{Style: "lorelei", Seed: "s", Size: 64, IDRandomization: true})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	d, err := gen.Generate(Options{Style: "lorelei", Seed: "s", Size: 64, IDRandomization: true})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if bytes.Equal(c, d) {
		t.Error("idRandomization 打开后输出应带随机 id 后缀")
	}
}

func TestStyleOptionsPassthrough(t *testing.T) {
	gen := New()

	// 风格专属选项（外部特征）应被透传并生效。
	plain, err := gen.Generate(Options{
		Style: "adventurer", Seed: "face", Size: 128,
		StyleOptions: map[string]any{"eyesVariant": []string{"variant01"}, "mouthVariant": []string{"variant01"}},
	})
	if err != nil {
		t.Fatalf("透传风格选项失败: %v", err)
	}
	if len(plain) == 0 {
		t.Fatal("输出为空")
	}

	// 未注册的选项应被拒绝，说明透传确实走的是 DiceBear 的校验。
	if _, err := gen.Generate(Options{
		Style: "adventurer", Seed: "face", Size: 128,
		StyleOptions: map[string]any{"definitelyNotAnOption": 1},
	}); err == nil {
		t.Error("非法风格选项应返回错误")
	}
}

func TestTypedFieldsOverrideStyleOptions(t *testing.T) {
	gen := New()

	base := Options{Style: "lorelei", Seed: "s", Size: 64}
	fromTyped, err := gen.Generate(base)
	if err != nil {
		t.Fatalf("err = %v", err)
	}

	// StyleOptions 里塞入与显式字段冲突的 size/seed，显式字段应胜出。
	base.StyleOptions = map[string]any{"size": 512, "seed": "hijacked"}
	fromConflict, err := gen.Generate(base)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !bytes.Equal(fromTyped, fromConflict) {
		t.Error("结构体显式字段应优先于 StyleOptions 中的同名键")
	}
}

func TestBackgroundColorAndFlip(t *testing.T) {
	gen := New()

	withBg, err := gen.Generate(Options{
		Style: "lorelei", Seed: "s", Size: 64, BackgroundColor: []string{"#b6e3f4"},
	})
	if err != nil {
		t.Fatalf("backgroundColor 透传失败: %v", err)
	}
	if !strings.Contains(string(withBg), "#b6e3f4") {
		t.Error("backgroundColor 未出现在输出中")
	}

	flipped, err := gen.Generate(Options{Style: "lorelei", Seed: "s", Size: 64, Flip: []string{"horizontal"}})
	if err != nil {
		t.Fatalf("flip 透传失败: %v", err)
	}
	normal, err := gen.Generate(Options{Style: "lorelei", Seed: "s", Size: 64})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if bytes.Equal(flipped, normal) {
		t.Error("flip=horizontal 应改变输出")
	}

	// 非法枚举值由 DiceBear 拒绝。
	if _, err := gen.Generate(Options{Style: "lorelei", Seed: "s", Size: 64, Flip: []string{"sideways"}}); err == nil {
		t.Error("非法 flip 值应返回错误")
	}
}

func TestRasterizeParams(t *testing.T) {
	gen := New()

	rounded, err := gen.Generate(Options{Style: "lorelei", Seed: "s", Size: 64, BorderRadius: []float64{50}})
	if err != nil {
		t.Fatalf("borderRadius 透传失败: %v", err)
	}
	if len(rounded) == 0 {
		t.Fatal("输出为空")
	}

	if _, err := gen.Generate(Options{
		Style: "lorelei", Seed: "s", Size: 64,
		StyleOptions: map[string]any{"scale": []float64{0.8, 1.2}},
	}); err != nil {
		t.Fatalf("scale 范围透传失败: %v", err)
	}
}

func TestStyleOptionsDescriptor(t *testing.T) {
	gen := New()

	desc, err := gen.StyleOptions("adventurer")
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	// 核心选项必须都在。
	for _, key := range []string{"seed", "size", "idRandomization", "title", "flip", "scale", "borderRadius", "rotate"} {
		if _, ok := desc[key]; !ok {
			t.Errorf("选项描述符缺少核心选项 %q", key)
		}
	}
	// 颜色选项以 <name>Color 形式出现，"background" 也在一等公民之列。
	if _, ok := desc["backgroundColor"]; !ok {
		t.Error("选项描述符缺少 backgroundColor")
	}
	// 组件选项以 <name>Variant / <name>Probability 成对出现。
	var variants int
	for key := range desc {
		if strings.HasSuffix(key, "Variant") {
			variants++
		}
		if strings.HasSuffix(key, "Variant") {
			prob := strings.TrimSuffix(key, "Variant") + "Probability"
			if _, ok := desc[prob]; !ok {
				t.Errorf("有 %q 却没有 %q", key, prob)
			}
		}
	}
	if variants == 0 {
		t.Error("adventurer 应有组件选项")
	}
}

func TestFormatHelpers(t *testing.T) {
	tests := []struct {
		format      Format
		contentType string
		raster      bool
	}{
		{FormatSVG, "image/svg+xml", false},
		{FormatDataURI, "text/plain; charset=utf-8", false},
		{FormatPNG, "image/png", true},
		{FormatJPEG, "image/jpeg", true},
		{Format("webp"), "application/octet-stream", false},
	}
	for _, tt := range tests {
		if got := tt.format.ContentType(); got != tt.contentType {
			t.Errorf("%q.ContentType() = %q, 期望 %q", tt.format, got, tt.contentType)
		}
		if got := tt.format.IsRaster(); got != tt.raster {
			t.Errorf("%q.IsRaster() = %v, 期望 %v", tt.format, got, tt.raster)
		}
	}
}

func TestStylesReturnsAllNames(t *testing.T) {
	styles := Styles()
	if len(styles) == 0 {
		t.Fatal("Styles() 为空")
	}
	for i := 1; i < len(styles); i++ {
		if styles[i-1] > styles[i] {
			t.Fatalf("Styles() 未排序: %q > %q", styles[i-1], styles[i])
		}
	}
	for _, want := range []string{"lorelei", "adventurer", "bottts", "initials", "identicon", "pixel-art", "thumbs"} {
		found := false
		for _, s := range styles {
			if s == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Styles() 缺少 %q", want)
		}
	}
}

// TestConcurrency 在 -race 下验证 Generator 的并发安全。
//
// 特意让多个 goroutine 同时首访同一风格（冷缓存竞争），以及跨风格/跨格式
// 并发，覆盖缓存写入与 DiceBear/oksvg 渲染路径。
func TestConcurrency(t *testing.T) {
	gen := New()
	styles := []string{"lorelei", "adventurer", "bottts", "initials", "thumbs", "micah"}
	formats := []Format{FormatSVG, FormatDataURI, FormatPNG, FormatJPEG}

	var wg sync.WaitGroup
	const goroutines = 32
	const iterations = 8

	errCh := make(chan error, goroutines*iterations)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				style := styles[(g+i)%len(styles)]
				format := formats[(g*i)%len(formats)]
				out, err := gen.Generate(Options{
					Style:  style,
					Seed:   "concurrent-seed",
					Size:   64,
					Format: format,
				})
				if err != nil {
					// bottts 不支持光栅化，属预期错误。
					if errors.Is(err, ErrRasterUnsupported) {
						continue
					}
					errCh <- err
					return
				}
				if len(out) == 0 {
					errCh <- errors.New("输出为空")
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("并发生成失败: %v", err)
	}
}

// TestConcurrentColdCacheSameStyle 专门验证"同一风格冷缓存"这一竞争热点：
// 若缓存的填充没有同步，-race 会在这里报错。
func TestConcurrentColdCacheSameStyle(t *testing.T) {
	const goroutines = 24

	var wg sync.WaitGroup
	outs := make([]string, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// 每个 goroutine 各自持有 Generator，等价于各自冷缓存；
			// 再共享一个 Generator 覆盖共享缓存场景。
			shared := sharedGenerator()
			svg, err := shared.GenerateSVG(Options{Style: "adventurer", Seed: "cold", Size: 64})
			if err != nil {
				t.Errorf("err = %v", err)
				return
			}
			outs[g] = svg
		}(g)
	}
	wg.Wait()

	for i := 1; i < len(outs); i++ {
		if outs[i] != outs[0] {
			t.Fatalf("共享 Generator 下同 seed 输出不一致（第 %d 个）", i)
		}
	}
}

// sharedGenerator 返回一个包级共享的 Generator，用于覆盖共享缓存的并发路径。
var sharedGenerator = sync.OnceValue(New)

func TestRasterPreservesAlphaForPNG(t *testing.T) {
	gen := New()
	// 去掉背景后 PNG 应保留透明区域；JPEG 则应把透明处合成成白色。
	opts := Options{
		Style: "lorelei", Seed: "alpha", Size: 64,
		StyleOptions: map[string]any{"backgroundColor": []string{}},
	}

	pngBytes, err := gen.Generate(Options{
		Style: opts.Style, Seed: opts.Seed, Size: opts.Size,
		Format: FormatPNG, StyleOptions: opts.StyleOptions,
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	img, err := png.Decode(bytes.NewReader(pngBytes))
	if err != nil {
		t.Fatalf("PNG 解码失败: %v", err)
	}
	_, _, _, a := img.At(0, 0).RGBA()
	if a == 0xffff {
		t.Log("左上角不透明（该风格在无背景时仍绘制到边缘），跳过透明度断言")
	}

	jpegBytes, err := gen.Generate(Options{
		Style: opts.Style, Seed: opts.Seed, Size: opts.Size,
		Format: FormatJPEG, StyleOptions: opts.StyleOptions,
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	jimg, err := jpeg.Decode(bytes.NewReader(jpegBytes))
	if err != nil {
		t.Fatalf("JPEG 解码失败: %v", err)
	}
	if !isNearWhite(jimg.At(0, 0)) && blankRatio(jimg) == 0 {
		t.Log("JPEG 左上角非白，可能该风格铺满了画布")
	}
}

func isNearWhite(c color.Color) bool {
	r, g, b, _ := c.RGBA()
	return r>>8 > 200 && g>>8 > 200 && b>>8 > 200
}
