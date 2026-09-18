package avatar_test

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/zavierswong/go-infra/avatar"
)

// Example 用固定 seed 生成可复现的 SVG 头像。
func Example() {
	gen := avatar.New()

	svg, err := gen.GenerateSVG(avatar.Options{
		Style: "lorelei",
		Seed:  "user-42", // 同 seed 永远得到同一个头像
		Size:  128,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(len(svg) > 0) // true
}

// ExampleGenerator_Generate_png 随机 seed + PNG 输出，适合注册时分配默认头像。
func ExampleGenerator_Generate_png() {
	gen := avatar.New()

	out, err := gen.Generate(avatar.Options{
		Size:   256,
		Format: avatar.FormatPNG,
		// Seed 留空 -> 每次都是随机头像
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(out[1] == 'P' && out[2] == 'N' && out[3] == 'G') // true（PNG 魔数）
	fmt.Println(avatar.FormatPNG.ContentType())                  // image/png
	// Output:
	// true
	// image/png
}

// ExampleGenerator_Generate_dataURI DataURI 可直接用于 <img src>，省一次请求。
func ExampleGenerator_Generate_dataURI() {
	gen := avatar.New()

	out, err := gen.Generate(avatar.Options{Seed: "user-42", Format: avatar.FormatDataURI})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(strings.HasPrefix(string(out), "data:image/svg+xml;charset=utf-8,"))
	// Output:
	// true
}

// ExampleGenerator_Generate_rasterFallback 光栅化失败时降级到 SVG。
//
// 少数风格使用了 SVG <mask>，Go 版光栅化器不支持；此时宁可报错，
// 也不静默输出一张白图。
func ExampleGenerator_Generate_rasterFallback() {
	gen := avatar.New()

	_, err := gen.Generate(avatar.Options{Style: "bottts", Format: avatar.FormatPNG})
	if errors.Is(err, avatar.ErrRasterUnsupported) {
		// 降级：改用 SVG（所有风格都支持），或换一个不使用遮罩的风格。
		svg, svgErr := gen.GenerateSVG(avatar.Options{Style: "bottts"})
		if svgErr != nil {
			log.Fatal(svgErr)
		}
		fmt.Println(len(svg) > 0) // true
		return
	}
	if err != nil {
		log.Fatal(err)
	}
}

// ExampleGenerator_StyleOptions 风格专属选项，以及如何查它的合法取值。
func ExampleGenerator_StyleOptions() {
	gen := avatar.New()

	// 先问这个风格支持哪些选项（适合给管理后台生成表单）。
	desc, err := gen.StyleOptions("adventurer")
	if err != nil {
		log.Fatal(err)
	}
	if _, ok := desc["eyesVariant"]; ok {
		fmt.Println("adventurer 支持 eyesVariant")
	}

	// 再按名字传参。
	out, err := gen.Generate(avatar.Options{
		Style: "adventurer",
		Seed:  "user-42",
		Size:  128,
		StyleOptions: map[string]any{
			"eyesVariant":  []string{"variant01"},
			"mouthVariant": []string{"variant01"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(out) > 0) // true
	// Output:
	// adventurer 支持 eyesVariant
	// true
}

// ExampleFormat_ContentType 写 HTTP 响应时直接取 Content-Type。
func ExampleFormat_ContentType() {
	fmt.Println(avatar.FormatSVG.ContentType())  // image/svg+xml
	fmt.Println(avatar.FormatJPEG.ContentType()) // image/jpeg
	fmt.Println(avatar.FormatJPEG.IsRaster())    // true
	// Output:
	// image/svg+xml
	// image/jpeg
	// true
}
