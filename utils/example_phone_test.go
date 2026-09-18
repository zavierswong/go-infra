package utils_test

import (
	"fmt"
	"log"

	"github.com/zavierswong/go-infra/utils"
)

// ExampleParsePhone 解析中国手机号：一个输入，多种标准形态。
func ExampleParsePhone() {
	p, err := utils.ParsePhone("138 0013 8000", "CN")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(p.E164)          // 入库/比对用这个
	fmt.Println(p.International) // 展示用
	fmt.Println(p.Region, p.Type)
	// Output:
	// +8613800138000
	// +86 138 0013 8000
	// CN mobile
}

// ExampleParsePhone_global 全球地区：第二个参数是"号码没带 + 前缀时按哪个地区解析"。
func ExampleParsePhone_global() {
	for _, in := range []struct{ raw, region string }{
		{"13800138000", "CN"},
		{"(415) 555-2671", "US"},
		{"07400 123456", "GB"},
		{"03-1234-5678", "JP"},
		{"8 (912) 345-67-89", "RU"},
	} {
		e164, err := utils.NormalizePhone(in.raw, in.region)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(e164)
	}
	// Output:
	// +8613800138000
	// +14155552671
	// +447400123456
	// +81312345678
	// +79123456789
}

// ExampleIsValidPhone 表单校验：解析失败也算非法，不返回错误。
func ExampleIsValidPhone() {
	fmt.Println(utils.IsValidPhone("13800138000", "CN"))
	// 没有默认地区又没写 + 前缀，无法确定国家码
	fmt.Println(utils.IsValidPhone("13800138000", ""))
	fmt.Println(utils.IsValidPhone("not-a-phone", "CN"))
	// Output:
	// true
	// false
	// false
}

// ExampleMaskPhone 日志脱敏：保留国家码与首尾，中间打码。
func ExampleMaskPhone() {
	fmt.Println(utils.MaskPhone("13800138000", "CN"))
	fmt.Println(utils.MaskPhone("+14155552671", ""))

	// 保留位数超过号码长度时自动压缩，保证至少打码 1 位
	fmt.Println(utils.MaskPhoneWith("12345", "CN", 3, 4))

	// 完全无法解析的输入拿不到国家码，退化为纯数字打码，仍然不泄漏原文
	fmt.Println(utils.MaskPhone("1", "CN"))
	// Output:
	// +86138****8000
	// +1415***2671
	// +86*2345
	// *
}

// ExamplePhoneRegion 从号码反查归属地区，号码自带 + 前缀时无需默认地区。
func ExamplePhoneRegion() {
	region, err := utils.PhoneRegion("+81-3-1234-5678", "")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(region)
	// Output:
	// JP
}
