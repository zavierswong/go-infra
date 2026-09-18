package utils

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestParsePhoneGlobalRegions(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		region  string
		e164    string
		region2 string
		ctype   PhoneType
	}{
		{"中国大陆手机号", "138 0013 8000", "CN", "+8613800138000", "CN", PhoneTypeMobile},
		{"中国国际前缀", "+86 138-0013-8000", "", "+8613800138000", "CN", PhoneTypeMobile},
		{"中国固定电话", "010-6552 8888", "CN", "+861065528888", "CN", PhoneTypeFixedLine},
		{"美国手机号", "(415) 555-2671", "US", "+14155552671", "US", PhoneTypeFixedOrMobile},
		{"美国带国际前缀", "+1 202 456 1111", "", "+12024561111", "US", PhoneTypeFixedOrMobile},
		{"英国手机号", "07400 123456", "GB", "+447400123456", "GB", PhoneTypeMobile},
		{"日本固定电话", "03-1234-5678", "JP", "+81312345678", "JP", PhoneTypeFixedLine},
		{"德国固定电话", "030 123456", "DE", "+4930123456", "DE", PhoneTypeFixedLine},
		{"印度手机号", "09999999999", "IN", "+919999999999", "IN", PhoneTypeMobile},
		{"巴西手机号", "11 91234-5678", "BR", "+5511912345678", "BR", PhoneTypeMobile},
		{"新加坡手机号", "8123 4567", "SG", "+6581234567", "SG", PhoneTypeMobile},
		{"俄罗斯手机号", "8 (912) 345-67-89", "RU", "+79123456789", "RU", PhoneTypeMobile},
		{"中国香港手机号", "6123 4567", "HK", "+85261234567", "HK", PhoneTypeMobile},
		{"中国台湾手机号", "0912 345 678", "TW", "+886912345678", "TW", PhoneTypeMobile},
		{"澳大利亚手机号", "0412 345 678", "AU", "+61412345678", "AU", PhoneTypeMobile},
		{"法国手机号", "06 12 34 56 78", "FR", "+33612345678", "FR", PhoneTypeMobile},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := ParsePhone(tt.raw, tt.region)
			if err != nil {
				t.Fatalf("ParsePhone(%q, %q) 返回错误: %v", tt.raw, tt.region, err)
			}
			if p.E164 != tt.e164 {
				t.Errorf("E164 = %q, 期望 %q", p.E164, tt.e164)
			}
			if p.Region != tt.region2 {
				t.Errorf("Region = %q, 期望 %q", p.Region, tt.region2)
			}
			if p.Type != tt.ctype {
				t.Errorf("Type = %q, 期望 %q", p.Type, tt.ctype)
			}
			if !p.Valid {
				t.Errorf("Valid = false, 期望 true（%q 在 %s 应为合法号码）", tt.raw, tt.region2)
			}
			if !p.Possible {
				t.Errorf("Possible = false, 期望 true")
			}
			if p.Raw != tt.raw {
				t.Errorf("Raw = %q, 期望 %q", p.Raw, tt.raw)
			}
			if p.CountryCode == 0 {
				t.Errorf("CountryCode = 0, 期望非零")
			}
			if p.National == "" || p.International == "" || p.RFC3966 == "" {
				t.Errorf("格式化字段不应为空: National=%q International=%q RFC3966=%q",
					p.National, p.International, p.RFC3966)
			}
		})
	}
}

func TestParsePhoneDefaultRegionRequiredWithoutPlus(t *testing.T) {
	// 没有默认地区时，纯本地格式无法确定国家码 -> 必须报错。
	_, err := ParsePhone("13800138000", "")
	if err == nil {
		t.Fatal("缺少默认地区且无 + 前缀时应返回错误")
	}

	// 同一输入在给定默认地区后应成功。
	p, err := ParsePhone("13800138000", "CN")
	if err != nil {
		t.Fatalf("带默认地区 CN 应成功, got err=%v", err)
	}
	if p.E164 != "+8613800138000" {
		t.Errorf("E164 = %q", p.E164)
	}
}

func TestParsePhoneRegionCaseInsensitive(t *testing.T) {
	lower, err := ParsePhone("13800138000", "cn")
	if err != nil {
		t.Fatalf("小写地区码应可用: %v", err)
	}
	upper, err := ParsePhone("13800138000", "CN")
	if err != nil {
		t.Fatalf("大写地区码应可用: %v", err)
	}
	if lower.E164 != upper.E164 || lower.Region != upper.Region {
		t.Errorf("大小写地区码结果不一致: %+v vs %+v", lower, upper)
	}
}

func TestParsePhoneErrors(t *testing.T) {
	t.Run("空输入", func(t *testing.T) {
		for _, raw := range []string{"", "   ", "\t"} {
			_, err := ParsePhone(raw, "CN")
			if !errors.Is(err, ErrPhoneEmpty) {
				t.Errorf("ParsePhone(%q) err = %v, 期望 ErrPhoneEmpty", raw, err)
			}
		}
	})

	t.Run("未知默认地区", func(t *testing.T) {
		_, err := ParsePhone("13800138000", "XX")
		if !errors.Is(err, ErrPhoneUnknownRegion) {
			t.Errorf("err = %v, 期望 ErrPhoneUnknownRegion", err)
		}
	})

	t.Run("垃圾输入", func(t *testing.T) {
		if _, err := ParsePhone("not-a-phone", "CN"); err == nil {
			t.Error("非号码文本应返回错误")
		}
	})

	t.Run("ZZ 等价于无默认地区", func(t *testing.T) {
		_, err := ParsePhone("13800138000", "ZZ")
		if err == nil {
			t.Error("ZZ 表示未知地区，无 + 前缀时应报错")
		}
	})
}

func TestParsePhoneInvalidButParseable(t *testing.T) {
	// 中国手机号段不存在 19999999999 之外，这里用明显非法的前缀：
	// 解析能拿到国家码，但 Valid 应为 false，且不返回 error。
	p, err := ParsePhone("+86 100 0000 0000", "")
	if err != nil {
		t.Fatalf("应能解析出国家码, got err=%v", err)
	}
	if p.CountryCode != 86 {
		t.Errorf("CountryCode = %d, 期望 86", p.CountryCode)
	}
	if p.Valid {
		t.Error("非法号码 Valid 应为 false")
	}
}

func TestNormalizePhone(t *testing.T) {
	tests := []struct {
		raw    string
		region string
		want   string
	}{
		{"138 0013 8000", "CN", "+8613800138000"},
		{"+86 (138) 0013-8000", "", "+8613800138000"},
		{"(202) 456-1111", "US", "+12024561111"},
		{"+81-3-1234-5678", "", "+81312345678"},
		{"020 7946 0958", "GB", "+442079460958"},
	}
	for _, tt := range tests {
		got, err := NormalizePhone(tt.raw, tt.region)
		if err != nil {
			t.Fatalf("NormalizePhone(%q, %q) err=%v", tt.raw, tt.region, err)
		}
		if got != tt.want {
			t.Errorf("NormalizePhone(%q, %q) = %q, 期望 %q", tt.raw, tt.region, got, tt.want)
		}
	}
}

func TestIsValidPhone(t *testing.T) {
	if !IsValidPhone("13800138000", "CN") {
		t.Error("13800138000 在 CN 应合法")
	}
	if IsValidPhone("13800138000", "") {
		t.Error("无默认地区时应视为非法（解析失败）")
	}
	if IsValidPhone("abc", "CN") {
		t.Error("垃圾输入应视为非法")
	}
	if IsValidPhone("", "CN") {
		t.Error("空输入应视为非法")
	}
}

func TestPhoneRegion(t *testing.T) {
	tests := []struct {
		raw    string
		region string
		want   string
	}{
		{"13800138000", "CN", "CN"},
		{"+14155552671", "", "US"},
		{"+819012345678", "", "JP"},
		{"+8613800138000", "", "CN"},
	}
	for _, tt := range tests {
		got, err := PhoneRegion(tt.raw, tt.region)
		if err != nil {
			t.Fatalf("PhoneRegion(%q, %q) err=%v", tt.raw, tt.region, err)
		}
		if got != tt.want {
			t.Errorf("PhoneRegion(%q, %q) = %q, 期望 %q", tt.raw, tt.region, got, tt.want)
		}
	}
}

func TestMaskPhone(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		region string
		want   string
	}{
		{"中国手机号", "13800138000", "CN", "+86138****8000"},
		{"中国手机号带分隔", "138 0013 8000", "CN", "+86138****8000"},
		{"美国号码", "+14155552671", "", "+1415***2671"},
		{"英国号码", "07400123456", "GB", "+44740***3456"},
		{"日本号码", "03-1234-5678", "JP", "+81312**5678"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MaskPhone(tt.raw, tt.region)
			if got != tt.want {
				t.Errorf("MaskPhone(%q, %q) = %q, 期望 %q", tt.raw, tt.region, got, tt.want)
			}
		})
	}
}

func TestMaskPhoneNeverLeaksFullNumber(t *testing.T) {
	// 只要输入里至少含 1 位数字，输出就必须含有打码字符。
	// 这条不变量是脱敏工具的安全底线：短号、非法号、无默认地区号都必须打码。
	inputs := []struct{ raw, region string }{
		{"1", "CN"},
		{"12", "CN"},
		{"123", "CN"},
		{"+8613800138000", ""},
		{"12345678", "CN"},
		{"13800138000", ""},
		{"+41 44 668 1800", ""},
		{"999999999999999999", "CN"},
		{"0", "US"},
	}
	for _, in := range inputs {
		got := MaskPhone(in.raw, in.region)
		if !strings.Contains(got, "*") {
			t.Errorf("MaskPhone(%q, %q) = %q, 未包含打码字符", in.raw, in.region, got)
		}
	}

	// 无数字输入：无可脱敏内容，返回空串（同样不泄漏）。
	for _, raw := range []string{"", "   ", "no-digits-here"} {
		if got := MaskPhone(raw, "CN"); got != "" {
			t.Errorf("MaskPhone(%q, %q) = %q, 期望空串", raw, "CN", got)
		}
	}
}

func TestMaskPhoneWithCustomRatio(t *testing.T) {
	tests := []struct {
		name             string
		raw              string
		region           string
		keepPre, keepSuf int
		want             string
	}{
		{"只留后 4 位", "13800138000", "CN", 0, 4, "+86*******8000"},
		{"只留前 3 位", "13800138000", "CN", 3, 0, "+86138********"},
		{"全打码", "13800138000", "CN", 0, 0, "+86***********"},
		{"负数视为 0", "13800138000", "CN", -5, -5, "+86***********"},
		// 保留位数之和超过号码长度时，先压 suffix 再压 prefix，保证至少 1 位打码。
		{"短号保留位数超限", "12345", "CN", 3, 4, "+86*2345"},
		// 美国 10 位号码：+1 后 10 位，默认 3+4 留出 3 位打码。
		{"美国默认", "4155552671", "US", 3, 4, "+1415***2671"},
		{"美国多留后缀", "4155552671", "US", 0, 6, "+1****552671"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MaskPhoneWith(tt.raw, tt.region, tt.keepPre, tt.keepSuf)
			if got != tt.want {
				t.Errorf("MaskPhoneWith(%q,%q,%d,%d) = %q, 期望 %q",
					tt.raw, tt.region, tt.keepPre, tt.keepSuf, got, tt.want)
			}
		})
	}
}

func TestSupportedPhoneRegionsReturnsCopy(t *testing.T) {
	first := SupportedPhoneRegions()
	if len(first) < 200 {
		t.Fatalf("支持地区数 = %d, 预期 200+", len(first))
	}
	for i := 1; i < len(first); i++ {
		if first[i-1] > first[i] {
			t.Fatalf("结果未排序: %q > %q", first[i-1], first[i])
		}
	}
	for _, want := range []string{"CN", "US", "GB", "JP", "DE", "IN", "BR", "SG", "HK", "TW"} {
		if !contains(first, want) {
			t.Errorf("缺少地区 %q", want)
		}
	}

	// 篡改返回值不得影响后续调用（验证返回的是副本）。
	first[0] = "ZZ"
	second := SupportedPhoneRegions()
	if second[0] == "ZZ" {
		t.Error("SupportedPhoneRegions 返回了内部 map，调用方可污染全局元数据")
	}
}

// TestPhoneConcurrency 在 -race 下验证并发调用无数据竞态。
// phonenumbers 内部有正则缓存，首次解析不同地区时会触发并发写入，
// 这里刻意让多个 goroutine 解析全不同的地区以覆盖该路径。
func TestPhoneConcurrency(t *testing.T) {
	regions := []string{"CN", "US", "GB", "JP", "DE", "FR", "IN", "BR", "SG", "HK", "TW", "AU", "RU", "ES", "IT"}
	samples := map[string]string{
		"CN": "13800138000", "US": "4155552671", "GB": "07400123456",
		"JP": "0312345678", "DE": "030123456", "FR": "0612345678",
		"IN": "9999999999", "BR": "11912345678", "SG": "81234567",
		"HK": "61234567", "TW": "912345678", "AU": "0412345678",
		"RU": "89123456789", "ES": "612345678", "IT": "3123456789",
	}

	const goroutines = 32
	const iterations = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				region := regions[(g+i)%len(regions)]
				raw := samples[region]
				p, err := ParsePhone(raw, region)
				if err != nil {
					t.Errorf("ParsePhone(%q, %q) err=%v", raw, region, err)
					return
				}
				if p.E164 == "" {
					t.Errorf("ParsePhone(%q, %q) 得到空 E164", raw, region)
					return
				}
				_ = MaskPhone(raw, region)
				_ = SupportedPhoneRegions()
			}
		}(g)
	}
	wg.Wait()
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
