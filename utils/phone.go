package utils

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/nyaruka/phonenumbers"
)

// 本文件封装手机号/电话号码的解析与格式化，底层是 Google libphonenumber
// 的 Go 移植（github.com/nyaruka/phonenumbers），覆盖全球 240+ 地区的
// 号码长度、前缀、运营商与号码类型规则。
//
// 并发安全：phonenumbers 在包初始化时一次性加载只读元数据，运行期唯一的
// 可变状态是一张受 sync.RWMutex 保护的正则缓存（见其 regexCache），因此
// 本文件的函数均为纯函数语义，可无锁并发调用。
// 注意：phonenumbers 的 GetSupportedRegions 等接口直接返回内部 map，
// 本包一律返回副本，避免调用方写入导致元数据被污染。

// PhoneType 是号码类型，取值语义与 libphonenumber 的 PhoneNumberType 一致。
//
// 使用字符串而非 iota，便于直接写入日志/JSON 而不必再做映射。
type PhoneType string

const (
	PhoneTypeFixedLine      PhoneType = "fixed_line"
	PhoneTypeMobile         PhoneType = "mobile"
	PhoneTypeFixedOrMobile  PhoneType = "fixed_line_or_mobile"
	PhoneTypeTollFree       PhoneType = "toll_free"
	PhoneTypePremiumRate    PhoneType = "premium_rate"
	PhoneTypeSharedCost     PhoneType = "shared_cost"
	PhoneTypeVoIP           PhoneType = "voip"
	PhoneTypePersonalNumber PhoneType = "personal_number"
	PhoneTypePager          PhoneType = "pager"
	PhoneTypeUAN            PhoneType = "uan"
	PhoneTypeVoicemail      PhoneType = "voicemail"
	PhoneTypeUnknown        PhoneType = "unknown"
)

// Phone 是一个已解析的电话号码。
//
// 各格式化字段由 libphonenumber 按地区规则生成，E164 是跨国存储与比对
// 的推荐形态。
type Phone struct {
	// Raw 是原始输入，便于出错时回溯。
	Raw string
	// E164 形如 +8613800138000，无分隔符，适合入库与跨系统传递。
	E164 string
	// National 形如 138 0013 8000（按归属地区的本地习惯分组）。
	National string
	// International 形如 +86 138 0013 8000。
	International string
	// RFC3966 形如 tel:+86-138-0013-8000，带分机时为 tel:...;ext=123。
	RFC3966 string
	// CountryCode 是国家码，如中国为 86。
	CountryCode int
	// Region 是号码归属地区，ISO 3166-1 alpha-2 大写，如 CN；无法判定时为 "ZZ"。
	Region string
	// Type 是号码类型（手机/固话/虚拟号等）。
	Type PhoneType
	// Extension 是分机号，无分机时为空串。
	Extension string
	// Valid 表示号码在该地区的号码计划中确实存在（长度、前缀均合法）。
	Valid bool
	// Possible 表示号码长度符合地区规则，但前缀可能仍无效。
	// 即 Valid => Possible，反向不成立。
	Possible bool
}

// IsMobile 报告号码是否可判定为移动电话。
//
// 部分地区（如美国、加拿大）固话与手机号段重叠，libphonenumber 只能给出
// PhoneTypeFixedOrMobile，此时本方法返回 false——不要用它做「非手机号即
// 拒绝」的判定，应先看 Type。
func (p *Phone) IsMobile() bool { return p.Type == PhoneTypeMobile }

// ParsePhone 解析一个电话号码。
//
// defaultRegion 是号码未带国际前缀时采用的默认地区，取 ISO 3166-1
// alpha-2 码（大小写不敏感），如 "CN"、"US"、"GB"；传空串表示没有默认
// 地区，此时号码必须自带 "+国家码" 前缀，否则返回错误。
//
// 输入容忍空格、连字符、括号、点号等常见分隔符，也容忍 IDD 前缀（如
// "00"）。号码超过 250 字符会被拒绝（libphonenumber 的防 ReDoS 上限）。
//
// 返回值在号码非法时也会给出已解析出的字段，Valid 为 false；只有「完全
// 无法解析出国家码」这类情况才返回 error。
func ParsePhone(raw, defaultRegion string) (*Phone, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, ErrPhoneEmpty
	}

	region := normalizeRegion(defaultRegion)
	if region != "" && !isSupportedRegion(region) {
		return nil, fmt.Errorf("%w: %q", ErrPhoneUnknownRegion, defaultRegion)
	}

	num, err := phonenumbers.Parse(raw, region)
	if err != nil {
		return nil, fmt.Errorf("utils: parse phone %q: %w", raw, err)
	}

	return &Phone{
		Raw:           raw,
		E164:          phonenumbers.Format(num, phonenumbers.E164),
		National:      phonenumbers.Format(num, phonenumbers.NATIONAL),
		International: phonenumbers.Format(num, phonenumbers.INTERNATIONAL),
		RFC3966:       phonenumbers.Format(num, phonenumbers.RFC3966),
		CountryCode:   int(num.GetCountryCode()),
		Region:        regionOf(num),
		Type:          phoneType(num),
		Extension:     num.GetExtension(),
		Valid:         phonenumbers.IsValidNumber(num),
		Possible:      phonenumbers.IsPossibleNumber(num),
	}, nil
}

// NormalizePhone 把号码归一化为 E.164 形态，适合入库和跨系统比对。
//
//	utils.NormalizePhone("138 0013 8000", "CN")     // +8613800138000
//	utils.NormalizePhone("(202) 456-1111", "US")    // +12024561111
//	utils.NormalizePhone("+81-3-1234-5678", "")     // +81312345678
func NormalizePhone(raw, defaultRegion string) (string, error) {
	p, err := ParsePhone(raw, defaultRegion)
	if err != nil {
		return "", err
	}
	if p.E164 == "" {
		return "", fmt.Errorf("%w: %q", ErrPhoneNotParseable, raw)
	}
	return p.E164, nil
}

// IsValidPhone 报告号码是否为该地区号码计划中的合法号码。
//
// 解析失败（如缺少默认地区且无国际前缀）时返回 false，不返回错误，
// 便于直接用于表单校验。
func IsValidPhone(raw, defaultRegion string) bool {
	p, err := ParsePhone(raw, defaultRegion)
	return err == nil && p.Valid
}

// PhoneRegion 返回号码归属地区的 ISO 3166-1 alpha-2 码，无法判定时返回 ""。
func PhoneRegion(raw, defaultRegion string) (string, error) {
	p, err := ParsePhone(raw, defaultRegion)
	if err != nil {
		return "", err
	}
	if p.Region == "ZZ" {
		return "", nil
	}
	return p.Region, nil
}

// 手机号脱敏的默认保留位数：前缀 3 位（如中国的号段 138）+ 后缀 4 位。
const (
	defaultPhoneKeepPrefix = 3
	defaultPhoneKeepSuffix = 4
)

// MaskPhone 对号码脱敏，用于日志等不应出现完整号码的场景，
// 与 SanitizeDSN 的设计意图一致。
//
// 保留国家码前缀与「前 3 位 + 后 4 位」，中间以 * 替代：
//
//	utils.MaskPhone("13800138000", "CN")     // +86138****8000
//	utils.MaskPhone("+14155552671", "")      // +1415***2671
//
// 号码无法解析时退化为「保留首尾、中间打码」的字符串处理，绝不返回原文；
// 输入过短（如 "12"）时全量打码，避免脱敏后信息量反而更大。
func MaskPhone(raw, defaultRegion string) string {
	return MaskPhoneWith(raw, defaultRegion, defaultPhoneKeepPrefix, defaultPhoneKeepSuffix)
}

// MaskPhoneWith 是 MaskPhone 的可定制版本，keepPrefix / keepSuffix 分别
// 指定保留的前缀位数与后缀位数（按国家码之后的号码部分计），负数视为 0。
//
// 核心约束：只要提取到的号码至少含 1 位数字，就必然至少有 1 位被打码。
// 当 keepPrefix+keepSuffix 过大时，先压缩 keepSuffix 至多保留 n-1 位，
// 再压缩 keepPrefix，从而不会出现「脱敏后仍是原号」的情况。
func MaskPhoneWith(raw, defaultRegion string, keepPrefix, keepSuffix int) string {
	if keepPrefix < 0 {
		keepPrefix = 0
	}
	if keepSuffix < 0 {
		keepSuffix = 0
	}

	national, prefix := maskParts(raw, defaultRegion)
	runes := []rune(national)
	n := len(runes)
	if n == 0 {
		// 提取不到数字，没有可脱敏的内容，也不会泄漏号码。
		return prefix
	}

	if keepPrefix+keepSuffix >= n {
		keepSuffix = min(keepSuffix, n-1)
		keepPrefix = min(keepPrefix, n-1-keepSuffix)
	}

	var b strings.Builder
	b.Grow(len(prefix) + n)
	b.WriteString(prefix)
	for i, r := range runes {
		if i < keepPrefix || i >= n-keepSuffix {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('*')
	}
	return b.String()
}

// maskParts 取出「国家码前缀」与「用于打码的号码部分」。
// 解析成功时号码部分取国家码之后的数字；失败时退化为剥掉非数字字符的
// 原文，前缀为空。
func maskParts(raw, defaultRegion string) (national, prefix string) {
	if p, err := ParsePhone(raw, defaultRegion); err == nil && p.E164 != "" {
		cc := fmt.Sprintf("+%d", p.CountryCode)
		return strings.TrimPrefix(p.E164, cc), cc
	}
	var b strings.Builder
	for _, r := range raw {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String(), ""
}

// SupportedPhoneRegions 返回库支持的地区码（ISO 3166-1 alpha-2），已排序。
//
// 返回的是副本：phonenumbers 的同名接口直接暴露内部 map，写入会污染
// 全局元数据并引发并发问题。
func SupportedPhoneRegions() []string {
	m := phonenumbers.GetSupportedRegions()
	out := make([]string, 0, len(m))
	for code := range m {
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}

// 解析过程中可能出现的错误，可用 errors.Is 判定。
var (
	// ErrPhoneEmpty 表示输入为空。
	ErrPhoneEmpty = errors.New("utils: phone number is empty")
	// ErrPhoneUnknownRegion 表示 defaultRegion 不是合法的 ISO 3166-1 alpha-2 码。
	ErrPhoneUnknownRegion = errors.New("utils: unknown default region")
	// ErrPhoneNotParseable 表示无法从输入中提取出号码。
	ErrPhoneNotParseable = errors.New("utils: phone number is not parseable")
)

func normalizeRegion(region string) string {
	region = strings.ToUpper(strings.TrimSpace(region))
	if region == "ZZ" {
		return ""
	}
	return region
}

// isSupportedRegion 报告地区码是否有元数据覆盖。
// 只读访问 phonenumbers 的内部 map（初始化后不再写入，故无竞态），
// 不把该 map 暴露给调用方。
func isSupportedRegion(region string) bool {
	_, ok := phonenumbers.GetSupportedRegions()[region]
	return ok
}

func regionOf(num *phonenumbers.PhoneNumber) string {
	if r := phonenumbers.GetRegionCodeForNumber(num); r != "" {
		return r
	}
	return phonenumbers.UNKNOWN_REGION
}

func phoneType(num *phonenumbers.PhoneNumber) PhoneType {
	switch phonenumbers.GetNumberType(num) {
	case phonenumbers.FIXED_LINE:
		return PhoneTypeFixedLine
	case phonenumbers.MOBILE:
		return PhoneTypeMobile
	case phonenumbers.FIXED_LINE_OR_MOBILE:
		return PhoneTypeFixedOrMobile
	case phonenumbers.TOLL_FREE:
		return PhoneTypeTollFree
	case phonenumbers.PREMIUM_RATE:
		return PhoneTypePremiumRate
	case phonenumbers.SHARED_COST:
		return PhoneTypeSharedCost
	case phonenumbers.VOIP:
		return PhoneTypeVoIP
	case phonenumbers.PERSONAL_NUMBER:
		return PhoneTypePersonalNumber
	case phonenumbers.PAGER:
		return PhoneTypePager
	case phonenumbers.UAN:
		return PhoneTypeUAN
	case phonenumbers.VOICEMAIL:
		return PhoneTypeVoicemail
	default:
		return PhoneTypeUnknown
	}
}
