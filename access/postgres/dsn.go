package postgres

import (
	"fmt"
	"net/url"
	"strings"
)

// 本文件负责 PostgreSQL 连接串（DSN）的解析、校验与「补缺」。
//
// PostgreSQL 的连接串有**两种等价写法**，都是 libpq 定义的，驱动必须都支持：
//
//	URL 形式：      postgres://user:pass@host:5432/dbname?sslmode=disable
//	keyword/value： host=127.0.0.1 port=5432 user=postgres password=123456 dbname=demo sslmode=disable
//
// MySQL 只有一种（"user:pass@tcp(host:port)/db"），所以 mysql 包直接交给驱动解析即可。
// PostgreSQL 这边不行：Config 里的覆盖项（sslmode、application_name、statement_timeout…）
// 需要合并进 DSN，而 keyword/value 形式的词法规则（空白分隔、单引号、反斜杠转义）
// 没有标准库可用，必须自己实现一份。
//
// 三条设计原则：
//
//  1. **只补缺，不覆盖**：用户显式写在 DSN 里的参数优先级最高；
//  2. **保持书写形式**：给 URL 就还 URL，给 keyword/value 就还 keyword/value，便于配置 diff；
//  3. **参数名统一小写**：libpq 对参数名大小写不敏感，但 pgx 不是 ——
//     它把认不出的 key 当作**服务端 GUC** 透传，`HOST=x` / `?SSLMODE=x`
//     会变成 `FATAL: unrecognized configuration parameter`。统一小写把这类坑抹平。

// dsnForm 是 DSN 的书写形式。
type dsnForm int

const (
	// formKeywordValue 形如 `host=a port=5432 dbname=b`（libpq 传统写法）
	formKeywordValue dsnForm = iota
	// formURL 形如 `postgres://user@host:5432/dbname`
	formURL
)

// kvPair 是连接串里的一个参数。
//
// 用切片而不是 map 是为了**保持用户书写的顺序**：格式化回来时不会重排，
// 配置 diff 里就看不到无意义的噪音。
type kvPair struct {
	key   string
	value string
}

// connInfo 是解析后的连接串。
//
// URL 形式的查询参数单独拆成 urlParams 而不是留在 url.RawQuery 里，有两个原因：
// 一是要统一小写；二是要避开标准库 url.Values.Encode() 的过度转义 —— 它会把 `/`
// 编成 %2F，而 gorm 的 postgres 驱动是用正则从 DSN 里**直接取时区字符串**去
// time.LoadLocation 的，`Asia%2FShanghai` 会报 unknown time zone，
// 表现为每条连接都建不起来且错误信息完全不提 DSN。
type connInfo struct {
	form      dsnForm
	url       *url.URL // form == formURL 时非 nil
	urlParams []kvPair // form == formURL 时非 nil，参数名已统一小写
	kvs       []kvPair // form == formKeywordValue 时非 nil
}

// dbnameKeys 是数据库名在两种形式下的可能写法。
//
// pgx 内部把 libpq 的 `dbname` 归一成 `database`（见 pgconn.connStringKeyAliases），
// 两种写法都认，所以这里也把两者当成同一个参数。
var dbnameKeys = []string{"dbname", "database"}

// parseConnInfo 解析 DSN。语法错误会在启动期返回，而不是等到第一次查询。
func parseConnInfo(raw string) (*connInfo, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return nil, fmt.Errorf("%w: Dsn 不能为空", ErrInvalidConfig)
	}

	if isURLDSN(s) {
		u, err := url.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("%w: DSN 无法解析为 URL: %v", ErrInvalidConfig, err)
		}
		params, err := parseRawQuery(u.RawQuery)
		if err != nil {
			return nil, err
		}
		u.RawQuery = "" // query 交给 urlParams 管理，避免两处状态不一致
		return &connInfo{form: formURL, url: u, urlParams: params}, nil
	}

	kvs, err := parseKeywordValueDSN(s)
	if err != nil {
		return nil, err
	}
	return &connInfo{form: formKeywordValue, kvs: kvs}, nil
}

// isURLDSN 判断是否 URL 形式。判据与 pgx 保持一致（只看前缀）。
func isURLDSN(s string) bool {
	return strings.HasPrefix(s, "postgres://") || strings.HasPrefix(s, "postgresql://")
}

// ---------------------------------------------------------------------------
// 读取
// ---------------------------------------------------------------------------

// get 按顺序查找第一个存在的参数。keys 用于处理别名（如 dbname / database）。
//
// 查找对大小写不敏感：连接串里的参数名在解析阶段已统一小写，
// 这里把入参也归一化，避免调用方为了查 "TimeZone" 还是 "timezone" 纠结。
func (c *connInfo) get(keys ...string) (string, bool) {
	if c.form == formURL {
		for _, raw := range keys {
			k := strings.ToLower(raw)
			if isDBNameKey(k) {
				// 取值顺序与 pgx 一致：查询参数优先于 path
				for _, alias := range dbnameKeys {
					if v, ok := c.urlParam(alias); ok {
						return v, true
					}
				}
				if name := dbNameFromPath(c.url); name != "" {
					return name, true
				}
				continue
			}
			if v, ok := c.urlParam(k); ok {
				return v, true
			}
		}
		return "", false
	}

	for _, p := range c.kvs {
		for _, raw := range keys {
			if p.key == strings.ToLower(raw) {
				return p.value, true
			}
		}
	}
	return "", false
}

// urlParam 查找 URL 查询参数（参数名需已小写）。
func (c *connInfo) urlParam(key string) (string, bool) {
	for _, p := range c.urlParams {
		if p.key == key {
			return p.value, true
		}
	}
	return "", false
}

// has 报告参数是否已被显式书写。
func (c *connInfo) has(key string) bool {
	keys := []string{key}
	if isDBNameKey(key) {
		keys = dbnameKeys
	}
	_, ok := c.get(keys...)
	return ok
}

// dbName 返回数据库名（URL 形式看查询参数与 path，keyword/value 形式看 dbname / database）。
func (c *connInfo) dbName() string {
	v, _ := c.get(dbnameKeys...)
	return v
}

// dbNameFromPath 从 URL 的 path 段取库名。
//
// 不做 url.PathUnescape：url.Parse 已经把 Path 解码过了，再解一次会破坏
// "库名里本来就含 %xx" 这种写法（`/a%2520b` 会被多解一层）。
func dbNameFromPath(u *url.URL) string {
	return strings.TrimPrefix(u.Path, "/")
}

// ---------------------------------------------------------------------------
// 写入
// ---------------------------------------------------------------------------

// setIfAbsent 仅在参数缺失时写入，返回是否真的写入。
//
// 这就是「覆盖项只在 DSN 没写的时候才生效」这条语义的落点：
// 用户在 DSN 里写 `sslmode=verify-full`、Config 里写 SSLMode="disable"，
// 结果必须仍然是 verify-full。
func (c *connInfo) setIfAbsent(key, value string) bool {
	if c.has(key) {
		return false
	}
	c.set(key, value)
	return true
}

// set 无条件写入参数（参数名统一小写），已有则就地更新（顺序不变）。
func (c *connInfo) set(key, value string) {
	key = strings.ToLower(key)

	if c.form == formURL {
		if isDBNameKey(key) {
			c.url.Path = "/" + value
			return
		}
		for i := range c.urlParams {
			if c.urlParams[i].key == key {
				c.urlParams[i].value = value
				return
			}
		}
		c.urlParams = append(c.urlParams, kvPair{key: key, value: value})
		return
	}

	for i := range c.kvs {
		if c.kvs[i].key == key {
			c.kvs[i].value = value
			return
		}
	}
	c.kvs = append(c.kvs, kvPair{key: key, value: value})
}

// String 还原成 DSN 字符串，保持原有的书写形式。
func (c *connInfo) String() string {
	if c.form == formURL {
		clone := *c.url
		clone.RawQuery = encodeURLParams(c.urlParams)
		return clone.String()
	}
	return formatKeywordValueDSN(c.kvs)
}

func isDBNameKey(k string) bool {
	for _, alias := range dbnameKeys {
		if k == alias {
			return true
		}
	}
	return false
}

// maskedPassword 是脱敏时替换密码用的占位串。
//
// 用 "xxxxx" 而不是 "***" 是有原因的：URL 形式下密码由 net/url 负责转义，
// `*` 会被编码成 %2A，日志里就变成 `postgres://user:%2A%2A%2A@host`，很难看。
// "xxxxx" 全是不需要转义的字符，同时与 pgx 自身的脱敏约定（redactPW）一致。
const maskedPassword = "xxxxx"

// sanitizeDSN 抹掉 DSN 里的密码，便于安全地写入日志或错误上报。
//
//	postgres://user:secret@host:5432/db    -> postgres://user:xxxxx@host:5432/db
//	host=h user=u password=secret dbname=d -> host=h user=u password=xxxxx dbname=d
//
// 不复用 utils.SanitizeDSN：那个函数只识别 `user:pass@host` 这种 URL 形态，
// PostgreSQL 的 keyword/value 写法（`password=xxx`）它认不出来，会原样放行。
func sanitizeDSN(dsn string) string {
	info, err := parseConnInfo(dsn)
	if err != nil {
		// 解析不了就不回显原文 —— 宁可少一条排查线索，也不冒泄漏密码的风险。
		return "***"
	}

	if info.form == formURL {
		if info.url.User != nil {
			if _, hasPassword := info.url.User.Password(); hasPassword {
				info.url.User = url.UserPassword(info.url.User.Username(), maskedPassword)
			}
		}
		return info.String()
	}

	masked := make([]kvPair, len(info.kvs))
	copy(masked, info.kvs)
	for i := range masked {
		if masked[i].key == "password" {
			masked[i].value = maskedPassword
		}
	}
	return formatKeywordValueDSN(masked)
}

// ---------------------------------------------------------------------------
// URL 形式的查询参数
// ---------------------------------------------------------------------------

// parseRawQuery 解析 URL 的原始查询串，并把参数名统一小写。
func parseRawQuery(raw string) ([]kvPair, error) {
	if raw == "" {
		return nil, nil
	}

	var out []kvPair
	for _, seg := range strings.Split(raw, "&") {
		if seg == "" {
			continue
		}

		rawKey, rawValue := seg, ""
		if i := strings.IndexByte(seg, '='); i >= 0 {
			rawKey, rawValue = seg[:i], seg[i+1:]
		}

		key, err := url.QueryUnescape(rawKey)
		if err != nil {
			return nil, fmt.Errorf("%w: DSN 查询参数名 %q 转义非法: %v", ErrInvalidConfig, rawKey, err)
		}
		value, err := url.QueryUnescape(rawValue)
		if err != nil {
			return nil, fmt.Errorf("%w: DSN 查询参数 %s 的取值转义非法: %v", ErrInvalidConfig, key, err)
		}
		out = append(out, kvPair{key: strings.ToLower(key), value: value})
	}
	return out, nil
}

// encodeURLParams 序列化 URL 查询参数。
func encodeURLParams(params []kvPair) string {
	parts := make([]string, 0, len(params))
	for _, p := range params {
		parts = append(parts, urlQueryEscape(p.key)+"="+urlQueryEscape(p.value))
	}
	return strings.Join(parts, "&")
}

// urlQueryEscape 按「最小必要」转义 URL 查询参数。
//
// 不用 url.Values.Encode()，因为它会把 `/` 编码成 %2F。
// 而 gorm 的 postgres 驱动是这样处理时区的：
//
//	result := timeZoneMatcher.FindStringSubmatch(DSN)   // 正则直接从 DSN 里抠字符串
//	loc, tzErr := time.LoadLocation(result[2])          // 期盼拿到 Asia/Shanghai
//
// 一旦时区被编码成 Asia%2FShanghai，LoadLocation 就会失败，
// 表现为**每一条连接都建不起来**，而错误信息又不提 DSN，极难排查。
//
// 保留集只含"不会破坏查询串结构"的字符：字母、数字、`-`、`_`、`.`、`~`、`/`。
// 注意 `+` 必须转义 —— 标准库解码时会把 `+` 当作空格。
func urlQueryEscape(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~', c == '/':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// keyword/value 形式的词法规则
//
// 与 libpq / pgx 保持一致：
//   - 参数之间用空白分隔（空格、\t、\n、\r、\v、\f）；
//   - `=` 两侧允许有空白（`host = 127.0.0.1` 合法）；
//   - 值可以用单引号包裹，引号内的 `\'` 与 `\\` 是转义；
//   - 不加引号的值里 `\` 同样是转义字符，值以空白结束。
//
// 「`=` 两侧允许空白」这条很容易漏：漏了就会把合法配置报成语法错误。
// ---------------------------------------------------------------------------

// isDSNSpace 判断是否为词法空白。与 pgx 的 asciiSpace 表一致。
func isDSNSpace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	}
	return false
}

// parseKeywordValueDSN 解析 keyword/value 形式的 DSN，参数名统一转小写。
func parseKeywordValueDSN(s string) ([]kvPair, error) {
	var out []kvPair

	i := 0
	for i < len(s) {
		for i < len(s) && isDSNSpace(s[i]) {
			i++
		}
		if i >= len(s) {
			break
		}

		// --- key：读到 '=' 或空白为止 ---
		start := i
		for i < len(s) && s[i] != '=' && !isDSNSpace(s[i]) {
			i++
		}
		key := strings.ToLower(s[start:i])

		// '=' 之前允许有空白
		for i < len(s) && isDSNSpace(s[i]) {
			i++
		}
		if i >= len(s) || s[i] != '=' {
			return nil, fmt.Errorf(
				"%w: DSN 第 %d 个字符处 \"%s\" 缺少 '='（keyword/value 形式须为 key=value，参数之间用空格分隔）",
				ErrInvalidConfig, start, s[start:i])
		}
		if key == "" {
			return nil, fmt.Errorf("%w: DSN 第 %d 个字符处参数名为空", ErrInvalidConfig, start)
		}
		i++ // 跳过 '='

		// '=' 之后同样允许有空白（libpq 的 "Skip whitespace after the equal sign"）。
		// 漏掉这一步的话，`host = 127.0.0.1` 会把 host 解析成空串，
		// 然后默默连到默认地址 —— 一个很难发现的配置失效。
		for i < len(s) && isDSNSpace(s[i]) {
			i++
		}

		// --- value：引号形式或裸值 ---
		value, next, err := scanDSNValue(s, i, key)
		if err != nil {
			return nil, err
		}
		i = next

		out = append(out, kvPair{key: key, value: value})
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("%w: DSN 未包含任何参数", ErrInvalidConfig)
	}
	return out, nil
}

// scanDSNValue 从 s[i] 开始读出一个参数值，返回值与下一个待处理下标。
func scanDSNValue(s string, i int, key string) (string, int, error) {
	var b strings.Builder

	if i < len(s) && s[i] == '\'' {
		i++ // 跳过起始引号
		for i < len(s) {
			switch {
			case s[i] == '\\':
				if i+1 >= len(s) {
					return "", 0, unterminatedEscape(key)
				}
				b.WriteByte(s[i+1])
				i += 2
			case s[i] == '\'':
				return b.String(), i + 1, nil // 跳过收尾引号
			default:
				b.WriteByte(s[i])
				i++
			}
		}
		return "", 0, fmt.Errorf("%w: DSN 中 %s 的值缺少收尾单引号", ErrInvalidConfig, key)
	}

	for i < len(s) && !isDSNSpace(s[i]) {
		if s[i] == '\\' {
			if i+1 >= len(s) {
				return "", 0, unterminatedEscape(key)
			}
			b.WriteByte(s[i+1])
			i += 2
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String(), i, nil
}

func unterminatedEscape(key string) error {
	return fmt.Errorf("%w: DSN 中 %s 的值以孤立的反斜杠结尾", ErrInvalidConfig, key)
}

// formatKeywordValueDSN 序列化 keyword/value 形式的 DSN。
func formatKeywordValueDSN(kvs []kvPair) string {
	var b strings.Builder
	for i, p := range kvs {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(p.key)
		b.WriteByte('=')
		b.WriteString(quoteDSNValue(p.value))
	}
	return b.String()
}

// quoteDSNValue 在必要时加单引号并转义。
//
// 只有含空白、单引号或反斜杠的值才需要引号；不加引号时**不能**引入引号，
// 否则会让 DSN 变得难以对照阅读。空值写成 `”`，与 `key=` 等价但更醒目。
func quoteDSNValue(v string) string {
	if v != "" && !strings.ContainsAny(v, " \t\n\r\v\f'\\") {
		return v
	}

	var b strings.Builder
	b.Grow(len(v) + 2)
	b.WriteByte('\'')
	for i := 0; i < len(v); i++ {
		if v[i] == '\'' || v[i] == '\\' {
			b.WriteByte('\\')
		}
		b.WriteByte(v[i])
	}
	b.WriteByte('\'')
	return b.String()
}
