// Package utils 提供与业务无关的通用小工具。
//
// 约定：本包只放"纯函数"（无全局状态、无副作用、可并发调用），
// 需要持有连接/句柄的组件请放到各自子包中。
// 例外：snowflake.go 的雪花 ID 是有状态生成器，因足够独立、零依赖
// 且高频共用而留在本包（见 snowflake.go）。
//
// 依赖代价：phone.go 引入了 github.com/nyaruka/phonenumbers。该库在
// init() 里加载全球号码元数据，而包的 init 无法被链接器裁剪，因此
// **任何 import 本包的程序都会固定多出约 5.5MB 二进制**，哪怕只用
// SanitizeDSN。对体积敏感的服务应改为单独 import phone 所在的包
// （把 phone.go 拆成独立顶层包即可，改动只有一行 import path）。
package utils

import "strings"

// SanitizeDSN 抹掉 DSN 中的密码，便于安全地写入日志。
//
//	amqp://guest:secret@127.0.0.1:5672/        -> amqp://guest:***@127.0.0.1:5672/
//	root:secret@tcp(127.0.0.1:3306)/app        -> root:***@tcp(127.0.0.1:3306)/app
//
// 不含密码的 DSN 原样返回。
//
// 定位规则（按 RFC 3986）：userinfo 只可能出现在 authority 段里，而
// authority 止于第一个 / ? #。在 authority 内取【最后一个】@ —— 密码里
// 允许出现未转义的 @，取第一个会把密码后半段当成 host 漏进日志
// （amqp://u:p@ss@host/ 旧实现会输出 amqp://u:***@ss@host/）。
// 同理，只在 authority 内找 @ 也顺带避免了把 query 里的 @（?opt=a@b）
// 误认成密码分隔符。
//
// 已知限制：密码含未转义的 / 时 authority 会被提前截断，此时无法识别
// userinfo，返回原样（宁可不脱敏也不改坏 DSN）。需要这种密码请先转义。
func SanitizeDSN(dsn string) string {
	start := 0
	if i := strings.Index(dsn, "://"); i >= 0 {
		start = i + len("://")
	}

	// authority 段：[start, end)。
	end := len(dsn)
	for i := start; i < len(dsn); i++ {
		if c := dsn[i]; c == '/' || c == '?' || c == '#' {
			end = i
			break
		}
	}

	atRel := strings.LastIndex(dsn[start:end], "@")
	if atRel < 0 {
		return dsn // 无 userinfo
	}
	at := start + atRel

	colonRel := strings.Index(dsn[start:at], ":")
	if colonRel < 0 {
		return dsn // 有 userinfo 但没带密码
	}
	colon := start + colonRel

	return dsn[:colon+1] + "***" + dsn[at:]
}
