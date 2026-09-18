package logger

import (
	"bytes"
	"io"
	"log/slog"
	"os"
)

// ANSI 转义序列（SGR）。仅允许出现在 text 格式的终端输出里，
// JSON 输出与文件轮转输出绝不携带，否则会污染结构化日志和采集端解析。
const (
	colorReset = "\x1b[0m"
	colorBold  = "\x1b[1m"
)

// levelColors 各日志级别对应的 ANSI 前景色
var levelColors = map[slog.Level]string{
	slog.LevelDebug: "\x1b[90m",   // 亮黑(灰)
	slog.LevelInfo:  "\x1b[32m",   // 绿
	slog.LevelWarn:  "\x1b[33m",   // 黄
	slog.LevelError: "\x1b[31m",   // 红
	LevelFatalValue: "\x1b[1;31m", // 加粗红: 致命且进程即将退出, 需要一眼可见
}

// colorEnabled 判断 writer 是否为交互式终端。零依赖实现：
// os.File.Stat() 的 ModeCharDevice 位即字符设备(tty)。
//
// 注意 /dev/null 也是字符设备，会被误判为终端——无害，可接受；
// 精确判定需要平台相关的 ioctl(TIOCGETA/TIOCGETP)，零依赖前提下不做。
func colorEnabled(w io.Writer) bool {
	// 行业约定(no-color.org)：NO_COLOR 非空时禁用彩色输出；TERM=dumb 同理
	if os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
		return false
	}
	// FORCE_COLOR 非空时强制开启，兜底 CI/远程终端探测失败的场景
	if os.Getenv("FORCE_COLOR") != "" {
		return true
	}
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// escLiteral 是 slog TextHandler 对 ESC 字节的转义产物。
// TextHandler 会对属性值中的不可打印字符做 C 风格转义：经 ReplaceAttr 注入的
// 真实 ESC 字节(0x1b)会被写成字面量 `\x1b` 四个 ASCII 字符，终端不会着色。
// 因此必须在 writer 层把它还原成真实 ESC 字节，这是 ReplaceAttr 方案能生效的前提。
var escLiteral = []byte(`\x1b`)

// colorUnescapeWriter 还原 TextHandler 转义掉的 ESC 字节，使 ANSI 色码真正生效。
//
// 为什么不在 handler 里解决：slog 的 TextHandler/JSONHandler 渲染层无法输出
// 控制字符，ReplaceAttr 注入的色码必然被转义；自研整套 handler 成本过高。
// 该 writer 只在 colored(text+stdout) 时接入，每条记录恰好对应一次 Write，
// 替换仅在字节流包含字面量 `\x1b` 时发生。风险：业务日志正文恰好包含字面量
// `\x1b` 四个字符时会被误还原——概率极低，且仅影响终端显示，文件输出不走此分支。
type colorUnescapeWriter struct {
	w io.Writer
}

func (c colorUnescapeWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, escLiteral) {
		p = bytes.ReplaceAll(p, escLiteral, []byte{0x1b})
	}
	if _, err := c.w.Write(p); err != nil {
		return 0, err
	}
	return len(p), nil
}
