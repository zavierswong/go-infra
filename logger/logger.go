package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// state 全局日志状态，整体原子替换，读路径无需加锁
type state struct {
	logger *slog.Logger
	level  *slog.LevelVar // 支持运行期动态调级
	closer io.Closer      // 文件输出时的轮转 writer，stdout/stderr 为 nil
	// out 与 cfg 是"重建 handler"所需的两样东西：SetHandlerWrapper 在
	// 运行期换 handler 时直接复用现有 out 与 cfg，不重新解析配置、更不
	// 关闭重开输出——否则重建会退化成默认配置，还会在关/开之间露出一个
	// 往已关闭句柄写日志的窗口。
	out io.Writer
	cfg Config
}

var (
	global      atomic.Pointer[state]
	mu          sync.Mutex // 串行化 Init/Close/SetHandlerWrapper，避免并发替换时漏关文件句柄
	defaultOnce sync.Once
	// handlerWrapper 已注册的 Handler 包装器，nil 表示不包装。
	// 用 atomic.Pointer 而非普通变量：build 也在 current() 的懒加载
	// 路径上被调用，那条路径不持锁。
	handlerWrapper atomic.Pointer[HandlerWrapper]
)

// callerSkip 固定调用深度，必须与 emit 的调用链保持一致：
// skip=0 为 runtime.Callers 自身，依次是 callerPC → emit → 包装函数 → 调用方，故取 4。
// 所有对外日志入口（包级 printf 与 Plog 方法）都必须直接调用 emit，否则 caller 会偏移。
const callerSkip = 4

// Init 初始化全局日志器。进程内调用一次即可，之后包级函数与 NewPlog 都可直接使用。
//
// 可重复调用以热更新配置：旧的日志器会被原子替换，若旧输出为文件则会关闭其句柄。
// 未调用 Init 时包级函数依然可用（默认 stdout + info 级别 + json 格式）。
func Init(cfg Config) error {
	cfg = normalize(cfg)
	st, err := build(cfg)
	if err != nil {
		return fmt.Errorf("初始化日志失败: %w", err)
	}

	mu.Lock()
	prev := global.Swap(st)
	mu.Unlock()

	if prev != nil && prev.closer != nil {
		_ = prev.closer.Close()
	}
	return nil
}

// HandlerWrapper 包装底层 slog.Handler，用于在 Write 之前改写/补充日志记录。
//
// 典型用法是接入链路追踪（tracing 包提供的包装器签名正好匹配）：
//
//	logger.SetHandlerWrapper(tracing.NewLogHandler)
//
// 接入后，凡是 ctx 里带 OTel span 的日志（包级函数、Plog、GORM 适配器）
// 都会自动补上 trace_id / span_id，不再需要每个调用点手工注入。
type HandlerWrapper func(slog.Handler) slog.Handler

// SetHandlerWrapper 注册 Handler 包装器；传 nil 表示撤销包装。
//
// # 为什么是"注册制"而不是"传入一个已包装好的 Handler"
//
// 传入现成 Handler 的写法（`logger.WrapHandler(tracing.NewLogHandler(Default().Handler()))`）
// 要求调用方先拿到一个**当前**的底层 Handler，于是把三件事绑死了：必须先 Init
// （否则拿不到目标配置的 handler）、拿到之后不能再 Init（重新 Init 会让包装
// 静默失效）、也不能依赖未 Init 时的懒加载默认实例。三处的失败形态都是
// "日志照样打，只是 trace_id 不见了"——没有编译错误、没有运行时报错。
//
// 注册制把套用时机挪到 build 内部，于是**注册与 Init 的先后顺序无关**：
//
//	logger.Init(cfg)                     // 先初始化
//	logger.SetHandlerWrapper(wrapper)    // 后注册：下面会按原配置重建，立即生效
//	logger.Init(otherCfg)                // 再 Init：build 时再次套用，不会丢
//
// # 语义细节
//
//   - 包装器在**每次构建 handler 时**套用，因此 Init 热替换、Close 回落、
//     未 Init 的懒加载默认实例都会带上它；
//   - 已存在的实例会**就地换 handler**：复用原有输出与配置，只重建 handler
//     本身，并继承运行期 SetLevel 的调整（不继承的话热调级会被静默回滚到
//     配置文件里的级别）；
//   - 只换 handler 意味着**不关闭、不重开日志文件**——Init 的热替换会关旧
//     句柄，那个窗口里并发写入会打到已关闭的文件上，观测接缝不该顺带引入
//     这个性质；
//   - 尚未初始化时不抢先建实例：current() 首次使用时自会读到新包装器，
//     否则"只 import logger 却不打日志"的进程会平白多出一个文件句柄。
func SetHandlerWrapper(w HandlerWrapper) {
	mu.Lock()
	defer mu.Unlock()

	if w == nil {
		handlerWrapper.Store(nil)
	} else {
		handlerWrapper.Store(&w)
	}

	prev := global.Load()
	if prev == nil {
		return
	}

	handler, levelVar := newHandler(prev.out, prev.cfg)
	if prev.level != nil {
		levelVar.Set(prev.level.Level())
	}
	global.Store(&state{
		logger: newLogger(handler, prev.cfg),
		level:  levelVar,
		closer: prev.closer, // 原样保留：输出不换，句柄自然不关
		out:    prev.out,
		cfg:    prev.cfg,
	})
}

// Close 关闭日志器：刷新并关闭文件句柄，随后回落到默认日志器(stdout)，
// 避免进程收尾阶段继续往已关闭的文件里写。可重复调用。
func Close() error {
	mu.Lock()
	defer mu.Unlock()

	prev := global.Load()
	var err error
	if prev != nil && prev.closer != nil {
		err = prev.closer.Close()
	}
	if st, buildErr := build(normalize(Config{})); buildErr == nil {
		global.Store(st)
	}
	return err
}

// Sync 刷新缓冲区，文件输出时等价于 fsync
func Sync() error {
	st := global.Load()
	if st == nil || st.closer == nil {
		return nil
	}
	if s, ok := st.closer.(interface{ Sync() error }); ok {
		return s.Sync()
	}
	return nil
}

// SetLevel 运行期调整日志级别（热生效，无需重新 Init）。
// 未 Init 时也会先落地默认实例，保证调用不会静默失效。
func SetLevel(level string) {
	current()
	if st := global.Load(); st != nil && st.level != nil {
		st.level.Set(parseLevel(level))
	}
}

// Default 返回当前全局 slog.Logger，便于与标准库/第三方库对接
func Default() *slog.Logger {
	return current()
}

// Ctx 返回附带 ctx 上下文字段（如 trace_id）的 slog.Logger。
// 直接用返回值打日志时，caller 仍会定位到你的调用处。
func Ctx(ctx context.Context) *slog.Logger {
	l := current()
	attrs := attrsFromCtx(ctx)
	if len(attrs) == 0 {
		return l
	}
	args := make([]any, 0, len(attrs))
	for _, a := range attrs {
		args = append(args, a)
	}
	return l.With(args...)
}

// With 返回附带固定字段的 slog.Logger
func With(args ...any) *slog.Logger {
	return current().With(args...)
}

// WithName 返回带 logger 名字段的 slog.Logger
func WithName(name string) *slog.Logger {
	return current().With(slog.String("logger", name))
}

// Debugf 调试日志，printf 风格
func Debugf(format string, args ...any) {
	emit(context.Background(), current(), slog.LevelDebug, message(format, args...), nil)
}

// Infof 普通日志，printf 风格
func Infof(format string, args ...any) {
	emit(context.Background(), current(), slog.LevelInfo, message(format, args...), nil)
}

// Warnf 告警日志，printf 风格
func Warnf(format string, args ...any) {
	emit(context.Background(), current(), slog.LevelWarn, message(format, args...), nil)
}

// Errorf 错误日志，printf 风格
func Errorf(format string, args ...any) {
	emit(context.Background(), current(), slog.LevelError, message(format, args...), nil)
}

// Fatalf 致命日志，printf 风格：按 fatal 级别输出后以状态码 1 退出进程。
//
// 与标准库 log.Fatal 语义对齐，但注意三点：
//   - 只能用于 main 或启动阶段；库/中间件里调用会直接终结宿主进程；
//   - 退出前会先 Sync 一次（os.Exit 不执行 defer，defer Close/Sync 都不会跑）；
//   - level 由 LevelFatalValue(=LevelError+4) 承载，故 Level=error 时依然会输出。
//
// 该函数不会返回，即使日志被级别过滤（例如阈值设为 fatal 之外的更高值）也会退出。
func Fatalf(format string, args ...any) {
	emit(context.Background(), current(), LevelFatalValue, message(format, args...), nil)
	exit(1)
}

// Plog 具名子日志器。推荐以结构化字段方式使用：
//
//	lg := logger.NewPlog("MySQL")
//	lg.Infof(ctx, "连接成功, 耗时: %s", elapsed)
//	lg.With("dsn", dsn).Warn(ctx, "连接池已满")
//
// Plog 零值可用，且 With/Named 返回副本，可安全并发使用。
type Plog struct {
	name  string
	attrs []slog.Attr
}

// NewPlog 创建具名子日志器
func NewPlog(name string) *Plog {
	return &Plog{name: name}
}

// Named 派生子日志器，名字以 "." 拼接
func (p *Plog) Named(name string) *Plog {
	n := *p
	if n.name == "" {
		n.name = name
	} else {
		n.name = n.name + "." + name
	}
	return &n
}

// With 派生带固定字段的子日志器，参数为 key/value 对或 slog.Attr
func (p *Plog) With(args ...any) *Plog {
	n := *p
	n.attrs = append(append(make([]slog.Attr, 0, len(p.attrs)+len(args)), p.attrs...), argsToAttrs(args)...)
	return &n
}

// Name 返回当前子日志器名字
func (p *Plog) Name() string { return p.name }

// Debugf 调试日志，printf 风格
func (p *Plog) Debugf(ctx context.Context, format string, args ...any) {
	emit(ctx, current(), slog.LevelDebug, message(format, args...), p.attrsFor(ctx))
}

// Infof 普通日志，printf 风格
func (p *Plog) Infof(ctx context.Context, format string, args ...any) {
	emit(ctx, current(), slog.LevelInfo, message(format, args...), p.attrsFor(ctx))
}

// Warnf 告警日志，printf 风格
func (p *Plog) Warnf(ctx context.Context, format string, args ...any) {
	emit(ctx, current(), slog.LevelWarn, message(format, args...), p.attrsFor(ctx))
}

// Errorf 错误日志，printf 风格
func (p *Plog) Errorf(ctx context.Context, format string, args ...any) {
	emit(ctx, current(), slog.LevelError, message(format, args...), p.attrsFor(ctx))
}

// Fatalf 致命日志，printf 风格：输出后以状态码 1 退出进程（语义见包级 Fatalf）
func (p *Plog) Fatalf(ctx context.Context, format string, args ...any) {
	emit(ctx, current(), LevelFatalValue, message(format, args...), p.attrsFor(ctx))
	exit(1)
}

// Fatal 致命日志，结构化风格（key/value 对）：输出后以状态码 1 退出进程
func (p *Plog) Fatal(ctx context.Context, msg string, args ...any) {
	emit(ctx, current(), LevelFatalValue, msg, p.attrsFor(ctx, args...))
	exit(1)
}

// Debug 调试日志，结构化风格（key/value 对）
func (p *Plog) Debug(ctx context.Context, msg string, args ...any) {
	emit(ctx, current(), slog.LevelDebug, msg, p.attrsFor(ctx, args...))
}

// Info 普通日志，结构化风格（key/value 对）
func (p *Plog) Info(ctx context.Context, msg string, args ...any) {
	emit(ctx, current(), slog.LevelInfo, msg, p.attrsFor(ctx, args...))
}

// Warn 告警日志，结构化风格（key/value 对）
func (p *Plog) Warn(ctx context.Context, msg string, args ...any) {
	emit(ctx, current(), slog.LevelWarn, msg, p.attrsFor(ctx, args...))
}

// Error 错误日志，结构化风格（key/value 对）
func (p *Plog) Error(ctx context.Context, msg string, args ...any) {
	emit(ctx, current(), slog.LevelError, msg, p.attrsFor(ctx, args...))
}

// attrsFor 组装日志字段：logger 名 → 固定字段 → 本次字段 → ctx 字段
func (p *Plog) attrsFor(ctx context.Context, args ...any) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(p.attrs)+len(args)/2+2)
	if p.name != "" {
		attrs = append(attrs, slog.String("logger", p.name))
	}
	attrs = append(attrs, p.attrs...)
	attrs = append(attrs, argsToAttrs(args)...)
	attrs = append(attrs, attrsFromCtx(ctx)...)
	return attrs
}

// exitFunc 退出函数，抽成变量以便测试注入。
// 直接调用 os.Exit 会让 go test 进程当场结束，无法断言输出与退出码。
var exitFunc = os.Exit

// exit 退出进程。os.Exit 不执行 defer，因此这里必须先刷盘，
// 否则文件输出场景下最后几条日志（含这条致命日志）可能丢在内核缓冲里。
func exit(code int) {
	_ = Sync()
	exitFunc(code)
}

// current 返回全局日志器；未 Init 时懒加载默认实例（stdout + info），保证"开箱即用"
func current() *slog.Logger {
	if st := global.Load(); st != nil {
		return st.logger
	}
	defaultOnce.Do(func() {
		if global.Load() == nil {
			if st, err := build(normalize(Config{})); err == nil {
				global.CompareAndSwap(nil, st)
			}
		}
	})
	if st := global.Load(); st != nil {
		return st.logger
	}
	return slog.Default()
}

// emit 统一出口。手工构造 Record 并携带调用方 PC，
// 这样包装函数不会污染 caller 信息（slog 默认取的是调用 Info 的那一层）。
//
// 注意：slog 的级别过滤发生在 Logger.log → Enabled，而 Handler.Handle 内部
// 并不做级别判断，所以这里必须显式调用 Enabled，否则低于配置级别的日志会被照写，
// 同时也保证了 SetLevel 热调级依然生效（LevelVar 每次读取当前值）。
func emit(ctx context.Context, l *slog.Logger, level slog.Level, msg string, attrs []slog.Attr) {
	if ctx == nil {
		ctx = context.Background() // 防御：部分自定义 Handler 会读 ctx
	}
	if !l.Handler().Enabled(ctx, level) {
		return
	}
	r := slog.NewRecord(time.Now(), level, msg, callerPC())
	if len(attrs) > 0 {
		r.AddAttrs(attrs...)
	}
	if err := l.Handler().Handle(ctx, r); err != nil {
		// 日志写失败时无处上报，退化为 stderr 兜底，避免静默丢日志
		fmt.Fprintf(os.Stderr, "logger: 写入日志失败: %v\n", err)
	}
}

// build 按配置构建日志器：先备好输出，再交给 newHandler 构建 handler。
func build(cfg Config) (*state, error) {
	var (
		out    io.Writer
		closer io.Closer
	)
	if isFileOutput(cfg.Output) {
		w, err := newRotateWriter(cfg.Output, cfg.Rotate)
		if err != nil {
			return nil, err
		}
		out, closer = w, w
	} else if cfg.Output == "stderr" {
		out = os.Stderr
	} else {
		out = os.Stdout
	}

	handler, levelVar := newHandler(out, cfg)
	return &state{
		logger: newLogger(handler, cfg),
		level:  levelVar,
		closer: closer,
		out:    out,
		cfg:    cfg,
	}, nil
}

// newHandler 基于**已就绪的输出**构建 handler（含色彩处理与包装器），
// 并返回承载当前级别的 LevelVar。
//
// 与 build 拆开，是为了让 SetHandlerWrapper 能复用现有输出重建 handler：
// 换 handler 不需要碰 writer，日志文件既不关闭也不重开。
func newHandler(out io.Writer, cfg Config) (slog.Handler, *slog.LevelVar) {
	levelVar := new(slog.LevelVar)
	levelVar.Set(parseLevel(cfg.Level))

	// 色彩输出：仅 text 格式且输出到 stdout/stderr 时开启，Init 时判定一次
	// （终端属性不会中途变化），不逐条日志重复探测。
	colored := false
	if Format(cfg.Format) == FormatText && cfg.Color != "off" && !isFileOutput(cfg.Output) {
		colored = cfg.Color == "on" || colorEnabled(out)
	}

	opts := &slog.HandlerOptions{
		Level:     levelVar,
		AddSource: !cfg.DisableCaller,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) > 0 { // 嵌套 group 内的同名字段不做格式化与着色
				return a
			}
			switch a.Key {
			case slog.TimeKey:
				if t, ok := a.Value.Any().(time.Time); ok {
					a.Value = slog.StringValue(t.Format(cfg.TimeFormat))
				}
			case slog.LevelKey:
				if lv, ok := a.Value.Any().(slog.Level); ok {
					s := levelName(lv)
					if colored {
						if c, ok := levelColors[lv]; ok {
							s = c + s + colorReset
						}
					}
					a.Value = slog.StringValue(s)
				}
			case slog.MessageKey:
				if colored { // 消息加粗；时间戳不着色，避免污染 logfmt/diff 解析
					a.Value = slog.StringValue(colorBold + a.Value.String() + colorReset)
				}
			case slog.SourceKey:
				// slog 原生的 source 不统一：text 只渲染 `文件:行号`（丢函数名），
				// JSON 渲染成 {function,file,line} 对象。这里统一改写为单个字符串
				// `函数:行号`（如 handler.GetUser:42），text 与 JSON 表示一致。
				// 注意：AddSource=false 时压根没有该属性，本分支不会被触发。
				if src, ok := a.Value.Any().(*slog.Source); ok && src != nil {
					return slog.String(slog.SourceKey, fmt.Sprintf("%s:%d", shortFuncName(src.Function), src.Line))
				}
			}
			return a
		},
	}

	var handler slog.Handler
	if Format(cfg.Format) == FormatText {
		if colored {
			// TextHandler 会把 ReplaceAttr 注入的 ESC 字节转义成字面量 `\x1b`，
			// 必须经该 writer 还原，终端才能真正显示颜色
			out = colorUnescapeWriter{w: out}
		}
		handler = slog.NewTextHandler(out, opts)
	} else {
		handler = slog.NewJSONHandler(out, opts)
	}

	// 包装器在**唯一的构建点**套用，这是"注册制"的全部要害：Init 热替换、
	// Close 回落、未 Init 时的懒加载默认实例都经由本函数，因此注册与初始化
	// 的先后顺序不影响结果。
	return applyHandlerWrapper(handler), levelVar
}

// newLogger 用 handler 组装 logger（附带 service 字段）。
func newLogger(handler slog.Handler, cfg Config) *slog.Logger {
	l := slog.New(handler)
	if cfg.Service != "" {
		l = l.With(slog.String("service", cfg.Service))
	}
	return l
}

// applyHandlerWrapper 套用已注册的包装器；未注册时原样返回。
func applyHandlerWrapper(h slog.Handler) slog.Handler {
	w := handlerWrapper.Load()
	if w == nil {
		return h
	}
	return (*w)(h)
}

// message 仅在有参数时格式化，避免无参数时误解析字符串中的 %
func message(format string, args ...any) string {
	if len(args) == 0 {
		return format
	}
	return fmt.Sprintf(format, args...)
}

// argsToAttrs 把 key/value 对或 slog.Attr 转为 []slog.Attr
func argsToAttrs(args []any) []slog.Attr {
	if len(args) == 0 {
		return nil
	}
	attrs := make([]slog.Attr, 0, (len(args)+1)/2)
	for i := 0; i < len(args); {
		switch v := args[i].(type) {
		case slog.Attr:
			attrs = append(attrs, v)
			i++
		case string:
			if i+1 < len(args) {
				attrs = append(attrs, slog.Any(v, args[i+1]))
				i += 2
			} else {
				attrs = append(attrs, slog.String("!badkey", v))
				i++
			}
		default:
			attrs = append(attrs, slog.Any("!badkey", v))
			i++
		}
	}
	return attrs
}

// callerPC 取调用方程序计数器，供 slog 生成 source 字段
func callerPC() uintptr {
	var pcs [1]uintptr
	if runtime.Callers(callerSkip, pcs[:]) == 0 {
		return 0
	}
	return pcs[0]
}

// shortFuncName 把 runtime 的完整函数名裁剪为"包名.函数名"：
// github.com/zavierswong/go-infra/logger.TestX → logger.TestX，
// main.main 原样返回；方法保留形如 logger.(*Plog).Infof。
// 目的是让 source 字段保持紧凑，同时保留可直接 grep 的定位信息。
func shortFuncName(fn string) string {
	// runtime 函数名形如 "<import path>.<pkg>.<Func>"，截到最后一个 '/' 之后
	if i := strings.LastIndexByte(fn, '/'); i >= 0 && i+1 < len(fn) {
		return fn[i+1:]
	}
	return fn
}
