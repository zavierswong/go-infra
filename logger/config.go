// Package logger 提供进程级全局日志器：一次 Init，全项目直接使用。
//
// 设计要点：
//   - 无需 Init 也能用：未初始化时使用懒加载的默认实例（stdout + info 级别），
//     避免"忘记初始化就 panic"以及初始化顺序耦合。
//   - 全局状态用 atomic.Pointer 承载，读路径无锁；Init 可重复调用以热更新配置。
//   - 不引入第三方依赖，底层为标准库 log/slog。
//
// 用法：
//
//	if err := logger.Init(logger.Config{Level: "debug", Format: "json", Service: "user-api"}); err != nil {
//	    return err
//	}
//	defer logger.Close()
//
//	logger.Infof("服务已启动, 端口: %d", 8080)          // 包级 printf 风格
//	logger.Ctx(ctx).Info("处理请求", "uid", 1001)       // 带 ctx 字段的结构化风格
//
//	lg := logger.NewPlog("MySQL")                       // 具名子日志器
//	lg.Errorf(ctx, "查询失败: %v", err)
package logger

import (
	"log/slog"
	"strings"
)

// Level 日志级别
type Level string

// 支持的日志级别
const (
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
	LevelFatal Level = "fatal" // 仅由 Fatalf 使用：输出后退出进程
)

// LevelFatalValue Fatalf 使用的 slog 级别值。
// slog 未内置 Fatal，惯例取 LevelError+4，保证它高于 error（Level=error 时依然输出）。
const LevelFatalValue = slog.LevelError + 4

// Format 日志输出格式
type Format string

// 支持的输出格式
const (
	FormatJSON Format = "json"
	FormatText Format = "text"
)

// 默认值
const (
	defaultLevel         = LevelInfo
	defaultFormat        = FormatJSON
	defaultOutput        = "stdout"
	defaultTimeFormat    = "2006-01-02T15:04:05.000Z07:00"
	defaultRotateSizeMB  = 100
	defaultRotateBackups = 7
	defaultRotateAgeDays = 30
)

// RotateConfig 文件轮转配置，仅在 Output 为文件路径时生效
type RotateConfig struct {
	MaxSizeMB  int  `mapstructure:"max_size_mb"`  // 单文件最大体积(MB), 默认 100
	MaxBackups int  `mapstructure:"max_backups"`  // 最多保留的历史文件数, 默认 7
	MaxAgeDays int  `mapstructure:"max_age_days"` // 历史文件最长保留天数, 默认 30, 负数表示不按时间清理
	Compress   bool `mapstructure:"compress"`     // 是否 gzip 压缩历史文件
}

// Config 日志配置
type Config struct {
	Level         string       `mapstructure:"level"`          // debug/info/warn/error, 默认 info
	Format        string       `mapstructure:"format"`         // json/text, 默认 json
	Output        string       `mapstructure:"output"`         // stdout/stderr/文件路径, 默认 stdout
	Service       string       `mapstructure:"service"`        // 服务名, 写入每条日志的 service 字段
	DisableCaller bool         `mapstructure:"disable_caller"` // 是否关闭调用者信息; 默认输出(函数名 + 文件:行号)
	Color         string       `mapstructure:"color"`          // text 格式的 ANSI 着色: auto(默认)/on/off
	TimeFormat    string       `mapstructure:"time_format"`    // 时间格式, 默认 RFC3339 毫秒精度
	Rotate        RotateConfig `mapstructure:"rotate"`         // 文件轮转, 仅文件输出时生效
}

// normalize 归一化配置：填充默认值并修正非法值
func normalize(cfg Config) Config {
	cfg.Level = strings.ToLower(strings.TrimSpace(cfg.Level))
	if cfg.Level == "" {
		cfg.Level = string(defaultLevel)
	}
	cfg.Format = strings.ToLower(strings.TrimSpace(cfg.Format))
	if cfg.Format == "" {
		cfg.Format = string(defaultFormat)
	}
	cfg.Output = strings.TrimSpace(cfg.Output)
	if cfg.Output == "" {
		cfg.Output = defaultOutput
	}
	if cfg.TimeFormat == "" {
		cfg.TimeFormat = defaultTimeFormat
	}
	cfg.Color = normalizeColor(cfg.Color)
	if cfg.Rotate.MaxSizeMB <= 0 {
		cfg.Rotate.MaxSizeMB = defaultRotateSizeMB
	}
	if cfg.Rotate.MaxBackups <= 0 {
		cfg.Rotate.MaxBackups = defaultRotateBackups
	}
	if cfg.Rotate.MaxAgeDays == 0 {
		cfg.Rotate.MaxAgeDays = defaultRotateAgeDays
	}
	return cfg
}

// parseLevel 解析日志级别，非法值回落到 info
func parseLevel(level string) slog.Level {
	switch Level(strings.ToLower(strings.TrimSpace(level))) {
	case LevelDebug:
		return slog.LevelDebug
	case LevelWarn:
		return slog.LevelWarn
	case LevelError:
		return slog.LevelError
	case LevelFatal:
		// 允许把阈值设为 fatal（只有致命日志才输出）；非法值仍回落 info
		return LevelFatalValue
	default:
		return slog.LevelInfo
	}
}

// levelName 渲染级别名。slog 对自定义级别输出的是 "ERROR+4" 这种偏移写法，
// 这里特判为 fatal，保证日志里出现的是可读、可检索的级别名。
func levelName(lv slog.Level) string {
	if lv == LevelFatalValue {
		return string(LevelFatal)
	}
	return strings.ToLower(lv.String())
}

// normalizeColor 归一化色彩输出模式，非法值回落到 auto
func normalizeColor(c string) string {
	switch strings.ToLower(strings.TrimSpace(c)) {
	case "on", "force", "true", "1":
		return "on"
	case "off", "none", "false", "0":
		return "off"
	default:
		return "auto"
	}
}

// isFileOutput 判断是否为文件输出
func isFileOutput(output string) bool {
	return output != "stdout" && output != "stderr"
}
