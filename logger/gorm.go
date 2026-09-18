package logger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	gormlogger "gorm.io/gorm/logger"
)

// 慢 SQL 与日志截断的默认值
const (
	defaultSlowThreshold = 200 * time.Millisecond
	maxSQLLen            = 4096 // 单条 SQL 最大记录长度，超出截断，避免日志被大 SQL 冲垮
)

// Mysql 实现 gorm.io/gorm/logger.Interface，把 GORM 的 SQL 日志接到本包的全局日志器上。
//
//	cfg := &logger.Mysql{
//	    Name:          "MySQL",
//	    SlowThreshold: 200 * time.Millisecond,
//	    LogLevel:      "info",   // silent/error/warn/info
//	}
//	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: cfg})
//
// 说明：
//   - 不缓存全局日志器，每次输出都取当前实例，因此 Init 热更新后 GORM 日志会跟着切换；
//   - LogMode 返回副本而不是就地修改，可安全地被 GORM 与业务方并发持有；
//   - SlowThreshold 为 0 时使用 200ms，负数表示关闭慢 SQL 告警。
type Mysql struct {
	Name                      string        // 日志器名字，写入 logger 字段，默认 gorm
	SlowThreshold             time.Duration // 慢 SQL 阈值，0=默认 200ms，负值=关闭
	LogLevel                  string        // 级别: silent/error/warn/info，默认 info
	IgnoreRecordNotFoundError bool          // 是否忽略 ErrRecordNotFound，默认不忽略

	level gormlogger.LogLevel // LogMode 显式设置后的级别，优先级最高
}

// 编译期断言：确保适配器始终满足 GORM 的日志接口
var _ gormlogger.Interface = (*Mysql)(nil)

// LogMode 返回指定级别的新实例（GORM 会持有返回值，不做就地修改）
func (m *Mysql) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	clone := *m
	clone.level = level
	return &clone
}

// Info 普通 SQL 日志
func (m *Mysql) Info(ctx context.Context, msg string, data ...interface{}) {
	if m.currentLevel() < gormlogger.Info {
		return
	}
	m.emit(ctx, slog.LevelInfo, sprintf(msg, data...), nil)
}

// Warn 告警 SQL 日志
func (m *Mysql) Warn(ctx context.Context, msg string, data ...interface{}) {
	if m.currentLevel() < gormlogger.Warn {
		return
	}
	m.emit(ctx, slog.LevelWarn, sprintf(msg, data...), nil)
}

// Error 错误 SQL 日志
func (m *Mysql) Error(ctx context.Context, msg string, data ...interface{}) {
	if m.currentLevel() < gormlogger.Error {
		return
	}
	m.emit(ctx, slog.LevelError, sprintf(msg, data...), nil)
}

// Trace 记录单条 SQL：错误 → error，慢查询 → warn，其余 → info
func (m *Mysql) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	level := m.currentLevel()
	if level <= gormlogger.Silent {
		return
	}

	elapsed := time.Since(begin)
	sql, rows := fc()

	attrs := []slog.Attr{
		slog.String("sql", truncate(sql)),
		slog.Int64("rows", rows),
		slog.String("elapsed", elapsed.String()),
	}

	switch {
	case err != nil && level >= gormlogger.Error:
		if m.IgnoreRecordNotFoundError && errors.Is(err, gormlogger.ErrRecordNotFound) {
			return
		}
		attrs = append(attrs, slog.String("error", err.Error()))
		m.emit(ctx, slog.LevelError, "SQL 执行失败", attrs)

	case m.slowThreshold() > 0 && elapsed > m.slowThreshold() && level >= gormlogger.Warn:
		m.emit(ctx, slog.LevelWarn, "慢 SQL", attrs)

	case level >= gormlogger.Info:
		m.emit(ctx, slog.LevelInfo, "SQL 执行", attrs)
	}
}

// emit 统一出口，字段顺序稳定，便于日志检索
func (m *Mysql) emit(ctx context.Context, level slog.Level, msg string, attrs []slog.Attr) {
	full := make([]slog.Attr, 0, len(attrs)+1)
	full = append(full, slog.String("logger", m.name()))
	full = append(full, attrs...)
	full = append(full, attrsFromCtx(ctx)...)
	emit(ctx, current(), level, msg, full)
}

// currentLevel 解析生效级别：LogMode 优先，其次 LogLevel 配置，最后 info
func (m *Mysql) currentLevel() gormlogger.LogLevel {
	if m.level != 0 {
		return m.level
	}
	switch strings.ToLower(strings.TrimSpace(m.LogLevel)) {
	case "silent":
		return gormlogger.Silent
	case "error":
		return gormlogger.Error
	case "warn":
		return gormlogger.Warn
	case "info":
		return gormlogger.Info
	default:
		return gormlogger.Info
	}
}

// slowThreshold 解析慢 SQL 阈值
func (m *Mysql) slowThreshold() time.Duration {
	if m.SlowThreshold < 0 {
		return 0
	}
	if m.SlowThreshold == 0 {
		return defaultSlowThreshold
	}
	return m.SlowThreshold
}

// name 返回日志器名
func (m *Mysql) name() string {
	if m.Name == "" {
		return "gorm"
	}
	return m.Name
}

// sprintf 仅有参数时才格式化
func sprintf(format string, args ...interface{}) string {
	if len(args) == 0 {
		return format
	}
	return fmt.Sprintf(format, args...)
}

// truncate 截断超长 SQL
func truncate(sql string) string {
	if len(sql) <= maxSQLLen {
		return sql
	}
	return sql[:maxSQLLen] + "...(已截断)"
}
