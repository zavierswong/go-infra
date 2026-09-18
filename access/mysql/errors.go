package mysql

import (
	"errors"

	mysqldrv "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"

	"github.com/zavierswong/go-infra/metrics"
)

// 本包的哨兵错误。调用方应使用 errors.Is 判定，而不是匹配错误文本。
var (
	// ErrInvalidConfig 配置非法（DSN 缺失或语法错误、取值为负、单位疑似写错等）。
	ErrInvalidConfig = errors.New("mysql: 配置非法")

	// ErrConnect 无法建立或恢复数据库连接（网络不通、认证失败、库不存在等）。
	ErrConnect = errors.New("mysql: 无法连接数据库")

	// ErrClosed 客户端已关闭。
	ErrClosed = errors.New("mysql: 客户端已关闭")

	// ErrNotConnected 客户端处于未连接状态（尚未初始化成功，或连接已被回收）。
	ErrNotConnected = errors.New("mysql: 尚未建立连接")
)

// classifyErr 把错误细化成 metrics 的低基数原因。
//
// metrics.ClassifyErr 只认通用原因（ctx、超时、EOF、零行）。
// 这里补上 MySQL 专有的分类，靠的是驱动返回的 *mysqldrv.MySQLError：
// 它的 Number 是稳定的服务端错误码，比匹配错误文本可靠得多。
//
// 这些分类直接决定 `..._errors_total{reason=...}` 的曲线是否可读：
// 例如"唯一键冲突"在业务上通常是可以重试的正常现象，
// 和"表不存在"混在同一个 reason 里就完全没法告警。
func classifyErr(err error) metrics.Reason {
	if err == nil {
		return metrics.ReasonNone
	}
	if errors.Is(err, ErrClosed) {
		return metrics.ReasonClosed
	}
	if errors.Is(err, ErrNotConnected) || errors.Is(err, ErrConnect) {
		return metrics.ReasonConnect
	}
	if errors.Is(err, ErrInvalidConfig) {
		return metrics.ReasonInvalid
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return metrics.ReasonNotFound
	}

	var myErr *mysqldrv.MySQLError
	if errors.As(err, &myErr) {
		switch myErr.Number {
		case 1062: // ER_DUP_ENTRY 唯一键冲突
			return metrics.ReasonConflict
		case 1213: // ER_LOCK_DEADLOCK 死锁，InnoDB 已回滚其中一个事务
			return metrics.ReasonConflict
		case 1205: // ER_LOCK_WAIT_TIMEOUT 等锁超时
			return metrics.ReasonConflict
		case 1048: // ER_BAD_NULL_ERROR
			return metrics.ReasonInvalid
		case 1054: // ER_BAD_FIELD_ERROR 未知列
			return metrics.ReasonInvalid
		case 1064: // ER_PARSE_ERROR 语法错误
			return metrics.ReasonInvalid
		case 1146: // ER_NO_SUCH_TABLE 表不存在
			return metrics.ReasonInvalid
		case 1040: // ER_CON_COUNT_ERROR 连接数超限
			return metrics.ReasonConnect
		case 1045: // ER_ACCESS_DENIED_ERROR 认证失败
			return metrics.ReasonConnect
		case 2006: // CR_SERVER_GONE_ERROR 服务端断开
			return metrics.ReasonConnect
		case 2013: // CR_SERVER_LOST 连接中途丢失
			return metrics.ReasonConnect
		}
	}

	return metrics.ClassifyErr(err)
}
