package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"

	"github.com/zavierswong/go-infra/metrics"
)

// 本包的哨兵错误。调用方应使用 errors.Is 判定，而不是匹配错误文本。
var (
	// ErrInvalidConfig 配置非法（DSN 缺失、语法错误、sslmode 取值非法、单位疑似写错等）。
	ErrInvalidConfig = errors.New("postgres: 配置非法")

	// ErrConnect 无法建立或恢复数据库连接（网络不通、认证失败、库不存在等）。
	ErrConnect = errors.New("postgres: 无法连接数据库")

	// ErrClosed 客户端已关闭。
	ErrClosed = errors.New("postgres: 客户端已关闭")

	// ErrNotConnected 客户端处于未连接状态（尚未初始化成功，或连接已被回收）。
	ErrNotConnected = errors.New("postgres: 尚未建立连接")
)

// ---------------------------------------------------------------------------
// SQLSTATE 判定
//
// PostgreSQL 的每个服务端错误都带一个五字符 SQLSTATE 码（规范见
// https://www.postgresql.org/docs/current/errcodes-appendix.html）。
// 驱动把它放在 *pgconn.PgError 里，本组函数负责从错误链中取出并分类 ——
// 业务代码据此决定"重试 / 提示用户 / 直接失败"，而**不要去匹配错误文本**
// （文本受 lc_messages 与版本影响，且没有稳定性保证）。
// ---------------------------------------------------------------------------

// 常用 SQLSTATE 码。集中定义，便于阅读与测试。
const (
	sqlStateUniqueViolation     = "23505" // unique_violation
	sqlStateForeignKeyViolation = "23503" // foreign_key_violation
	sqlStateNotNullViolation    = "23502" // not_null_violation
	sqlStateCheckViolation      = "23514" // check_violation
	sqlStateSerializationFail   = "40001" // serialization_failure
	sqlStateDeadlockDetected    = "40P01" // deadlock_detected
	sqlStateQueryCanceled       = "57014" // query_canceled（statement_timeout 也用它）
	sqlStateTooManyConnections  = "53300" // too_many_connections
)

// retryableStates 是"原样重试有意义"的 SQLSTATE 白名单。
//
// 刻意**没有**包含的几类，理由值得记一下：
//   - 23505 唯一键冲突：重试还是冲突，属于业务冲突而非瞬时故障；
//   - 57014 query_canceled：statement_timeout 触发时用它，重试同样会超时；
//   - 08007 transaction_resolution_unknown：COMMIT 结果未知，
//     重试可能把同一笔业务重复写入，必须交给业务侧幂等处理；
//   - 22xxx 数据异常、42xxx 语法/权限错误：改数据或改代码才有用。
var retryableStates = map[string]struct{}{
	// 事务回滚类：并发冲突，重试即可
	sqlStateSerializationFail: {},
	sqlStateDeadlockDetected:  {},
	// Class 08 连接异常：连接断了，重建连接再试
	"08000": {}, // connection_exception
	"08001": {}, // sqlclient_unable_to_establish_sqlconnection
	"08003": {}, // connection_does_not_exist
	"08004": {}, // sqlserver_rejected_establishment_of_sqlconnection
	"08006": {}, // connection_failure
	"08P01": {}, // protocol_violation
	// Class 53/57 资源与运维介入：瞬时不可用
	sqlStateTooManyConnections: {}, // insufficient_resources
	"57P01":                    {}, // admin_shutdown
	"57P02":                    {}, // crash_shutdown
	"57P03":                    {}, // cannot_connect_now
}

// SQLState 从错误链中取出 PostgreSQL 的 SQLSTATE 码，取不到时返回空串。
//
// 依赖 pgx 的 *pgconn.PgError（gorm 会把驱动错误原样包在 Error 里，
// errors.As 能穿透）。纯网络层的错误没有 SQLSTATE，此时返回空串。
func SQLState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// ConstraintName 取出违反约束时服务端给出的约束名，取不到时返回空串。
//
// 把数据库约束名映射成业务错误时非常好用：
//
//	if postgres.IsUniqueViolation(err) {
//	    switch postgres.ConstraintName(err) {
//	    case "idx_users_email":
//	        return ErrEmailAlreadyTaken
//	    }
//	}
//
// 注意约束名依赖建表时的显式命名（`CONSTRAINT uk_users_email UNIQUE(email)`），
// 让 PostgreSQL 自动生成的 `users_email_key` 之类名字同样可用，但可读性差。
func ConstraintName(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.ConstraintName
	}
	return ""
}

// IsRetryable 判断这个错误"原样重试"是否有意义。
//
// 只按 SQLSTATE 白名单判定，**不看错误文本**，也不把超时算作可重试：
// 超时的写操作可能已经落库，重试有重复写入风险，必须由业务侧用幂等键解决。
// 因此这里对 context.DeadlineExceeded 特意返回 false。
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	_, ok := retryableStates[SQLState(err)]
	return ok
}

// IsIntegrityViolation 判断是否为完整性约束冲突（SQLSTATE Class 23）。
func IsIntegrityViolation(err error) bool {
	return hasSQLStateClass(err, "23")
}

// IsUniqueViolation 判断是否为唯一键/主键冲突（23505）。
func IsUniqueViolation(err error) bool { return SQLState(err) == sqlStateUniqueViolation }

// IsForeignKeyViolation 判断是否为外键冲突（23503）。
func IsForeignKeyViolation(err error) bool { return SQLState(err) == sqlStateForeignKeyViolation }

// IsNotNullViolation 判断是否为非空约束冲突（23502）。
func IsNotNullViolation(err error) bool { return SQLState(err) == sqlStateNotNullViolation }

// IsCheckViolation 判断是否为 CHECK 约束冲突（23514）。
func IsCheckViolation(err error) bool { return SQLState(err) == sqlStateCheckViolation }

// IsSerializationFailure 判断是否为序列化失败（40001）。
//
// 只有在 REPEATABLE READ / SERIALIZABLE 隔离级别下才会出现：
// 事务快照已失效，**必须整个事务重试**（单独重试那条语句没有意义）。
func IsSerializationFailure(err error) bool { return SQLState(err) == sqlStateSerializationFail }

// IsDeadlock 判断是否被服务端判定为死锁而回滚（40P01）。同样需要整个事务重试。
func IsDeadlock(err error) bool { return SQLState(err) == sqlStateDeadlockDetected }

// IsQueryCanceled 判断语句是否被取消（57014）。
//
// 两种情况都落在同一个码上：`statement_timeout` 到点，或有人执行了
// `pg_cancel_backend()`。可用它给"慢查询超时"打一个明确的上报标签。
func IsQueryCanceled(err error) bool { return SQLState(err) == sqlStateQueryCanceled }

// hasSQLStateClass 判断 SQLSTATE 是否属于某个类别（前两位）。
func hasSQLStateClass(err error, class string) bool {
	code := SQLState(err)
	return len(code) >= 2 && code[:2] == class
}

// classifyErr 把错误归到 metrics.Reason 上，供监控打"失败原因"标签。
//
// 分层顺序是有讲究的，与 access/mysql 的 classifyErr 保持同形：
//
//  1. 先认本包的哨兵错误 —— 它们携带的信息（"配置非法" vs "连不上"）
//     比任何驱动错误都更准，且不需要遍历错误链里的驱动细节；
//  2. 再认 GORM 的语义错误 —— gorm.ErrRecordNotFound 在数据库看来
//     根本不是错误（零行是合法结果），必须单独归成 not_found，
//     否则"查无此记录"会被算成错误率（见 metrics.Event.IsError）；
//  3. 最后按 SQLSTATE 归类 —— 它最精确，但只有**服务端**返回的错误才有，
//     dial 失败、TLS 握手失败这类纯网络错误拿不到码，只能落到最后兜底；
//  4. 都认不出才交给 metrics.ClassifyErr（超时/取消/关闭等通用判定）。
//
// 这里刻意**不**复用 IsRetryable：那个函数回答的是"原样重试有没有意义"，
// 与"该打什么标签"是两个问题，混用会让标签含义随重试策略变化而漂移。
func classifyErr(err error) metrics.Reason {
	if err == nil {
		return metrics.ReasonNone
	}

	switch {
	case errors.Is(err, ErrClosed):
		return metrics.ReasonClosed
	case errors.Is(err, ErrNotConnected), errors.Is(err, ErrConnect):
		return metrics.ReasonConnect
	case errors.Is(err, ErrInvalidConfig):
		return metrics.ReasonInvalid
	case errors.Is(err, gorm.ErrRecordNotFound):
		return metrics.ReasonNotFound
	}

	switch SQLState(err) {
	case sqlStateUniqueViolation, sqlStateForeignKeyViolation,
		sqlStateNotNullViolation, sqlStateCheckViolation:
		// 完整性约束冲突：重试无用，属于业务冲突而非系统故障。
		return metrics.ReasonConflict

	case sqlStateSerializationFail, sqlStateDeadlockDetected:
		// 并发冲突与死锁：PostgreSQL 会主动回滚其中一方，整个事务重试即可。
		return metrics.ReasonConflict

	case sqlStateQueryCanceled:
		// statement_timeout 到点，或有人执行了 pg_cancel_backend()。
		return metrics.ReasonTimeout

	case sqlStateTooManyConnections:
		// 服务端连接数打满，属于"被拒绝"，不是网络故障。
		return metrics.ReasonRejected
	}

	// 剩下的按 SQLSTATE 大类兜底，覆盖上面没点名的码。
	switch {
	case hasSQLStateClass(err, "40"): // transaction_rollback
		return metrics.ReasonConflict
	case hasSQLStateClass(err, "22"): // data_exception：数据本身不合法
		return metrics.ReasonInvalid
	case hasSQLStateClass(err, "42"): // syntax_error_or_access_rule_violation
		return metrics.ReasonInvalid
	case hasSQLStateClass(err, "08"): // connection_exception
		return metrics.ReasonConnect
	case hasSQLStateClass(err, "53"): // insufficient_resources
		return metrics.ReasonRejected
	case hasSQLStateClass(err, "57"): // operator_intervention：管理动作或服务端关机
		return metrics.ReasonConnect
	}

	return metrics.ClassifyErr(err)
}
