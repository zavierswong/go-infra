package mongo

import (
	"errors"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// 本包的哨兵错误。调用方应使用 errors.Is 判定，而不是匹配错误文本。
var (
	// ErrInvalidConfig 配置非法（URI 缺失、语法错误、scheme 不支持、取值疑似单位写错等）。
	ErrInvalidConfig = errors.New("mongo: 配置非法")

	// ErrConnect 无法建立或恢复连接（网络不通、认证失败、副本集名不符、
	// serverSelectionTimeout 内选不到可用节点等）。
	ErrConnect = errors.New("mongo: 无法连接数据库")

	// ErrClosed 客户端已关闭。
	ErrClosed = errors.New("mongo: 客户端已关闭")

	// ErrNotConnected 客户端处于未连接状态（尚未初始化成功，或连接已被回收）。
	ErrNotConnected = errors.New("mongo: 尚未建立连接")
)

// ---------------------------------------------------------------------------
// 服务端错误码判定
//
// MongoDB 的每个服务端错误都带一个数字码与一个稳定的**名字**（codeName）。
// 驱动把码放在 mongo.CommandError / mongo.WriteError / mongo.WriteException 里，
// 这三者都实现了 mongo.ServerError 接口。本组函数负责从错误链中取出并分类 ——
// 业务代码据此决定"重试 / 提示用户 / 直接失败"，而**不要去匹配错误文本**
// （文本受服务端版本与 locale 影响，且没有稳定性保证）。
//
// 错误码全表见 https://www.mongodb.com/docs/manual/reference/error-codes/
// ---------------------------------------------------------------------------

// 常用服务端错误码。集中定义，便于阅读与测试。
const (
	errCodeHostUnreachable       = 6     // HostUnreachable
	errCodeHostNotFound          = 7     // HostNotFound
	errCodeNetworkTimeout        = 89    // NetworkTimeout
	errCodeShutdownInProgress    = 91    // ShutdownInProgress
	errCodeReadConcernMajority   = 134   // ReadConcernMajorityNotAvailableYet
	errCodePrimarySteppedDown    = 189   // PrimarySteppedDown
	errCodeExceededTimeLimit     = 262   // ExceededTimeLimit
	errCodeSocketException       = 9001  // SocketException
	errCodeNotWritablePrimary    = 10107 // NotWritablePrimary
	errCodeInterruptedAtShutdown = 11600 // InterruptedAtShutdown
	errCodeInterruptedDueToRepl  = 11602 // InterruptedDueToReplStateChange
	errCodeNotPrimaryNoSecondary = 13435 // NotPrimaryNoSecondaryOk
	errCodeNotPrimaryOrSecondary = 13436 // NotPrimaryOrSecondary

	errCodeUnauthorized              = 13    // Unauthorized
	errCodeAuthenticationFailed      = 18    // AuthenticationFailed
	errCodeNamespaceNotFound         = 26    // NamespaceNotFound（集合/库不存在）
	errCodeCursorNotFound            = 43    // CursorNotFound（游标超时或已被服务端回收）
	errCodeWriteConcernFailed        = 64    // WriteConcernFailed
	errCodeUnknownReplWriteConcern   = 79    // UnknownReplWriteConcern（w 值无法满足）
	errCodeUnsatisfiableWriteConcern = 100   // UnsatisfiableWriteConcern
	errCodeDuplicateKey              = 11000 // DuplicateKey
	errCodeDuplicateKeyUpdate        = 11001 // DuplicateKey（update 触发）
	errCodeNoSuchTransaction         = 251   // NoSuchTransaction
)

// writeConcernCodes 是"确认级别未被满足"的独立错误码。
//
// 这三个码会作为**普通命令错误**出现（不是 WriteException 里的 WriteConcernError），
// 典型场景是副本集降级、或 w 值超过了当前可用的节点数。
var writeConcernCodes = map[int]struct{}{
	errCodeWriteConcernFailed:        {},
	errCodeUnknownReplWriteConcern:   {},
	errCodeUnsatisfiableWriteConcern: {},
}

// retryableCodes 是"原样重试有意义"的服务端错误码白名单。
//
// **这份清单与驱动内部的 retryableCodes 保持一致**
// （见 x/mongo/driver/errors.go），不是凭经验列的：
// 副本集主从切换、节点重启、网络抖动都落在这里面。
var retryableCodes = map[int]struct{}{
	errCodeHostUnreachable:       {},
	errCodeHostNotFound:          {},
	errCodeNetworkTimeout:        {},
	errCodeShutdownInProgress:    {},
	errCodeReadConcernMajority:   {},
	errCodePrimarySteppedDown:    {},
	errCodeExceededTimeLimit:     {},
	errCodeSocketException:       {},
	errCodeNotWritablePrimary:    {},
	errCodeInterruptedAtShutdown: {},
	errCodeInterruptedDueToRepl:  {},
	errCodeNotPrimaryNoSecondary: {},
	errCodeNotPrimaryOrSecondary: {},
}

// 错误标签。标签是**服务端主动附加**的语义化标记，比错误码更贴近"该怎么办"：
// 同一个码在不同上下文下重试策略可能不同，标签则直接告诉你答案。
const (
	// labelTransientTransactionError 标记"事务可以整体重试"。
	// 拿到它说明事务快照已失效，**必须重跑整个事务**（单独重试某条语句没有意义）。
	labelTransientTransactionError = "TransientTransactionError"

	// labelUnknownTransactionCommitResult 标记"COMMIT 结果未知"。
	//
	// 这类错误**只能重试 commit**，绝不能重跑整个事务 ——
	// 事务可能已经提交成功，重跑会造成重复写入。
	labelUnknownTransactionCommitResult = "UnknownTransactionCommitResult"

	// labelRetryableWriteError 标记"这条写操作可以重试"（服务端 4.4+ 会主动附加）。
	labelRetryableWriteError = "RetryableWriteError"

	// labelNetworkError 标记链路层故障。
	labelNetworkError = "NetworkError"
)

// ---------------------------------------------------------------------------
// 取值
// ---------------------------------------------------------------------------

// ServerErrorCode 取出服务端错误码，取不到时返回 0。
//
// 依赖驱动的 mongo.ServerError 接口（mongo.CommandError、mongo.WriteError、
// mongo.WriteException、mongo.BulkWriteException 都实现了它），
// errors.As 能穿透 mongo 包的包装层。纯网络层错误没有服务端码，此时返回 0。
//
// 批量写可能一次返回多个错误码，这里返回**第一个** ——
// 需要全部码时请直接断言 mongo.ServerError 自己遍历 ErrorCodes()。
func ServerErrorCode(err error) int {
	var se mongo.ServerError
	if errors.As(err, &se) {
		if codes := se.ErrorCodes(); len(codes) > 0 {
			return codes[0]
		}
	}
	return 0
}

// ServerErrorName 取出服务端错误码的**名字**（如 "DuplicateKey"、"PrimarySteppedDown"）。
//
// 比数字码可读得多，适合打日志。取不到时返回空串 ——
// 注意 mongo.WriteError 不带 Name 字段，所以批量写的子错误这里会拿到空串，
// 此时用 ServerErrorCode 更可靠。
func ServerErrorName(err error) string {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return ce.Name
	}
	return ""
}

// ServerErrorMessage 取出服务端返回的错误文案，取不到时返回空串。
func ServerErrorMessage(err error) string {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return ce.Message
	}
	return ""
}

// HasErrorLabel 判断错误链上是否带有某个标签。
func HasErrorLabel(err error, label string) bool {
	var le mongo.LabeledError
	return errors.As(err, &le) && le.HasErrorLabel(label)
}

// ---------------------------------------------------------------------------
// 分类判断
// ---------------------------------------------------------------------------

// IsNoDocuments 判断是否为"查不到文档"（mongo.ErrNoDocuments）。
//
// 这是本包最重要的一条判定：它对应 SQL 的 sql.ErrNoRows、Redis 的 goredis.Nil，
// 在业务上通常是**正常结果**（例如"这个用户还没建过档案"）。
// FindOne 命中零行就会返回它，如果把它计入错误率，
// 任何一个"查了但没有"的正常路径都会表现为故障。
func IsNoDocuments(err error) bool { return errors.Is(err, mongo.ErrNoDocuments) }

// IsDuplicateKey 判断是否为唯一索引冲突（11000 / 11001 等）。
//
// 驱动自带的 mongo.IsDuplicateKeyError 会额外识别分片集合与 mongos 的几种变形，
// 所以这里直接复用它，不自己判码。
func IsDuplicateKey(err error) bool { return mongo.IsDuplicateKeyError(err) }

// IsNetworkError 判断是否为链路层错误（带 NetworkError 标签）。
func IsNetworkError(err error) bool { return mongo.IsNetworkError(err) }

// IsTimeout 判断是否为超时。
//
// 驱动自带的 mongo.IsTimeout 覆盖面比"看 ctx 错误"广得多，它同时识别：
// 连接池等待超时（**这是 Go 驱动里唯一会报出来的池超时**，见 README 的注意事项）、
// net.Error 的 Timeout、服务端 maxTimeMS 到点、以及上述两种超时标签。
func IsTimeout(err error) bool { return mongo.IsTimeout(err) }

// IsCursorNotFound 判断游标是否已失效（43）。
//
// 常见于长时间遍历（服务端默认 10 分钟空闲即回收游标）或主从切换。
// 它不是数据错误，而是"这个游标得重开"，所以**不能**当成查询失败上报。
func IsCursorNotFound(err error) bool { return ServerErrorCode(err) == errCodeCursorNotFound }

// IsNamespaceNotFound 判断库或集合不存在（26）。
//
// 注意：对不存在的集合执行 `find` **不会**返回它，只会返回零行
// （IsNoDocuments）；`drop`、`createIndexes` 这类 DDL 才会。
func IsNamespaceNotFound(err error) bool { return ServerErrorCode(err) == errCodeNamespaceNotFound }

// IsUnauthorized 判断是否为鉴权/授权失败（13/18）。
//
// 它几乎总是配置问题（账号密码错、authSource 选错、角色不足），
// 重试没有意义，应当直接失败并告警。
func IsUnauthorized(err error) bool {
	code := ServerErrorCode(err)
	return code == errCodeUnauthorized || code == errCodeAuthenticationFailed
}

// IsWriteConcernError 判断写操作的**确认级别**是否未被满足（64 等）。
//
// 这类错误的语义很微妙：命令本身执行成功了，只是"写入没有被复制到足够多的节点"
// 或"没有落盘"。inspect 数据时会发现文档**可能已经在主节点上**，
// 所以它不是"没写进去"，而是"没写稳" —— 不能简单地当失败重试。
func IsWriteConcernError(err error) bool {
	var we mongo.WriteException
	if errors.As(err, &we) && we.WriteConcernError != nil {
		return true
	}
	// 也可能以独立命令错误的形式出现（见 writeConcernCodes）。
	_, ok := writeConcernCodes[ServerErrorCode(err)]
	return ok
}

// IsTransactionRetryable 判断事务是否可以**整体重试**（TransientTransactionError 标签）。
//
// 拿到 true 时应当丢弃当前事务，重新 `StartTransaction` 并重跑全部语句。
func IsTransactionRetryable(err error) bool {
	return HasErrorLabel(err, labelTransientTransactionError)
}

// IsCommitRetryable 判断事务是否可以**只重试 commit**（UnknownTransactionCommitResult 标签）。
//
// 与 IsTransactionRetryable 的区别至关重要：
// 拿到这个标签说明 COMMIT 的响应丢了，事务**可能已经提交**。
// 重跑整个事务会造成重复写入；正确做法是只重新执行 commit，
// 并让 commit 本身带幂等语义（多数情况下由服务端的事务表保证）。
func IsCommitRetryable(err error) bool {
	return HasErrorLabel(err, labelUnknownTransactionCommitResult)
}

// IsRetryable 判断这个错误"原样重试"是否有意义。
//
// 判定顺序：先看**瞬时**标签与网络错误，再看服务端码白名单。
// 刻意**不看错误文本**，也**不把客户端超时算作可重试**：
// ctx 超时的写操作可能已经落库，重试有重复写入风险，必须由业务侧用幂等键解决。
// 因此 context.DeadlineExceeded 在这里返回 false
// （注意与服务端的 errCodeExceededTimeLimit=262 区分：后者是服务端主动放弃，
// 可以安全重试，它确实在下面的白名单里）。
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if HasErrorLabel(err, labelTransientTransactionError) ||
		HasErrorLabel(err, labelRetryableWriteError) ||
		HasErrorLabel(err, labelNetworkError) {
		return true
	}
	if IsNetworkError(err) {
		return true
	}
	_, ok := retryableCodes[ServerErrorCode(err)]
	return ok
}
