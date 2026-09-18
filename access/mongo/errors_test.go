package mongo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// 本文件测 errors.go 的对外判定函数。
//
// 这些函数是**业务代码决定"重试 / 提示用户 / 直接失败"的依据**，
// 所以判定必须精确：判宽了会把不可重试的错误反复重试（写放大、长尾延迟），
// 判窄了会把瞬时故障直接暴露给用户。所有断言都基于**服务端错误码与标签**，
// 不依赖错误文本 —— 文本受服务端版本与 locale 影响，没有稳定性保证。

// timeoutErr 是一个实现了 net.Error 的超时错误。
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// ---------------------------------------------------------------------------
// 取值
// ---------------------------------------------------------------------------

func TestServerErrorCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"nil", nil, 0},
		{"普通错误没有服务端码", errors.New("connection reset by peer"), 0},
		{"CommandError", mongo.CommandError{Code: 13, Name: "Unauthorized"}, 13},
		{"WriteError 内的码", mongo.WriteError{Code: 11000}, 11000},
		{
			// 批量写一次可能返回多个码，这里取**第一个** ——
			// 需要全部码时调用方应自己断言 mongo.ServerError 遍历 ErrorCodes()。
			"WriteException 取第一个",
			mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: 11000}, {Code: 11001}}},
			11000,
		},
		{
			// errors.As 必须能穿透包装层，否则业务里 `fmt.Errorf("...: %w", err)`
			// 之后判定就全失效了。
			"包装后仍能取出",
			fmt.Errorf("保存订单失败: %w", mongo.CommandError{Code: 11600}),
			11600,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ServerErrorCode(tc.err); got != tc.want {
				t.Errorf("ServerErrorCode = %d，期望 %d", got, tc.want)
			}
		})
	}
}

func TestServerErrorNameAndMessage(t *testing.T) {
	err := mongo.CommandError{Code: 11000, Name: "DuplicateKey", Message: "E11000 duplicate key error"}

	if got := ServerErrorName(err); got != "DuplicateKey" {
		t.Errorf("ServerErrorName = %q，期望 DuplicateKey", got)
	}
	if got := ServerErrorMessage(err); got != "E11000 duplicate key error" {
		t.Errorf("ServerErrorMessage = %q，期望 E11000 duplicate key error", got)
	}

	// 取不到时返回空串，而不是 panic 或 "unknown"。
	// 注意 mongo.WriteError 不带 Name 字段，所以批量写的子错误这里会是空串。
	if got := ServerErrorName(errors.New("x")); got != "" {
		t.Errorf("非服务端错误的名字应为空串，实际 %q", got)
	}
	if got := ServerErrorMessage(errors.New("x")); got != "" {
		t.Errorf("非服务端错误的文案应为空串，实际 %q", got)
	}
	// WriteException 没有 Name/Message（它们在各子错误里），这里也必须是空串 ——
	// 想拿子错误的码请用 ServerErrorCode。
	we := mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: 11000, Message: "dup"}}}
	if got := ServerErrorName(we); got != "" {
		t.Errorf("WriteException 没有顶层 Name，应为空串，实际 %q", got)
	}
}

func TestHasErrorLabel(t *testing.T) {
	withLabel := mongo.CommandError{Code: 251, Labels: []string{labelTransientTransactionError}}
	if !HasErrorLabel(withLabel, labelTransientTransactionError) {
		t.Error("应能识别出 TransientTransactionError 标签")
	}
	if HasErrorLabel(withLabel, labelUnknownTransactionCommitResult) {
		t.Error("不应识别出未附加的标签")
	}
	if HasErrorLabel(errors.New("x"), labelTransientTransactionError) {
		t.Error("普通错误不应有任何标签")
	}
	// 包装后仍然要能识别。
	wrapped := fmt.Errorf("事务失败: %w", withLabel)
	if !HasErrorLabel(wrapped, labelTransientTransactionError) {
		t.Error("包装后应仍能识别标签")
	}
}

// ---------------------------------------------------------------------------
// 分类判定
// ---------------------------------------------------------------------------

func TestIsNoDocuments(t *testing.T) {
	if !IsNoDocuments(mongo.ErrNoDocuments) {
		t.Error("mongo.ErrNoDocuments 应被识别")
	}
	// 这是**最重要的一条判定**：FindOne 命中零行会返回它，
	// 而"查了但没有"在业务上通常是正常结果。包装后失去识别能力，
	// 就意味着任何一个正常路径都会被计入错误率。
	if !IsNoDocuments(fmt.Errorf("查用户档案: %w", mongo.ErrNoDocuments)) {
		t.Error("包装后的 ErrNoDocuments 应仍能被识别")
	}
	if IsNoDocuments(errors.New("no documents")) {
		t.Error("不应靠错误文本判定")
	}
	if IsNoDocuments(nil) {
		t.Error("nil 不是 ErrNoDocuments")
	}
}

func TestIsDuplicateKey(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"CommandError 11000", mongo.CommandError{Code: 11000}, true},
		{"CommandError 11001（update 触发）", mongo.CommandError{Code: 11001}, true},
		{"WriteException 内的 11000", mongo.WriteException{
			WriteErrors: mongo.WriteErrors{{Code: 11000}},
		}, true},
		{"WriteError 11000", mongo.WriteError{Code: 11000}, true},
		{"其他码不算", mongo.CommandError{Code: 13}, false},
		{"普通错误不算", errors.New("dup"), false},
		{"nil", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsDuplicateKey(tc.err); got != tc.want {
				t.Errorf("IsDuplicateKey = %v，期望 %v（err=%v）", got, tc.want, tc.err)
			}
		})
	}
}

func TestIsNetworkError(t *testing.T) {
	// 驱动的判定依据是 NetworkError **标签**（服务端 4.4+ 会主动附加），
	// 而不是看错误类型 —— 网络故障的表现形式太多，标签才是权威信号。
	if !IsNetworkError(mongo.CommandError{Labels: []string{labelNetworkError}}) {
		t.Error("带 NetworkError 标签的错误应被识别")
	}
	if IsNetworkError(mongo.CommandError{Code: 9001}) {
		t.Error("只有错误码、没有标签时不应判定为网络错误")
	}
	if IsNetworkError(errors.New("connection refused")) {
		t.Error("不应靠错误文本判定")
	}
}

func TestIsTimeout(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"ctx 超时", context.DeadlineExceeded, true},
		{"包装后的 ctx 超时", fmt.Errorf("查询超时: %w", context.DeadlineExceeded), true},
		{"net.Error", timeoutErr{}, true},
		{"服务端 maxTimeMS 到点（码 50）", mongo.CommandError{Code: 50}, true},
		{"按 codeName 识别 MaxTimeMSExpired", mongo.CommandError{Name: "MaxTimeMSExpired"}, true},
		{"NetworkTimeoutError 标签", mongo.CommandError{Labels: []string{"NetworkTimeoutError"}}, true},
		{"ExceededTimeLimitError 标签", mongo.CommandError{Labels: []string{"ExceededTimeLimitError"}}, true},
		// 262 是**服务端**主动放弃（ExceededTimeLimit），它不是"客户端超时"。
		// 这两个概念在重试策略上完全不同，所以这里必须是 false，
		// 由 IsRetryable 通过码白名单把 262 收进去。
		{"服务端 262 不是 IsTimeout", mongo.CommandError{Code: 262}, false},
		{"ctx 取消不是超时", context.Canceled, false},
		{"普通错误", errors.New("bad"), false},
		{"唯一键冲突", mongo.CommandError{Code: 11000}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsTimeout(tc.err); got != tc.want {
				t.Errorf("IsTimeout = %v，期望 %v（err=%v）", got, tc.want, tc.err)
			}
		})
	}
}

func TestIsCursorNotFound(t *testing.T) {
	// 游标失效不是数据错误，而是"这个游标得重开"。
	if !IsCursorNotFound(mongo.CommandError{Code: 43}) {
		t.Error("码 43 应被识别为游标失效")
	}
	if IsCursorNotFound(mongo.CommandError{Code: 26}) {
		t.Error("26 是 NamespaceNotFound，不是游标失效")
	}
}

func TestIsNamespaceNotFound(t *testing.T) {
	// 注意：对不存在的集合执行 find **不会**返回 26，只会返回零行；
	// 只有 drop / createIndexes 这类 DDL 才会。
	if !IsNamespaceNotFound(mongo.CommandError{Code: 26}) {
		t.Error("码 26 应被识别为库/集合不存在")
	}
	if IsNamespaceNotFound(mongo.ErrNoDocuments) {
		t.Error("ErrNoDocuments 不是 26 —— 两者要区分开")
	}
}

func TestIsUnauthorized(t *testing.T) {
	for _, code := range []int32{13, 18} {
		if !IsUnauthorized(mongo.CommandError{Code: code}) {
			t.Errorf("码 %d 应被识别为鉴权失败", code)
		}
	}
	if IsUnauthorized(mongo.CommandError{Code: 11000}) {
		t.Error("唯一键冲突不是鉴权问题")
	}
	// 鉴权失败几乎总是配置问题，重试没有意义 ——
	// IsRetryable 必须对它返回 false。
	if IsRetryable(mongo.CommandError{Code: 18}) {
		t.Error("鉴权失败不应被判为可重试")
	}
}

func TestIsWriteConcernError(t *testing.T) {
	// 两种表现形式都要覆盖：WriteException 里的 WriteConcernError，
	// 以及作为普通命令错误出现的独立错误码（副本集降级时会这样）。
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			"WriteException 里的 WriteConcernError",
			mongo.WriteException{WriteConcernError: &mongo.WriteConcernError{Code: 64, Message: "waiting for replication timed out"}},
			true,
		},
		{"独立命令错误 64", mongo.CommandError{Code: 64}, true},
		{"独立命令错误 79", mongo.CommandError{Code: 79}, true},
		{"独立命令错误 100", mongo.CommandError{Code: 100}, true},
		{"普通写错误不算", mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: 11000}}}, false},
		{"其他码不算", mongo.CommandError{Code: 13}, false},
		{"nil", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsWriteConcernError(tc.err); got != tc.want {
				t.Errorf("IsWriteConcernError = %v，期望 %v", got, tc.want)
			}
		})
	}
}

func TestTransactionLabels(t *testing.T) {
	// 这两个标签的区别至关重要，混用会造成重复写入：
	//
	//	TransientTransactionError        → 事务快照失效，**整个事务重跑**
	//	UnknownTransactionCommitResult   → COMMIT 的响应丢了，**只能重试 commit**
	transient := mongo.CommandError{Code: 251, Labels: []string{labelTransientTransactionError}}
	commitUnknown := mongo.CommandError{Code: 91, Labels: []string{labelUnknownTransactionCommitResult}}

	if !IsTransactionRetryable(transient) {
		t.Error("TransientTransactionError 应被识别为可整体重试")
	}
	if IsCommitRetryable(transient) {
		t.Error("TransientTransactionError 不是 UnknownTransactionCommitResult")
	}

	if !IsCommitRetryable(commitUnknown) {
		t.Error("UnknownTransactionCommitResult 应被识别为可重试 commit")
	}
	if IsTransactionRetryable(commitUnknown) {
		t.Error("UnknownTransactionCommitResult **不是**可整体重试 —— 事务可能已提交，重跑会重复写入")
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},

		// 标签优先：标签是服务端主动附加的语义化标记，比错误码更贴近"该怎么办"。
		{"TransientTransactionError 标签", mongo.CommandError{Labels: []string{labelTransientTransactionError}}, true},
		{"RetryableWriteError 标签", mongo.CommandError{Labels: []string{labelRetryableWriteError}}, true},
		{"NetworkError 标签", mongo.CommandError{Labels: []string{labelNetworkError}}, true},

		// 码白名单：这份清单与驱动内部的 retryableCodes 保持一致。
		{"HostUnreachable 6", mongo.CommandError{Code: 6}, true},
		{"HostNotFound 7", mongo.CommandError{Code: 7}, true},
		{"NetworkTimeout 89", mongo.CommandError{Code: 89}, true},
		{"ShutdownInProgress 91", mongo.CommandError{Code: 91}, true},
		{"ReadConcernMajorityNotAvailableYet 134", mongo.CommandError{Code: 134}, true},
		{"PrimarySteppedDown 189", mongo.CommandError{Code: 189}, true},
		{"ExceededTimeLimit 262", mongo.CommandError{Code: 262}, true},
		{"SocketException 9001", mongo.CommandError{Code: 9001}, true},
		{"NotWritablePrimary 10107", mongo.CommandError{Code: 10107}, true},
		{"InterruptedAtShutdown 11600", mongo.CommandError{Code: 11600}, true},
		{"InterruptedDueToReplStateChange 11602", mongo.CommandError{Code: 11602}, true},
		{"NotPrimaryNoSecondaryOk 13435", mongo.CommandError{Code: 13435}, true},
		{"NotPrimaryOrSecondary 13436", mongo.CommandError{Code: 13436}, true},

		// 不该重试的。
		{"唯一键冲突", mongo.CommandError{Code: 11000}, false},
		{"鉴权失败", mongo.CommandError{Code: 13}, false},
		{"参数错误", mongo.CommandError{Code: 2}, false},
		{"普通错误", errors.New("bad"), false},

		// ⚠️ 客户端超时**不**算可重试。
		//
		// ctx 超时的写操作可能已经落库，重试会造成重复写入 ——
		// 这必须由业务侧用幂等键解决，而不是交给通用重试逻辑。
		// 注意与服务端的 262 区分：后者是服务端主动放弃，确实在白名单里。
		{"ctx 超时不可重试", context.DeadlineExceeded, false},
		{"ctx 取消不可重试", context.Canceled, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsRetryable(tc.err); got != tc.want {
				t.Errorf("IsRetryable = %v，期望 %v（err=%v）", got, tc.want, tc.err)
			}
		})
	}
}

func TestRetryableCodesMatchDriverList(t *testing.T) {
	// 把清单本身钉住：这份清单与驱动的 x/mongo/driver/errors.go 一致，
	// 少一个码会让某些主从切换场景不再自动重试（用户看到失败），
	// 多一个码则可能把不该重试的操作反复重试。
	want := map[int]bool{
		6: true, 7: true, 89: true, 91: true, 134: true, 189: true, 262: true,
		9001: true, 10107: true, 11600: true, 11602: true, 13435: true, 13436: true,
	}

	for code := range want {
		if _, ok := retryableCodes[code]; !ok {
			t.Errorf("retryableCodes 缺少错误码 %d", code)
		}
	}
	for code := range retryableCodes {
		if !want[code] {
			t.Errorf("retryableCodes 多出错误码 %d（与驱动的清单不一致？）", code)
		}
	}
	if len(retryableCodes) != len(want) {
		t.Errorf("retryableCodes 有 %d 项，期望 %d 项", len(retryableCodes), len(want))
	}
}

func TestWriteConcernCodes(t *testing.T) {
	// 这三个码是"确认级别未被满足"，不是"写失败" ——
	// 它们会以普通命令错误的形式出现（副本集降级、w 值超过可用节点数）。
	want := []int{64, 79, 100}
	for _, code := range want {
		if _, ok := writeConcernCodes[code]; !ok {
			t.Errorf("writeConcernCodes 缺少错误码 %d", code)
		}
	}
	if len(writeConcernCodes) != len(want) {
		t.Errorf("writeConcernCodes 有 %d 项，期望 %d 项", len(writeConcernCodes), len(want))
	}
}

func TestSensitiveQueryKeysAreLowercase(t *testing.T) {
	// 脱敏表里的键必须全小写：MongoDB 的 URI 参数名大小写不敏感，
	// 查表前会 ToLower，键写成大写就永远命中不了 —— 而那种 bug
	// 的表现是"口令明文进了日志"，不会报错。
	for key := range sensitiveQueryKeys {
		if key != strings.ToLower(key) {
			t.Errorf("sensitiveQueryKeys 里的键 %q 不是全小写", key)
		}
	}
	// 两个已知的私钥口令参数都要在表里。
	for _, must := range []string{"tlscertificatekeyfilepassword", "sslclientcertificatekeypassword"} {
		if _, ok := sensitiveQueryKeys[must]; !ok {
			t.Errorf("sensitiveQueryKeys 缺少 %q", must)
		}
	}
}

// ---------------------------------------------------------------------------
// classifyErr：把错误细化成 metrics 的低基数原因
// ---------------------------------------------------------------------------

func TestClassifyErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string // metrics.Reason 的字符串值
	}{
		{"nil", nil, "none"},

		// 最重要的一条：查不到文档是**正常业务结果**，不是故障。
		{"ErrNoDocuments", mongo.ErrNoDocuments, "not_found"},
		{"包装后的 ErrNoDocuments", fmt.Errorf("查档案: %w", mongo.ErrNoDocuments), "not_found"},

		{"客户端已断开", mongo.ErrClientDisconnected, "closed"},
		{"本包 ErrClosed", ErrClosed, "closed"},
		{"本包 ErrNotConnected", ErrNotConnected, "connect"},
		{"本包 ErrConnect", ErrConnect, "connect"},
		{"本包 ErrInvalidConfig", ErrInvalidConfig, "invalid"},

		// 业务冲突与并发争用：不是"数据库坏了"，不该和真正的故障混在一条曲线里。
		{"唯一键冲突", mongo.CommandError{Code: 11000}, "conflict"},
		{"事务可整体重试（并发争用）", mongo.CommandError{Labels: []string{labelTransientTransactionError}}, "conflict"},

		{"鉴权失败（配置问题）", mongo.CommandError{Code: 18}, "invalid"},

		{"游标失效", mongo.CommandError{Code: 43}, "not_found"},
		{"集合不存在", mongo.CommandError{Code: 26}, "not_found"},

		// 写确认级别没被满足：命令执行了、数据可能写进去了，但没写"稳"。
		{"写关注未满足", mongo.CommandError{Code: 64}, "rejected"},

		{"ctx 超时", context.DeadlineExceeded, "timeout"},
		{"net 超时", timeoutErr{}, "timeout"},
		{"网络错误", mongo.CommandError{Labels: []string{labelNetworkError}}, "connect"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := string(classifyErr(tc.err))
			if got != tc.want {
				t.Errorf("classifyErr = %q，期望 %q（err=%v）", got, tc.want, tc.err)
			}
		})
	}
}

func TestClassifyErrReasonsAreLowCardinality(t *testing.T) {
	// classifyErr 的返回值会直接成为指标标签，所以它必须是**闭集**：
	// 无论喂什么错误，都不能冒出新的取值 —— 那会造成标签基数爆炸。
	inputs := []error{
		nil,
		errors.New("随便一个错误"),
		fmt.Errorf("嵌套: %w", errors.New("底层")),
		mongo.CommandError{Code: 12345, Message: "某个未来才有的错误码"},
		mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: 99999}}},
		context.Canceled,
		timeoutErr{},
	}
	allowed := map[string]bool{
		"none": true, "timeout": true, "canceled": true, "closed": true,
		"connect": true, "not_found": true, "invalid": true, "conflict": true,
		"rejected": true, "unknown": true,
	}
	for _, in := range inputs {
		got := string(classifyErr(in))
		if !allowed[got] {
			t.Errorf("classifyErr(%v) 返回了清单外的取值 %q", in, got)
		}
	}
}
