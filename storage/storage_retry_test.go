package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// ---- 通用 mock ----

// mockStorage 实现拆分后的 Storage + MultipartUploader，
// 各方法可注入失败行为与调用计数。
type mockStorage struct {
	putCalls  int
	getCalls  int
	pageCalls int

	putErrs   []error // 第 n 次调用的错误（nil 成功）；超出取最后一个
	putBodies []string
	getErrs   []error

	pageErrs  []error
	pagePages [][]string
	pageNexts []string

	initCalls int
	initErrs  []error

	consumeBody bool // put 时是否消费 body（模拟真实 SDK 读完流）
}

// netError 模拟 SDK 在网络故障时透传的底层错误（*url.Error / *net.OpError
// 都实现 net.Error）。重试分类只对这类"确认是网络故障"的未知来源错误放行。
type netError struct{ msg string }

func (e *netError) Error() string   { return e.msg }
func (e *netError) Timeout() bool   { return true }
func (e *netError) Temporary() bool { return true }

func netFail(msg string) error { return &netError{msg: msg} }

func (m *mockStorage) PutObject(_ context.Context, _ string, reader io.Reader, _ int64, _ string) error {
	m.putCalls++
	data, _ := io.ReadAll(reader)
	m.putBodies = append(m.putBodies, string(data))
	return m.nthErr(m.putErrs, m.putCalls)
}

func (m *mockStorage) GetObject(_ context.Context, _ string) (io.ReadCloser, error) {
	m.getCalls++
	return io.NopCloser(strings.NewReader("x")), m.nthErr(m.getErrs, m.getCalls)
}

func (m *mockStorage) DeleteObject(context.Context, string) error { return nil }
func (m *mockStorage) GetObjectURL(string) string                 { return "http://mock/bucket/key" }
func (m *mockStorage) GetPresignedURL(context.Context, string, time.Duration) (string, error) {
	return "http://mock/signed", nil
}
func (m *mockStorage) PresignedPutURL(context.Context, string, string, time.Duration) (string, error) {
	return "http://mock/signed-put", nil
}
func (m *mockStorage) IsExist(context.Context, string) (bool, error) { return true, nil }
func (m *mockStorage) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}
func (m *mockStorage) CopyObject(context.Context, string, string) error { return nil }
func (m *mockStorage) GetObjectInfo(context.Context, string) (*ObjectInfo, error) {
	return &ObjectInfo{Key: "k"}, nil
}
func (m *mockStorage) GetProvider(context.Context) string { return "mock" }

func (m *mockStorage) ListObjectsPage(_ context.Context, _ string, _ int, marker string) ([]string, string, error) {
	m.pageCalls++
	i := m.pageCalls - 1
	if i < len(m.pageErrs) && m.pageErrs[i] != nil {
		return nil, "", m.pageErrs[i]
	}
	var keys []string
	if i < len(m.pagePages) {
		keys = m.pagePages[i]
	}
	next := ""
	if i < len(m.pageNexts) {
		next = m.pageNexts[i]
	}
	_ = marker
	return keys, next, nil
}

func (m *mockStorage) InitMultipartUpload(context.Context, string, string) (string, error) {
	m.initCalls++
	return "upload-id", m.nthErr(m.initErrs, m.initCalls)
}
func (m *mockStorage) PresignPartURL(context.Context, string, string, int, time.Duration) (string, error) {
	return "http://mock/part", nil
}
func (m *mockStorage) CompleteMultipartUpload(context.Context, string, string, []MultipartUploadPart) error {
	return nil
}
func (m *mockStorage) AbortMultipartUpload(context.Context, string, string) error { return nil }

// nthErr 取第 n 次调用的错误；未设置返回 nil。
func (m *mockStorage) nthErr(errs []error, n int) error {
	if len(errs) == 0 {
		return nil
	}
	if n-1 < len(errs) {
		return errs[n-1]
	}
	return errs[len(errs)-1]
}

// 编译期约束：mock 同时实现两个接口。
var (
	_ Storage           = (*mockStorage)(nil)
	_ MultipartUploader = (*mockStorage)(nil)
)

func newTestRetry(cfg Config) (*RetryStorage, *mockStorage) {
	m := &mockStorage{}
	rs := NewRetryStorage(m, cfg.MaxRetries, 10*time.Millisecond)
	rs.SetLimits(Limits{
		MaxFileSize:         cfg.MaxFileSize,
		MaxParts:            cfg.MaxParts,
		PresignedURLExpires: time.Duration(cfg.PresignedURLExpires) * time.Second,
	})
	return rs, m
}

// TestRetryGenericError 通用错误（无状态码 → 视为网络错误）按次数重试。
func TestRetryGenericError(t *testing.T) {
	rs, m := newTestRetry(Config{MaxRetries: 2})
	m.putErrs = []error{netFail("net broken"), netFail("net broken"), nil}

	err := rs.PutObject(context.Background(), "k", strings.NewReader("body"), 4, "text/plain")
	if err != nil {
		t.Fatalf("重试后应成功: %v", err)
	}
	if m.putCalls != 3 {
		t.Fatalf("应尝试 3 次, got %d", m.putCalls)
	}
}

// TestNonRetryableNotRetried 403/404/欠费类 4xx 不重试，立即上抛。
func TestNonRetryableNotRetried(t *testing.T) {
	for _, sc := range []int{403, 404, 402} {
		rs, m := newTestRetry(Config{MaxRetries: 3})
		m.putErrs = []error{minio.ErrorResponse{StatusCode: sc, Code: "X"}}

		err := rs.PutObject(context.Background(), "k", strings.NewReader("b"), 1, "text/plain")
		if err == nil {
			t.Fatalf("sc=%d 应返回错误", sc)
		}
		if m.putCalls != 1 {
			t.Errorf("sc=%d 不应重试, attempts=%d", sc, m.putCalls)
		}
	}
}

// TestRetryableStatuses 408/429/5xx 重试。
func TestRetryableStatuses(t *testing.T) {
	for _, sc := range []int{408, 429, 500, 503} {
		rs, m := newTestRetry(Config{MaxRetries: 2})
		m.putErrs = []error{
			minio.ErrorResponse{StatusCode: sc},
			minio.ErrorResponse{StatusCode: sc},
			nil,
		}
		if err := rs.PutObject(context.Background(), "k", strings.NewReader("b"), 1, "text/plain"); err != nil {
			t.Errorf("sc=%d 重试后应成功: %v", sc, err)
		}
		if m.putCalls != 3 {
			t.Errorf("sc=%d 应尝试 3 次, got %d", sc, m.putCalls)
		}
	}
}

// TestPutObjectReplayBodyWithSeeker 可 Seek 的 reader：重试自动回卷，
// 每次尝试收到的都是完整 body（修复"重试上传空对象"缺陷）。
func TestPutObjectReplayBodyWithSeeker(t *testing.T) {
	rs, m := newTestRetry(Config{MaxRetries: 2})
	m.consumeBody = true // 模拟 SDK 读完流后才报错
	m.putErrs = []error{netFail("net 1"), netFail("net 2"), nil}

	body := "complete-body"
	if err := rs.PutObject(context.Background(), "k", bytes.NewReader([]byte(body)), int64(len(body)), "text/plain"); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if m.putCalls != 3 {
		t.Fatalf("应尝试 3 次, got %d", m.putCalls)
	}
	for i, got := range m.putBodies {
		if got != body {
			t.Errorf("第 %d 次尝试应收完整 body %q, got %q", i+1, body, got)
		}
	}
}

// TestPutObjectNotReplayableStopsRetry 不可回卷的 reader：消费后不再重试。
func TestPutObjectNotReplayableStopsRetry(t *testing.T) {
	rs, m := newTestRetry(Config{MaxRetries: 3})
	m.consumeBody = true
	m.putErrs = []error{netFail("boom")} // 之后全部沿用该错误

	pr, pw := io.Pipe() // 不可 Seek
	go func() { _, _ = pw.Write([]byte("data")); _ = pw.Close() }()

	err := rs.PutObject(context.Background(), "k", pr, 4, "text/plain")
	if !errors.Is(err, ErrNotReplayable) {
		t.Fatalf("应返回 ErrNotReplayable, got %v", err)
	}
	if m.putCalls != 1 {
		t.Fatalf("不可重放时只应尝试 1 次, got %d", m.putCalls)
	}
}

// TestMaxFileSize 超限直接拒绝，不打到存储。
func TestMaxFileSize(t *testing.T) {
	rs, m := newTestRetry(Config{MaxRetries: 2, MaxFileSize: 10})

	err := rs.PutObject(context.Background(), "k", strings.NewReader("big body"), 100, "text/plain")
	if err == nil {
		t.Fatal("超限应报错")
	}
	if m.putCalls != 0 {
		t.Fatalf("超限不应打到存储, putCalls=%d", m.putCalls)
	}

	// 未设置限制时不拦截
	rs2, m2 := newTestRetry(Config{MaxRetries: 1})
	if err := rs2.PutObject(context.Background(), "k", strings.NewReader("big body"), 100, "text/plain"); err != nil {
		t.Fatalf("未设限制不应报错: %v", err)
	}
	_ = m2
}

// TestMaxParts 分片数超限拒绝。
func TestMaxParts(t *testing.T) {
	rs, _ := newTestRetry(Config{MaxParts: 3})
	parts := make([]MultipartUploadPart, 5)

	if err := rs.CompleteMultipartUpload(context.Background(), "k", "id", parts); err == nil {
		t.Fatal("分片数超限应报错")
	}
}

// TestListObjectsPageRetry 分页只重试当前页，已完成页不重复累积。
func TestListObjectsPageRetry(t *testing.T) {
	rs, m := newTestRetry(Config{MaxRetries: 2})
	m.pageErrs = []error{netFail("net")}
	m.pagePages = [][]string{nil, {"a", "b"}} // 第 1 次失败，第 2 次成功
	m.pageNexts = []string{"", "b", ""}

	keys, next, err := rs.ListObjectsPage(context.Background(), "p/", 2, "")
	if err != nil {
		t.Fatalf("ListObjectsPage: %v", err)
	}
	if m.pageCalls != 2 {
		t.Fatalf("应尝试 2 次, got %d", m.pageCalls)
	}
	if fmt.Sprint(keys) != "[a b]" || next != "b" {
		t.Fatalf("keys=%v next=%q", keys, next)
	}
}

// TestListObjectsPaginated 全量 List 内部循环分页直到 next 为空。
func TestListObjectsPaginated(t *testing.T) {
	rs, m := newTestRetry(Config{MaxRetries: 1})
	m.pagePages = [][]string{{"a"}, {"b", "c"}}
	m.pageNexts = []string{"a", ""}

	keys, err := rs.ListObjects(context.Background(), "p/")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if fmt.Sprint(keys) != "[a b c]" {
		t.Fatalf("应跨页累积 [a b c], got %v", keys)
	}
	if m.pageCalls != 2 {
		t.Fatalf("应翻 2 页, got %d", m.pageCalls)
	}
}

// TestRetryCtxCancel ctx 取消后立即终止，不再继续尝试。
func TestRetryCtxCancel(t *testing.T) {
	rs, m := newTestRetry(Config{MaxRetries: 5, RetryDelay: 1000}) // 1s 退避
	m.putErrs = []error{netFail("always fail")}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	if err := rs.PutObject(ctx, "k", strings.NewReader("b"), 1, "text/plain"); err == nil {
		t.Fatal("ctx 取消应返回错误")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("ctx 取消应立即退出, 耗时 %v", elapsed)
	}
}

// ---- 集成测试（本地 MinIO 可用时执行） ----

// TestStorageOperations 测试存储操作
func TestStorageOperations(t *testing.T) {
	cfg := Config{
		Type:       StorageTypeMinio,
		Endpoint:   "localhost:9000",
		Bucket:     "test-bucket",
		SecretID:   "minioadmin",
		SecretKey:  "minioadmin",
		UseSSL:     false,
		MaxRetries: 3,
		RetryDelay: 100,
		Timeout:    10,
		PublicRead: true, // 测试拼接 URL 语义
	}

	if err := Setup(cfg); err != nil {
		t.Skipf("跳过测试，存储初始化失败: %v", err)
		return
	}

	ctx := context.Background()
	testKey := "test/test-file.txt"
	testContent := []byte("Hello, Storage Test!")

	t.Run("PutObject", func(t *testing.T) {
		if err := PutFile(ctx, testKey, bytes.NewReader(testContent), int64(len(testContent)), "text/plain"); err != nil {
			t.Errorf("上传文件失败: %v", err)
		}
	})

	t.Run("IsExist", func(t *testing.T) {
		exists, err := IsFileExist(ctx, testKey)
		if err != nil {
			t.Errorf("检查文件存在失败: %v", err)
		}
		if !exists {
			t.Error("文件应该存在")
		}
	})

	t.Run("GetObjectURL", func(t *testing.T) {
		url := GetFileURL(testKey)
		// 公开读桶：拼接 URL 不应重复出现两次 bucket。
		if strings.Count(url, "test-bucket") != 1 {
			t.Errorf("URL 不应重复拼 bucket: %s", url)
		}
	})

	t.Run("GetPresignedURL", func(t *testing.T) {
		url, err := GetFilePresignedURL(ctx, testKey, 1*time.Hour)
		if err != nil || url == "" {
			t.Errorf("预签名 URL 失败: %v, %q", err, url)
		}
	})

	t.Run("GetObjectInfo", func(t *testing.T) {
		info, err := GetStorage().GetObjectInfo(ctx, testKey)
		if err != nil {
			t.Fatalf("获取对象信息失败: %v", err)
		}
		if info.Size != int64(len(testContent)) {
			t.Errorf("文件大小不匹配，期望 %d，实际 %d", len(testContent), info.Size)
		}
	})

	t.Run("ListObjectsPage", func(t *testing.T) {
		keys, next, err := GetStorage().ListObjectsPage(ctx, "test/", 10, "")
		if err != nil {
			t.Fatalf("分页列出失败: %v", err)
		}
		if len(keys) == 0 {
			t.Error("应该至少有一个对象")
		}
		t.Logf("本页 %d 个对象, next=%q", len(keys), next)
	})

	t.Run("CopyObject", func(t *testing.T) {
		dstKey := "test/test-file-copy.txt"
		if err := GetStorage().CopyObject(ctx, testKey, dstKey); err != nil {
			t.Fatalf("复制对象失败: %v", err)
		}
		defer DeleteFile(ctx, dstKey)

		if exists, _ := IsFileExist(ctx, dstKey); !exists {
			t.Error("复制的文件应该存在")
		}
	})

	t.Run("DeleteObject", func(t *testing.T) {
		if err := DeleteFile(ctx, testKey); err != nil {
			t.Fatalf("删除文件失败: %v", err)
		}
		if exists, _ := IsFileExist(ctx, testKey); exists {
			t.Error("文件应该已被删除")
		}
	})
}

// TestRetryMechanism 保留原重试语义测试。
func TestRetryMechanism(t *testing.T) {
	rs, m := newTestRetry(Config{MaxRetries: 2})
	m.putErrs = []error{netFail("fail"), netFail("fail"), nil}

	if err := rs.PutObject(context.Background(), "test-key", strings.NewReader("test"), 4, "text/plain"); err != nil {
		t.Errorf("重试后应该成功: %v", err)
	}
	if m.putCalls != 3 {
		t.Errorf("应该尝试 3 次，实际 %d", m.putCalls)
	}
}

// ---- 只实现 Storage、不实现 MultipartUploader 的 mock ----

// noMultipartStorage 只实现 Storage、不实现 MultipartUploader，
// 用于覆盖「底层不支持分片上传」的路径。
type noMultipartStorage struct {
	putCalls int
	getCalls int
	getErrs  []error
	closed   int
}

func (m *noMultipartStorage) nthErr(errs []error, n int) error {
	if len(errs) == 0 {
		return nil
	}
	if n-1 < len(errs) {
		return errs[n-1]
	}
	return errs[len(errs)-1]
}

func (m *noMultipartStorage) PutObject(_ context.Context, _ string, _ io.Reader, _ int64, _ string) error {
	m.putCalls++
	return nil
}

// GetObject 返回「非 nil body + 非 nil error」：真实 SDK（如 COS 在
// CRC64 校验失败时）就会出现这种组合，用于验证重试不会漏关 body。
func (m *noMultipartStorage) GetObject(_ context.Context, _ string) (io.ReadCloser, error) {
	m.getCalls++
	return &closeCountingReader{onClose: func() { m.closed++ }},
		m.nthErr(m.getErrs, m.getCalls)
}

func (m *noMultipartStorage) DeleteObject(context.Context, string) error { return nil }
func (m *noMultipartStorage) GetObjectURL(string) string                 { return "" }
func (m *noMultipartStorage) GetPresignedURL(context.Context, string, time.Duration) (string, error) {
	return "", nil
}
func (m *noMultipartStorage) PresignedPutURL(context.Context, string, string, time.Duration) (string, error) {
	return "", nil
}
func (m *noMultipartStorage) IsExist(context.Context, string) (bool, error) { return true, nil }
func (m *noMultipartStorage) ListObjects(context.Context, string) ([]string, error) {
	return nil, nil
}
func (m *noMultipartStorage) ListObjectsPage(context.Context, string, int, string) ([]string, string, error) {
	return nil, "", nil
}
func (m *noMultipartStorage) CopyObject(context.Context, string, string) error { return nil }
func (m *noMultipartStorage) GetObjectInfo(context.Context, string) (*ObjectInfo, error) {
	return &ObjectInfo{Key: "k"}, nil
}
func (m *noMultipartStorage) GetProvider(context.Context) string { return "plain" }

// closeCountingReader 记录 Close 是否被调用。
type closeCountingReader struct {
	onClose func()
}

func (c *closeCountingReader) Read([]byte) (int, error) { return 0, io.EOF }
func (c *closeCountingReader) Close() error {
	if c.onClose != nil {
		c.onClose()
	}
	return nil
}

// TestInitMultipartUploadUnsupported 回归测试：底层不支持分片上传时，
// InitMultipartUpload 必须返回错误而不是 panic。
//
// 同文件里其余三个分片方法都有 if s.uploader == nil 检查，只有
// InitMultipartUpload 漏了 —— 而 README 明确鼓励「只依赖 Storage 接口」
// 的 mock 用法，这条路径是会被走到的。
func TestInitMultipartUploadUnsupported(t *testing.T) {
	s := NewRetryStorage(&noMultipartStorage{}, 1, time.Millisecond)

	if _, err := s.InitMultipartUpload(context.Background(), "k", "text/plain"); err == nil {
		t.Fatal("底层不支持分片上传时应返回错误，而不是 panic 或返回空 uploadID")
	}
	// 其余三个方法有检查，行为应一致。
	if _, err := s.PresignPartURL(context.Background(), "k", "u", 1, time.Minute); err == nil {
		t.Fatal("PresignPartURL 应返回错误")
	}
	if err := s.AbortMultipartUpload(context.Background(), "k", "u"); err == nil {
		t.Fatal("AbortMultipartUpload 应返回错误")
	}
}

// TestGetObjectRetryClosesStaleBody 回归测试：重试时上一次留下的 body
// 必须被关闭。
//
// 底层允许「非 nil body + 非 nil error」的组合；旧实现直接覆盖 result
// 变量，把这些 body 连同 HTTP 连接一起漏掉。
func TestGetObjectRetryClosesStaleBody(t *testing.T) {
	m := &noMultipartStorage{getErrs: []error{netFail("boom"), netFail("boom")}}
	s := NewRetryStorage(m, 2, time.Millisecond)

	if _, err := s.GetObject(context.Background(), "k"); err == nil {
		t.Fatal("两次都失败时应返回错误")
	}
	if m.getCalls != 3 {
		t.Fatalf("应尝试 3 次（1 次 + 2 次重试）, got %d", m.getCalls)
	}
	if m.closed != 3 {
		t.Fatalf("每次失败带回的 body 都应被关闭, closed=%d want 3", m.closed)
	}
}

// TestSetLimitsConcurrent 回归测试：SetLimits 与并发读写不构成数据竞争。
//
// 旧实现里 maxFileSize / maxParts 是普通字段，运行期 SetLimits（配置热更新）
// 与并发的 PutObject / CompleteMultipartUpload 读写同一字段就是一条 race。
func TestSetLimitsConcurrent(t *testing.T) {
	s := NewRetryStorage(&mockStorage{}, 1, time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			s.SetLimits(Limits{MaxFileSize: int64(i), MaxParts: i%3 + 1})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = s.PutObject(context.Background(), "k", strings.NewReader("x"), 1, "text/plain")
			_ = s.CompleteMultipartUpload(context.Background(), "k", "u",
				[]MultipartUploadPart{{PartNumber: 1, ETag: "e"}})
		}
	}()
	wg.Wait()
}

// ---- 错误分类与分片初始化重试 ----

// TestRetryableErrorClassification 分类断言：SDK 状态码优先，
// 未知来源只放行"确认的网络故障"。
func TestRetryableErrorClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"ctx 取消", context.Canceled, false},
		{"网络错误", netFail("dial tcp: connection refused"), true},
		{"半包", io.ErrUnexpectedEOF, true},
		{"未知业务错误", errors.New("配额不足"), false},
		{"未知业务错误（包装网络错误）", fmt.Errorf("wrap: %w", netFail("reset by peer")), true},
		{"minio 403", minio.ErrorResponse{StatusCode: 403}, false},
		{"minio 404", minio.ErrorResponse{StatusCode: 404}, false},
		{"minio 500", minio.ErrorResponse{StatusCode: 500}, true},
		{"minio 429", minio.ErrorResponse{StatusCode: 429}, true},
		{"minio 无服务端响应", minio.ErrorResponse{StatusCode: 0}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := retryableError(c.err); got != c.want {
				t.Errorf("retryableError(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// TestUnknownErrorNotRetried 回归测试：自定义 Storage 实现返回的业务错误
// 不再被当成网络错误重试（旧实现兜底 sc==0 → true，会放大成 N 次无效调用）。
func TestUnknownErrorNotRetried(t *testing.T) {
	rs, m := newTestRetry(Config{MaxRetries: 3})
	m.putErrs = []error{errors.New("配额不足")}

	if err := rs.PutObject(context.Background(), "k", strings.NewReader("b"), 1, "text/plain"); err == nil {
		t.Fatal("应返回错误")
	}
	if m.putCalls != 1 {
		t.Fatalf("业务错误不应重试, attempts=%d", m.putCalls)
	}
}

// TestInitMultipartUploadNoRetryByDefault 回归测试：分片初始化默认不重试 ——
// 它是非幂等调用，重试留下的孤儿 uploadID 无法回收且持续计费。
func TestInitMultipartUploadNoRetryByDefault(t *testing.T) {
	rs, m := newTestRetry(Config{MaxRetries: 3})
	m.initErrs = []error{netFail("dial timeout")}

	if _, err := rs.InitMultipartUpload(context.Background(), "k", "text/plain"); err == nil {
		t.Fatal("失败时应返回错误")
	}
	if m.initCalls != 1 {
		t.Fatalf("默认不应重试分片初始化（重试会留下孤儿 uploadID）, attempts=%d", m.initCalls)
	}
}

// TestInitMultipartUploadRetryOptIn 显式开启后按 MaxRetries 重试。
func TestInitMultipartUploadRetryOptIn(t *testing.T) {
	rs, m := newTestRetry(Config{MaxRetries: 2})
	m.initErrs = []error{netFail("dial timeout"), nil}
	rs.SetRetryInitMultipart(true)

	got, err := rs.InitMultipartUpload(context.Background(), "k", "text/plain")
	if err != nil {
		t.Fatalf("重试后应成功: %v", err)
	}
	if got != "upload-id" {
		t.Fatalf("uploadID=%q", got)
	}
	if m.initCalls != 2 {
		t.Fatalf("开启后应尝试 2 次, got %d", m.initCalls)
	}
}

// TestDefaultMaxPartsEffective 回归测试：未显式注入 MaxParts 时，
// 分片数上限应当是 DefaultMaxParts，而不是完全不限制。
func TestDefaultMaxPartsEffective(t *testing.T) {
	s := NewRetryStorage(&mockStorage{}, 1, time.Millisecond)

	parts := make([]MultipartUploadPart, DefaultMaxParts+1)
	for i := range parts {
		parts[i] = MultipartUploadPart{PartNumber: i + 1, ETag: "e"}
	}
	if err := s.CompleteMultipartUpload(context.Background(), "k", "u", parts); err == nil {
		t.Fatal("默认分片数上限应生效，超过 DefaultMaxParts 应被拒绝")
	}
}

// TestPutObjectRetryRewindsToInitialOffset 回归测试：reader 已定位在
// 非 0 偏移时，重试必须回卷到**初始偏移**而不是绝对 0。
// 旧实现 rewind() 硬编码 Seek(0, SeekStart)：跳过文件头后上传正文、
// 复用已部分读取的 *os.File 等场景下，重试会把偏移 0 起的错误内容
// 静默上传（对象写成功、无任何报错、内容错位）。
func TestPutObjectRetryRewindsToInitialOffset(t *testing.T) {
	m := &mockStorage{putErrs: []error{netFail("first attempt fails"), nil}, consumeBody: true}
	rs := NewRetryStorage(m, 1, time.Millisecond)

	r := bytes.NewReader([]byte("AAAABBBB"))
	if _, err := r.Seek(4, io.SeekStart); err != nil { // 意图上传 "BBBB"
		t.Fatalf("seek: %v", err)
	}
	if err := rs.PutObject(context.Background(), "k", r, 4, "text/plain"); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	if m.putCalls != 2 {
		t.Fatalf("应尝试 2 次, got %d", m.putCalls)
	}
	if m.putBodies[0] != "BBBB" {
		t.Errorf("首次上传应为偏移 4 起的内容, got %q", m.putBodies[0])
	}
	if m.putBodies[1] != "BBBB" {
		t.Errorf("重试必须回卷到初始偏移（旧实现会错误地上传 %q）, got %q",
			"AAAABBBB", m.putBodies[1])
	}
}
