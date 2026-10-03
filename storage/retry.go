package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/minio/minio-go/v7"
	"github.com/tencentyun/cos-go-sdk-v5"
	"github.com/zavierswong/go-infra/logger"
)

// ErrNotReplayable PutObject 重试时请求体已被消费且不支持 Seek 回卷，
// 无法安全重试。
var ErrNotReplayable = errors.New("storage: reader 已被消费且不可重放，放弃重试")

// DefaultMaxParts 分片数默认上限。
const DefaultMaxParts = 10000

// DefaultPresignedURLExpires 私有桶 GetObjectURL 的默认签名有效期。
const DefaultPresignedURLExpires = 3600 * time.Second

// RetryStorage 带重试的存储包装器。
//
// 重试策略（相比"无脑重试"的三点修正）：
//  1. 错误分类：仅网络错误（无 HTTP 响应）、408/429/5xx 重试；
//     4xx 业务错误（403 鉴权、404 不存在、欠费类）重试只会放大损失，直接上抛；
//  2. 请求体重放：PutObject 用计数包装 reader，已消费字节 >0 且不支持
//     io.Seeker 回卷时不重试（盲重试会把空体/半截体发给存储）；
//  3. ctx 感知：重试间退避睡眠可被 ctx 取消打断。
type RetryStorage struct {
	storage    Storage
	uploader   MultipartUploader
	maxRetries int
	retryDelay time.Duration

	// 下面两个字段可被 SetLimits 在运行期改写，而读方是并发的
	// PutObject / CompleteMultipartUpload，因此必须是 atomic：
	// 普通的 int64/int 读写在这里就是一条真实的 data race。
	maxFileSize atomic.Int64 // >0 生效
	maxParts    atomic.Int32 // >0 生效，默认 DefaultMaxParts

	// retryInit 是否允许重试 InitMultipartUpload。默认关闭，理由见
	// InitMultipartUpload 的注释：该调用非幂等，重试会制造孤儿 uploadID。
	retryInit atomic.Bool
}

// Limits 通过 Setup 注入的库层限制。
type Limits struct {
	MaxFileSize int64
	MaxParts    int

	// PresignedURLExpires 不作用于 RetryStorage —— 签名有效期由各后端的
	// Config 决定（GetObjectURL 接口没有 expires 参数可透传）。
	// 这里保留字段仅为配置结构兼容。
	PresignedURLExpires time.Duration
}

// NewRetryStorage 创建带重试功能的存储实例。
// 限制（文件大小/分片数）通过 SetLimits 注入，缺省不限制大小。
func NewRetryStorage(storage Storage, maxRetries int, retryDelay time.Duration) *RetryStorage {
	if retryDelay <= 0 {
		retryDelay = 100 * time.Millisecond
	}
	rs := &RetryStorage{
		storage:    storage,
		uploader:   asMultipartUploader(storage),
		maxRetries: maxRetries,
		retryDelay: retryDelay,
	}
	// 缺省就有分片数上限：不设的话「配置里忘了填 max_parts」等于完全不限制，
	// 与 Config 默认值和 README 的承诺都不一致。
	rs.maxParts.Store(DefaultMaxParts)
	return rs
}

// SetLimits 注入库层限制（Setup 自动调用；直接使用 NewRetryStorage 的
// 调用方无需设置即保持旧行为）。
//
// 字段用 atomic 读写，因此可以在运行期调用而不与并发的读写路径构成竞态。
func (s *RetryStorage) SetLimits(l Limits) {
	s.maxFileSize.Store(l.MaxFileSize)
	if l.MaxParts > 0 {
		s.maxParts.Store(int32(l.MaxParts))
	}
}

// asMultipartUploader 把实现断言为 MultipartUploader（三家实现均同时实现）。
func asMultipartUploader(s Storage) MultipartUploader {
	mp, _ := s.(MultipartUploader)
	return mp
}

// retry 统一重试执行。fn 内部自行处理"结果捕获 + 请求体重放"。
func (s *RetryStorage) retry(ctx context.Context, operation string, fn func() error) error {
	var err error
	for i := 0; i <= s.maxRetries; i++ {
		if cerr := ctx.Err(); cerr != nil {
			return fmt.Errorf("存储操作 %s 被 ctx 取消: %w", operation, cerr)
		}
		err = fn()
		if err == nil {
			return nil
		}
		// 请求体不可重放是确定性失败，重试只会重复失败。
		if errors.Is(err, ErrNotReplayable) {
			return err
		}
		if !retryableError(err) {
			return err // 鉴权/不存在/欠费类：重试无意义，立即上抛
		}
		if i < s.maxRetries {
			logger.Warnf("存储操作 %s 失败，第 %d 次重试: %v", operation, i+1, err)
			if sleepErr := sleepCtx(ctx, s.retryDelay*time.Duration(i+1)); sleepErr != nil {
				return fmt.Errorf("存储操作 %s 退避中被取消: %w", operation, sleepErr)
			}
		}
	}
	return fmt.Errorf("存储操作 %s 失败，已重试 %d 次: %w", operation, s.maxRetries, err)
}

// retryableError 判定错误是否值得重试。按"确定性从强到弱"的顺序判定：
//
//  1. ctx 取消：调用方已放弃，重试只是多打一次存储，不重试；
//  2. 从三家 SDK 的错误里提取 HTTP 状态码：
//     状态码 > 0 → 只有 408 / 429 / 5xx 可重试，其余 4xx（403 鉴权、
//     404 不存在、402 欠费）是确定性失败，重试只放大损失；
//     匹配到 SDK 错误但状态码为 0 → 请求根本没到服务端（网络/超时），可重试；
//  3. 未匹配到任何 SDK 错误类型：只认网络类错误（实现 net.Error，即 SDK
//     透传的 *url.Error / *net.OpError）与 io.ErrUnexpectedEOF（连接中断
//     导致的半包）。
//
// 第 3 步是"兜底一律重试"的收窄版本：Storage 接口允许业务方自行实现
// （mock、本地磁盘、S3 兼容网关），它们返回的是普通 error
// （errors.New("业务规则拒绝")、参数校验失败、配额不足……）。这些错误
// 重试 N 次只会产生 N 次无效调用，且把真实原因埋进"已重试 N 次"的包装里。
func retryableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}

	var sc int
	var sdkErr bool

	var ossErr oss.ServiceError
	if errors.As(err, &ossErr) {
		sc, sdkErr = ossErr.StatusCode, true
	}
	var minioErr minio.ErrorResponse
	if !sdkErr && errors.As(err, &minioErr) {
		sc, sdkErr = minioErr.StatusCode, true
	}
	var cosErr *cos.ErrorResponse
	if !sdkErr && errors.As(err, &cosErr) {
		sdkErr = true
		if cosErr.Response != nil {
			sc = cosErr.Response.StatusCode
		}
	}
	if sdkErr {
		if sc == 0 {
			return true // 无服务端响应：网络/超时
		}
		return sc == 408 || sc == 429 || sc >= 500
	}

	// 未知来源：只重试明确的网络故障。
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF)
}

// sleepCtx 可被 ctx 取消的退避。
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// countingReader 记录已消费字节数，供 PutObject 判断能否安全重试。
type countingReader struct {
	r io.Reader
	n int64

	// base 是 reader 的初始偏移（构造时记录）。回卷必须恢复到 base，
	// 而不是绝对 0：调用方可能传入已定位在非 0 偏移的 Seeker
	//（跳过文件头后上传正文、复用已部分读取的 *os.File 等），
	// Seek(0, SeekStart) 会让重试上传"偏移 0 起"的错误内容 —— 静默
	// 数据损坏，且只有恰好重试过的请求损坏，极难排查。
	base     int64
	haveBase bool
}

func newCountingReader(r io.Reader) *countingReader {
	c := &countingReader{r: r}
	if sk, ok := r.(io.Seeker); ok {
		if pos, err := sk.Seek(0, io.SeekCurrent); err == nil {
			c.base, c.haveBase = pos, true
		}
	}
	return c
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// rewind 把 reader 回卷到构造时的初始偏移。无法确定初始偏移或不支持
// Seek 时返回 false（按不可重放处理）。
func (c *countingReader) rewind() bool {
	if !c.haveBase {
		return false
	}
	if sk, ok := c.r.(io.Seeker); ok {
		if _, err := sk.Seek(c.base, io.SeekStart); err == nil {
			c.n = 0
			return true
		}
	}
	return false
}

// ---- Storage 接口实现 ----

// PutObject 上传文件（带重试；MaxFileSize 超限直接拒绝）。
// 重试安全性：reader 已被消费时自动 Seek 回卷重试；不可回卷则放弃重试，
// 避免把空体/半截体上传成损坏对象。
func (s *RetryStorage) PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	if maxSize := s.maxFileSize.Load(); maxSize > 0 && size > maxSize {
		return fmt.Errorf("文件大小 %d 超过限制 %d", size, maxSize)
	}

	var cr = newCountingReader(reader)
	return s.retry(ctx, "PutObject", func() error {
		if cr.n > 0 {
			// 上一轮尝试消费了请求体：回卷到初始偏移才允许重试。
			if !cr.rewind() {
				return fmt.Errorf("%w（已消费 %d 字节）", ErrNotReplayable, cr.n)
			}
		}
		return s.storage.PutObject(ctx, key, cr, size, contentType)
	})
}

// GetObject 获取文件（带重试；仅建流失败会重试，流建立后由调用方读取）。
//
// 重试前会关掉上一次的 body：底层实现允许「非 nil body + 非 nil error」
// （例如 COS 在 CRC64 校验失败时返回 resp 与 err 却不关 body），旧实现直接
// 覆盖 result 变量，把这些 body 连同它们的 HTTP 连接一起漏掉。
func (s *RetryStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	var result io.ReadCloser
	err := s.retry(ctx, "GetObject", func() error {
		// 上一次尝试留下的 body 必须关掉，否则连接无法复用、fd 泄漏。
		// 上一次尝试留下的 body 必须关掉，否则连接无法复用、fd 泄漏。
		if result != nil {
			_ = result.Close()
			result = nil
		}
		var err error
		result, err = s.storage.GetObject(ctx, key)
		if err != nil && result != nil {
			// 失败路径同样可能带回一个已建立的流。
			_ = result.Close()
			result = nil
		}
		return err
	})
	// 最终失败时把 body 也收干净（正常成功路径由调用方负责 Close）。
	if err != nil && result != nil {
		_ = result.Close()
		result = nil
	}
	return result, err
}

// DeleteObject 删除文件（带重试）
func (s *RetryStorage) DeleteObject(ctx context.Context, key string) error {
	return s.retry(ctx, "DeleteObject", func() error {
		return s.storage.DeleteObject(ctx, key)
	})
}

// GetObjectURL 获取文件访问 URL（透传，签名计算不需要重试）
func (s *RetryStorage) GetObjectURL(key string) string {
	return s.storage.GetObjectURL(key)
}

// GetPresignedURL 获取预签名 GET URL（带重试）
func (s *RetryStorage) GetPresignedURL(ctx context.Context, key string, expires time.Duration) (string, error) {
	var result string
	err := s.retry(ctx, "GetPresignedURL", func() error {
		var err error
		result, err = s.storage.GetPresignedURL(ctx, key, expires)
		return err
	})
	return result, err
}

// PresignedPutURL 获取预签名 PUT URL（带重试）
func (s *RetryStorage) PresignedPutURL(ctx context.Context, key string, contentType string, expires time.Duration) (string, error) {
	var result string
	err := s.retry(ctx, "PresignedPutURL", func() error {
		var err error
		result, err = s.storage.PresignedPutURL(ctx, key, contentType, expires)
		return err
	})
	return result, err
}

// IsExist 判断文件是否存在（带重试）
func (s *RetryStorage) IsExist(ctx context.Context, key string) (bool, error) {
	var result bool
	err := s.retry(ctx, "IsExist", func() error {
		var err error
		result, err = s.storage.IsExist(ctx, key)
		return err
	})
	return result, err
}

// ListObjects 列出指定前缀的全部对象（带重试；全量入内存，大前缀用 ListObjectsPage）
func (s *RetryStorage) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	var all []string
	var marker string
	for {
		keys, next, err := s.ListObjectsPage(ctx, prefix, 0, marker)
		if err != nil {
			return nil, err
		}
		all = append(all, keys...)
		// next 未推进说明后端游标没有前进，继续循环只会原地打转。
		if next == "" || next == marker {
			return all, nil
		}
		marker = next
	}
}

// ListObjectsPage 分页列出对象键（带重试，仅重试当前页）。
// 相比 ListObjects 全量重试，分页重试不会重复累积已完成页的数据。
func (s *RetryStorage) ListObjectsPage(ctx context.Context, prefix string, maxKeys int, marker string) ([]string, string, error) {
	var keys []string
	var next string
	err := s.retry(ctx, "ListObjectsPage", func() error {
		var err error
		keys, next, err = s.storage.ListObjectsPage(ctx, prefix, maxKeys, marker)
		return err
	})
	return keys, next, err
}

// CopyObject 复制对象（带重试）
func (s *RetryStorage) CopyObject(ctx context.Context, srcKey, dstKey string) error {
	return s.retry(ctx, "CopyObject", func() error {
		return s.storage.CopyObject(ctx, srcKey, dstKey)
	})
}

// GetObjectInfo 获取对象信息（带重试）
func (s *RetryStorage) GetObjectInfo(ctx context.Context, key string) (*ObjectInfo, error) {
	var result *ObjectInfo
	err := s.retry(ctx, "GetObjectInfo", func() error {
		var err error
		result, err = s.storage.GetObjectInfo(ctx, key)
		return err
	})
	return result, err
}

func (s *RetryStorage) GetProvider(ctx context.Context) string {
	return s.storage.GetProvider(ctx)
}

// ---- MultipartUploader 接口实现 ----

// InitMultipartUpload 初始化分片上传。
//
// 【默认不重试】—— 这个调用是【非幂等】的：重试（或响应丢失后重发）会在
// 服务端再建一个 uploadID，只有最后一个被返回，其余成为孤儿分片上传并
// 持续计费；而孤儿 uploadID 我们根本拿不到，也就无从 Abort 回收。
// 相比"一次网络抖动就失败"，"悄悄留下一堆计费中的孤儿分片"代价更大，
// 因此这里默认 fail-fast，让调用方在失败路径上显式重试并自行决定是否
// 需要清理。
//
// 明确接受该代价的场景（如离线批处理、孤儿可接受的短生命周期任务）可用
// [RetryStorage.SetRetryInitMultipart] 打开重试。
func (s *RetryStorage) InitMultipartUpload(ctx context.Context, key string, contentType string) (string, error) {
	if s.uploader == nil {
		return "", errors.New("storage: 底层实现不支持分片上传")
	}
	if !s.retryInit.Load() {
		return s.uploader.InitMultipartUpload(ctx, key, contentType)
	}
	var result string
	err := s.retry(ctx, "InitMultipartUpload", func() error {
		var err error
		result, err = s.uploader.InitMultipartUpload(ctx, key, contentType)
		return err
	})
	return result, err
}

// SetRetryInitMultipart 打开/关闭 InitMultipartUpload 的重试（默认关闭）。
// 打开前请确认业务能容忍孤儿分片上传（见 InitMultipartUpload 的说明）。
func (s *RetryStorage) SetRetryInitMultipart(enabled bool) {
	s.retryInit.Store(enabled)
}

// PresignPartURL 生成分片上传预签名 URL（纯计算，无需重试）
func (s *RetryStorage) PresignPartURL(ctx context.Context, key string, uploadID string, partNumber int, expires time.Duration) (string, error) {
	if s.uploader == nil {
		return "", errors.New("storage: 底层实现不支持分片上传")
	}
	return s.uploader.PresignPartURL(ctx, key, uploadID, partNumber, expires)
}

// CompleteMultipartUpload 合并分片（带重试；校验分片数上限）
func (s *RetryStorage) CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []MultipartUploadPart) error {
	if s.uploader == nil {
		return errors.New("storage: 底层实现不支持分片上传")
	}
	if maxParts := int(s.maxParts.Load()); maxParts > 0 && len(parts) > maxParts {
		return fmt.Errorf("分片数 %d 超过限制 %d", len(parts), maxParts)
	}
	return s.retry(ctx, "CompleteMultipartUpload", func() error {
		return s.uploader.CompleteMultipartUpload(ctx, key, uploadID, parts)
	})
}

// AbortMultipartUpload 取消分片上传（带重试）
func (s *RetryStorage) AbortMultipartUpload(ctx context.Context, key string, uploadID string) error {
	if s.uploader == nil {
		return errors.New("storage: 底层实现不支持分片上传")
	}
	return s.retry(ctx, "AbortMultipartUpload", func() error {
		return s.uploader.AbortMultipartUpload(ctx, key, uploadID)
	})
}
