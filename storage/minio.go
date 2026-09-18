package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinioStorage MinIO 存储
type MinioStorage struct {
	client  *minio.Client
	core    minio.Core
	bucket  string
	baseURL string
	public  bool // 桶是否公开读（决定 GetObjectURL 走拼接还是签名）
	expires int  // 私有桶 GetObjectURL 签名有效期(秒)，0 用默认
}

// NewMinioStorage 创建 MinIO 存储实例
func NewMinioStorage(cfg Config) (*MinioStorage, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket 不能为空")
	}
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("endpoint 不能为空")
	}
	if cfg.SecretID == "" || cfg.SecretKey == "" {
		return nil, fmt.Errorf("secret_id 和 secret_key 不能为空")
	}
	endpoint := cfg.Endpoint
	useSSL := cfg.UseSSL
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		useSSL = strings.HasPrefix(endpoint, "https://")
		u, err := url.Parse(endpoint)
		if err != nil {
			return nil, fmt.Errorf("解析 endpoint 失败, %w", err)
		}
		endpoint = u.Host
	}
	// 创建 MinIO 客户端
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.SecretID, cfg.SecretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("创建 MinIO 客户端失败: %w", err)
	}

	// 检查 bucket 是否存在，不存在则创建
	ctx := context.Background()
	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("检查 bucket 是否存在失败: %w", err)
	}

	if !exists {
		err = client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("创建 bucket 失败: %w", err)
		}
	}

	baseURL := cfg.BaseURL
	if baseURL == "" {
		protocol := "http"
		if useSSL {
			protocol = "https"
		}
		// 默认 baseURL 已包含 bucket；GetObjectURL 直接拼 key 不再重复拼。
		baseURL = fmt.Sprintf("%s://%s/%s", protocol, endpoint, cfg.Bucket)
	}

	return &MinioStorage{
		client:  client,
		core:    minio.Core{Client: client},
		bucket:  cfg.Bucket,
		baseURL: baseURL,
		public:  cfg.PublicRead,
		expires: cfg.PresignedURLExpires,
	}, nil
}

// PutObject 上传文件
func (s *MinioStorage) PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("上传文件失败: %w", err)
	}

	return nil
}

// GetObject 获取文件
func (s *MinioStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	object, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("获取文件失败: %w", err)
	}

	return object, nil
}

// DeleteObject 删除文件
func (s *MinioStorage) DeleteObject(ctx context.Context, key string) error {
	err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("删除文件失败: %w", err)
	}

	return nil
}

// GetObjectURL 获取文件访问 URL。
// 公开读桶：baseURL（已含 bucket）+ key 的永久 URL；
// 私有桶：带默认有效期的预签名 GET URL（生成失败返回空串）。
func (s *MinioStorage) GetObjectURL(key string) string {
	if s.public {
		return fmt.Sprintf("%s/%s", s.baseURL, key)
	}
	expires := time.Duration(s.expires) * time.Second
	if expires <= 0 {
		expires = DefaultPresignedURLExpires
	}
	u, err := s.GetPresignedURL(context.Background(), key, expires)
	if err != nil {
		return ""
	}
	return u
}

// GetPresignedURL 获取预签名 GET URL
func (s *MinioStorage) GetPresignedURL(ctx context.Context, key string, expires time.Duration) (string, error) {
	presignedURL, err := s.client.PresignedGetObject(ctx, s.bucket, key, expires, nil)
	if err != nil {
		return "", fmt.Errorf("生成[%s]预签名URL失败, %w", key, err)
	}

	return presignedURL.String(), nil
}

// PresignedPutURL 获取预签名 PUT URL（MinIO 不在签名中绑定 Content-Type，客户端上传时自行携带）
func (s *MinioStorage) PresignedPutURL(ctx context.Context, key string, contentType string, expires time.Duration) (string, error) {
	presignedURL, err := s.client.PresignedPutObject(ctx, s.bucket, key, expires)
	if err != nil {
		return "", fmt.Errorf("生成[%s]预签名上传URL失败, %w", key, err)
	}
	return presignedURL.String(), nil
}

// IsExist 判断文件是否存在
func (s *MinioStorage) IsExist(ctx context.Context, key string) (bool, error) {
	_, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return false, nil
		}
		return false, fmt.Errorf("检查文件是否存在失败: %w", err)
	}

	return true, nil
}

// ListObjects 列出指定前缀的全部对象（内部循环分页，全量入内存；
// 大前缀请用 ListObjectsPage）
func (s *MinioStorage) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	var all []string
	marker := ""
	for {
		keys, next, err := s.ListObjectsPage(ctx, prefix, 0, marker)
		if err != nil {
			return nil, err
		}
		all = append(all, keys...)
		if next == "" {
			return all, nil
		}
		marker = next
	}
}

// listPageSize 是单次列举的页大小。S3 / MinIO 的 max-keys 上限为 1000，
// 也是未显式指定时的服务端默认值。
const listPageSize = 1000

// ListObjectsPage 分页列出对象键。nextMarker 为本页最后一个 key
// （下一页用 StartAfter 续读）；maxKeys <= 0 时用页大小上限。
func (s *MinioStorage) ListObjectsPage(ctx context.Context, prefix string, maxKeys int, marker string) ([]string, string, error) {
	if maxKeys <= 0 || maxKeys > listPageSize {
		maxKeys = listPageSize
	}
	opts := minio.ListObjectsOptions{
		Prefix:     prefix,
		Recursive:  true,
		StartAfter: marker,
		MaxKeys:    maxKeys,
	}

	var (
		keys    []string
		listErr error
	)
	objectCh := s.client.ListObjects(ctx, s.bucket, opts)
	// 必须把 channel 读完：minio-go 在后台 goroutine 里往它发送条目、
	// 缓冲只有 1，中途 return 会让生产者永久卡在发送上，连同对应的
	// HTTP 连接一起泄漏（SDK 注释明确要求调用方排空）。
	// 所以这里记录错误后继续迭代，而不是直接 return。
	for object := range objectCh {
		if object.Err != nil {
			if listErr == nil {
				listErr = object.Err
			}
			continue
		}
		keys = append(keys, object.Key)
	}
	if listErr != nil {
		return nil, "", fmt.Errorf("列出对象失败: %w", listErr)
	}

	// 拿满一页才认为可能还有更多。ObjectInfo 不暴露 IsTruncated，
	// 只能用「条目数达到页大小」这个保守判据（S3 允许服务端在
	// IsTruncated=true 时返回不足一页，此时会提前终止 —— 不漏数据
	// 的前提是外层按 next 续读，而提前终止只会少读，不会错读）。
	next := ""
	if len(keys) >= maxKeys {
		next = keys[len(keys)-1]
	}
	return keys, next, nil
}

// CopyObject 复制对象
func (s *MinioStorage) CopyObject(ctx context.Context, srcKey, dstKey string) error {
	srcOpts := minio.CopySrcOptions{
		Bucket: s.bucket,
		Object: srcKey,
	}

	dstOpts := minio.CopyDestOptions{
		Bucket: s.bucket,
		Object: dstKey,
	}

	_, err := s.client.CopyObject(ctx, dstOpts, srcOpts)
	if err != nil {
		return fmt.Errorf("复制对象失败: %w", err)
	}

	return nil
}

// GetObjectInfo 获取对象信息
func (s *MinioStorage) GetObjectInfo(ctx context.Context, key string) (*ObjectInfo, error) {
	objInfo, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("获取对象信息失败: %w", err)
	}

	info := &ObjectInfo{
		Key:          key,
		Size:         objInfo.Size,
		ContentType:  objInfo.ContentType,
		LastModified: objInfo.LastModified,
		ETag:         objInfo.ETag,
	}

	return info, nil
}

func (s *MinioStorage) GetProvider(ctx context.Context) string {
	return "本地存储"
}

// InitMultipartUpload 初始化分片上传
func (s *MinioStorage) InitMultipartUpload(ctx context.Context, key string, contentType string) (string, error) {
	uploadID, err := s.core.NewMultipartUpload(ctx, s.bucket, key, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return "", fmt.Errorf("初始化分片上传失败: %w", err)
	}
	return uploadID, nil
}

// PresignPartURL 生成分片上传预签名 PUT URL（uploadId/partNumber 绑定进签名）
func (s *MinioStorage) PresignPartURL(ctx context.Context, key string, uploadID string, partNumber int, expires time.Duration) (string, error) {
	params := url.Values{}
	params.Set("uploadId", uploadID)
	params.Set("partNumber", strconv.Itoa(partNumber))
	u, err := s.client.Presign(ctx, http.MethodPut, s.bucket, key, expires, params)
	if err != nil {
		return "", fmt.Errorf("生成分片上传 URL 失败: %w", err)
	}
	return u.String(), nil
}

// CompleteMultipartUpload 合并分片完成上传
func (s *MinioStorage) CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []MultipartUploadPart) error {
	minioParts := make([]minio.CompletePart, len(parts))
	for i, p := range parts {
		minioParts[i] = minio.CompletePart{PartNumber: p.PartNumber, ETag: p.ETag}
	}
	if _, err := s.core.CompleteMultipartUpload(ctx, s.bucket, key, uploadID, minioParts, minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("完成分片上传失败: %w", err)
	}
	return nil
}

// AbortMultipartUpload 取消分片上传
func (s *MinioStorage) AbortMultipartUpload(ctx context.Context, key string, uploadID string) error {
	if err := s.core.AbortMultipartUpload(ctx, s.bucket, key, uploadID); err != nil {
		return fmt.Errorf("取消分片上传失败: %w", err)
	}
	return nil
}
