package storage

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// AliyunStorage 阿里云 OSS 存储
type AliyunStorage struct {
	client  *oss.Client
	bucket  *oss.Bucket
	baseURL string
	public  bool // 桶是否公开读（决定 GetObjectURL 走拼接还是签名）
	expires int  // 私有桶 GetObjectURL 签名有效期(秒)，0 用默认
}

// NewAliyunStorage 创建阿里云 OSS 存储实例
func NewAliyunStorage(cfg Config) (*AliyunStorage, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket 不能为空")
	}
	if cfg.SecretID == "" || cfg.SecretKey == "" {
		return nil, fmt.Errorf("secret_id 和 secret_key 不能为空")
	}

	// 构建 Endpoint
	endpoint := cfg.Endpoint
	if endpoint == "" {
		if cfg.Region == "" {
			return nil, fmt.Errorf("region 不能为空")
		}
		// 默认使用公网 endpoint
		endpoint = fmt.Sprintf("https://oss-%s.aliyuncs.com", cfg.Region)
	}

	// 创建 OSS 客户端
	client, err := oss.New(endpoint, cfg.SecretID, cfg.SecretKey, oss.Timeout(int64(cfg.Timeout), int64(cfg.Timeout)))
	if err != nil {
		return nil, fmt.Errorf("创建 OSS 客户端失败: %w", err)
	}

	// 获取 Bucket
	bucket, err := client.Bucket(cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("获取 Bucket 失败: %w", err)
	}

	// 默认 baseURL 已含 bucket（bucket.endpoint），GetObjectURL 直接拼 key。
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = fmt.Sprintf("https://%s.%s", cfg.Bucket, strings.TrimPrefix(endpoint, "https://"))
	}

	return &AliyunStorage{
		client:  client,
		bucket:  bucket,
		baseURL: baseURL,
		public:  cfg.PublicRead,
		expires: cfg.PresignedURLExpires,
	}, nil
}

// PutObject 上传文件（oss SDK 无原生 ctx 参数，经 oss.WithContext 注入，
// 取消/超时会中断传输）
func (s *AliyunStorage) PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	options := []oss.Option{
		oss.WithContext(ctx),
		oss.ContentType(contentType),
		oss.ContentLength(size),
	}

	err := s.bucket.PutObject(key, reader, options...)
	if err != nil {
		return fmt.Errorf("上传文件失败: %w", err)
	}

	return nil
}

// GetObject 获取文件
func (s *AliyunStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	reader, err := s.bucket.GetObject(key, oss.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("获取文件失败: %w", err)
	}

	return reader, nil
}

// DeleteObject 删除文件
func (s *AliyunStorage) DeleteObject(ctx context.Context, key string) error {
	err := s.bucket.DeleteObject(key, oss.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("删除文件失败: %w", err)
	}

	return nil
}

// GetObjectURL 获取文件访问 URL。
// 公开读桶：baseURL（已含 bucket）+ key 的永久 URL；
// 私有桶：带默认有效期的预签名 GET URL（生成失败返回空串）。
func (s *AliyunStorage) GetObjectURL(key string) string {
	if s.public {
		return fmt.Sprintf("%s/%s", s.baseURL, key)
	}
	expires := time.Duration(s.expires) * time.Second
	if expires <= 0 {
		expires = DefaultPresignedURLExpires
	}
	signed, err := s.bucket.SignURL(key, oss.HTTPGet, int64(expires.Seconds()))
	if err != nil {
		return ""
	}
	return signed
}

// GetPresignedURL 获取预签名 GET URL
func (s *AliyunStorage) GetPresignedURL(ctx context.Context, key string, expires time.Duration) (string, error) {
	signedURL, err := s.bucket.SignURL(key, oss.HTTPGet, int64(expires.Seconds()))
	if err != nil {
		return "", fmt.Errorf("生成预签名 URL 失败: %w", err)
	}

	return signedURL, nil
}

// PresignedPutURL 获取预签名 PUT URL，contentType 绑定进签名
func (s *AliyunStorage) PresignedPutURL(ctx context.Context, key string, contentType string, expires time.Duration) (string, error) {
	signedURL, err := s.bucket.SignURL(key, oss.HTTPPut, int64(expires.Seconds()), oss.ContentType(contentType))
	if err != nil {
		return "", fmt.Errorf("生成预签名上传 URL 失败: %w", err)
	}
	return signedURL, nil
}

// IsExist 判断文件是否存在
func (s *AliyunStorage) IsExist(ctx context.Context, key string) (bool, error) {
	exist, err := s.bucket.IsObjectExist(key, oss.WithContext(ctx))
	if err != nil {
		return false, fmt.Errorf("检查文件是否存在失败: %w", err)
	}

	return exist, nil
}

// ListObjects 列出指定前缀的全部对象（内部循环分页，全量入内存；
// 大前缀请用 ListObjectsPage）
func (s *AliyunStorage) ListObjects(ctx context.Context, prefix string) ([]string, error) {
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

// ListObjectsPage 分页列出对象键。nextMarker 取服务端 NextMarker
// （IsTruncated 时必定返回）；maxKeys <=0 用服务端默认（1000）。
func (s *AliyunStorage) ListObjectsPage(ctx context.Context, prefix string, maxKeys int, marker string) ([]string, string, error) {
	options := []oss.Option{
		oss.WithContext(ctx),
		oss.Prefix(prefix),
	}
	// max-keys 的合法区间是 1..1000：SDK 的 addParam 只跳过 nil、不跳过 0，
	// 直接传 0 会把 max-keys=0 原样拼进 query，交给服务端后要么报错
	// 要么返回异常页。因此 <=0 时干脆不带这个参数，让服务端用默认值。
	if maxKeys > 0 {
		options = append(options, oss.MaxKeys(maxKeys))
	}
	if marker != "" {
		options = append(options, oss.Marker(marker))
	}

	lsRes, err := s.bucket.ListObjects(options...)
	if err != nil {
		return nil, "", fmt.Errorf("列出对象失败: %w", err)
	}

	keys := make([]string, 0, len(lsRes.Objects))
	for _, obj := range lsRes.Objects {
		keys = append(keys, obj.Key)
	}

	next := ""
	if lsRes.IsTruncated {
		next = lsRes.NextMarker
		if next == "" && len(keys) > 0 {
			next = keys[len(keys)-1] // 兜底：以本页最后一个 key 继续
		}
	}
	return keys, next, nil
}

// CopyObject 复制对象
func (s *AliyunStorage) CopyObject(ctx context.Context, srcKey, dstKey string) error {
	_, err := s.bucket.CopyObject(srcKey, dstKey, oss.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("复制对象失败: %w", err)
	}

	return nil
}

// GetObjectInfo 获取对象信息
func (s *AliyunStorage) GetObjectInfo(ctx context.Context, key string) (*ObjectInfo, error) {
	meta, err := s.bucket.GetObjectMeta(key, oss.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("获取对象信息失败: %w", err)
	}

	info := &ObjectInfo{
		Key:         key,
		ContentType: meta.Get("Content-Type"),
		ETag:        meta.Get("ETag"),
	}

	// 解析 Content-Length
	if contentLength := meta.Get("Content-Length"); contentLength != "" {
		fmt.Sscanf(contentLength, "%d", &info.Size)
	}

	// 解析 Last-Modified
	if lastModified := meta.Get("Last-Modified"); lastModified != "" {
		info.LastModified, _ = time.Parse(time.RFC1123, lastModified)
	}

	return info, nil
}

func (s *AliyunStorage) GetProvider(ctx context.Context) string {
	return "阿里云"
}

// InitMultipartUpload 初始化分片上传
func (s *AliyunStorage) InitMultipartUpload(ctx context.Context, key string, contentType string) (string, error) {
	imur, err := s.bucket.InitiateMultipartUpload(key, oss.WithContext(ctx), oss.ContentType(contentType))
	if err != nil {
		return "", fmt.Errorf("初始化分片上传失败: %w", err)
	}
	return imur.UploadID, nil
}

// PresignPartURL 生成分片上传预签名 PUT URL（uploadId/partNumber 为 OSS 签名子资源）
func (s *AliyunStorage) PresignPartURL(ctx context.Context, key, uploadID string, partNumber int, expires time.Duration) (string, error) {
	signedURL, err := s.bucket.SignURL(key, oss.HTTPPut, int64(expires.Seconds()),
		oss.AddParam("uploadId", uploadID),
		oss.AddParam("partNumber", strconv.Itoa(partNumber)),
	)
	if err != nil {
		return "", fmt.Errorf("生成分片上传 URL 失败: %w", err)
	}
	return signedURL, nil
}

// CompleteMultipartUpload 合并分片完成上传
func (s *AliyunStorage) CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []MultipartUploadPart) error {
	ossParts := make([]oss.UploadPart, len(parts))
	for i, p := range parts {
		ossParts[i] = oss.UploadPart{PartNumber: p.PartNumber, ETag: p.ETag}
	}
	imur := oss.InitiateMultipartUploadResult{
		Bucket:   s.bucket.BucketName,
		Key:      key,
		UploadID: uploadID,
	}
	if _, err := s.bucket.CompleteMultipartUpload(imur, ossParts, oss.WithContext(ctx)); err != nil {
		return fmt.Errorf("完成分片上传失败: %w", err)
	}
	return nil
}

// AbortMultipartUpload 取消分片上传
func (s *AliyunStorage) AbortMultipartUpload(ctx context.Context, key string, uploadID string) error {
	imur := oss.InitiateMultipartUploadResult{
		Bucket:   s.bucket.BucketName,
		Key:      key,
		UploadID: uploadID,
	}
	if err := s.bucket.AbortMultipartUpload(imur, oss.WithContext(ctx)); err != nil {
		return fmt.Errorf("取消分片上传失败: %w", err)
	}
	return nil
}
