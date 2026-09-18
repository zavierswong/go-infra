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

	"github.com/tencentyun/cos-go-sdk-v5"
)

// QCloudStorage 腾讯云 COS 存储
type QCloudStorage struct {
	client  *cos.Client
	bucket  string
	region  string
	baseURL string
	// apiURL 构造时的 bucketURL（已考虑 Config.Endpoint 覆盖），
	// 用于拼 CopyObject 的源对象 URL —— 它必须指向 API 域名，
	// 不能拿 BaseURL（那是给终端用户访问的公开域名）。
	apiURL  string
	public  bool // 桶是否公开读（决定 GetObjectURL 走拼接还是签名）
	expires int  // 私有桶 GetObjectURL 签名有效期(秒)，0 用默认
}

// NewQCloudStorage 创建腾讯云 COS 存储实例
func NewQCloudStorage(cfg Config) (*QCloudStorage, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket 不能为空")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("region 不能为空")
	}
	if cfg.SecretID == "" || cfg.SecretKey == "" {
		return nil, fmt.Errorf("secret_id 和 secret_key 不能为空")
	}

	// 构建 Bucket URL
	bucketURL := fmt.Sprintf("https://%s.cos.%s.myqcloud.com", cfg.Bucket, cfg.Region)
	if cfg.Endpoint != "" {
		bucketURL = cfg.Endpoint
	}

	u, err := url.Parse(bucketURL)
	if err != nil {
		return nil, fmt.Errorf("解析 bucket URL 失败: %w", err)
	}

	// 创建 COS 客户端
	b := &cos.BaseURL{BucketURL: u}
	client := cos.NewClient(b, &http.Client{
		Timeout: time.Duration(cfg.Timeout) * time.Second,
		Transport: &cos.AuthorizationTransport{
			SecretID:  cfg.SecretID,
			SecretKey: cfg.SecretKey,
		},
	})

	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = bucketURL
	}

	s := &QCloudStorage{
		client:  client,
		bucket:  cfg.Bucket,
		region:  cfg.Region,
		baseURL: baseURL,
		apiURL:  bucketURL,
		public:  cfg.PublicRead,
		expires: cfg.PresignedURLExpires,
	}

	return s, nil
}

// PutObject 上传文件
func (s *QCloudStorage) PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	opt := &cos.ObjectPutOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
			ContentType:   contentType,
			ContentLength: size,
		},
	}

	_, err := s.client.Object.Put(ctx, key, reader, opt)
	if err != nil {
		return fmt.Errorf("上传文件失败: %w", err)
	}

	return nil
}

// GetObject 获取文件
func (s *QCloudStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := s.client.Object.Get(ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("获取文件失败: %w", err)
	}

	return resp.Body, nil
}

// DeleteObject 删除文件
func (s *QCloudStorage) DeleteObject(ctx context.Context, key string) error {
	_, err := s.client.Object.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("删除文件失败: %w", err)
	}

	return nil
}

// GetObjectURL 获取文件访问 URL。
// 公开读桶：baseURL + key 的永久 URL；
// 私有桶：带默认有效期的预签名 GET URL（生成失败返回空串）。
func (s *QCloudStorage) GetObjectURL(key string) string {
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
func (s *QCloudStorage) GetPresignedURL(ctx context.Context, key string, expires time.Duration) (string, error) {
	presignedURL, err := s.client.Object.GetPresignedURL(ctx, http.MethodGet, key, s.client.GetCredential().SecretID, s.client.GetCredential().SecretKey, expires, nil)
	if err != nil {
		return "", fmt.Errorf("生成预签名 URL 失败: %w", err)
	}

	return presignedURL.String(), nil
}

// PresignedPutURL 获取预签名 PUT URL，contentType 绑定进签名
func (s *QCloudStorage) PresignedPutURL(ctx context.Context, key string, contentType string, expires time.Duration) (string, error) {
	opt := &cos.PresignedURLOptions{
		Header: &http.Header{},
	}
	opt.Header.Set("Content-Type", contentType)
	presignedURL, err := s.client.Object.GetPresignedURL(ctx, http.MethodPut, key,
		s.client.GetCredential().SecretID, s.client.GetCredential().SecretKey, expires, opt)
	if err != nil {
		return "", fmt.Errorf("生成预签名上传 URL 失败: %w", err)
	}
	return presignedURL.String(), nil
}

// IsExist 判断文件是否存在
func (s *QCloudStorage) IsExist(ctx context.Context, key string) (bool, error) {
	_, err := s.client.Object.Head(ctx, key, nil)
	if err != nil {
		if cos.IsNotFoundError(err) {
			return false, nil
		}
		return false, fmt.Errorf("检查文件是否存在失败: %w", err)
	}

	return true, nil
}

// ListObjects 列出指定前缀的全部对象（内部循环分页，全量入内存；
// 大前缀请用 ListObjectsPage）
func (s *QCloudStorage) ListObjects(ctx context.Context, prefix string) ([]string, error) {
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
// （COS 在 IsTruncated 时必定返回）；maxKeys <=0 用服务端默认（1000）。
func (s *QCloudStorage) ListObjectsPage(ctx context.Context, prefix string, maxKeys int, marker string) ([]string, string, error) {
	opt := &cos.BucketGetOptions{
		Prefix: prefix,
		Marker: marker,
	}
	if maxKeys > 0 {
		opt.MaxKeys = maxKeys
	}

	result, _, err := s.client.Bucket.Get(ctx, opt)
	if err != nil {
		return nil, "", fmt.Errorf("列出对象失败: %w", err)
	}

	keys := make([]string, 0, len(result.Contents))
	for _, obj := range result.Contents {
		keys = append(keys, obj.Key)
	}

	next := ""
	if result.IsTruncated {
		next = result.NextMarker
		if next == "" && len(keys) > 0 {
			next = keys[len(keys)-1] // 兜底：以本页最后一个 key 继续
		}
	}
	return keys, next, nil
}

// CopyObject 复制对象
//
// 源 URL 用构造期的 apiURL（会随 Config.Endpoint 变化）而不是写死
// myqcloud.com，否则私有云、加速域名、自定义域名场景下必定失败；
// srcKey 逐段做 PathEscape，中文/空格/`?` 之类的键才不会被截断或误解析。
func (s *QCloudStorage) CopyObject(ctx context.Context, srcKey, dstKey string) error {
	segs := strings.Split(srcKey, "/")
	for i, seg := range segs {
		segs[i] = url.PathEscape(seg)
	}
	sourceURL := fmt.Sprintf("%s/%s", s.apiURL, strings.Join(segs, "/"))
	_, _, err := s.client.Object.Copy(ctx, dstKey, sourceURL, nil)
	if err != nil {
		return fmt.Errorf("复制对象失败: %w", err)
	}

	return nil
}

// GetObjectInfo 获取对象信息
func (s *QCloudStorage) GetObjectInfo(ctx context.Context, key string) (*ObjectInfo, error) {
	resp, err := s.client.Object.Head(ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("获取对象信息失败: %w", err)
	}

	info := &ObjectInfo{
		Key:         key,
		ContentType: resp.Header.Get("Content-Type"),
		ETag:        resp.Header.Get("ETag"),
	}

	// 解析 Content-Length
	if contentLength := resp.Header.Get("Content-Length"); contentLength != "" {
		fmt.Sscanf(contentLength, "%d", &info.Size)
	}

	// 解析 Last-Modified
	if lastModified := resp.Header.Get("Last-Modified"); lastModified != "" {
		info.LastModified, _ = time.Parse(time.RFC1123, lastModified)
	}

	return info, nil
}

func (s *QCloudStorage) GetProvider(ctx context.Context) string {
	return "腾讯云"
}

// InitMultipartUpload 初始化分片上传
func (s *QCloudStorage) InitMultipartUpload(ctx context.Context, key string, contentType string) (string, error) {
	opt := &cos.InitiateMultipartUploadOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
			ContentType: contentType,
		},
	}
	result, _, err := s.client.Object.InitiateMultipartUpload(ctx, key, opt)
	if err != nil {
		return "", fmt.Errorf("初始化分片上传失败: %w", err)
	}
	return result.UploadID, nil
}

// PresignPartURL 生成分片上传预签名 PUT URL
func (s *QCloudStorage) PresignPartURL(ctx context.Context, key string, uploadID string, partNumber int, expires time.Duration) (string, error) {
	q := url.Values{}
	q.Set("uploadId", uploadID)
	q.Set("partNumber", strconv.Itoa(partNumber))
	opt := &cos.PresignedURLOptions{Query: &q}
	u, err := s.client.Object.GetPresignedURL(ctx, http.MethodPut, key,
		s.client.GetCredential().SecretID, s.client.GetCredential().SecretKey, expires, opt)
	if err != nil {
		return "", fmt.Errorf("生成分片上传 URL 失败: %w", err)
	}
	return u.String(), nil
}

// CompleteMultipartUpload 合并分片完成上传
func (s *QCloudStorage) CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []MultipartUploadPart) error {
	cosParts := make([]cos.Object, len(parts))
	for i, p := range parts {
		cosParts[i] = cos.Object{PartNumber: p.PartNumber, ETag: p.ETag}
	}
	_, _, err := s.client.Object.CompleteMultipartUpload(ctx, key, uploadID,
		&cos.CompleteMultipartUploadOptions{Parts: cosParts})
	if err != nil {
		return fmt.Errorf("完成分片上传失败: %w", err)
	}
	return nil
}

// AbortMultipartUpload 取消分片上传
func (s *QCloudStorage) AbortMultipartUpload(ctx context.Context, key string, uploadID string) error {
	_, err := s.client.Object.AbortMultipartUpload(ctx, key, uploadID)
	if err != nil {
		return fmt.Errorf("取消分片上传失败: %w", err)
	}
	return nil
}
