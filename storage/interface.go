package storage

import (
	"context"
	"io"
	"time"
)

// Storage 对象存储核心接口。
//
// 为避免胖接口，分片上传相关能力拆分到 MultipartUploader；
// 三家实现（QCloud/Aliyun/Minio）与 RetryStorage 同时实现两者，
// 需要 MultipartUploader 时对 Storage 断言即可：
//
//	var mp storage.MultipartUploader
//	mp, ok := storage.GetStorage().(storage.MultipartUploader)
type Storage interface {
	// PutObject 上传文件
	// key: 对象键（文件路径）
	// reader: 文件内容读取器（size 须与实际内容一致）
	// size: 文件大小（字节）
	// contentType: 文件 MIME 类型
	PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error

	// GetObject 获取文件
	// key: 对象键（文件路径）
	// 返回文件内容读取器和错误；调用方负责 Close
	GetObject(ctx context.Context, key string) (io.ReadCloser, error)

	// DeleteObject 删除文件
	DeleteObject(ctx context.Context, key string) error

	// GetObjectURL 获取对象访问 URL。
	// 公开读桶（Config.PublicRead=true）返回永久 URL；
	// 私有桶返回带 Config.PresignedURLExpires 有效期（默认 1h）的
	// 预签名 URL，生成失败返回空串（需要精确错误请用 GetPresignedURL）。
	//
	// 注意：BaseURL 为自定义 CDN 域名时仅适用于公开读桶。
	GetObjectURL(key string) string

	// GetPresignedURL 获取预签名 GET URL（用于临时访问私有文件）
	GetPresignedURL(ctx context.Context, key string, expires time.Duration) (string, error)

	// PresignedPutURL 获取预签名 PUT URL（供客户端直传文件，不经过后端）
	// contentType: 文件 MIME 类型（将绑定进签名，客户端上传时须携带相同 Content-Type）
	PresignedPutURL(ctx context.Context, key string, contentType string, expires time.Duration) (string, error)

	// IsExist 判断文件是否存在
	IsExist(ctx context.Context, key string) (bool, error)

	// ListObjects 列出指定前缀下的【全部】对象键（内部循环分页直到取完）。
	// 会把全部 key 载入内存——大前缀（万级以上对象）请用 ListObjectsPage。
	ListObjects(ctx context.Context, prefix string) ([]string, error)

	// ListObjectsPage 分页列出对象键。
	// maxKeys <=0 时用服务端默认（一般 1000）；marker 传上一页返回的
	// NextMarker（首页传 ""）。
	// 返回本页 keys 与下一页起始 marker（isTruncated=false 时为 ""）。
	ListObjectsPage(ctx context.Context, prefix string, maxKeys int, marker string) (keys []string, nextMarker string, err error)

	// CopyObject 复制对象（同桶内）
	CopyObject(ctx context.Context, srcKey, dstKey string) error

	// GetObjectInfo 获取对象信息
	GetObjectInfo(ctx context.Context, key string) (*ObjectInfo, error)

	// GetProvider 获取存储提供商
	GetProvider(ctx context.Context) string
}

// MultipartUploader 分片上传接口（客户端直传大文件场景）。
// 与 Storage 解耦：不用分片上传的使用方 mock/依赖时只需 Storage。
type MultipartUploader interface {
	// InitMultipartUpload 初始化分片上传，返回 uploadID
	InitMultipartUpload(ctx context.Context, key string, contentType string) (uploadID string, err error)

	// PresignPartURL 生成分片上传预签名 PUT URL（客户端直传分片，不经过后端）
	PresignPartURL(ctx context.Context, key string, uploadID string, partNumber int, expires time.Duration) (string, error)

	// CompleteMultipartUpload 合并分片，完成上传
	CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []MultipartUploadPart) error

	// AbortMultipartUpload 取消分片上传，释放已上传分片
	AbortMultipartUpload(ctx context.Context, key string, uploadID string) error
}

// ObjectInfo 对象信息
type ObjectInfo struct {
	Key          string    // 对象键
	Size         int64     // 文件大小（字节）
	ContentType  string    // 内容类型
	LastModified time.Time // 最后修改时间
	ETag         string    // 实体标签
}

// UploadResult 上传结果
type UploadResult struct {
	Key  string // 对象键
	URL  string // 访问 URL
	Size int64  // 文件大小
	ETag string // 实体标签
}

// MultipartUploadPart 分片上传的已完成分片信息
type MultipartUploadPart struct {
	PartNumber int    // 分片编号（从 1 开始）
	ETag       string // 对象存储返回的分片 ETag
}
