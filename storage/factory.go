package storage

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/zavierswong/go-infra/logger"
)

// globalStorage 用 atomic 承载：Setup 与并发的 GetStorage / PutFile 等
// 便捷函数读写同一个变量，裸的全局变量是一条真实的 data race，
// 且读方可能看到"写了一半"的实例指针。
var globalStorage atomic.Pointer[Storage]

// currentStorage 取当前全局实例，未初始化返回 nil。
func currentStorage() Storage {
	if p := globalStorage.Load(); p != nil {
		return *p
	}
	return nil
}

// StorageType 存储类型
const (
	StorageTypeQCloud = "qcloud" // 腾讯云 COS
	StorageTypeAliyun = "aliyun" // 阿里云 OSS
	StorageTypeMinio  = "minio"  // MinIO
)

// Setup 初始化全局存储（含重试包装与库层限制）。
// 多实例需求请直接使用 NewQCloudStorage / NewAliyunStorage / NewMinioStorage
// + NewRetryStorage 自行组装。
func Setup(cfg Config) error {
	if cfg.Type == "" {
		return fmt.Errorf("存储类型不能为空")
	}

	var storage Storage
	var err error

	switch cfg.Type {
	case StorageTypeQCloud:
		storage, err = NewQCloudStorage(cfg)
	case StorageTypeAliyun:
		storage, err = NewAliyunStorage(cfg)
	case StorageTypeMinio:
		storage, err = NewMinioStorage(cfg)
	default:
		return fmt.Errorf("不支持的存储类型: %s", cfg.Type)
	}
	if err != nil {
		return fmt.Errorf("初始化存储失败: %w", err)
	}

	rs := NewRetryStorage(storage, cfg.MaxRetries, time.Duration(cfg.RetryDelay)*time.Millisecond)
	rs.SetLimits(Limits{
		MaxFileSize:         cfg.MaxFileSize,
		MaxParts:            cfg.MaxParts,
		PresignedURLExpires: time.Duration(cfg.PresignedURLExpires) * time.Second,
	})
	var inst Storage = rs
	globalStorage.Store(&inst)

	logger.Infof("存储初始化成功, 类型: %s, 存储桶: %s", cfg.Type, cfg.Bucket)
	return nil
}

// GetStorage 获取全局存储实例（未初始化时为 nil）。
// 分片上传能力需断言：GetStorage().(MultipartUploader)。
func GetStorage() Storage {
	return currentStorage()
}

// ---- 全局便捷函数（未初始化时报错） ----

// PutFile 上传文件
func PutFile(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	s := currentStorage()
	if s == nil {
		return fmt.Errorf("存储未初始化")
	}
	return s.PutObject(ctx, key, reader, size, contentType)
}

// GetFile 获取文件
func GetFile(ctx context.Context, key string) (io.ReadCloser, error) {
	s := currentStorage()
	if s == nil {
		return nil, fmt.Errorf("存储未初始化")
	}
	return s.GetObject(ctx, key)
}

// DeleteFile 删除文件
func DeleteFile(ctx context.Context, key string) error {
	s := currentStorage()
	if s == nil {
		return fmt.Errorf("存储未初始化")
	}
	return s.DeleteObject(ctx, key)
}

// GetFileURL 获取文件访问 URL
func GetFileURL(key string) string {
	s := currentStorage()
	if s == nil {
		return ""
	}
	return s.GetObjectURL(key)
}

// GetFilePresignedURL 获取文件预签名 GET URL
func GetFilePresignedURL(ctx context.Context, key string, expires time.Duration) (string, error) {
	s := currentStorage()
	if s == nil {
		return "", fmt.Errorf("存储未初始化")
	}
	return s.GetPresignedURL(ctx, key, expires)
}

// GetPresignedPutURL 获取文件预签名 PUT URL（供客户端直传）
func GetPresignedPutURL(ctx context.Context, key string, contentType string, expires time.Duration) (string, error) {
	s := currentStorage()
	if s == nil {
		return "", fmt.Errorf("存储未初始化")
	}
	return s.PresignedPutURL(ctx, key, contentType, expires)
}

// IsFileExist 判断文件是否存在
func IsFileExist(ctx context.Context, key string) (bool, error) {
	s := currentStorage()
	if s == nil {
		return false, fmt.Errorf("存储未初始化")
	}
	return s.IsExist(ctx, key)
}
