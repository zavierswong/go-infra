package storage

// Config 存储配置。
type Config struct {
	Type       string `mapstructure:"type"`        // 存储类型: qcloud, aliyun, minio
	Region     string `mapstructure:"region"`      // 区域
	Bucket     string `mapstructure:"bucket"`      // 存储桶名称
	SecretID   string `mapstructure:"secret_id"`   // 访问密钥ID
	SecretKey  string `mapstructure:"secret_key"`  // 访问密钥Key
	Endpoint   string `mapstructure:"endpoint"`    // 自定义域名或 MinIO endpoint
	UseSSL     bool   `mapstructure:"use_ssl"`     // 是否使用 HTTPS
	BaseURL    string `mapstructure:"base_url"`    // 访问 URL 前缀（须已包含 bucket 或为 CDN 域名）
	MaxRetries int    `mapstructure:"max_retries"` // 失败重试次数
	RetryDelay int    `mapstructure:"retry_delay"` // 重试延迟时间(毫秒)
	Timeout    int    `mapstructure:"timeout"`     // 请求超时时间(秒)

	// PublicRead 桶是否公开读。true 时 GetObjectURL 返回永久拼接 URL；
	// false（默认，私有桶）时 GetObjectURL 返回预签名 URL（有效期
	// PresignedURLExpires）。
	PublicRead bool `mapstructure:"public_read"`

	// ── 库层生效的限制（0 或缺省表示关闭/取默认）──
	MaxFileSize         int64 `mapstructure:"max_file_size"`         // 单文件最大大小 (bytes)，>0 时 PutObject 超限直接报错
	MaxParts            int   `mapstructure:"max_parts"`             // 最大分片数，>0 时 CompleteMultipartUpload 校验；缺省 10000
	PresignedURLExpires int   `mapstructure:"presigned_url_expires"` // 私有桶 GetObjectURL 的签名有效期 (秒)，缺省 3600
}
