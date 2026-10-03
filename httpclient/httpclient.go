// Package httpclient 提供统一超时 / 重试 / 熔断 / 日志注入的 HTTP Client 工厂，
// 替代散落各处的 http.DefaultClient。
//
// 职责分层（由外到内）：
//
//	http.Client（总超时，覆盖全部重试）
//	  └─ breakerTransport   熔断：请求级 Allow/Record（可选）
//	      └─ retryTransport 重试：幂等方法默认重试，指数退避
//	        └─ loggingTransport 日志：每次尝试一条 slog 记录
//	          └─ 底层 Transport（默认 http.DefaultTransport）
package httpclient

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/zavierswong/go-infra/breaker"
)

// 默认值（Config 对应字段 <=0 时生效）。
const (
	DefaultTimeout    = 30 * time.Second
	DefaultMaxRetries = 2
	DefaultBackoff    = 100 * time.Millisecond
	DefaultMaxBackoff = 2 * time.Second
)

// errUnreplayable 表示请求体无法重放（GetBody 为 nil），重试时由
// retryTransport 原样返回给上层，本轮尝试的结果不受影响。
var errUnreplayable = errors.New("httpclient: request body is not replayable")

// Config 客户端工厂配置。零值字段使用默认值，可直接 New(Config{}) 使用。
type Config struct {
	// Name 实例标识，写进每条日志的 "client" 字段；多实例时必填，
	// 否则日志无法区分来源。
	Name string

	// Timeout 是单次请求的总超时（含全部重试与退避），
	// 通过 http.Client.Timeout 生效。<=0 用 DefaultTimeout。
	Timeout time.Duration

	// MaxRetries 是失败后的额外重试次数（总尝试 = 1 + MaxRetries）。
	// 本库不区分「零值」与「未设置」：字段 <=0 且未显式置 RetryDisabled
	// 时按 DefaultMaxRetries 处理；想要 0 次重试请设 RetryDisabled: true。
	MaxRetries int

	// RetryDisabled 显式关闭重试（因为 0 是合法的"未设置"，无法表达
	// "我不想要默认的 2 次"）。
	RetryDisabled bool

	// Backoff 初始退避时长，<=0 用 DefaultBackoff。
	Backoff time.Duration

	// MaxBackoff 退避上限，<=0 用 DefaultMaxBackoff。
	MaxBackoff time.Duration

	// RetryShould 自定义重试判定。为 nil 时用默认策略：
	//   - 方法幂等（GET/HEAD/PUT/DELETE/OPTIONS/TRACE）——非幂等请求
	//     （POST/PATCH）**不**自动重试：5xx 意味着下游可能已经执行，
	//     重试等于重复副作用，需要时请显式提供本函数打开；
	//   - 网络错误（err != nil，且不是 ctx 主动取消）；
	//   - HTTP 429 / 500 / 502 / 503 / 504。
	//
	// 自定义判定只决定"结果是否值得重试"，可重放性检查始终生效：
	// 请求体存在但 GetBody 为 nil 时永远不重试。
	RetryShould func(resp *http.Response, err error) bool

	// Breaker 可选熔断器（推荐传入 breaker.New 的实例）。
	// 熔断在请求粒度生效：Allow 失败直接返回 breaker.ErrOpen，
	// 结果为网络错误或 5xx 记为失败，其余记为成功。
	Breaker *breaker.Breaker

	// Logger 用于请求日志注入；nil 时用 slog.Default()。
	// 每次尝试（含重试）各产生一条记录，字段：
	// client / method / url / status / duration / error。
	Logger *slog.Logger

	// Transport 自定义底层 RoundTripper（注入认证、tracing 等）；
	// nil 时基于 http.DefaultTransport 克隆。
	Transport http.RoundTripper
}

// Client 是工厂产物。内嵌 *http.Client，全部标准方法
// （Do/Get/Head/Post/CloseIdleConnections）直接可用；
// 本包只是在其 Transport 上叠加治理能力。
type Client struct {
	*http.Client

	cfg Config
}

// New 构造客户端。cfg 零值字段取默认值，归一化后的配置可通过
// (*Client).Config 读取。
func New(cfg Config) *Client {
	cfg.normalize()

	var rt http.RoundTripper = cfg.Transport
	if rt == nil {
		// 克隆一份，避免修改全局 DefaultTransport 的共享字段。
		base := http.DefaultTransport.(*http.Transport).Clone()
		rt = base
	}
	rt = loggingTransport{next: rt, name: cfg.Name, logger: cfg.logger()}
	if cfg.retryEnabled() {
		rt = retryTransport{next: rt, cfg: cfg}
	}
	if cfg.Breaker != nil {
		rt = breakerTransport{next: rt, br: cfg.Breaker}
	}

	return &Client{
		Client: &http.Client{
			Timeout:   cfg.Timeout,
			Transport: rt,
		},
		cfg: cfg,
	}
}

// Config 返回归一化后的配置副本（默认值已填充），便于诊断。
func (c *Client) Config() Config { return c.cfg }

func (cfg *Config) normalize() {
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = DefaultBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = DefaultMaxBackoff
	}
	if cfg.MaxBackoff < cfg.Backoff {
		cfg.MaxBackoff = cfg.Backoff
	}
}

func (cfg Config) logger() *slog.Logger {
	if cfg.Logger != nil {
		return cfg.Logger
	}
	return slog.Default()
}

func (cfg Config) retryEnabled() bool { return !cfg.RetryDisabled }

func (cfg Config) maxRetries() int {
	if cfg.RetryDisabled {
		return 0
	}
	if cfg.MaxRetries <= 0 {
		return DefaultMaxRetries
	}
	return cfg.MaxRetries
}
