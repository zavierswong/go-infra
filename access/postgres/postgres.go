// Package postgres 封装 PostgreSQL 的连接与**连接池**管理。
//
// 在 gorm.io/driver/postgres + jackc/pgx/v5 之上，补齐了五件容易写错的事：
//
//  1. **连接池调优**：暴露 MaxOpenConns / MaxIdleConns / ConnMaxLifetime / ConnMaxIdleTime，
//     并按 GOMAXPROCS 推导合理的默认值。database/sql 的 MaxIdleConns 默认只有 2，
//     高 QPS 下空闲连接会被瞬间抢光，之后每个请求都要重新拨号 ——
//     对 PostgreSQL 而言这要 fork 一个后端进程，代价比 MySQL 更高。
//  2. **可信的初始化结果**：显式 Ping 探活并带超时，DSN 语法错误、库名缺失、
//     sslmode 取值非法全部提前到启动期暴露。
//  3. **服务端会话参数可配置**：application_name / search_path / TimeZone /
//     statement_timeout / idle_in_transaction_session_timeout 等只能在建连时下发的参数，
//     由 Config 统一注入 DSN（仅在 DSN 未显式指定时生效）。
//  4. **可控的重试**：由 Config.DialAttempts 显式控制，不会无限阻塞在初始化里。
//  5. **可恢复的关闭**：Close 幂等、不死锁、真正关闭连接池；重建时先关旧池，不泄漏。
//
// 关于 DSN：PostgreSQL 支持 URL 与 keyword/value 两种写法，本包都支持，
// 并且**保持原书写形式**（给 URL 还 URL），详见 dsn.go。
//
// 与 mysql 包的关系：API 形状刻意保持一致，从 mysql 迁过来只需换包名。
// 差异集中在两处 —— 连接串有两种写法、以及多了 PostgreSQL 专有的会话参数。
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/zavierswong/go-infra/logger"
)

// gormLog 是 GORM 的 SQL 日志适配器。
//
// 它实现的是 gorm.io/gorm/logger.Interface，与具体数据库无关，
// 因此直接复用 logger 包里那个历史名字叫 Mysql 的类型（见 logger/gorm.go），
// 这里用别名换一个中性名字，避免在 PostgreSQL 的代码里读到 Mysql 造成困惑。
type gormLog = logger.Mysql

// Postgres 是一个 PostgreSQL 客户端，持有一个 gorm.DB 与其底层的 *sql.DB 连接池。
//
// 所有导出方法都可并发调用。Close 之后调用 DB / SQL 会返回 nil，
// 请先用 Closed 判断，或用 HealthCheck 探活。
type Postgres struct {
	cfg Config
	dsn string
	log *logger.Plog

	// mu 是**唯一**保护 db / pool / closed / healthy 的锁。
	mu      sync.RWMutex
	db      *gorm.DB
	pool    *sql.DB
	closed  bool
	healthy bool

	// rebuildMu 串行化"重建连接池"，避免监控协程与调用方同时重建。
	rebuildMu sync.Mutex

	// ctx 在 Close 时取消，用于让监控循环与正在进行的建连立刻退出。
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Open 建立连接池并启动健康监控。
//
// 返回的 *Postgres 必须由调用方负责 Close。初始化失败会按 Config.DialAttempts
// 做有限次重试，因此不会无限阻塞在初始化里。
func Open(cfg Config) (*Postgres, error) {
	cfg, err := cfg.ready()
	if err != nil {
		return nil, err
	}
	dsn, err := cfg.dsn()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &Postgres{
		cfg:    cfg,
		dsn:    dsn,
		log:    logger.NewPlog("PostgreSQL"),
		ctx:    ctx,
		cancel: cancel,
	}

	if err := p.dial(ctx); err != nil {
		cancel()
		return nil, err
	}

	p.wg.Add(1)
	go p.monitor()

	p.log.Infof(ctx,
		"PostgreSQL 已连接: 最大连接=%d 最大空闲=%d 连接寿命=%s 空闲寿命=%s",
		cfg.MaxOpenConns, cfg.MaxIdleConns, cfg.ConnMaxLifetime, cfg.ConnMaxIdleTime)
	return p, nil
}

// dial 按 Config.DialAttempts 建连，退避从 DialBackoff 指数增长到 DialMaxBackoff。
//
// DialAttempts 为负表示无限重试，但每次尝试前都会检查 ctx，
// 因此 Close 能立刻中断这个过程。
func (p *Postgres) dial(ctx context.Context) error {
	attempts := p.cfg.DialAttempts
	infinite := attempts < 0
	backoff := p.cfg.DialBackoff

	var lastErr error
	for attempt := 1; infinite || attempt <= attempts; attempt++ {
		if p.Closed() {
			return ErrClosed
		}
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return lastErr
			}
			return err
		}

		db, pool, err := p.open(ctx)
		if err == nil {
			p.adopt(db, pool)
			if attempt > 1 {
				p.log.Infof(ctx, "第[%d]次尝试连接 PostgreSQL 成功", attempt)
			}
			return nil
		}

		lastErr = err
		p.log.Warnf(ctx, "第[%d]次连接 PostgreSQL 失败: %v", attempt, err)

		if !infinite && attempt >= attempts {
			break
		}

		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(backoff):
		}

		if backoff *= 2; backoff > p.cfg.DialMaxBackoff {
			backoff = p.cfg.DialMaxBackoff
		}
	}
	return lastErr
}

// open 构造一个 GORM 实例与它的连接池，并验证连通性。
func (p *Postgres) open(ctx context.Context) (*gorm.DB, *sql.DB, error) {
	db, err := gorm.Open(gormpostgres.New(gormpostgres.Config{
		DSN: p.dsn,
		// 只有 PgBouncer(transaction 模式) 这类"后端连接会被复用"的部署才需要打开。
		PreferSimpleProtocol: p.cfg.PreferSimpleProtocol,
	}), &gorm.Config{
		// 关键：关掉 GORM 的自动 Ping。
		//
		// GORM 的自动 Ping 用的是**不带 ctx** 的 ConnPool.Ping()，
		// TCP 不可达时要等操作系统的连接超时（macOS 约 75s，Linux 约 2min）才返回，
		// 启动阶段会看起来像"卡死"。下面我们自己用带超时的 ctx 探活。
		DisableAutomaticPing: true,
		Logger: &gormLog{
			Name:          "PostgreSQL",
			SlowThreshold: p.cfg.SlowThreshold,
			LogLevel:      p.cfg.LogLevel,
		},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("%w: 打开数据库失败: %v", ErrConnect, err)
	}

	pool, err := db.DB()
	if err != nil {
		return nil, nil, fmt.Errorf("%w: 获取底层连接池失败: %v", ErrConnect, err)
	}
	applyPool(pool, p.cfg)

	// 真实探活：既验证连通性，也把"库名/账号/sslmode 写错"这类问题提前暴露。
	// 注意这里复用调用方的 ctx（dial 传入的 p.ctx），再叠加 ConnectTimeout。
	pingCtx, cancel := context.WithTimeout(ctx, p.cfg.ConnectTimeout)
	defer cancel()
	if err := pool.PingContext(pingCtx); err != nil {
		_ = pool.Close()
		return nil, nil, fmt.Errorf("%w: 连通性检查失败: %v", ErrConnect, err)
	}

	// 探活通过后再挂监控回调：挂在 `&gorm.Config{}` 之后、失败返回之前的话，
	// 每次重试都会给一个立刻被丢弃的 gorm 实例注册一遍回调，
	// 注册失败的告警也会重复刷屏。
	p.registerMetrics(db)

	return db, pool, nil
}

// applyPool 把连接池参数写进 database/sql。
//
// 这是"可观测、可调优"的落点，四项各有明确职责：
//   - SetMaxOpenConns    ：并发上限，保护数据库不被连接数压垮；
//   - SetMaxIdleConns    ：**热连接数量**，直接决定高 QPS 下是否需要反复拨号；
//   - SetConnMaxLifetime ：防止拿到服务端/中间件已单方面关闭的坏连接；
//   - SetConnMaxIdleTime ：低频时段回收冗余连接，降低服务端后端进程数。
func applyPool(pool *sql.DB, cfg Config) {
	pool.SetMaxOpenConns(cfg.MaxOpenConns)
	pool.SetMaxIdleConns(cfg.MaxIdleConns)
	pool.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	pool.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
}

// adopt 用新连接池替换旧的，并关闭旧池。
//
// 必须在持锁状态下复查 closed：Close 不抢 rebuildMu，因此"重建已建成新池、
// 尚未挂载"与"Close"可以交错发生。少了这一步复查，新池会被挂到已关闭的
// 客户端上、此后再无人关闭（连接池及其后台协程泄漏），DB() 也会在
// Closed()==true 之后重新返回非 nil，破坏文档承诺。
func (p *Postgres) adopt(db *gorm.DB, pool *sql.DB) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		// 客户端已关闭：新池就地丢弃。
		if pool != nil {
			_ = pool.Close()
		}
		return
	}
	old := p.pool
	p.db, p.pool = db, pool
	p.healthy = true
	p.mu.Unlock()

	// 在锁外关闭旧连接池：Close 可能阻塞，持锁做会拖住所有读。
	if old != nil && old != pool {
		_ = old.Close()
	}
}

// DB 返回 GORM 实例。客户端已关闭时返回 nil。
func (p *Postgres) DB() *gorm.DB {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.db
}

// SQL 返回底层连接池。客户端已关闭时返回 nil。
func (p *Postgres) SQL() *sql.DB {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pool
}

// Stats 返回连接池统计，可直接接入 Prometheus 做监控。
//
// 常看的几个指标：
//   - OpenConnections：当前打开连接数，长期贴近 MaxOpenConns 说明池子偏小；
//   - InUse / Idle：使用中与空闲的连接数；
//   - WaitCount / WaitDuration：拿不到连接的等待，**大于 0 就说明 MaxOpenConns 需要调大**；
//   - MaxIdleClosed / MaxLifetimeClosed：因空闲或寿命被回收的次数，
//     占比过高说明 MaxIdleConns 太小或 ConnMaxLifetime 太短。
func (p *Postgres) Stats() sql.DBStats {
	pool := p.SQL()
	if pool == nil {
		return sql.DBStats{}
	}
	return pool.Stats()
}

// HealthCheck 探活，可用于健康检查接口。
func (p *Postgres) HealthCheck(ctx context.Context) error {
	p.mu.RLock()
	pool, closed := p.pool, p.closed
	p.mu.RUnlock()

	if closed {
		return ErrClosed
	}
	if pool == nil {
		return ErrNotConnected
	}
	if err := pool.PingContext(ctx); err != nil {
		return fmt.Errorf("%w: %v", ErrConnect, err)
	}
	return nil
}

// Closed 报告客户端是否已关闭。
func (p *Postgres) Closed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

// Healthy 报告最近一次后台探活的结果。
func (p *Postgres) Healthy() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.healthy && !p.closed
}

// Config 返回生效后的配置（已补齐默认值）。
func (p *Postgres) Config() Config { return p.cfg }

// DSN 返回**抹掉密码**后最终生效的连接串，便于排查"到底连的是哪个库"。
//
// 之所以在包内自己实现脱敏而不用 utils.SanitizeDSN：
// 那个函数只认 `user:pass@host` 这种 URL 形态，
// 而 PostgreSQL 的 keyword/value 形式写法是 `password=xxx`，它认不出来。
func (p *Postgres) DSN() string { return sanitizeDSN(p.dsn) }

// Reconnect 主动重建连接池：先建新池、就绪后再关旧池，期间旧池仍可服务。
//
// 一般不需要调用：sql.DB 本身就是连接池，服务端恢复后下一次查询会自动拨号。
// 需要它的场景是"连接池确实卡死"或"DSN 变了"，也可通过
// Config.RebuildAfterFailures 让监控协程自动触发。
func (p *Postgres) Reconnect() error {
	p.rebuildMu.Lock()
	defer p.rebuildMu.Unlock()

	if p.Closed() {
		return ErrClosed
	}
	return p.dial(p.ctx)
}

// Close 关闭连接池并停止监控。可重复调用，第二次起直接返回 nil。
func (p *Postgres) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	pool := p.pool
	p.db, p.pool = nil, nil
	p.healthy = false
	p.mu.Unlock()

	// 顺序很关键：先取消 ctx 让监控循环与在建连接退出，再关闭连接池，
	// 最后等协程收敛。全程不持锁，避免与读操作互相等待。
	p.cancel()

	var err error
	if pool != nil {
		if cerr := pool.Close(); cerr != nil {
			err = fmt.Errorf("关闭 PostgreSQL 连接池失败: %w", cerr)
		}
	}
	p.wg.Wait()

	p.log.Infof(context.Background(), "PostgreSQL 连接已关闭")
	return err
}

// monitor 周期性探活。
//
// 只做**观测**（更新 Healthy、打日志）；需要重建时由 RebuildAfterFailures 显式开启，
// 重建走 Reconnect 的正常路径（先建新池、就绪后再关旧池）。
func (p *Postgres) monitor() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.cfg.HealthCheckInterval)
	defer ticker.Stop()

	failures := 0
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
		}

		err := p.ping()
		switch {
		case err == nil:
			if failures > 0 {
				p.log.Infof(p.ctx, "PostgreSQL 连接已恢复（此前连续失败 %d 次）", failures)
			}
			failures = 0
			p.setHealthy(true)

		case errors.Is(err, ErrClosed):
			return

		default:
			failures++
			p.setHealthy(false)
			p.log.Warnf(p.ctx, "PostgreSQL 健康检查失败（连续 %d 次）: %v", failures, err)

			if need := p.cfg.RebuildAfterFailures; need > 0 && failures >= need {
				p.log.Warnf(p.ctx, "连续失败 %d 次，主动重建连接池", failures)
				if rerr := p.Reconnect(); rerr != nil {
					p.log.Errorf(p.ctx, "重建连接池失败: %v", rerr)
				} else {
					failures = 0
				}
			}
		}
	}
}

// ping 用带超时的上下文探活。快速路径只读一次锁，不阻塞调用方。
func (p *Postgres) ping() error {
	p.mu.RLock()
	pool, closed := p.pool, p.closed
	p.mu.RUnlock()

	if closed {
		return ErrClosed
	}
	if pool == nil {
		return ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.ConnectTimeout)
	defer cancel()
	return pool.PingContext(ctx)
}

func (p *Postgres) setHealthy(v bool) {
	p.mu.Lock()
	p.healthy = v
	p.mu.Unlock()
}
