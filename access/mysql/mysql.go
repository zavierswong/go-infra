// Package mysql 封装 MySQL 的连接与**连接池**管理。
//
// 在 gorm.io/gorm + go-sql-driver/mysql 之上，补齐了四件容易写错的事：
//
//  1. **连接池调优**：暴露 MaxOpenConns / MaxIdleConns / ConnMaxLifetime / ConnMaxIdleTime，
//     并按 GOMAXPROCS 推导合理的默认值。
//     database/sql 的 MaxIdleConns 默认只有 2，高 QPS 下空闲连接会被瞬间抢光，
//     之后每个请求都要重新拨号 —— 这是本包最想帮使用者避开的一个性能陷阱。
//  2. **可信的初始化结果**：gorm.Open 是懒连接，不 Ping 的话即使 MySQL 不存在
//     也会返回 nil error。Open 会做一次真实探活，并把 DSN 语法错误提前到启动期。
//  3. **可控的重试**：旧版在 sync.Once 里无限重连，MySQL 不可达时首个调用方
//     **永久阻塞**且永远拿不到 error。现在由 Config.DialAttempts 显式控制。
//  4. **可恢复的关闭**：Close 幂等、不死锁、真正关闭连接池；重建时先关旧池，
//     不会像旧版那样反复重连却持续泄漏。
//
// 与旧版 API 的关系：Get / GetDB / GetSql 仅为平滑迁移保留并已标记 Deprecated。
// 新代码请使用 Open，它返回的 *MySQL 由调用方持有，支持多实例（多库、读写分离、并行测试）。
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/zavierswong/go-infra/logger"
)

// MySQL 是一个 MySQL 客户端，持有一个 gorm.DB 与其底层的 *sql.DB 连接池。
//
// 所有导出方法都可并发调用。Close 之后调用 DB / SQL 会返回 nil，
// 请先用 Closed 判断，或用 HealthCheck 探活。
type MySQL struct {
	cfg Config
	dsn string
	log *logger.Plog

	// mu 是**唯一**保护 db / pool / closed / healthy 的锁。
	//
	// 旧版有两把锁：写入方（connect/Close）持结构体字段 m.mu，
	// 读取方（GetDB/GetSql/Stats/ping）持包级 mu —— 两者互不相干，
	// 等于完全没有互斥，并发读写 db 字段构成真实的数据竞争。
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
// 返回的 *MySQL 必须由调用方负责 Close。初始化失败会按 Config.DialAttempts
// 做有限次重试，因此不会像旧版那样无限阻塞在初始化里。
func Open(cfg Config) (*MySQL, error) {
	cfg, err := cfg.ready()
	if err != nil {
		return nil, err
	}
	dsn, err := cfg.dsn()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	m := &MySQL{
		cfg:    cfg,
		dsn:    dsn,
		log:    logger.NewPlog("MySQL"),
		ctx:    ctx,
		cancel: cancel,
	}

	if err := m.dial(ctx); err != nil {
		cancel()
		return nil, err
	}

	m.wg.Add(1)
	go m.monitor()

	m.log.Infof(ctx,
		"MySQL 已连接: 最大连接=%d 最大空闲=%d 连接寿命=%s 空闲寿命=%s",
		cfg.MaxOpenConns, cfg.MaxIdleConns, cfg.ConnMaxLifetime, cfg.ConnMaxIdleTime)
	return m, nil
}

// dial 按 Config.DialAttempts 建连，退避从 DialBackoff 指数增长到 DialMaxBackoff。
//
// DialAttempts 为负表示无限重试，但每次尝试前都会检查 ctx，
// 因此 Close 能立刻中断这个过程 —— 旧版的无限循环没有这个出口。
func (m *MySQL) dial(ctx context.Context) error {
	attempts := m.cfg.DialAttempts
	infinite := attempts < 0
	backoff := m.cfg.DialBackoff

	var lastErr error
	for attempt := 1; infinite || attempt <= attempts; attempt++ {
		if m.Closed() {
			return ErrClosed
		}
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return lastErr
			}
			return err
		}

		db, pool, err := m.open()
		if err == nil {
			m.adopt(db, pool)
			if attempt > 1 {
				m.log.Infof(ctx, "第[%d]次尝试连接 MySQL 成功", attempt)
			}
			return nil
		}

		lastErr = err
		m.log.Warnf(ctx, "第[%d]次连接 MySQL 失败: %v", attempt, err)

		if !infinite && attempt >= attempts {
			break
		}

		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(backoff):
		}

		if backoff *= 2; backoff > m.cfg.DialMaxBackoff {
			backoff = m.cfg.DialMaxBackoff
		}
	}
	return lastErr
}

// open 构造一个 GORM 实例与它的连接池，并验证连通性。
func (m *MySQL) open() (*gorm.DB, *sql.DB, error) {
	db, err := gorm.Open(gormmysql.Open(m.dsn), &gorm.Config{
		Logger: &logger.Mysql{
			Name:          "MySQL",
			SlowThreshold: m.cfg.SlowThreshold,
			LogLevel:      m.cfg.LogLevel,
		},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("%w: 打开数据库失败: %v", ErrConnect, err)
	}

	pool, err := db.DB()
	if err != nil {
		return nil, nil, fmt.Errorf("%w: 获取底层连接池失败: %v", ErrConnect, err)
	}
	applyPool(pool, m.cfg)

	// gorm.Open 是**懒连接**：它只解析 DSN 并构造结构体，不会真的连数据库。
	// 旧版因此把"配置能解析"误当成"连接成功"（connect 永远返回 nil）。
	// 这里补一次真实探活，让 Open 的返回值可信。
	pingCtx, cancel := context.WithTimeout(context.Background(), m.cfg.ConnectTimeout)
	defer cancel()
	if err := pool.PingContext(pingCtx); err != nil {
		_ = pool.Close()
		return nil, nil, fmt.Errorf("%w: 连通性检查失败: %v", ErrConnect, err)
	}

	// 探活通过后再挂监控回调：挂在 `&gorm.Config{}` 之后、失败返回之前的话，
	// 每次重试都会给一个立刻被丢弃的 gorm 实例注册一遍回调，
	// 注册失败的告警也会重复刷屏。
	m.registerMetrics(db)

	return db, pool, nil
}

// applyPool 把连接池参数写进 database/sql。
//
// 这是本包"提高性能"的落点，四项各有明确职责：
//   - SetMaxOpenConns    ：并发上限，保护数据库不被连接数压垮；
//   - SetMaxIdleConns    ：**热连接数量**，直接决定高 QPS 下是否需要反复拨号；
//   - SetConnMaxLifetime ：防止拿到服务端已按 wait_timeout 关闭的坏连接；
//   - SetConnMaxIdleTime ：低频时段回收冗余连接，降低服务端连接占用。
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
func (m *MySQL) adopt(db *gorm.DB, pool *sql.DB) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		// 客户端已关闭：新池就地丢弃。
		if pool != nil {
			_ = pool.Close()
		}
		return
	}
	old := m.pool
	m.db, m.pool = db, pool
	m.healthy = true
	m.mu.Unlock()

	// 在锁外关闭旧连接池：Close 可能阻塞，持锁做会拖住所有读。
	// 旧版重连时直接覆盖字段、从不关闭旧池，反复重连会持续泄漏连接池及其后台协程。
	if old != nil && old != pool {
		_ = old.Close()
	}
}

// DB 返回 GORM 实例。客户端已关闭时返回 nil。
func (m *MySQL) DB() *gorm.DB {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.db
}

// SQL 返回底层连接池。客户端已关闭时返回 nil。
func (m *MySQL) SQL() *sql.DB {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pool
}

// Stats 返回连接池统计，可直接接入 Prometheus 做监控。
//
// 常看的几个指标：
//   - OpenConnections：当前打开连接数，长期贴近 MaxOpenConns 说明池子偏小；
//   - InUse / Idle：使用中与空闲的连接数；
//   - WaitCount / WaitDuration：拿不到连接的等待，**大于 0 就说明 MaxOpenConns 需要调大**；
//   - MaxIdleClosed / MaxLifetimeClosed：因空闲或寿命被回收的次数，
//     占比过高说明 MaxIdleConns 太小或 ConnMaxLifetime 太短。
func (m *MySQL) Stats() sql.DBStats {
	pool := m.SQL()
	if pool == nil {
		return sql.DBStats{}
	}
	return pool.Stats()
}

// HealthCheck 探活，可用于健康检查接口。
func (m *MySQL) HealthCheck(ctx context.Context) error {
	m.mu.RLock()
	pool, closed := m.pool, m.closed
	m.mu.RUnlock()

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
func (m *MySQL) Closed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.closed
}

// Healthy 报告最近一次后台探活的结果。
func (m *MySQL) Healthy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.healthy && !m.closed
}

// Config 返回生效后的配置（已补齐默认值）。
func (m *MySQL) Config() Config { return m.cfg }

// Reconnect 主动重建连接池：先建新池、就绪后再关旧池，期间旧池仍可服务。
//
// 一般不需要调用：sql.DB 本身就是连接池，服务端恢复后下一次查询会自动拨号。
// 需要它的场景是"连接池确实卡死"或"DSN 变了"，也可通过
// Config.RebuildAfterFailures 让监控协程自动触发。
func (m *MySQL) Reconnect() error {
	m.rebuildMu.Lock()
	defer m.rebuildMu.Unlock()

	if m.Closed() {
		return ErrClosed
	}
	return m.dial(m.ctx)
}

// Close 关闭连接池并停止监控。可重复调用，第二次起直接返回 nil。
func (m *MySQL) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	pool := m.pool
	m.db, m.pool = nil, nil
	m.healthy = false
	m.mu.Unlock()

	// 顺序很关键：先取消 ctx 让监控循环与在建连接退出，再关闭连接池，
	// 最后等协程收敛。全程不持锁，避免与读操作互相等待。
	//
	// 旧版正是在持锁状态下 `return r.channel.Close()`（同类写法），
	// 导致 defer 的 Unlock 不执行，之后所有操作抢锁永久阻塞。
	m.cancel()

	var err error
	if pool != nil {
		if cerr := pool.Close(); cerr != nil {
			err = fmt.Errorf("关闭 MySQL 连接池失败: %w", cerr)
		}
	}
	m.wg.Wait()

	m.log.Infof(context.Background(), "MySQL 连接已关闭")
	return err
}

// monitor 周期性探活。
//
// 与旧版的区别：旧版探活失败就进入无限重连，且重连期间监控循环被阻塞、
// 旧连接池被丢弃。这里改为只做**观测**（更新 Healthy、打日志），
// 需要重建时由 RebuildAfterFailures 显式开启，重建走 Reconnect 的正常路径。
func (m *MySQL) monitor() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cfg.HealthCheckInterval)
	defer ticker.Stop()

	failures := 0
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
		}

		err := m.ping()
		switch {
		case err == nil:
			if failures > 0 {
				m.log.Infof(m.ctx, "MySQL 连接已恢复（此前连续失败 %d 次）", failures)
			}
			failures = 0
			m.setHealthy(true)

		case errors.Is(err, ErrClosed):
			return

		default:
			failures++
			m.setHealthy(false)
			m.log.Warnf(m.ctx, "MySQL 健康检查失败（连续 %d 次）: %v", failures, err)

			if need := m.cfg.RebuildAfterFailures; need > 0 && failures >= need {
				m.log.Warnf(m.ctx, "连续失败 %d 次，主动重建连接池", failures)
				if rerr := m.Reconnect(); rerr != nil {
					m.log.Errorf(m.ctx, "重建连接池失败: %v", rerr)
				} else {
					failures = 0
				}
			}
		}
	}
}

// ping 用带超时的上下文探活。快速路径只读一次锁，不阻塞调用方。
func (m *MySQL) ping() error {
	m.mu.RLock()
	pool, closed := m.pool, m.closed
	m.mu.RUnlock()

	if closed {
		return ErrClosed
	}
	if pool == nil {
		return ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(m.ctx, m.cfg.ConnectTimeout)
	defer cancel()
	return pool.PingContext(ctx)
}

func (m *MySQL) setHealthy(v bool) {
	m.mu.Lock()
	m.healthy = v
	m.mu.Unlock()
}
