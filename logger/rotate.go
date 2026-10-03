package logger

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// rotateWriter 按体积轮转的日志写入器，零依赖实现：
//   - 每次写入前检查体积，超过上限即轮转；
//   - 历史文件命名为 <日志文件>.<时间戳>[.gz]；
//   - 轮转后按"数量 + 天数"双条件清理历史文件。
//
// 内部加锁，可被多个 goroutine 并发写入；启用压缩时 gzip 在持锁内同步完成，
// 换来的是"不会与写入交错"的确定性（代价是单次轮转期间的写入会短暂阻塞）。
type rotateWriter struct {
	mu   sync.Mutex
	path string
	cfg  RotateConfig
	file *os.File
	size int64

	// lastRotateFail 上次轮转失败的时刻。轮转失败时降级为继续写当前
	// 文件（宁可超限也不丢日志），但 size 仍超限会让每次 Write 都重试
	// 轮转（每次一次 rename 系统调用）；用冷却窗口把重试频率压到
	// rotateRetryCooldown 一次。
	lastRotateFail time.Time
}

// rotateRetryCooldown 轮转失败后的重试冷却。期间直接降级写当前文件。
const rotateRetryCooldown = 30 * time.Second

// newRotateWriter 创建轮转写入器并打开日志文件
func newRotateWriter(path string, cfg RotateConfig) (*rotateWriter, error) {
	w := &rotateWriter{path: path, cfg: cfg}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}

// open 打开（或创建）日志文件并记录当前体积
func (w *rotateWriter) open() error {
	if dir := filepath.Dir(w.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("创建日志目录 %s 失败: %w", dir, err)
		}
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("打开日志文件 %s 失败: %w", w.path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("获取日志文件信息失败: %w", err)
	}
	w.file, w.size = f, info.Size()
	return nil
}

// Write 实现 io.Writer
//
// 轮转失败时降级为继续写当前文件，而不是把这条日志丢掉：
// 旧实现在 rename 失败时直接 return，而 size 已被 open() 重置为当前
// 体积（仍超限），下一次 Write 又会触发轮转、又失败 —— 目录权限被改、
// 只读文件系统、容器磁盘异常等场景下**全部日志持续丢失**。
// 日志轮转是非核心动作，核心动作是把日志写下去。
func (w *rotateWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil { // 被 Close 之后仍允许继续写（自动重开）
		if err := w.open(); err != nil {
			return 0, err
		}
	}

	maxSize := int64(w.cfg.MaxSizeMB) << 20
	if maxSize > 0 && w.size+int64(len(p)) > maxSize && time.Since(w.lastRotateFail) >= rotateRetryCooldown {
		if err := w.rotate(); err != nil {
			w.lastRotateFail = time.Now()
			// 降级：不丢日志，继续写当前文件。错误只打到 stderr
			//（与 gzip 失败同等的提示强度），不影响本条与后续写入。
			fmt.Fprintf(os.Stderr, "logger: 日志轮转失败（降级为继续写入当前文件）: %v\n", err)
		}
	}

	if w.file == nil {
		// 轮转失败且重开也失败：rotate 已把错误打进 stderr，
		// 这里不再重试 open（避免每次 Write 都撞同一个错误），直接报错。
		return 0, fmt.Errorf("日志文件 %s 不可写（轮转失败后未恢复）", w.path)
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, err
}

// rotate 轮转当前日志文件：关闭 → 改名为历史文件 →（可选压缩）→ 清理 → 重开
func (w *rotateWriter) rotate() error {
	if w.file != nil {
		_ = w.file.Sync()
		_ = w.file.Close()
		w.file = nil
	}

	backup := w.backupPath()
	if err := os.Rename(w.path, backup); err != nil {
		// 改名失败时不能丢日志句柄：立即重开继续写
		if openErr := w.open(); openErr != nil {
			return fmt.Errorf("日志轮转失败(改名 %s): %w; 重开文件亦失败: %v", backup, err, openErr)
		}
		return fmt.Errorf("日志轮转失败(改名 %s): %w", backup, err)
	}

	if w.cfg.Compress {
		if err := gzipFile(backup); err != nil {
			// 压缩失败不影响主流程，原文件仍在
			fmt.Fprintf(os.Stderr, "logger: 压缩历史日志 %s 失败: %v\n", backup, err)
		}
	}

	w.prune()

	if err := w.open(); err != nil {
		return err
	}
	w.size = 0
	return nil
}

// backupPath 生成不与既有文件冲突的历史文件名
func (w *rotateWriter) backupPath() string {
	base := w.path + "." + time.Now().Format("20060102T150405.000")
	for i := 1; i <= 1000; i++ {
		candidate := base
		if i > 1 {
			candidate = fmt.Sprintf("%s-%d", base, i)
		}
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
	}
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano())
}

// prune 清理历史日志：最多保留 MaxBackups 个，且不超过 MaxAgeDays 天
func (w *rotateWriter) prune() {
	dir := filepath.Dir(w.path)
	prefix := filepath.Base(w.path) + "."

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	backups := make([]os.FileInfo, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		backups = append(backups, info)
	}
	// 按修改时间倒序：索引 >= MaxBackups 的即超出保留数量
	sort.Slice(backups, func(i, j int) bool { return backups[i].ModTime().After(backups[j].ModTime()) })

	var deadline time.Time
	if w.cfg.MaxAgeDays > 0 {
		deadline = time.Now().AddDate(0, 0, -w.cfg.MaxAgeDays)
	}
	for i, info := range backups {
		tooMany := i >= w.cfg.MaxBackups
		tooOld := !deadline.IsZero() && info.ModTime().Before(deadline)
		if tooMany || tooOld {
			_ = os.Remove(filepath.Join(dir, info.Name()))
		}
	}
}

// Sync 刷盘
func (w *rotateWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	return w.file.Sync()
}

// Close 关闭文件句柄，可重复调用（之后再次 Write 会自动重开）
func (w *rotateWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// gzipFile 把文件压缩为 <path>.gz，成功后删除原文件
func gzipFile(path string) (err error) {
	src, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := src.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	dstPath := path + ".gz"
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := dst.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			_ = os.Remove(dstPath) // 压缩不完整则丢弃半成品
		}
	}()

	zw := gzip.NewWriter(dst)
	zw.Name = filepath.Base(path)
	zw.ModTime = time.Now()
	if _, err = io.Copy(zw, src); err != nil {
		_ = zw.Close()
		return err
	}
	if err = zw.Close(); err != nil {
		return err
	}
	if err = dst.Sync(); err != nil {
		return err
	}
	return os.Remove(path)
}
