// Package supervisor 提供开发环境友好的进程管理：以代码 API 定义进程，
// 启动即落 pidfile，停止时先发优雅信号、超时再 SIGKILL 兜底。
//
// 定位是"库"而非常驻守护进程（不是 supervisord）：
//   - 典型用法是开发环境用 Go 程序/脚本拉起和停止本地依赖进程；
//   - 停止语义与本仓库 shutdown 包互补：被管进程内部用 shutdown
//     收 SIGTERM 走清理钩子，supervisor 只负责发信号和兜底强杀。
//
// 平台边界：信号控制为 Unix-only（macOS/Linux）；Windows 下 SIGTERM
// 无意义，本包不做支持。
package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// 错误语义。
var (
	// ErrAlreadyRunning 目标进程已有存活实例（pidfile 存在且探活通过）。
	ErrAlreadyRunning = errors.New("supervisor: process already running")
	// ErrNotRunning 请求停止/查询的进程未注册且无 pidfile。
	ErrNotRunning = errors.New("supervisor: process not running")
	// ErrInvalidName 进程名非法（含路径分隔符，会破坏 pidfile 布局）。
	ErrInvalidName = errors.New("supervisor: invalid process name")
)

// 默认值。
const (
	DefaultStopTimeout = 10 * time.Second
	// killGrace SIGKILL 之后的等待上限；正常情况下进程会瞬间消失，
	// 超过它说明系统异常，立即报错返回。
	killGrace = 3 * time.Second
)

// Process 进程定义。Name/Command 必填，其余零值取默认。
type Process struct {
	// Name 进程唯一标识，也是 pidfile 名（<name>.pid）。
	// 不允许包含路径分隔符与 ".."。
	Name string

	// Command 可执行文件路径（建议绝对路径或可在 PATH 中解析的名称）。
	Command string
	// Args 命令参数。
	Args []string
	// Dir 工作目录；空串继承当前进程。
	Dir string
	// Env 子进程环境变量（完整替换，同 os/exec 语义：非增量）。
	// nil 继承当前进程。
	Env []string

	// Stdout / Stderr 子进程输出；nil 时丢弃（io.Discard）。
	// 开发环境调试可传 os.Stdout / os.Stderr。
	Stdout io.Writer
	Stderr io.Writer

	// StopSignal 优雅停止信号，nil 用 SIGTERM。
	StopSignal os.Signal
	// StopTimeout 发信号后等待退出的上限，<=0 用 DefaultStopTimeout；
	// 超时后 SIGKILL 强杀。
	StopTimeout time.Duration
}

// Config 管理器配置。
type Config struct {
	// PidDir pidfile 目录（自动创建），空串用
	// os.TempDir()/go-infra-supervisor。
	PidDir string

	// Logger 生命周期日志；nil 用 slog.Default()。
	Logger *slog.Logger
}

// Status 进程状态快照。
type Status struct {
	Name     string
	Pid      int
	Running  bool
	ExitCode int // 仅 !Running 且有退出信息时有效；未知为 -1
}

// entry 是 Manager 对每个进程的追踪记录。
type entry struct {
	spec    Process
	cmd     *exec.Cmd
	done    chan struct{} // cmd.Wait 返回后关闭；nil 表示孤儿（仅 pidfile）
	exitErr error
}

// Manager 进程管理器。并发安全；一个 Manager 可管理多个进程。
type Manager struct {
	cfg Config
	log *slog.Logger

	mu    sync.Mutex
	procs map[string]*entry
}

// New 构造管理器并确保 PidDir 存在。
func New(cfg Config) *Manager {
	if cfg.PidDir == "" {
		cfg.PidDir = filepath.Join(os.TempDir(), "go-infra-supervisor")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	_ = os.MkdirAll(cfg.PidDir, 0o755) // 失败推迟到写 pidfile 时报错
	return &Manager{cfg: cfg, log: log, procs: make(map[string]*entry)}
}

// Start 拉起进程并写 pidfile。已有存活实例返回 ErrAlreadyRunning。
// 成功后由独立 goroutine reap 子进程（避免僵尸），
// 退出信息可通过 Status().ExitCode 查询。
func (m *Manager) Start(p Process) (int, error) {
	if !validName(p.Name) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidName, p.Name)
	}
	if p.Command == "" {
		return 0, errors.New("supervisor: empty command")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if e, ok := m.procs[p.Name]; ok {
		if alive := probe(e); alive {
			return 0, ErrAlreadyRunning
		}
		// 上次实例已退出：复用槽位重新启动。
	}
	// 不在内存但 pidfile 存活（父进程重启后的孤儿）同样拒绝重复启动。
	if pid, ok := readPid(m.pidPath(p.Name)); ok && signal0(pid) {
		return 0, ErrAlreadyRunning
	}

	cmd := exec.Command(p.Command, p.Args...)
	cmd.Dir = p.Dir
	cmd.Env = p.Env
	cmd.Stdout = orDiscard(p.Stdout)
	cmd.Stderr = orDiscard(p.Stderr)

	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("supervisor: start %s: %w", p.Name, err)
	}
	if err := m.writePid(p.Name, cmd.Process.Pid); err != nil {
		// pidfile 写失败：进程已拉起但状态不可追踪，立刻停掉并报错，
		// 不留"活着但不可管理"的实例。
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return 0, fmt.Errorf("supervisor: write pidfile %s: %w", p.Name, err)
	}

	e := &entry{spec: normalize(p), cmd: cmd, done: make(chan struct{})}
	m.procs[p.Name] = e
	go func() {
		e.exitErr = cmd.Wait() // 唯一 Waiter
		close(e.done)
		m.log.Info("supervisor: process exited",
			"name", p.Name, "pid", cmd.Process.Pid, "error", errString(e.exitErr))
	}()

	m.log.Info("supervisor: process started",
		"name", p.Name, "pid", cmd.Process.Pid, "command", p.Command)
	return cmd.Process.Pid, nil
}

// Stop 优雅停止：发 StopSignal → 等 StopTimeout（或 ctx 取消）→
// SIGKILL 兜底 → reap + 删 pidfile。
//
// 幂等：进程已死或 pidfile 残留时清理后返回 nil；完全未注册且无
// pidfile 返回 ErrNotRunning。父进程重启后可对孤儿 pidfile 停止
// （无 Wait 句柄，轮询探活直到消失）。
func (m *Manager) Stop(ctx context.Context, name string) error {
	if !validName(name) {
		return fmt.Errorf("%w: %q", ErrInvalidName, name)
	}

	m.mu.Lock()
	e, hasEntry := m.procs[name]
	m.mu.Unlock()

	if !hasEntry {
		// 孤儿路径：只有 pidfile。
		return m.stopOrphan(ctx, name)
	}
	if !probe(e) {
		// 已退出：清理现场，幂等返回。
		m.cleanup(name, e, e.cmd.Process.Pid)
		return nil
	}
	return m.stopRunning(ctx, name, e)
}

func (m *Manager) stopRunning(ctx context.Context, name string, e *entry) error {
	sig := e.spec.StopSignal
	if sig == nil {
		sig = syscall.SIGTERM
	}
	timeout := e.spec.StopTimeout
	if timeout <= 0 {
		timeout = DefaultStopTimeout
	}
	pid := e.cmd.Process.Pid

	m.log.Info("supervisor: stopping", "name", name, "pid", pid, "signal", sig.String())
	if err := e.cmd.Process.Signal(sig); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			// 进程恰好在发信号前退出：done 必然已经（或即将）关闭。
			<-e.done
			m.cleanup(name, e, pid)
			return nil
		}
		// 其他失败（权限不足、pid 被复用等）不等于"进程已退出"，
		// 此时进程多半还活着 —— 走 SIGKILL 兜底。
		// 旧实现在这里直接 `<-e.done`，既不看错误类型也没有 ctx 保护，
		// 遇到非 ErrProcessDone 的失败会永久阻塞。
		m.log.Warn("supervisor: signal failed, killing",
			"name", name, "pid", pid, "error", err)
		return m.killAndWait(ctx, name, e)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-e.done: // 优雅退出
	case <-timer.C:
		m.log.Warn("supervisor: graceful stop timed out, killing",
			"name", name, "pid", pid, "timeout", timeout.String())
		return m.killAndWait(ctx, name, e)
	case <-ctx.Done():
		m.log.Warn("supervisor: stop canceled by ctx, killing", "name", name, "pid", pid)
		return m.killAndWait(ctx, name, e)
	}
	m.cleanup(name, e, pid)
	return nil
}

func (m *Manager) killAndWait(ctx context.Context, name string, e *entry) error {
	if err := e.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		m.log.Error("supervisor: kill failed", "name", name, "error", err)
	}
	select {
	case <-e.done:
	case <-time.After(killGrace):
		return fmt.Errorf("supervisor: process %s did not die after SIGKILL", name)
	case <-ctx.Done():
		// ctx 已取消时仍给一个有限宽限期：SIGKILL 刚发出，进程可能还没
		// 被 reap，但绝不能因为 ctx 取消就无限等下去。
		m.log.Warn("supervisor: ctx canceled while killing", "name", name)
		return fmt.Errorf("supervisor: stop %s canceled: %w", name, ctx.Err())
	}
	m.cleanup(name, e, e.cmd.Process.Pid)
	return nil
}

// stopOrphan 处理"pidfile 在但句柄不在"（父进程重启）的情况：
// 找回 pid → 发信号 → 轮询探活直到消失或超时 → SIGKILL → 删 pidfile。
// 子进程此时由 init/launchd 收养，无需 reap。
func (m *Manager) stopOrphan(ctx context.Context, name string) error {
	pid, ok := readPid(m.pidPath(name))
	if !ok || !signal0(pid) {
		// 无 pidfile 或进程已死：清掉残留文件。
		_ = os.Remove(m.pidPath(name))
		return ErrNotRunning
	}

	spec := m.specOf(name)
	sig := spec.StopSignal
	if sig == nil {
		sig = syscall.SIGTERM
	}
	timeout := spec.StopTimeout
	if timeout <= 0 {
		timeout = DefaultStopTimeout
	}

	m.log.Info("supervisor: stopping orphan", "name", name, "pid", pid, "signal", sig.String())
	proc, err := os.FindProcess(pid) // Unix 上总是成功
	if err != nil {
		m.cleanup(name, nil, pid)
		return fmt.Errorf("supervisor: find process %s (pid %d): %w", name, pid, err)
	}
	if err := proc.Signal(sig); err != nil {
		m.cleanup(name, nil, pid)
		return nil // 信号失败 = 已退出
	}

	deadline := time.After(timeout)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			m.log.Warn("supervisor: orphan stop timed out, killing",
				"name", name, "pid", pid, "timeout", timeout.String())
			_ = proc.Kill()
			return m.waitForOrphanGone(ctx, proc, name, pid)
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			if !signal0(pid) {
				m.cleanup(name, nil, pid)
				return nil
			}
		}
	}
}

func (m *Manager) waitForOrphanGone(ctx context.Context, proc *os.Process, name string, pid int) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			if !signal0(pid) {
				m.cleanup(name, nil, pid)
				return nil
			}
		}
	}
}

// Restart 停止（若在跑）后用原定义重新拉起。未启动过返回 ErrNotRunning。
func (m *Manager) Restart(ctx context.Context, name string) error {
	m.mu.Lock()
	e, ok := m.procs[name]
	m.mu.Unlock()
	if !ok {
		return ErrNotRunning
	}
	if err := m.Stop(ctx, name); err != nil && !errors.Is(err, ErrNotRunning) {
		return err
	}
	_, err := m.Start(e.spec)
	return err
}

// Status 查询单个进程。未注册且无 pidfile 返回 ErrNotRunning。
func (m *Manager) Status(name string) (Status, error) {
	if !validName(name) {
		return Status{}, fmt.Errorf("%w: %q", ErrInvalidName, name)
	}
	m.mu.Lock()
	e, hasEntry := m.procs[name]
	m.mu.Unlock()

	if hasEntry {
		select {
		case <-e.done:
			return Status{Name: name, Pid: e.cmd.Process.Pid, ExitCode: exitCode(e.exitErr)}, nil
		default:
			return Status{Name: name, Pid: e.cmd.Process.Pid, Running: true, ExitCode: -1}, nil
		}
	}
	pid, ok := readPid(m.pidPath(name))
	if !ok {
		return Status{}, ErrNotRunning
	}
	return Status{Name: name, Pid: pid, Running: signal0(pid), ExitCode: -1}, nil
}

// List 返回全部已知进程（内存 + pidfile 目录）的状态。
func (m *Manager) List() []Status {
	m.mu.Lock()
	known := make(map[string]struct{}, len(m.procs))
	statuses := make([]Status, 0, len(m.procs))
	for name, e := range m.procs {
		known[name] = struct{}{}
		select {
		case <-e.done:
			statuses = append(statuses, Status{Name: name, Pid: e.cmd.Process.Pid, ExitCode: exitCode(e.exitErr)})
		default:
			statuses = append(statuses, Status{Name: name, Pid: e.cmd.Process.Pid, Running: true, ExitCode: -1})
		}
	}
	m.mu.Unlock()

	entries, err := os.ReadDir(m.cfg.PidDir)
	if err != nil {
		return statuses
	}
	for _, f := range entries {
		if !strings.HasSuffix(f.Name(), ".pid") {
			continue
		}
		name := strings.TrimSuffix(f.Name(), ".pid")
		if _, ok := known[name]; ok {
			continue
		}
		st, err := m.Status(name)
		if err == nil {
			statuses = append(statuses, st)
		}
	}
	return statuses
}

// ---- 内部工具 ----

func (m *Manager) pidPath(name string) string {
	return filepath.Join(m.cfg.PidDir, name+".pid")
}

func (m *Manager) writePid(name string, pid int) error {
	return os.WriteFile(m.pidPath(name), []byte(fmt.Sprint(pid)), 0o644)
}

func (m *Manager) cleanup(name string, e *entry, pid int) {
	// 只删"确实属于本次操作的进程"的 pidfile。
	//
	// Stop 在释放 m.mu 之后会阻塞最长 StopTimeout（还要加 SIGKILL 的
	// 宽限期），期间完全可能有另一个 goroutine 用同名 Start 起新进程并
	// 写入新 pidfile。无校验地 os.Remove 会把正在运行的新进程变成
	// "活着但不可管理的孤儿"，List/Status/后续 Stop 全部失真。
	if pid > 0 {
		if cur, ok := readPid(m.pidPath(name)); ok && cur != pid {
			m.log.Warn("supervisor: skip stale cleanup, pidfile belongs to another process",
				"name", name, "stopped_pid", pid, "pidfile_pid", cur)
			return
		}
	}
	_ = os.Remove(m.pidPath(name))
}

func (m *Manager) specOf(name string) Process {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.procs[name]; ok {
		return e.spec
	}
	return Process{Name: name}
}

// probe 探活：有 Wait 句柄看 done，否则 signal 0。
func probe(e *entry) bool {
	if e.done != nil {
		select {
		case <-e.done:
			return false
		default:
			return true
		}
	}
	return signal0(e.cmd.Process.Pid)
}

// signal0 用 kill(pid, 0) 探活（不真正发信号）。
func signal0(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) == nil
}

func readPid(path string) (int, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	var pid int
	if _, err := fmt.Sscanf(string(data), "%d", &pid); err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

func exitCode(err error) int {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	if err == nil {
		return 0
	}
	return -1
}

func validName(name string) bool {
	if name == "" || strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
		return false
	}
	return true
}

func normalize(p Process) Process {
	if p.StopTimeout <= 0 {
		p.StopTimeout = DefaultStopTimeout
	}
	return p
}

func orDiscard(w io.Writer) io.Writer {
	if w == nil {
		return io.Discard
	}
	return w
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
