package supervisor_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/zavierswong/go-infra/logger"
	"github.com/zavierswong/go-infra/supervisor"
)

// ExampleManager_Start 开发环境拉起一个本地服务并优雅停止。
func ExampleManager_Start() {
	m := supervisor.New(supervisor.Config{
		PidDir: os.TempDir(),     // pidfile 目录，自动创建
		Logger: logger.Default(), // 生命周期日志
	})

	proc := supervisor.Process{
		Name:    "local-api",
		Command: "/usr/local/bin/api-server",
		Args:    []string{"--port", "8080", "--dev"},
		Stdout:  os.Stdout, // 开发调试：输出透传
		Stderr:  os.Stderr,
		// StopSignal 缺省 SIGTERM；StopTimeout 缺省 10s
	}

	pid, err := m.Start(proc)
	if err != nil {
		if errors.Is(err, supervisor.ErrAlreadyRunning) {
			// pidfile 探活发现旧实例还在，先停再启或直接复用
			_ = m.Stop(context.Background(), proc.Name)
			_, _ = m.Start(proc)
		}
		return
	}
	_ = pid

	// ... 使用一段时间后优雅停止：
	// SIGTERM → 等 10s → 仍存活则 SIGKILL → 删 pidfile
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = m.Stop(ctx, proc.Name)
}

// ExampleManager_Status 状态查询与重启。
func ExampleManager_Status() {
	m := supervisor.New(supervisor.Config{Logger: slog.Default()})

	st, err := m.Status("local-api")
	if errors.Is(err, supervisor.ErrNotRunning) {
		slog.Info("not running")
		return
	}
	slog.Info("status", "pid", st.Pid, "running", st.Running, "exit_code", st.ExitCode)

	// 用原定义重启（Stop + Start）。
	_ = m.Restart(context.Background(), "local-api")

	for _, s := range m.List() {
		slog.Info("process", "name", s.Name, "running", s.Running)
	}
}
