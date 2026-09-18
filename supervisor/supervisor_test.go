package supervisor_test

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/zavierswong/go-infra/supervisor"
)

// ---- helper process：用测试二进制自重入模拟被管进程 ----
//
// 父进程侧：Command = os.Args[0]，Env 携带模式与就绪文件路径。
// helper 启动后先 touch 就绪文件，父进程轮询该文件完成握手——
// 不用 stdout 握手，避免 exec 拷贝协程阻塞与 Restart 后的重复读取问题。

const (
	envHelper = "GO_INFRA_HELPER"
	envMode   = "GO_INFRA_HELPER_MODE"
	envReady  = "GO_INFRA_HELPER_READY"
)

// helperProc 构造被管进程定义与就绪等待函数。
// wait() 轮询就绪文件，观测到后删除它，因此支持 Restart 后再次握手。
func helperProc(t *testing.T, mode string) (supervisor.Process, func()) {
	t.Helper()
	ready := filepath.Join(t.TempDir(), "ready")
	proc := supervisor.Process{
		Name:        "helper-" + mode,
		Command:     os.Args[0],
		Args:        []string{"-test.run=TestHelperProcess", "--"},
		Env:         append(os.Environ(), envHelper+"=1", envMode+"="+mode, envReady+"="+ready),
		StopTimeout: 2 * time.Second,
	}
	wait := func() {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, err := os.Stat(ready); err == nil {
				_ = os.Remove(ready)
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("helper %s 未在 5s 内就绪", mode)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return proc, wait
}

// TestHelperProcess 仅作为 helper 进程的入口，正常测试直接 return。
func TestHelperProcess(t *testing.T) {
	if os.Getenv(envHelper) == "" {
		return
	}
	ready := os.Getenv(envReady)
	switch os.Getenv(envMode) {
	case "graceful": // 收 SIGTERM 后正常退出
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM)
		_ = os.WriteFile(ready, nil, 0o644)
		<-ch
		os.Exit(0)
	case "graceful-exit-1": // 收 SIGTERM 后以 1 退出
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM)
		_ = os.WriteFile(ready, nil, 0o644)
		<-ch
		os.Exit(1)
	case "ignorant": // 忽略 SIGTERM，必须靠 SIGKILL
		signal.Ignore(syscall.SIGTERM)
		_ = os.WriteFile(ready, nil, 0o644)
		time.Sleep(60 * time.Second)
	}
	os.Exit(0)
}

func newManager(t *testing.T) (*supervisor.Manager, string) {
	t.Helper()
	dir := t.TempDir()
	m := supervisor.New(supervisor.Config{PidDir: dir})
	return m, dir
}

// TestStartStopGraceful 优雅停止全流程：启动 → 探活 → SIGTERM 退出 →
// pidfile 清理 → 状态回收。
func TestStartStopGraceful(t *testing.T) {
	m, dir := newManager(t)
	proc, wait := helperProc(t, "graceful")

	pid, err := m.Start(proc)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if pid <= 0 {
		t.Fatalf("应返回有效 pid, got %d", pid)
	}
	wait()

	st, err := m.Status(proc.Name)
	if err != nil || !st.Running || st.Pid != pid {
		t.Fatalf("Status = %+v, err %v", st, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Stop(ctx, proc.Name); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	st, err = m.Status(proc.Name)
	if err != nil {
		t.Fatalf("停止后 Status 不应报错: %v", err)
	}
	if st.Running {
		t.Fatal("停止后不应再 Running")
	}
	if st.ExitCode != 0 {
		t.Fatalf("优雅退出 ExitCode 应为 0, got %d", st.ExitCode)
	}
	if _, err := os.Stat(filepath.Join(dir, proc.Name+".pid")); !os.IsNotExist(err) {
		t.Fatalf("pidfile 应被删除, stat err = %v", err)
	}
}

// TestStopIdempotent 重复停止幂等；停止未注册进程返回 ErrNotRunning。
func TestStopIdempotent(t *testing.T) {
	m, _ := newManager(t)
	proc, wait := helperProc(t, "graceful")
	if _, err := m.Start(proc); err != nil {
		t.Fatal(err)
	}
	wait()

	ctx := context.Background()
	if err := m.Stop(ctx, proc.Name); err != nil {
		t.Fatalf("第一次 Stop: %v", err)
	}
	if err := m.Stop(ctx, proc.Name); err != nil {
		t.Fatalf("重复 Stop 应幂等, got %v", err)
	}
	if err := m.Stop(ctx, "never-started"); !errors.Is(err, supervisor.ErrNotRunning) {
		t.Fatalf("未注册进程应返回 ErrNotRunning, got %v", err)
	}
}

// TestAlreadyRunning 存活实例不允许重复启动。
func TestAlreadyRunning(t *testing.T) {
	m, _ := newManager(t)
	proc, wait := helperProc(t, "graceful")
	if _, err := m.Start(proc); err != nil {
		t.Fatal(err)
	}
	wait()
	t.Cleanup(func() { _ = m.Stop(context.Background(), proc.Name) })

	if _, err := m.Start(proc); !errors.Is(err, supervisor.ErrAlreadyRunning) {
		t.Fatalf("重复启动应返回 ErrAlreadyRunning, got %v", err)
	}
}

// TestRestart 用原定义重新拉起，新 pid 生效。
func TestRestart(t *testing.T) {
	m, _ := newManager(t)
	proc, wait := helperProc(t, "graceful")

	pid1, err := m.Start(proc)
	if err != nil {
		t.Fatal(err)
	}
	wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Restart(ctx, proc.Name); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	wait() // 新实例已就绪

	st, err := m.Status(proc.Name)
	if err != nil || !st.Running {
		t.Fatalf("重启后应 Running, status %+v err %v", st, err)
	}
	if st.Pid == pid1 {
		t.Fatal("重启后应是新进程（pid 变化）")
	}
	if err := m.Stop(ctx, proc.Name); err != nil {
		t.Fatalf("Stop after restart: %v", err)
	}
}

// TestKillFallback 被管进程忽略 SIGTERM 时超时 SIGKILL 兜底。
func TestKillFallback(t *testing.T) {
	m, _ := newManager(t)
	proc, wait := helperProc(t, "ignorant")
	proc.StopTimeout = 300 * time.Millisecond

	if _, err := m.Start(proc); err != nil {
		t.Fatal(err)
	}
	wait()

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Stop(ctx, proc.Name); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// 300ms 优雅窗口 + kill，远小于进程 60s 睡眠 → 必是被杀。
	st, err := m.Status(proc.Name)
	if err != nil {
		t.Fatal(err)
	}
	if st.Running || st.ExitCode == 0 {
		t.Fatalf("被强杀进程 Running=%v ExitCode=%d", st.Running, st.ExitCode)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("兜底强杀耗时过长: %v", elapsed)
	}
}

// TestOrphanTakeover 父进程重启后，新 Manager 可凭 pidfile 接管停止。
func TestOrphanTakeover(t *testing.T) {
	pidDir := t.TempDir()
	m1 := supervisor.New(supervisor.Config{PidDir: pidDir})
	proc, wait := helperProc(t, "graceful")
	// 孤儿场景 pidfile 目录需跨 Manager 共享，固定 name。
	proc.Name = "orphan"
	if _, err := m1.Start(proc); err != nil {
		t.Fatal(err)
	}
	wait()

	// 模拟父进程重启：全新 Manager，同一 PidDir，内存里没有该进程。
	m2 := supervisor.New(supervisor.Config{PidDir: pidDir})
	st, err := m2.Status(proc.Name)
	if err != nil || !st.Running {
		t.Fatalf("新 Manager 应通过 pidfile 探活, status %+v err %v", st, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m2.Stop(ctx, proc.Name); err != nil {
		t.Fatalf("孤儿停止: %v", err)
	}
	if _, err := m2.Status(proc.Name); !errors.Is(err, supervisor.ErrNotRunning) {
		t.Fatalf("孤儿清理后应 ErrNotRunning, got %v", err)
	}
}

// TestList 内存与 pidfile 目录的进程都会列出。
func TestList(t *testing.T) {
	m, _ := newManager(t)
	p1, wait1 := helperProc(t, "graceful")
	p1.Name = "svc-a"
	p2, wait2 := helperProc(t, "graceful")
	p2.Name = "svc-b"

	if _, err := m.Start(p1); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Start(p2); err != nil {
		t.Fatal(err)
	}
	wait1()
	wait2()
	t.Cleanup(func() {
		ctx := context.Background()
		_ = m.Stop(ctx, p1.Name)
		_ = m.Stop(ctx, p2.Name)
	})

	list := m.List()
	if len(list) != 2 {
		t.Fatalf("应列出 2 个进程, got %+v", list)
	}
	seen := map[string]bool{}
	for _, st := range list {
		seen[st.Name] = true
		if !st.Running {
			t.Errorf("%s 应 Running, got %+v", st.Name, st)
		}
	}
	if !seen["svc-a"] || !seen["svc-b"] {
		t.Fatalf("应包含 svc-a/svc-b, got %+v", list)
	}
}

// TestInvalidName 进程名非法直接拒绝。
func TestInvalidName(t *testing.T) {
	m, _ := newManager(t)
	proc, _ := helperProc(t, "graceful")
	proc.Name = "../evil"
	if _, err := m.Start(proc); !errors.Is(err, supervisor.ErrInvalidName) {
		t.Fatalf("应返回 ErrInvalidName, got %v", err)
	}
	if err := m.Stop(context.Background(), "a/b"); !errors.Is(err, supervisor.ErrInvalidName) {
		t.Fatalf("应返回 ErrInvalidName, got %v", err)
	}
}

// TestExitCodeNonZero 记录非零退出码。
func TestExitCodeNonZero(t *testing.T) {
	m, _ := newManager(t)
	proc, wait := helperProc(t, "graceful-exit-1")
	if _, err := m.Start(proc); err != nil {
		t.Fatal(err)
	}
	wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Stop(ctx, proc.Name); err != nil {
		t.Fatalf("Stop: %v", err) // 优雅路径：退出码不作为 Stop 错误
	}
	st, err := m.Status(proc.Name)
	if err != nil {
		t.Fatal(err)
	}
	if st.ExitCode != 1 {
		t.Fatalf("ExitCode 应为 1, got %d", st.ExitCode)
	}
}

// TestConcurrentOperations 并发启停同一批进程不竞态（-race 把关）。
// TestStopStartRaceKeepsPidfile 并发 Stop / Start / Restart 同名进程时，
// 不得出现"进程还在跑、pidfile 却被上一次 Stop 删掉"的状态。
//
// 背景：Stop 在释放 m.mu 之后会阻塞最长 StopTimeout，期间同名 Start 完全
// 可能复用槽位并写入新 pidfile；旧实现的 cleanup 是无条件 os.Remove，
// 会把新进程的 pidfile 删掉，让它变成"活着但不可管理"的孤儿。
// 现在的 cleanup 只删除 pid 对得上的那个 pidfile。
func TestStopStartRaceKeepsPidfile(t *testing.T) {
	m, dir := newManager(t)
	proc, _ := helperProc(t, "graceful")
	proc.Name = "race"
	proc.StopTimeout = 100 * time.Millisecond
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 6; j++ {
				switch (i + j) % 3 {
				case 0:
					_, _ = m.Start(proc)
				case 1:
					_ = m.Stop(ctx, proc.Name)
				case 2:
					_ = m.Restart(ctx, proc.Name)
				}
			}
		}(i)
	}
	wg.Wait()

	// 收尾：停止，随后校验"运行中 ⇒ pidfile 存在"这一不变量。
	_ = m.Stop(ctx, proc.Name)

	st, err := m.Status(proc.Name)
	if err != nil {
		return // 未运行：无 pidfile 是正确的
	}
	if st.Running {
		if _, serr := os.Stat(filepath.Join(dir, proc.Name+".pid")); serr != nil {
			t.Fatalf("进程 %d 仍在运行，pidfile 却被删除", st.Pid)
		}
	}
}

func TestConcurrentOperations(t *testing.T) {
	m, _ := newManager(t)
	proc, _ := helperProc(t, "graceful")
	proc.Name = "conc"

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = m.Start(proc)
			_ = m.Stop(ctx, proc.Name)
			_, _ = m.Status(proc.Name)
			_ = m.List()
		}()
	}
	wg.Wait()
}
