package shutdown_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/zavierswong/go-infra/shutdown"
)

// killSelf 向本进程发信号。测试里用 SIGUSR1 避免误伤编辑器/终端
// 对 SIGINT/SIGTERM 的监听。
func killSelf(t *testing.T) {
	t.Helper()
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("kill: %v", err)
	}
}

// TestSignalTriggersHooks 信号触发：业务 ctx 取消 → 钩子逆序执行。
func TestSignalTriggersHooks(t *testing.T) {
	r := shutdown.New(shutdown.Config{
		Signals: []os.Signal{syscall.SIGUSR1},
		Timeout: 5 * time.Second,
	})

	var order []string
	mu := sync.Mutex{}
	record := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, s)
	}

	r.Add("db", func(context.Context) error { record("db"); return nil })
	r.Add("mq", func(context.Context) error { record("mq"); return nil })
	r.Add("http", func(context.Context) error { record("http"); return nil })

	runDone := make(chan error, 1)
	go func() {
		runDone <- r.Run(func(ctx context.Context) error {
			<-ctx.Done() // 业务等退出信号
			record("run-exit")
			return nil
		})
	}()

	time.Sleep(50 * time.Millisecond) // 让 signal.Notify 就位
	killSelf(t)

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run 未在信号后返回")
	}

	// 期望：业务先退出，钩子按注册逆序（http → mq → db）。
	want := []string{"run-exit", "http", "mq", "db"}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("关闭顺序 = %v, want %v", order, want)
	}
}

// TestFirstSignalMustNotForceExit 回归测试：第一个信号绝不能被"二次信号兜底"
// 协程抢走 —— 否则进程会直接 os.Exit(1)，跳过全部清理（该 bug 是概率性的，
// 命中率约 50%，必须重复多轮才能稳定复现）。
//
// 注意：一旦回归，本用例不会失败，而是整个测试进程被 os.Exit(1) 杀掉。
func TestFirstSignalMustNotForceExit(t *testing.T) {
	const rounds = 20
	for i := 0; i < rounds; i++ {
		r := shutdown.New(shutdown.Config{
			Signals: []os.Signal{syscall.SIGUSR1},
			Timeout: 3 * time.Second,
		})

		var cleaned bool
		r.Add("cleanup", func(context.Context) error { cleaned = true; return nil })

		runDone := make(chan error, 1)
		go func() {
			runDone <- r.Run(func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			})
		}()

		time.Sleep(20 * time.Millisecond) // 让 signal.Notify 与兜底协程就位
		killSelf(t)                       // 只发一次：必须是优雅退出
		select {
		case err := <-runDone:
			if err != nil {
				t.Fatalf("round %d: Run: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("round %d: 首信号未触发优雅退出", i)
		}
		if !cleaned {
			t.Fatalf("round %d: 首信号被兜底协程抢走，钩子未执行", i)
		}
	}
}

// TestRunSelfExit 业务自行结束也执行钩子，错误被汇总。
func TestRunSelfExit(t *testing.T) {
	r := shutdown.New(shutdown.Config{Timeout: 5 * time.Second})

	var ran bool
	r.Add("cleanup", func(context.Context) error { ran = true; return nil })

	boom := errors.New("biz failed")
	err := r.Run(func(context.Context) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("业务错误应上抛, got %v", err)
	}
	if !ran {
		t.Fatal("业务自行结束时钩子也应执行")
	}
}

// TestHookTimeout 钩子超时不阻断后续钩子。
func TestHookTimeout(t *testing.T) {
	r := shutdown.New(shutdown.Config{Timeout: 5 * time.Second, HookTimeout: 50 * time.Millisecond})

	var second bool
	r.AddTimeout("slow", 50*time.Millisecond, func(ctx context.Context) error {
		<-ctx.Done() // 挂到超时
		return ctx.Err()
	})
	r.Add("fast", func(context.Context) error { second = true; return nil })

	err := r.Run(func(context.Context) error { return nil })
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("超时钩子错误应上抛, got %v", err)
	}
	if !second {
		t.Fatal("超时钩子不应阻断后续钩子")
	}
}

// TestBudgetExceeded 业务卡死时总预算兜底，钩子仍执行。
func TestBudgetExceeded(t *testing.T) {
	r := shutdown.New(shutdown.Config{
		Signals: []os.Signal{syscall.SIGUSR1},
		Timeout: 200 * time.Millisecond,
	})

	var cleaned bool
	r.Add("cleanup", func(context.Context) error { cleaned = true; return nil })

	runDone := make(chan error, 1)
	go func() {
		runDone <- r.Run(func(ctx context.Context) error {
			<-ctx.Done()
			time.Sleep(5 * time.Second) // 无视取消，卡死
			return nil
		})
	}()

	time.Sleep(50 * time.Millisecond)
	killSelf(t)

	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("预算耗尽应返回错误")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("总预算未生效")
	}
	if !cleaned {
		t.Fatal("预算耗尽后钩子仍应执行")
	}
}

// TestHookPanicRecovered 钩子 panic 不阻断后续钩子。
func TestHookPanicRecovered(t *testing.T) {
	r := shutdown.New(shutdown.Config{Timeout: 5 * time.Second})

	var second bool
	r.Add("panic", func(context.Context) error { panic("cleanup bug") })
	r.Add("second", func(context.Context) error { second = true; return nil })

	err := r.Run(func(context.Context) error { return nil })
	if err == nil {
		t.Fatal("panic 应转为错误")
	}
	if !second {
		t.Fatal("panic 不应阻断后续钩子")
	}
}
