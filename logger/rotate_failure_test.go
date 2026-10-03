package logger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestRotateFailureKeepsWriting 回归测试：轮转（rename）失败时必须降级为
// 继续写当前文件，而不是让每一次 Write 都失败 —— 旧实现在 rename 失败后
// 直接 return，而 size 已被重开动作重置为当前体积（仍超限），下一次
// Write 又触发轮转、又失败，目录权限异常等场景下全部日志持续丢失。
func TestRotateFailureKeepsWriting(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root 用户 chmod 只读目录仍可写，无法模拟 rename 失败")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	w, err := newRotateWriter(path, RotateConfig{MaxSizeMB: 1, MaxBackups: 3, MaxAgeDays: -1})
	if err != nil {
		t.Fatalf("newRotateWriter: %v", err)
	}

	line := strings.Repeat("x", 1023) + "\n"
	// 写满 1MB 触发一次正常轮转（此刻目录可写，应成功）。
	for i := 0; i < 1100; i++ {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatalf("轮转前写入不应失败: %v", err)
		}
	}
	// 正常轮转应已发生：当前文件重新从接近 0 开始。
	w.mu.Lock()
	afterGoodRotate := w.size
	w.mu.Unlock()
	if afterGoodRotate > 1<<20 {
		t.Fatalf("应已发生一次正常轮转, 当前文件体积 = %d", afterGoodRotate)
	}

	// 目录改为只读 → rename 必然失败 → 进入降级路径。
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	// 轮转失败后的写入必须全部成功（宁可超限也不丢日志）。
	for i := 0; i < 100; i++ {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatalf("轮转失败后写入不应失败（旧实现会持续丢日志）: 第 %d 条: %v", i+1, err)
		}
	}

	// 内容确实落盘了。
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() < int64(100*len(line)) {
		t.Fatalf("降级写入未落盘, 文件体积 = %d", info.Size())
	}

	// 冷却窗口过后应重试轮转（仍会失败，但不影响后续写入）。
	w.mu.Lock()
	w.lastRotateFail = time.Now().Add(-2 * rotateRetryCooldown)
	w.mu.Unlock()
	if _, err := w.Write([]byte(line)); err != nil {
		t.Fatalf("冷却后再次尝试轮转仍应保证写入: %v", err)
	}
}
