package logger

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	gormlogger "gorm.io/gorm/logger"
)

// setupFileLogger 把全局日志器指向临时文件，便于断言输出内容
func setupFileLogger(t *testing.T, cfg Config) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app.log")
	cfg.Output = path
	if err := Init(cfg); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	t.Cleanup(func() { _ = Close() })
	return path
}

// readLog 读取日志文件内容
func readLog(t *testing.T, path string) string {
	t.Helper()
	if err := Sync(); err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取日志文件失败: %v", err)
	}
	return string(data)
}

// assertContains 断言内容包含子串，并输出实际内容便于排查
func assertContains(t *testing.T, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Errorf("日志内容缺少 %q\n实际内容:\n%s", want, got)
	}
}

// assertMatch 断言内容匹配正则（用于含行号等不定值的场景）
func assertMatch(t *testing.T, got, pattern string) {
	t.Helper()
	if !regexp.MustCompile(pattern).MatchString(got) {
		t.Errorf("日志内容不匹配 %q\n实际内容:\n%s", pattern, got)
	}
}

// TestDefaultUsableBeforeInit 未调用 Init 也应可直接打日志（懒加载默认实例）
func TestDefaultUsableBeforeInit(t *testing.T) {
	global.Store(nil) // 模拟"从未初始化"

	if Default() == nil {
		t.Fatal("未初始化时 Default() 不应为 nil")
	}
	// 下面两行会打到默认输出(stdout)，重点是不 panic、不丢调用链
	Infof("未初始化日志器, 端口: %d", 8080)
	NewPlog("Redis").Warnf(context.Background(), "未初始化子日志器: %s", "ok")

	if err := Sync(); err != nil {
		t.Fatalf("未初始化时 Sync 不应报错: %v", err)
	}
	if err := Close(); err != nil {
		t.Fatalf("未初始化时 Close 不应报错: %v", err)
	}
}

// TestInitFileOutputAndCaller 校验 json 落盘、service 字段、caller 定位与 printf 格式化
func TestInitFileOutputAndCaller(t *testing.T) {
	path := setupFileLogger(t, Config{Level: "debug", Format: "json", Service: "user-api"})

	Infof("服务启动, 端口: %d", 8080)
	Debugf("调试信息 %s", "ok")

	content := readLog(t, path)
	t.Logf("日志内容:\n%s", content)

	assertContains(t, content, `"level":"info"`)
	assertContains(t, content, `"msg":"服务启动, 端口: 8080"`) // printf 参数被正确展开
	assertContains(t, content, `"service":"user-api"`)
	assertContains(t, content, `"level":"debug"`)
	assertMatch(t, content, `"source":"logger\.TestInitFileOutputAndCaller:\d+"`) // caller 指向调用方
	if strings.Contains(content, "logger.go") {
		t.Errorf("caller 被包装函数污染了:\n%s", content)
	}
}

// TestLevelFilterAndHotReload 级别过滤生效，且支持运行期热调级
func TestLevelFilterAndHotReload(t *testing.T) {
	path := setupFileLogger(t, Config{Level: "warn", Format: "text"})

	Infof("这条不该出现")
	Warnf("这条应该出现")

	content := readLog(t, path)
	assertContains(t, content, "这条应该出现")
	if strings.Contains(content, "这条不该出现") {
		t.Errorf("info 级别日志未按配置过滤: %s", content)
	}

	SetLevel("debug")
	Infof("热调级后应该出现")
	assertContains(t, readLog(t, path), "热调级后应该出现")
}

// TestPlogNamedWithTraceID 具名子日志器、固定字段、trace_id 透传与 With 的不可变性
func TestPlogNamedWithTraceID(t *testing.T) {
	path := setupFileLogger(t, Config{Level: "debug", Format: "json"})

	lg := NewPlog("MySQL")
	ctx := WithTraceID(context.Background(), "trace-123")

	lg.Infof(ctx, "连接成功, 耗时: %s", "12ms")
	sub := lg.Named("pool").With("dsn", "root:***@tcp(127.0.0.1:3306)/app")
	sub.Warn(ctx, "连接池已满", "inUse", 8, "max", 10)
	lg.Error(ctx, "原始日志器不应带上 With 字段")

	content := readLog(t, path)
	t.Logf("日志内容:\n%s", content)

	assertContains(t, content, `"logger":"MySQL"`)
	assertContains(t, content, `"trace_id":"trace-123"`)
	assertContains(t, content, `"dsn":"root:***@tcp(127.0.0.1:3306)/app"`)
	assertContains(t, content, `"logger":"MySQL.pool"`)
	assertContains(t, content, `"inUse":8`)
	assertContains(t, content, `"msg":"连接成功, 耗时: 12ms"`)

	// With 返回副本：原日志器的日志里不得出现 dsn 字段
	for _, line := range strings.Split(strings.TrimSpace(content), "\n") {
		if strings.Contains(line, "原始日志器不应带上") && strings.Contains(line, `"dsn"`) {
			t.Errorf("With 修改了原日志器，日志行: %s", line)
		}
	}
	// name 不会被 Named 改动
	if lg.Name() != "MySQL" {
		t.Errorf("Named 应返回副本, 原名字被改成 %s", lg.Name())
	}
}

// TestConcurrentLogging 并发写日志不应出现数据竞争（配合 -race 运行）
func TestConcurrentLogging(t *testing.T) {
	path := setupFileLogger(t, Config{Level: "info", Format: "json"})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			lg := NewPlog("worker")
			for j := 0; j < 20; j++ {
				lg.Infof(context.Background(), "任务 %d-%d 完成", id, j)
			}
		}(i)
	}
	wg.Wait()

	content := readLog(t, path)
	if n := strings.Count(content, `"msg"`); n != 160 {
		t.Errorf("并发写日志丢失, 期望 160 条, 实际 %d 条", n)
	}
}

// TestRotateBySizeAndCompress 体积轮转 + 数量保留 + gzip 压缩
func TestRotateBySizeAndCompress(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	w, err := newRotateWriter(path, RotateConfig{MaxSizeMB: 1, MaxBackups: 2, MaxAgeDays: 30, Compress: true})
	if err != nil {
		t.Fatalf("创建轮转写入器失败: %v", err)
	}
	defer func() { _ = w.Close() }()

	chunk := make([]byte, 1100*1024) // 1.1MB，每写一次告一段落就触发一次轮转
	for i := 0; i < 3; i++ {
		if _, err := w.Write(chunk); err != nil {
			t.Fatalf("第 %d 次写入失败: %v", i+1, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}

	backups := listBackups(t, dir, path)
	t.Logf("保留的历史文件: %v", backups)
	if len(backups) != 2 {
		t.Errorf("MaxBackups=2 应只保留 2 个历史文件, 实际 %d 个: %v", len(backups), backups)
	}
	for _, name := range backups {
		if !strings.HasSuffix(name, ".gz") {
			t.Errorf("开启 Compress 后历史文件应为 .gz: %s", name)
		}
	}
}

// TestPruneByAge 按天清理历史文件
func TestPruneByAge(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	stale := path + ".20200101T000000.000"
	if err := os.WriteFile(stale, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().AddDate(0, 0, -40)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}

	w, err := newRotateWriter(path, RotateConfig{MaxSizeMB: 1, MaxBackups: 10, MaxAgeDays: 30})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(make([]byte, 1100*1024)); err != nil { // 触发一次轮转 → 触发清理
		t.Fatal(err)
	}
	_ = w.Close()

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("超过 MaxAgeDays 的历史文件应被删除: %s", stale)
	}
}

// listBackups 列出历史文件名
func listBackups(t *testing.T, dir, path string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	prefix := filepath.Base(path) + "."
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) {
			names = append(names, e.Name())
		}
	}
	return names
}

// TestGormAdapterLevelMapping 配置级别映射与 LogMode 的副本语义
func TestGormAdapterLevelMapping(t *testing.T) {
	cases := []struct {
		level string
		want  gormlogger.LogLevel
	}{
		{"silent", gormlogger.Silent},
		{"error", gormlogger.Error},
		{"warn", gormlogger.Warn},
		{"info", gormlogger.Info},
		{"INFO", gormlogger.Info},
		{"", gormlogger.Info},
		{"unknown", gormlogger.Info},
	}
	for _, c := range cases {
		m := &Mysql{LogLevel: c.level}
		if got := m.currentLevel(); got != c.want {
			t.Errorf("LogLevel=%q 期望 %v, 实际 %v", c.level, c.want, got)
		}
	}

	base := &Mysql{LogLevel: "warn"}
	copied, ok := base.LogMode(gormlogger.Silent).(*Mysql)
	if !ok {
		t.Fatal("LogMode 应返回 *Mysql")
	}
	if copied == base {
		t.Error("LogMode 应返回副本而不是自身")
	}
	if base.currentLevel() != gormlogger.Warn {
		t.Error("LogMode 不应修改原对象的级别")
	}
	if copied.currentLevel() != gormlogger.Silent {
		t.Error("副本应使用 LogMode 指定的级别")
	}

	// 慢 SQL 阈值：0 → 默认 200ms，负值 → 关闭
	if got := (&Mysql{}).slowThreshold(); got != defaultSlowThreshold {
		t.Errorf("未配置 SlowThreshold 时应使用默认值, 实际 %v", got)
	}
	if got := (&Mysql{SlowThreshold: -1}).slowThreshold(); got != 0 {
		t.Errorf("负数应关闭慢 SQL 告警, 实际 %v", got)
	}
}

// TestGormAdapterTrace 慢 SQL 走 warn、错误走 error、Silent 时不输出
func TestGormAdapterTrace(t *testing.T) {
	path := setupFileLogger(t, Config{Level: "debug", Format: "json"})
	ctx := context.Background()
	fc := func() (string, int64) { return "SELECT * FROM users WHERE id = 1", 1 }

	slow := &Mysql{Name: "MySQL", LogLevel: "info", SlowThreshold: time.Nanosecond}
	slow.Trace(ctx, time.Now().Add(-time.Second), fc, nil)

	failing := &Mysql{Name: "MySQL", LogLevel: "info"}
	failing.Trace(ctx, time.Now(), fc, context.DeadlineExceeded)

	quiet := &Mysql{Name: "MySQL", LogLevel: "silent"}
	quiet.Trace(ctx, time.Now(), fc, nil)

	ignored := &Mysql{Name: "MySQL", LogLevel: "info", IgnoreRecordNotFoundError: true}
	ignored.Trace(ctx, time.Now(), fc, gormlogger.ErrRecordNotFound)

	content := readLog(t, path)
	t.Logf("日志内容:\n%s", content)

	assertContains(t, content, "慢 SQL")
	assertContains(t, content, `"level":"warn"`)
	assertContains(t, content, "SQL 执行失败")
	assertContains(t, content, context.DeadlineExceeded.Error())
	assertContains(t, content, `"sql":"SELECT * FROM users WHERE id = 1"`)
	assertContains(t, content, `"rows":1`)
	if strings.Contains(content, gormlogger.ErrRecordNotFound.Error()) {
		t.Errorf("IgnoreRecordNotFoundError=true 时不应记录 record not found: %s", content)
	}
	if n := strings.Count(content, `"logger":"MySQL"`); n != 2 {
		t.Errorf("Silent 级别的 SQL 不应输出, 期望 2 条, 实际 %d 条:\n%s", n, content)
	}
}

// TestInitTwiceReplacesLogger 重复 Init 应替换日志器并关闭旧文件句柄
func TestInitTwiceReplacesLogger(t *testing.T) {
	first := setupFileLogger(t, Config{Level: "info", Format: "json"})
	Infof("写入第一个文件")

	second := filepath.Join(t.TempDir(), "second.log")
	if err := Init(Config{Level: "info", Format: "json", Output: second}); err != nil {
		t.Fatalf("二次 Init 失败: %v", err)
	}
	Infof("写入第二个文件")

	firstContent := readLog(t, first)
	secondContent := readLog(t, second)
	assertContains(t, firstContent, "写入第一个文件")
	if strings.Contains(firstContent, "写入第二个文件") {
		t.Error("二次 Init 后不应再写入旧文件")
	}
	assertContains(t, secondContent, "写入第二个文件")
}

// swapStdout 临时把 os.Stdout 指向管道，返回读取管道全部内容的函数。
// 用于断言 stdout 输出的渲染结果（text 色彩等）。必须在 Init 之前调用，
// 因为 build() 在构造时即捕获 os.Stdout。
func swapStdout(t *testing.T) func() string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(r)
		done <- string(data)
	}()
	return func() string {
		_ = w.Close() // 关闭写端，ReadAll 返回
		os.Stdout = old
		return <-done
	}
}

// TestCallerSourceFields 调用者信息默认开启，统一输出为字符串 `函数:行号`
// （slog 原生 text 只给文件:行号、JSON 给 {function,file,line} 对象，这里都改写为单字段）
func TestCallerSourceFields(t *testing.T) {
	t.Run("text 输出 func:line 字符串", func(t *testing.T) {
		path := setupFileLogger(t, Config{Level: "debug", Format: "text"})
		Infof("带调用者信息的日志")

		content := readLog(t, path)
		t.Logf("日志内容:\n%s", content)

		assertMatch(t, content, `source=logger\.TestCallerSourceFields\.func\d+:\d+`)
		if strings.Contains(content, "logger.go:") {
			t.Errorf("caller 被包装函数污染了:\n%s", content)
		}
	})

	t.Run("JSON 同为字符串形式", func(t *testing.T) {
		path := setupFileLogger(t, Config{Level: "debug", Format: "json"})
		Infof("结构化调用者信息")

		content := readLog(t, path)
		t.Logf("日志内容:\n%s", content)

		assertMatch(t, content, `"source":"logger\.TestCallerSourceFields\.func\d+:\d+"`)
		// 不再输出原生 source 对象与额外的 func 字段
		for _, notWant := range []string{`"function"`, `"file"`, `"line"`, `"func"`} {
			if strings.Contains(content, notWant) {
				t.Errorf("JSON 的 source 应为字符串, 不应出现 %s:\n%s", notWant, content)
			}
		}
	})

	t.Run("DisableCaller 时无 source 字段", func(t *testing.T) {
		path := setupFileLogger(t, Config{Level: "debug", Format: "text", DisableCaller: true})
		Infof("关闭调用者信息")

		content := readLog(t, path)
		if strings.Contains(content, "source=") || strings.Contains(content, "func=") {
			t.Errorf("DisableCaller=true 时不应输出 source/func 字段:\n%s", content)
		}
	})
}

// TestShortFuncName 函数名裁剪
func TestShortFuncName(t *testing.T) {
	cases := map[string]string{
		"github.com/zavierswong/go-infra/logger.TestX":         "logger.TestX",
		"github.com/zavierswong/go-infra/logger.(*Plog).Infof": "logger.(*Plog).Infof",
		"main.main": "main.main",
		"":          "",
		"a/b/":      "a/b/", // 结尾为 '/' 时原样返回，避免越界
	}
	for in, want := range cases {
		if got := shortFuncName(in); got != want {
			t.Errorf("shortFuncName(%q) 期望 %q, 实际 %q", in, want, got)
		}
	}
}

// TestTextColorOutput 色彩开关：Color=on 时 text 输出携带 ANSI 色码
// （level 按级别着色，message 加粗），且关闭/JSON 时绝不携带
func TestTextColorOutput(t *testing.T) {
	t.Run("on 强制开启", func(t *testing.T) {
		read := swapStdout(t)
		if err := Init(Config{Format: "text", Color: "on"}); err != nil {
			t.Fatal(err)
		}
		Infof("彩色日志 %d", 1)
		Warnf("警告日志")

		content := read()
		// 断言用双引号字符串承载真实 ESC 字节(0x1b)：TextHandler 先转义、
		// colorUnescapeWriter 再还原，最终输出为真实色码，属性值仍带引号
		assertContains(t, content, "level=\"\x1b[32minfo\x1b[0m\"") // info 绿
		assertContains(t, content, "level=\"\x1b[33mwarn\x1b[0m\"") // warn 黄
		assertContains(t, content, "msg=\"\x1b[1m彩色日志 1\x1b[0m\"")  // 消息加粗且 printf 已展开
	})

	t.Run("auto 非终端不输出色码", func(t *testing.T) {
		read := swapStdout(t) // 管道不是字符设备，auto 应回落为无色
		if err := Init(Config{Format: "text"}); err != nil {
			t.Fatal(err)
		}
		Infof("无色日志")
		if content := read(); strings.Contains(content, "\x1b[") {
			t.Errorf("auto 模式下非终端输出不应含 ANSI 色码:\n%s", content)
		}
	})

	t.Run("off 显式关闭", func(t *testing.T) {
		read := swapStdout(t)
		if err := Init(Config{Format: "text", Color: "on"}); err != nil {
			t.Fatal(err)
		}
		_ = Init(Config{Format: "text", Color: "off"}) // 热替换为关闭
		Infof("关闭色彩")
		if content := read(); strings.Contains(content, "\x1b[") {
			t.Errorf("Color=off 不应含 ANSI 色码:\n%s", content)
		}
	})

	t.Run("JSON 不携带色码", func(t *testing.T) {
		read := swapStdout(t)
		if err := Init(Config{Format: "json", Color: "on"}); err != nil {
			t.Fatal(err)
		}
		Infof("结构化日志")
		if content := read(); strings.Contains(content, "\x1b[") {
			t.Errorf("JSON 输出绝不应含 ANSI 色码:\n%s", content)
		}
	})

	t.Run("FORCE_COLOR 环境变量兜底", func(t *testing.T) {
		t.Setenv("FORCE_COLOR", "1")
		read := swapStdout(t)
		if err := Init(Config{Format: "text"}); err != nil { // auto + FORCE_COLOR → 开
			t.Fatal(err)
		}
		Infof("环境变量开启")
		if content := read(); !strings.Contains(content, "\x1b[") {
			t.Errorf("FORCE_COLOR=1 时 auto 模式应输出色码:\n%s", content)
		}
	})

	t.Run("NO_COLOR 环境变量关闭", func(t *testing.T) {
		t.Setenv("NO_COLOR", "1")
		read := swapStdout(t)
		if err := Init(Config{Format: "text"}); err != nil {
			t.Fatal(err)
		}
		Infof("环境变量关闭")
		if content := read(); strings.Contains(content, "\x1b[") {
			t.Errorf("NO_COLOR=1 时不应输出色码:\n%s", content)
		}
	})
}

// TestNormalizeColor 色彩模式归一化：非法值回落 auto
func TestNormalizeColor(t *testing.T) {
	cases := map[string]string{
		"": "auto", "auto": "auto", "AUTO": "auto", "unknown": "auto",
		"on": "on", "FORCE": "on", "true": "on", "1": "on",
		"off": "off", "FALSE": "off", "0": "off", "none": "off",
	}
	for in, want := range cases {
		if got := normalizeColor(in); got != want {
			t.Errorf("normalizeColor(%q) 期望 %q, 实际 %q", in, want, got)
		}
	}
}

// TestParseLevelFatal fatal 阈值可配置，且非法值仍回落 info
func TestParseLevelFatal(t *testing.T) {
	if got := parseLevel("fatal"); got != LevelFatalValue {
		t.Errorf("parseLevel(fatal) 期望 %v, 实际 %v", LevelFatalValue, got)
	}
	if got := parseLevel("FATAL"); got != LevelFatalValue {
		t.Errorf("level 应大小写不敏感, 实际 %v", got)
	}
	if LevelFatalValue <= slog.LevelError {
		t.Errorf("fatal 级别必须高于 error, 实际 %v", LevelFatalValue)
	}
	if got := parseLevel("unknown"); got != slog.LevelInfo {
		t.Errorf("非法级别应回落 info, 实际 %v", got)
	}
}

// stubExit 替换 exitFunc 以便在同进程内断言退出码
func stubExit(t *testing.T) *int {
	t.Helper()
	code := -1
	prev := exitFunc
	exitFunc = func(c int) { code = c }
	t.Cleanup(func() { exitFunc = prev })
	return &code
}

// TestFatalLogsAndExits 致命日志：按 fatal 级别落盘、退出码为 1，
// 且 Level=error 时依然输出（fatal 高于 error）
func TestFatalLogsAndExits(t *testing.T) {
	path := setupFileLogger(t, Config{Level: "error", Format: "json"})
	code := stubExit(t)

	Fatalf("启动失败: %s", "端口被占用")

	if *code != 1 {
		t.Errorf("Fatalf 应以状态码 1 退出, 实际 %d", *code)
	}
	content := readLog(t, path)
	t.Logf("日志内容:\n%s", content)
	assertContains(t, content, `"level":"fatal"`)
	assertContains(t, content, `"msg":"启动失败: 端口被占用"`) // printf 参数已展开
	assertMatch(t, content, `"source":"logger\.TestFatalLogsAndExits:\d+"`)
}

// TestPlogFatalLogsAndExits 子日志器的 Fatal/Fatalf 行为一致，且带上 logger 字段
func TestPlogFatalLogsAndExits(t *testing.T) {
	path := setupFileLogger(t, Config{Level: "debug", Format: "text"})
	code := stubExit(t)

	NewPlog("MySQL").Fatalf(context.Background(), "连接池初始化失败: %v", context.Canceled)

	if *code != 1 {
		t.Errorf("Plog.Fatalf 应以状态码 1 退出, 实际 %d", *code)
	}
	content := readLog(t, path)
	assertContains(t, content, "level=fatal")
	assertContains(t, content, "连接池初始化失败: context canceled")
	assertContains(t, content, "logger=MySQL")
}

// TestFatalExitsProcess 子进程验证真实 os.Exit(1)：注入 exitFunc 只能验证调用，
// 不能验证进程真的退出（这正是 Fatal 最容易出错的地方）。
func TestFatalExitsProcess(t *testing.T) {
	if os.Getenv("LOGGER_TEST_FATAL_SUBPROCESS") == "1" {
		// 子进程：必须真的退出，下一行不应被执行
		Fatalf("致命错误: %s", "boom")
		os.Exit(99)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestFatalExitsProcess")
	cmd.Env = append(os.Environ(), "LOGGER_TEST_FATAL_SUBPROCESS=1")
	out, err := cmd.CombinedOutput()

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("子进程应异常退出, 实际 err=%v, 输出:\n%s", err, out)
	}
	if got := exitErr.ExitCode(); got != 1 {
		t.Errorf("Fatalf 应退出码 1, 实际 %d, 输出:\n%s", got, out)
	}
	assertContains(t, string(out), "致命错误: boom")
	assertContains(t, string(out), `"level":"fatal"`)
}
