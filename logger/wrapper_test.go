package logger

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// markerWrapper 是个最小包装器替身：给每条记录补一个 wrapped=yes 字段。
//
// 用"改写记录"而不是"计数"来断言，是因为这才是 tracing.NewLogHandler 真正
// 要做的事——在 Write 之前往 Record 上追加字段。只验调用次数会漏掉
// "包装器被调用了但记录没被改到"这一类退化。
func markerWrapper(h slog.Handler) slog.Handler { return markerHandler{Handler: h} }

type markerHandler struct{ slog.Handler }

func (h markerHandler) Handle(ctx context.Context, r slog.Record) error {
	r.AddAttrs(slog.String("wrapped", "yes"))
	return h.Handler.Handle(ctx, r)
}

func (h markerHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return markerHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h markerHandler) WithGroup(name string) slog.Handler {
	return markerHandler{Handler: h.Handler.WithGroup(name)}
}

// wrapperCtxKey 给「包装器能否读到调用方 ctx」的用例用。
// 定在包级是因为 ctx 的键类型要能被 handler 与用例共享。
type wrapperCtxKey struct{}

// ctxProbeHandler 只在 ctx 里带得出探针值时补字段——模拟 tracing 的
// "span 存在才追加 trace_id"。
type ctxProbeHandler struct{ slog.Handler }

func (h ctxProbeHandler) Handle(ctx context.Context, r slog.Record) error {
	if v, _ := ctx.Value(wrapperCtxKey{}).(string); v != "" {
		r.AddAttrs(slog.String("ctx_seen", v))
	}
	return h.Handler.Handle(ctx, r)
}

func (h ctxProbeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return ctxProbeHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h ctxProbeHandler) WithGroup(name string) slog.Handler {
	return ctxProbeHandler{Handler: h.Handler.WithGroup(name)}
}

// resetWrapper 保证用例之间不互相污染全局包装器注册。
// 撤销动作本身要能被观测到，所以单独一条用例守它（见 NilRestores）。
func resetWrapper(t *testing.T) {
	t.Helper()
	t.Cleanup(func() { SetHandlerWrapper(nil) })
}

// TestSetHandlerWrapperBeforeInit 注册在 Init 之前。
func TestSetHandlerWrapperBeforeInit(t *testing.T) {
	resetWrapper(t)
	SetHandlerWrapper(markerWrapper)

	path := setupFileLogger(t, Config{Format: "json"})
	Infof("hello %s", "world")

	got := readLog(t, path)
	assertContains(t, got, `"wrapped":"yes"`)
	assertContains(t, got, "hello world")
}

// TestSetHandlerWrapperAfterInitTakesEffectImmediately 注册在 Init 之后也必须立即生效。
//
// 这条是"注册制"的核心价值所在：换成"传入已包装好的 Handler"的写法，
// 后注册只能是静默无效——日志照打，只是字段没了。用例同时断言
// **注册之前写下的那条不带字段**，避免"全量重写历史记录"式的假通过。
func TestSetHandlerWrapperAfterInitTakesEffectImmediately(t *testing.T) {
	resetWrapper(t)

	path := setupFileLogger(t, Config{Format: "json"})
	Infof("before-register")
	if got := readLog(t, path); strings.Contains(got, "wrapped") {
		t.Fatalf("尚未注册包装器，不应出现包装器字段。实际内容:\n%s", got)
	}

	SetHandlerWrapper(markerWrapper)
	Infof("after-register")

	got := readLog(t, path)
	if n := strings.Count(got, `"wrapped":"yes"`); n != 1 {
		t.Errorf("只应有注册后那一条带包装器字段, 实际 %d 条:\n%s", n, got)
	}
	assertContains(t, got, "after-register")
}

// TestSetHandlerWrapperNilRestoresPlainOutput 传 nil 撤销包装。
func TestSetHandlerWrapperNilRestoresPlainOutput(t *testing.T) {
	resetWrapper(t)
	SetHandlerWrapper(markerWrapper)

	path := setupFileLogger(t, Config{Format: "json"})
	Infof("wrapped-line")
	assertContains(t, readLog(t, path), `"wrapped":"yes"`)

	SetHandlerWrapper(nil)
	Infof("plain-line")

	got := readLog(t, path)
	if n := strings.Count(got, `"wrapped":"yes"`); n != 1 {
		t.Errorf("撤销后不应再出现包装器字段, 实际 %d 条:\n%s", n, got)
	}
	assertContains(t, got, "plain-line")
}

// TestSetHandlerWrapperSurvivesReinit 包装器不能在重新 Init 时丢掉。
func TestSetHandlerWrapperSurvivesReinit(t *testing.T) {
	resetWrapper(t)
	SetHandlerWrapper(markerWrapper)

	path := setupFileLogger(t, Config{Format: "json"})
	// 第二次 Init：走热替换路径，handler 被整个重建
	if err := Init(Config{Format: "json", Output: path}); err != nil {
		t.Fatalf("二次 Init 失败: %v", err)
	}
	Infof("after-reinit")

	assertContains(t, readLog(t, path), `"wrapped":"yes"`)
}

// TestSetHandlerWrapperKeepsConfigAndHotLevel 换 handler 时不许顺带改掉
// 输出格式、service 字段与运行期调过的级别。
//
// 这是"就地换 handler"与"按配置整体重建"的分水岭：整体重建会把
// SetLevel 的热调级静默回滚到配置文件里的级别，而调用方只是加了个
// 观测包装器。三条断言分别守级别、格式、service 字段。
func TestSetHandlerWrapperKeepsConfigAndHotLevel(t *testing.T) {
	resetWrapper(t)

	path := setupFileLogger(t, Config{Format: "text", Service: "svc"})
	SetLevel("error")

	SetHandlerWrapper(markerWrapper)

	Infof("filtered-out") // 级别已被调成 error，不该落盘
	Errorf("appeared")

	got := readLog(t, path)
	if strings.Contains(got, "filtered-out") {
		t.Errorf("注册包装器后 SetLevel 的热调级被回滚了。实际内容:\n%s", got)
	}
	assertContains(t, got, "appeared")
	assertContains(t, got, "wrapped=yes") // text 格式
	assertContains(t, got, "service=svc") // service 字段未被重置
	if strings.Contains(got, `{"time"`) {
		t.Errorf("输出格式被重置成 json 了。实际内容:\n%s", got)
	}
}

// TestHandlerWrapperReceivesCallerContext 包装器必须拿到**调用方的 ctx**。
//
// tracing.NewLogHandler 从 ctx 里取 span，一旦 emit 传的是
// context.Background()，trace_id 会永远为空且不报错。这条用例把
// "ctx 透传"钉成可观测量：带 ctx 的调用有字段，不带的没有。
func TestHandlerWrapperReceivesCallerContext(t *testing.T) {
	resetWrapper(t)
	SetHandlerWrapper(func(h slog.Handler) slog.Handler {
		return ctxProbeHandler{Handler: h}
	})

	path := setupFileLogger(t, Config{Format: "json"})

	ctx := context.WithValue(context.Background(), wrapperCtxKey{}, "seen")
	NewPlog("probe").Info(ctx, "with-ctx")
	Infof("without-ctx")

	got := readLog(t, path)
	if n := strings.Count(got, `"ctx_seen":"seen"`); n != 1 {
		t.Errorf("应恰好有一条日志读到 ctx 中的值, 实际 %d 条:\n%s", n, got)
	}
	assertContains(t, got, "without-ctx")
}

// TestApplyHandlerWrapperOnBuild 白盒钉住"套用点在构建 handler 处"。
//
// 未 Init 的懒加载默认实例与 Close 的回落都走 build → newHandler，
// 这条直接调 build 验证那两条路径也带包装器；同时也覆盖了 out 复用路径
// （SetHandlerWrapper 用同一个 out 重建 handler）。
func TestApplyHandlerWrapperOnBuild(t *testing.T) {
	resetWrapper(t)
	SetHandlerWrapper(markerWrapper)

	path := filepath.Join(t.TempDir(), "lazy.log")
	st, err := build(normalize(Config{Format: "json", Output: path}))
	if err != nil {
		t.Fatalf("build 失败: %v", err)
	}
	st.logger.Info("lazy-default")
	if st.closer != nil {
		_ = st.closer.Close()
	}
	assertContains(t, readLog(t, path), `"wrapped":"yes"`)
}

// TestSetHandlerWrapperReusesOutput 白盒钉住"换 handler 不动输出"。
//
// 这是"就地换 handler"与"按配置整体重建"的分水岭：整体重建会新建一个
// 轮转 writer，于是**关闭旧句柄**——那个窗口里并发写入会打到已关闭的
// 文件上。断言 closer 与 cfg 两个实例指针都没变，正是"复用"的可观测量。
func TestSetHandlerWrapperReusesOutput(t *testing.T) {
	resetWrapper(t)
	setupFileLogger(t, Config{Format: "json"})

	before := global.Load()
	if before == nil || before.closer == nil {
		t.Fatalf("前置条件不成立：文件输出应带 closer")
	}

	SetHandlerWrapper(markerWrapper)

	after := global.Load()
	if after.closer != before.closer {
		t.Error("换 handler 时输出被替换了：应复用同一个 writer，不关闭也不重开文件")
	}
	if after.cfg != before.cfg {
		t.Errorf("换 handler 时配置被改写: %+v → %+v", before.cfg, after.cfg)
	}
	if after.logger == nil {
		t.Error("换 handler 后 logger 为空")
	}
}

// TestSetHandlerWrapperConcurrentWithLogging 在 -race 下守两件事：
// 包装器注册与打日志并发时无数据竞争；反复换 handler 不会把日志器换坏。
//
// 断言的是"风暴之后日志器仍然可用"，而不是"过程中一条不丢"：
// 后者在热替换语义下本就不成立，写死它会得到一条时红时绿的用例。
func TestSetHandlerWrapperConcurrentWithLogging(t *testing.T) {
	resetWrapper(t)
	path := setupFileLogger(t, Config{Format: "json"})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					Infof("concurrent")
				}
			}
		}()
	}

	for i := 0; i < 50; i++ {
		if i%2 == 0 {
			SetHandlerWrapper(markerWrapper)
		} else {
			SetHandlerWrapper(nil)
		}
	}

	close(stop)
	wg.Wait()

	SetHandlerWrapper(nil)
	Infof("tail-after-storm")
	assertContains(t, readLog(t, path), "tail-after-storm")
}
