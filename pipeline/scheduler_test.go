package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestEngine wires a ScriptedRunner against a temp cache.
func newTestEngine(t *testing.T, cfg *Config) (*Engine, *ScriptedRunner) {
	t.Helper()
	r := NewScriptedRunner()
	return &Engine{Runner: r}, r
}

func mustBuild(t *testing.T, cfg *Config) {
	t.Helper()
	if _, err := BuildGraph(cfg); err != nil {
		t.Fatalf("graph error: %v", err)
	}
}

func okResult() CommandResult { return CommandResult{} }
func errResult(msg string) CommandResult {
	return CommandResult{ExitCode: 1, Err: errors.New(msg)}
}

func TestSchedulingAndCaching(t *testing.T) {
	t.Run("business=调度与并发", func(t *testing.T) {
		t.Run("用例=独立步骤全部成功且按依赖拓扑推进", func(t *testing.T) {
			cfg := &Config{Concurrency: 3, Steps: []StepSpec{
				{Name: "a", Command: "a"},
				{Name: "b", Command: "b", Depends: []string{"a"}},
				{Name: "c", Command: "c", Depends: []string{"a"}},
				{Name: "d", Command: "d", Depends: []string{"b", "c"}},
			}}
			eng, _ := newTestEngine(t, cfg)
			rep, err := eng.Run(context.Background(), cfg, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if !rep.OK() {
				t.Fatalf("pipeline not OK: %+v", rep.Results)
			}
			for _, name := range []string{"a", "b", "c", "d"} {
				if rep.Results[name].Status != StatusSuccess {
					t.Fatalf("step %s status=%s", name, rep.Results[name].Status)
				}
			}
			t.Log("结果: a/b/c/d 全部 success")
		})

		t.Run("用例=并发上限生效(在跑数不超过 concurrency)", func(t *testing.T) {
			cfg := &Config{Concurrency: 2, Steps: []StepSpec{
				{Name: "a", Command: "a"},
				{Name: "b", Command: "b"},
				{Name: "c", Command: "c"},
				{Name: "d", Command: "d"},
			}}
			mustBuild(t, cfg)
			r := NewScriptedRunner()
			var inFlight, maxObserved int32
			var mu sync.Mutex
			for _, s := range cfg.Steps {
				name := s.Name
				r.ScriptDelay(name, 30*time.Millisecond)
				r.ScriptHook(name, func(int) {
					cur := atomic.AddInt32(&inFlight, 1)
					mu.Lock()
					if cur > maxObserved {
						maxObserved = cur
					}
					mu.Unlock()
					time.Sleep(20 * time.Millisecond)
					atomic.AddInt32(&inFlight, -1)
				})
			}
			g, _ := BuildGraph(cfg)
			exec := NewExecutor(NewCache(t.TempDir()), r)
			sched, _ := NewScheduler(cfg, g, exec)
			rep := sched.Run(context.Background())
			if !rep.OK() {
				t.Fatalf("pipeline not OK: %+v", rep.Results)
			}
			if maxObserved > 2 {
				t.Fatalf("concurrency limit violated: observed %d > 2", maxObserved)
			}
			if maxObserved < 2 {
				t.Fatalf("expected parallelism to reach 2, got %d", maxObserved)
			}
			t.Logf("结果: 观测到最大并发=%d (上限 2)", maxObserved)
		})

		t.Run("用例=缓存命中跳过执行且不改写结果", func(t *testing.T) {
			cfg := &Config{Concurrency: 2, Steps: []StepSpec{
				{Name: "a", Command: "echo a"},
				{Name: "b", Command: "echo b", Depends: []string{"a"}},
			}}
			cacheDir := t.TempDir()
			eng, r := newTestEngine(t, cfg)
			r.ScriptStep("a", okResult())
			r.ScriptStep("b", okResult())
			rep1, err := eng.Run(context.Background(), cfg, cacheDir)
			if err != nil || !rep1.OK() {
				t.Fatalf("first run failed: %v %+v", err, rep1.Results)
			}
			calls1 := len(r.Calls)
			if calls1 != 2 {
				t.Fatalf("first run expected 2 executions, got %d", calls1)
			}
			rep2, err := eng.Run(context.Background(), cfg, cacheDir)
			if err != nil || !rep2.OK() {
				t.Fatalf("second run failed: %v", err)
			}
			if len(r.Calls) != calls1 {
				t.Fatalf("cached steps must not execute: %d -> %d calls", calls1, len(r.Calls))
			}
			if !rep2.Results["a"].FromCache || !rep2.Results["b"].FromCache {
				t.Fatalf("both steps should be cached: %+v %+v", rep2.Results["a"], rep2.Results["b"])
			}
			t.Logf("结果: 首次执行 2 步, 第二次 0 执行, 状态 a=%s b=%s", rep2.Results["a"].Status, rep2.Results["b"].Status)
		})

		t.Run("用例=上游输入变化时下游缓存级联失效", func(t *testing.T) {
			cacheDir := t.TempDir()
			build := func(v string) *Config {
				return &Config{Concurrency: 2, Steps: []StepSpec{
					{Name: "gen", Command: "gen", Inputs: []InputSpec{{Name: "v", Value: v}}},
					{Name: "pkg", Command: "pkg", Depends: []string{"gen"}},
				}}
			}
			eng, _ := newTestEngine(t, build("v1"))
			r := eng.Runner.(*ScriptedRunner)
			r.ScriptStep("gen", okResult())
			r.ScriptStep("pkg", okResult())
			if rep, err := eng.Run(context.Background(), build("v1"), cacheDir); err != nil || !rep.OK() {
				t.Fatalf("run1: %v %+v", err, rep)
			}
			if rep, _ := eng.Run(context.Background(), build("v1"), cacheDir); !rep.OK() ||
				rep.Results["gen"].Status != StatusCached || rep.Results["pkg"].Status != StatusCached {
				t.Fatalf("v1 rerun should be fully cached")
			}
			rep, _ := eng.Run(context.Background(), build("v2"), cacheDir)
			if rep.Results["gen"].Status != StatusSuccess || rep.Results["pkg"].Status != StatusSuccess {
				t.Fatalf("after input change both should rerun: gen=%s pkg=%s",
					rep.Results["gen"].Status, rep.Results["pkg"].Status)
			}
			t.Log("结果: v1->v2 后 gen 与 pkg 均重新执行")
		})
	})
}

func TestRetryAndFailure(t *testing.T) {
	t.Run("business=失败重试", func(t *testing.T) {
		t.Run("用例=失败后重试成功", func(t *testing.T) {
			cfg := &Config{Concurrency: 1, Steps: []StepSpec{
				{Name: "a", Command: "flaky", Retries: 2},
			}}
			mustBuild(t, cfg)
			r := NewScriptedRunner()
			r.ScriptStep("a", errResult("boom1"), errResult("boom2"), okResult())
			g, _ := BuildGraph(cfg)
			sched, _ := NewScheduler(cfg, g, NewExecutor(NewCache(t.TempDir()), r))
			rep := sched.Run(context.Background())
			res := rep.Results["a"]
			if res.Status != StatusSuccess || res.Attempts != 3 {
				t.Fatalf("want success on attempt 3, got %s attempts=%d", res.Status, res.Attempts)
			}
			t.Logf("结果: 前两次失败第三次成功, attempts=%d", res.Attempts)
		})

		t.Run("用例=重试耗尽标记 failed 且记录 StepError", func(t *testing.T) {
			cfg := &Config{Concurrency: 1, Steps: []StepSpec{
				{Name: "a", Command: "bad", Retries: 1},
			}}
			mustBuild(t, cfg)
			r := NewScriptedRunner()
			r.ScriptStep("a", errResult("boom1"), errResult("boom2"))
			g, _ := BuildGraph(cfg)
			sched, _ := NewScheduler(cfg, g, NewExecutor(NewCache(t.TempDir()), r))
			rep := sched.Run(context.Background())
			res := rep.Results["a"]
			if res.Status != StatusFailed || res.Attempts != 2 {
				t.Fatalf("want failed after 2 attempts, got %s %d", res.Status, res.Attempts)
			}
			var se *StepError
			if !errors.As(res.Err, &se) {
				t.Fatalf("want *StepError, got %T %v", res.Err, res.Err)
			}
			if len(r.Calls) != 2 {
				t.Fatalf("expected exactly 2 attempts, got %d", len(r.Calls))
			}
			t.Logf("结果: failed attempts=%d err=%v", res.Attempts, res.Err)
		})

		t.Run("用例=失败步骤不留半成品产物", func(t *testing.T) {
			dir := t.TempDir()
			out := dir + "/artifact.txt"
			cfg := &Config{Concurrency: 1, Steps: []StepSpec{
				{Name: "a", Command: "writes-then-fails", Retries: 0, Outputs: []string{out}},
			}}
			mustBuild(t, cfg)
			// Simulate a command that produced an artifact before failing.
			if err := writeFile(out, "partial"); err != nil {
				t.Fatal(err)
			}
			r := NewScriptedRunner()
			r.ScriptStep("a", errResult("fail after writing artifact"))
			g, _ := BuildGraph(cfg)
			sched, _ := NewScheduler(cfg, g, NewExecutor(NewCache(t.TempDir()), r))
			rep := sched.Run(context.Background())
			if rep.Results["a"].Status != StatusFailed {
				t.Fatalf("want failed, got %s", rep.Results["a"].Status)
			}
			if _, err := statFile(out); !errors.Is(err, errNotExist) {
				t.Fatalf("artifact should have been cleaned, stat err=%v", err)
			}
			t.Log("结果: 失败后声明的产物已删除")
		})
	})

	t.Run("business=失败传播", func(t *testing.T) {
		t.Run("用例=失败步骤的后继全部 skipped 且错误可区分", func(t *testing.T) {
			cfg := &Config{Concurrency: 4, Steps: []StepSpec{
				{Name: "build", Command: "fail"},
				{Name: "unit", Command: "unit", Depends: []string{"build"}},
				{Name: "integ", Command: "integ", Depends: []string{"build"}},
				{Name: "deploy", Command: "deploy", Depends: []string{"unit", "integ"}},
			}}
			mustBuild(t, cfg)
			r := NewScriptedRunner()
			r.ScriptStep("build", errResult("compile error"))
			g, _ := BuildGraph(cfg)
			sched, _ := NewScheduler(cfg, g, NewExecutor(NewCache(t.TempDir()), r))
			rep := sched.Run(context.Background())
			if rep.Results["build"].Status != StatusFailed {
				t.Fatalf("build should fail, got %s", rep.Results["build"].Status)
			}
			for _, name := range []string{"unit", "integ", "deploy"} {
				res := rep.Results[name]
				if res.Status != StatusSkipped {
					t.Fatalf("%s should be skipped, got %s", name, res.Status)
				}
				var sk *SkipError
				if !errors.As(res.Err, &sk) {
					t.Fatalf("%s error should be *SkipError, got %T", name, res.Err)
				}
			}
			if len(rep.Failed()) != 1 || len(rep.Skipped()) != 3 {
				t.Fatalf("want 1 failed 3 skipped, got %d %d", len(rep.Failed()), len(rep.Skipped()))
			}
			t.Logf("结果: build=failed(自身失败), unit/integ/deploy=skipped(被上游拖累)")
		})

		t.Run("用例=与失败无关的分支继续执行", func(t *testing.T) {
			cfg := &Config{Concurrency: 4, Steps: []StepSpec{
				{Name: "bad", Command: "fail"},
				{Name: "victim", Command: "v", Depends: []string{"bad"}},
				{Name: "good", Command: "ok"},
				{Name: "good2", Command: "ok2", Depends: []string{"good"}},
			}}
			mustBuild(t, cfg)
			r := NewScriptedRunner()
			r.ScriptStep("bad", errResult("nope"))
			g, _ := BuildGraph(cfg)
			sched, _ := NewScheduler(cfg, g, NewExecutor(NewCache(t.TempDir()), r))
			rep := sched.Run(context.Background())
			if rep.Results["good"].Status != StatusSuccess || rep.Results["good2"].Status != StatusSuccess {
				t.Fatalf("unrelated branch must succeed: good=%s good2=%s",
					rep.Results["good"].Status, rep.Results["good2"].Status)
			}
			if rep.Results["victim"].Status != StatusSkipped {
				t.Fatalf("victim should be skipped, got %s", rep.Results["victim"].Status)
			}
			t.Log("结果: bad/victim 失败链处理, good/good2 正常完成")
		})

		t.Run("用例=钻石依赖中一个上游失败则汇聚点跳过并指明来源", func(t *testing.T) {
			cfg := &Config{Concurrency: 4, Steps: []StepSpec{
				{Name: "l", Command: "fail"},
				{Name: "r", Command: "ok"},
				{Name: "join", Command: "join", Depends: []string{"l", "r"}},
			}}
			mustBuild(t, cfg)
			r := NewScriptedRunner()
			r.ScriptStep("l", errResult("left boom"))
			g, _ := BuildGraph(cfg)
			sched, _ := NewScheduler(cfg, g, NewExecutor(NewCache(t.TempDir()), r))
			rep := sched.Run(context.Background())
			if rep.Results["r"].Status != StatusSuccess {
				t.Fatalf("r should succeed independently, got %s", rep.Results["r"].Status)
			}
			if rep.Results["join"].Status != StatusSkipped {
				t.Fatalf("join should be skipped, got %s", rep.Results["join"].Status)
			}
			if !strings.Contains(rep.Results["join"].Err.Error(), "l") {
				t.Fatalf("skip error should name upstream, got %v", rep.Results["join"].Err)
			}
			t.Logf("结果: r 成功, join=skipped 原因=%v", rep.Results["join"].Err)
		})
	})
}

func TestCancellation(t *testing.T) {
	t.Run("business=取消与超时", func(t *testing.T) {
		t.Run("用例=外部取消后运行中的步骤为 canceled,后继不再执行,产物清理", func(t *testing.T) {
			dir := t.TempDir()
			out := dir + "/partial.txt"
			cfg := &Config{Concurrency: 2, Steps: []StepSpec{
				{Name: "slow", Command: "sleep", Outputs: []string{out}},
				{Name: "after", Command: "after", Depends: []string{"slow"}},
				{Name: "other", Command: "other"},
			}}
			mustBuild(t, cfg)
			r := NewScriptedRunner()
			started := make(chan struct{})
			r.ScriptHook("slow", func(int) { close(started) })
			r.ScriptDelay("slow", 5*time.Second)
			if err := writeFile(out, "partial"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			g, _ := BuildGraph(cfg)
			sched, _ := NewScheduler(cfg, g, NewExecutor(NewCache(t.TempDir()), r))
			go func() {
				<-started
				cancel()
			}()
			rep := sched.Run(ctx)
			if rep.Results["slow"].Status != StatusCanceled {
				t.Fatalf("slow should be canceled, got %s (err=%v)", rep.Results["slow"].Status, rep.Results["slow"].Err)
			}
			if rep.Results["after"].Status != StatusSkipped && rep.Results["after"].Status != StatusCanceled {
				t.Fatalf("after must not succeed, got %s", rep.Results["after"].Status)
			}
			if rep.Results["after"].Status == StatusSuccess {
				t.Fatal("successor ran after cancellation")
			}
			if _, err := statFile(out); !errors.Is(err, errNotExist) {
				t.Fatalf("partial artifact must be removed, err=%v", err)
			}
			t.Logf("结果: slow=canceled, after=%s, 半成品产物已清理", rep.Results["after"].Status)
		})

		t.Run("用例=全局超时触发整线取消且不死锁", func(t *testing.T) {
			cfg := &Config{Concurrency: 2, Timeout: &Duration{Duration: 120 * time.Millisecond}, Steps: []StepSpec{
				{Name: "slow1", Command: "sleep"},
				{Name: "slow2", Command: "sleep"},
				{Name: "late", Command: "late", Depends: []string{"slow1"}},
			}}
			mustBuild(t, cfg)
			r := NewScriptedRunner()
			r.ScriptDelay("slow1", 5*time.Second)
			r.ScriptDelay("slow2", 5*time.Second)
			done := make(chan struct{})
			g, _ := BuildGraph(cfg)
			sched, _ := NewScheduler(cfg, g, NewExecutor(NewCache(t.TempDir()), r))
			var rep *Report
			go func() {
				rep = sched.Run(context.Background())
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("scheduler deadlocked after global timeout")
			}
			if rep.OK() {
				t.Fatal("pipeline should not be OK after timeout")
			}
			for _, name := range []string{"slow1", "slow2"} {
				if rep.Results[name].Status != StatusCanceled {
					t.Fatalf("%s should be canceled, got %s", name, rep.Results[name].Status)
				}
			}
			if rep.Results["late"].Status == StatusSuccess || rep.Results["late"].Status == StatusCached {
				t.Fatalf("late must not run, got %s", rep.Results["late"].Status)
			}
			t.Logf("结果: slow1/slow2=canceled, late=%s, 调度器准时退出无死锁", rep.Results["late"].Status)
		})

		t.Run("用例=单步超时算失败尝试并按配置重试", func(t *testing.T) {
			cfg := &Config{Concurrency: 1, Steps: []StepSpec{
				{Name: "a", Command: "sleep", Retries: 1, Timeout: &Duration{Duration: 40 * time.Millisecond}},
			}}
			mustBuild(t, cfg)
			r := NewScriptedRunner()
			// ScriptedRunner honors ctx, so both attempts hit the deadline.
			r.ScriptDelay("a", 5*time.Second)
			g, _ := BuildGraph(cfg)
			sched, _ := NewScheduler(cfg, g, NewExecutor(NewCache(t.TempDir()), r))
			rep := sched.Run(context.Background())
			res := rep.Results["a"]
			if res.Status != StatusFailed || res.Attempts != 2 {
				t.Fatalf("want failed after 2 attempts, got %s attempts=%d", res.Status, res.Attempts)
			}
			t.Logf("结果: 单步超时重试 2 次后 failed, attempts=%d", res.Attempts)
		})
	})
}
