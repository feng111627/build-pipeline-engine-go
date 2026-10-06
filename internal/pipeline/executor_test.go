package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newExecutor(dir string, conc int) *Executor {
	return &Executor{
		Concurrency: conc,
		WorkDir:     dir,
		Cache:       &CacheStore{Dir: filepath.Join(dir, ".cache")},
	}
}

func resultByName(t *testing.T, results []Result, name string) Result {
	t.Helper()
	for _, r := range results {
		if r.Step.Name == name {
			return r
		}
	}
	t.Fatalf("结果中找不到步骤 %s", name)
	return Result{}
}

// 业务：调度 —— 互不依赖的步骤并发执行，且受并发上限约束。
func TestScheduleConcurrency(t *testing.T) {
	dir := t.TempDir()
	mkSteps := func() []Step {
		var steps []Step
		for i := 0; i < 3; i++ {
			steps = append(steps, Step{
				Name:    fmt.Sprintf("s%d", i),
				Command: []string{"sh", "-c", "sleep 0.3"},
			})
		}
		return steps
	}

	t.Run("并发上限为1时串行执行", func(t *testing.T) {
		start := time.Now()
		results := newExecutor(dir, 1).Run(context.Background(), mkSteps())
		elapsed := time.Since(start)
		for _, r := range results {
			if r.Status != StatusSuccess {
				t.Fatalf("步骤 %s 状态异常: %v", r.Step.Name, r.Status)
			}
		}
		if elapsed < 850*time.Millisecond {
			t.Fatalf("并发上限1时应近似串行(>=0.9s), 实际 %v", elapsed)
		}
		t.Logf("用例通过: 串行耗时 %v", elapsed.Round(time.Millisecond))
	})

	t.Run("互不依赖的步骤可并发", func(t *testing.T) {
		dir2 := t.TempDir() // 换缓存目录避免命中
		start := time.Now()
		results := newExecutor(dir2, 3).Run(context.Background(), mkSteps())
		elapsed := time.Since(start)
		for _, r := range results {
			if r.Status != StatusSuccess {
				t.Fatalf("步骤 %s 状态异常: %v", r.Step.Name, r.Status)
			}
		}
		if elapsed > 700*time.Millisecond {
			t.Fatalf("并发3时应在~0.3s完成, 实际 %v", elapsed)
		}
		t.Logf("用例通过: 并发耗时 %v", elapsed.Round(time.Millisecond))
	})

	t.Run("依赖就绪后才执行", func(t *testing.T) {
		dir3 := t.TempDir()
		steps := []Step{
			{Name: "first", Command: []string{"sh", "-c", "sleep 0.2; echo ok > order.txt"}},
			{Name: "second", Command: []string{"sh", "-c", "test -f order.txt"}, Deps: []string{"first"}},
		}
		results := newExecutor(dir3, 2).Run(context.Background(), steps)
		if resultByName(t, results, "second").Status != StatusSuccess {
			t.Fatalf("依赖步骤未按序执行: %+v", results)
		}
		t.Log("用例通过: 依赖先于后继执行")
	})
}

// 业务：缓存 —— 命中跳过执行，输入变化后失效重跑。
func TestExecutorCache(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "in.txt", "v1")
	// 命令把输入内容追加到 log.txt，便于观察是否真的执行了。
	step := Step{
		Name:    "build",
		Command: []string{"sh", "-c", "cat in.txt >> log.txt"},
		Inputs:  []string{"in.txt"},
	}
	steps := []Step{step}
	exec := newExecutor(dir, 1)

	t.Run("首次运行执行命令", func(t *testing.T) {
		results := exec.Run(context.Background(), steps)
		if results[0].Status != StatusSuccess || results[0].Attempts != 1 {
			t.Fatalf("首次运行应执行成功: %+v", results[0])
		}
		t.Log("用例通过: 首次运行 SUCCESS")
	})

	t.Run("输入未变时命中缓存跳过", func(t *testing.T) {
		results := exec.Run(context.Background(), steps)
		if results[0].Status != StatusCached || results[0].Attempts != 0 {
			t.Fatalf("应命中缓存: %+v", results[0])
		}
		data, _ := os.ReadFile(filepath.Join(dir, "log.txt"))
		if string(data) != "v1" {
			t.Fatalf("缓存命中不应重复执行命令, log=%q", data)
		}
		t.Log("用例通过: 第二次运行 CACHED, 命令未重复执行")
	})

	t.Run("输入变化后缓存失效重新执行", func(t *testing.T) {
		writeFile(t, dir, "in.txt", "v2")
		results := exec.Run(context.Background(), steps)
		if results[0].Status != StatusSuccess {
			t.Fatalf("输入变化后应重新执行: %+v", results[0])
		}
		data, _ := os.ReadFile(filepath.Join(dir, "log.txt"))
		if string(data) != "v1v2" {
			t.Fatalf("应追加新内容, log=%q", data)
		}
		t.Log("用例通过: 输入变化 -> 缓存失效 -> 重新执行")
	})
}

// 业务：失败重试与传播 —— 重试耗尽后后继跳过，无关分支继续。
func TestFailureRetryAndPropagation(t *testing.T) {
	dir := t.TempDir()
	steps := []Step{
		// 每次执行把计数写入 tries.txt 并失败，用于验证重试次数。
		{Name: "flaky", Retries: 2, Command: []string{"sh", "-c",
			"n=$(cat tries.txt 2>/dev/null || echo 0); echo $((n+1)) > tries.txt; exit 1"}},
		{Name: "downstream", Deps: []string{"flaky"},
			Command: []string{"sh", "-c", "echo ran > downstream.txt"}},
		{Name: "independent", Command: []string{"sh", "-c", "echo ran > independent.txt"}},
	}
	results := newExecutor(dir, 3).Run(context.Background(), steps)

	t.Run("失败步骤按配置重试后标记FAILED", func(t *testing.T) {
		r := resultByName(t, results, "flaky")
		if r.Status != StatusFailed {
			t.Fatalf("期望 FAILED, 得到 %v", r.Status)
		}
		if r.Attempts != 3 { // 首次 + 2 次重试
			t.Fatalf("期望尝试 3 次, 得到 %d", r.Attempts)
		}
		data, _ := os.ReadFile(filepath.Join(dir, "tries.txt"))
		if string(data) != "3\n" {
			t.Fatalf("命令实际执行次数应为 3, 文件内容 %q", data)
		}
		t.Logf("用例通过: 重试耗尽 FAILED, attempts=%d", r.Attempts)
	})

	t.Run("后继步骤被标记SKIPPED且不执行", func(t *testing.T) {
		r := resultByName(t, results, "downstream")
		if r.Status != StatusSkipped {
			t.Fatalf("期望 SKIPPED_UPSTREAM, 得到 %v", r.Status)
		}
		if r.Attempts != 0 {
			t.Fatalf("被跳过的步骤不应执行, attempts=%d", r.Attempts)
		}
		if _, err := os.Stat(filepath.Join(dir, "downstream.txt")); !os.IsNotExist(err) {
			t.Fatal("被跳过的步骤不应产生任何产物")
		}
		t.Log("用例通过: 后继 SKIPPED_UPSTREAM, 未执行、无产物")
	})

	t.Run("无关分支继续执行成功", func(t *testing.T) {
		r := resultByName(t, results, "independent")
		if r.Status != StatusSuccess {
			t.Fatalf("无关分支应 SUCCESS, 得到 %v", r.Status)
		}
		if _, err := os.Stat(filepath.Join(dir, "independent.txt")); err != nil {
			t.Fatal("无关分支应正常产出")
		}
		t.Log("用例通过: 无关分支 SUCCESS")
	})

	t.Run("失败步骤不写缓存", func(t *testing.T) {
		store := &CacheStore{Dir: filepath.Join(dir, ".cache")}
		if store.Hit("flaky", "any") {
			t.Fatal("失败步骤不应留下缓存记录")
		}
		t.Log("用例通过: 失败不留半成品缓存")
	})
}

// 业务：外部取消 —— 运行中的步骤被中断，未开始的步骤标记 CANCELED。
func TestCancellation(t *testing.T) {
	dir := t.TempDir()
	steps := []Step{
		{Name: "slow", Command: []string{"sh", "-c", "sleep 30"}},
		{Name: "after", Deps: []string{"slow"}, Command: []string{"sh", "-c", "echo ran > after.txt"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	results := newExecutor(dir, 2).Run(ctx, steps)
	elapsed := time.Since(start)

	t.Run("运行中的步骤被取消", func(t *testing.T) {
		r := resultByName(t, results, "slow")
		if r.Status != StatusCanceled {
			t.Fatalf("期望 CANCELED, 得到 %v", r.Status)
		}
		if elapsed > 5*time.Second {
			t.Fatalf("取消应及时生效, 实际耗时 %v", elapsed)
		}
		t.Logf("用例通过: slow CANCELED, 耗时 %v", elapsed.Round(time.Millisecond))
	})

	t.Run("未开始的后继步骤也被取消且不执行", func(t *testing.T) {
		r := resultByName(t, results, "after")
		if r.Status != StatusCanceled && r.Status != StatusSkipped {
			t.Fatalf("期望 CANCELED/SKIPPED, 得到 %v", r.Status)
		}
		if _, err := os.Stat(filepath.Join(dir, "after.txt")); !os.IsNotExist(err) {
			t.Fatal("取消后不应留下后继步骤的产物")
		}
		t.Log("用例通过: 后继未执行、无半成品产物")
	})

	t.Run("被取消的步骤不写缓存", func(t *testing.T) {
		store := &CacheStore{Dir: filepath.Join(dir, ".cache")}
		if store.Hit("slow", "any") {
			t.Fatal("被取消的步骤不应留下缓存记录")
		}
		t.Log("用例通过: 取消不留半成品缓存")
	})
}
