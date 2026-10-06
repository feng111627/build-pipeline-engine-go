package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestShellIntegration runs a real sh -c pipeline end to end, including
// the on-disk cache, so we also exercise ShellRunner process handling.
func TestShellIntegration(t *testing.T) {
	t.Run("business=端到端真实命令", func(t *testing.T) {
		dir := t.TempDir()
		marker := filepath.Join(dir, "out.txt")
		cfg := &Config{Concurrency: 2, Steps: []StepSpec{
			{Name: "produce", Command: "echo real-output > " + marker, Outputs: []string{marker}},
			{Name: "consume", Command: "grep real-output " + marker, Depends: []string{"produce"}},
		}}
		eng := &Engine{Runner: &ShellRunner{Dir: dir, Stdout: os.Stdout, Stderr: os.Stderr}}
		cacheDir := filepath.Join(dir, "cache")

		t.Run("用例=首次执行产物生成且流水线成功", func(t *testing.T) {
			rep, err := eng.Run(context.Background(), cfg, cacheDir)
			if err != nil {
				t.Fatal(err)
			}
			if !rep.OK() {
				t.Fatalf("pipeline failed: %+v", rep.Results)
			}
			b, err := os.ReadFile(marker)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != "real-output\n" {
				t.Fatalf("unexpected artifact content: %q", b)
			}
			t.Logf("结果: 产物内容=%q, produce/consume 均 success", string(b))
		})

		t.Run("用例=第二次执行全部命中缓存不再启命令", func(t *testing.T) {
			rep, err := eng.Run(context.Background(), cfg, cacheDir)
			if err != nil {
				t.Fatal(err)
			}
			if rep.Results["produce"].Status != StatusCached || rep.Results["consume"].Status != StatusCached {
				t.Fatalf("both should be cached: %s %s", rep.Results["produce"].Status, rep.Results["consume"].Status)
			}
			t.Log("结果: produce/consume 均 cached")
		})

		t.Run("用例=命令改变后缓存失效重跑", func(t *testing.T) {
			changed := &Config{Concurrency: 2, Steps: []StepSpec{
				{Name: "produce", Command: "echo real-output-v2 > " + marker, Outputs: []string{marker}},
				{Name: "consume", Command: "grep real-output-v2 " + marker, Depends: []string{"produce"}},
			}}
			rep, err := eng.Run(context.Background(), changed, cacheDir)
			if err != nil {
				t.Fatal(err)
			}
			if rep.Results["produce"].Status != StatusSuccess {
				t.Fatalf("produce should rerun, got %s", rep.Results["produce"].Status)
			}
			if rep.Results["consume"].Status != StatusSuccess {
				t.Fatalf("consume should rerun (dep key changed), got %s", rep.Results["consume"].Status)
			}
			b, _ := os.ReadFile(marker)
			if string(b) != "real-output-v2\n" {
				t.Fatalf("artifact not updated: %q", b)
			}
			t.Logf("结果: 命令变更后两步重跑, 新产物=%q", string(b))
		})
	})
}
