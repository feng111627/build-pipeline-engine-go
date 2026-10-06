package pipeline

import (
	"errors"
	"testing"
)

func TestGraph(t *testing.T) {
	t.Run("business=依赖图解析与拓扑序", func(t *testing.T) {
		t.Run("用例=无依赖时全部就绪且顺序按名称确定", func(t *testing.T) {
			cfg := &Config{Concurrency: 2, Steps: []StepSpec{
				{Name: "c", Command: "x"},
				{Name: "a", Command: "x"},
				{Name: "b", Command: "x"},
			}}
			g, err := BuildGraph(cfg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got := g.Order()
			want := []string{"a", "b", "c"}
			if !equalSlice(got, want) {
				t.Fatalf("order = %v, want %v", got, want)
			}
			t.Logf("结果: 拓扑序=%v 符合预期", got)
		})

		t.Run("用例=依赖决定先后且同层按名称排序", func(t *testing.T) {
			cfg := &Config{Concurrency: 2, Steps: []StepSpec{
				{Name: "d", Command: "x", Depends: []string{"b", "c"}},
				{Name: "b", Command: "x", Depends: []string{"a"}},
				{Name: "c", Command: "x", Depends: []string{"a"}},
				{Name: "a", Command: "x"},
			}}
			g, err := BuildGraph(cfg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got := g.Order()
			t.Logf("结果: 拓扑序=%v", got)
			index := map[string]int{}
			for i, n := range got {
				index[n] = i
			}
			if !(index["a"] < index["b"] && index["b"] < index["d"] &&
				index["a"] < index["c"] && index["c"] < index["d"]) {
				t.Fatalf("dependency order violated: %v", got)
			}
			if got[0] != "a" || got[len(got)-1] != "d" {
				t.Fatalf("endpoints wrong: %v", got)
			}
		})

		t.Run("用例=重复定义报 ErrDuplicateStep", func(t *testing.T) {
			cfg := &Config{Steps: []StepSpec{
				{Name: "a", Command: "1"},
				{Name: "a", Command: "2"},
			}}
			_, err := BuildGraph(cfg)
			var ce *ConfigError
			if !errors.As(err, &ce) || ce.Kind != ErrDuplicateStep {
				t.Fatalf("want duplicate_step ConfigError, got %v", err)
			}
			t.Logf("结果: 可区分错误 kind=%s steps=%v", ce.Kind, ce.Steps)
		})

		t.Run("用例=指向不存在的步骤报 ErrUnknownDep", func(t *testing.T) {
			cfg := &Config{Steps: []StepSpec{
				{Name: "a", Command: "x", Depends: []string{"ghost"}},
			}}
			_, err := BuildGraph(cfg)
			var ce *ConfigError
			if !errors.As(err, &ce) || ce.Kind != ErrUnknownDep {
				t.Fatalf("want unknown_dependency ConfigError, got %v", err)
			}
			if !equalSlice(ce.Steps, []string{"a", "ghost"}) {
				t.Fatalf("error steps = %v", ce.Steps)
			}
			t.Logf("结果: 可区分错误 kind=%s steps=%v", ce.Kind, ce.Steps)
		})

		t.Run("用例=环形依赖报 ErrCycle 并给出环路径", func(t *testing.T) {
			cfg := &Config{Steps: []StepSpec{
				{Name: "a", Command: "x", Depends: []string{"b"}},
				{Name: "b", Command: "x", Depends: []string{"c"}},
				{Name: "c", Command: "x", Depends: []string{"a"}},
			}}
			_, err := BuildGraph(cfg)
			var ce *ConfigError
			if !errors.As(err, &ce) || ce.Kind != ErrCycle {
				t.Fatalf("want cycle ConfigError, got %v", err)
			}
			if len(ce.Steps) < 2 || ce.Steps[0] != ce.Steps[len(ce.Steps)-1] {
				t.Fatalf("cycle path must close on itself, got %v", ce.Steps)
			}
			t.Logf("结果: 可区分错误 kind=%s 环路径=%v", ce.Kind, ce.Steps)
		})

		t.Run("用例=自依赖也是环", func(t *testing.T) {
			cfg := &Config{Steps: []StepSpec{
				{Name: "a", Command: "x", Depends: []string{"a"}},
			}}
			_, err := BuildGraph(cfg)
			var ce *ConfigError
			if !errors.As(err, &ce) || ce.Kind != ErrCycle {
				t.Fatalf("want cycle ConfigError, got %v", err)
			}
			t.Logf("结果: 环路径=%v", ce.Steps)
		})

		t.Run("用例=空步骤名报 ErrEmptyName", func(t *testing.T) {
			cfg := &Config{Steps: []StepSpec{{Name: "", Command: "x"}}}
			_, err := BuildGraph(cfg)
			var ce *ConfigError
			if !errors.As(err, &ce) || ce.Kind != ErrEmptyName {
				t.Fatalf("want empty_name ConfigError, got %v", err)
			}
			t.Logf("结果: 可区分错误 kind=%s", ce.Kind)
		})
	})
}

func equalSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
