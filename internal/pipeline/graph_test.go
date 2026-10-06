package pipeline

import (
	"strings"
	"testing"
)

// 业务：依赖图校验 —— 重复定义 / 缺失依赖 / 环形依赖必须可区分地报错。
func TestValidate(t *testing.T) {
	t.Run("合法DAG无错误", func(t *testing.T) {
		steps := []Step{
			{Name: "a", Command: []string{"true"}},
			{Name: "b", Command: []string{"true"}, Deps: []string{"a"}},
			{Name: "c", Command: []string{"true"}, Deps: []string{"a", "b"}},
		}
		if err := Validate(steps); err != nil {
			t.Fatalf("期望无错误, 得到 %v", err)
		}
		t.Log("用例通过: 合法 DAG 校验通过")
	})

	t.Run("重复定义报ErrDuplicateStep", func(t *testing.T) {
		steps := []Step{
			{Name: "a", Command: []string{"true"}},
			{Name: "a", Command: []string{"false"}},
		}
		err := Validate(steps)
		if err == nil || err.Kind != ErrDuplicateStep {
			t.Fatalf("期望 ErrDuplicateStep, 得到 %v", err)
		}
		if !strings.Contains(err.Error(), "a") {
			t.Fatalf("错误信息应包含重复步骤名: %v", err)
		}
		t.Logf("用例通过: 重复定义 -> %v", err)
	})

	t.Run("依赖不存在报ErrMissingDep", func(t *testing.T) {
		steps := []Step{
			{Name: "a", Command: []string{"true"}, Deps: []string{"ghost"}},
		}
		err := Validate(steps)
		if err == nil || err.Kind != ErrMissingDep {
			t.Fatalf("期望 ErrMissingDep, 得到 %v", err)
		}
		if !strings.Contains(err.Error(), "ghost") {
			t.Fatalf("错误信息应包含缺失依赖名: %v", err)
		}
		t.Logf("用例通过: 缺失依赖 -> %v", err)
	})

	t.Run("环形依赖报ErrCycle", func(t *testing.T) {
		steps := []Step{
			{Name: "a", Command: []string{"true"}, Deps: []string{"c"}},
			{Name: "b", Command: []string{"true"}, Deps: []string{"a"}},
			{Name: "c", Command: []string{"true"}, Deps: []string{"b"}},
		}
		err := Validate(steps)
		if err == nil || err.Kind != ErrCycle {
			t.Fatalf("期望 ErrCycle, 得到 %v", err)
		}
		t.Logf("用例通过: 环形依赖 -> %v", err)
	})

	t.Run("自依赖也是环", func(t *testing.T) {
		steps := []Step{
			{Name: "a", Command: []string{"true"}, Deps: []string{"a"}},
		}
		err := Validate(steps)
		if err == nil || err.Kind != ErrCycle {
			t.Fatalf("期望 ErrCycle, 得到 %v", err)
		}
		t.Logf("用例通过: 自依赖 -> %v", err)
	})
}

// 业务：拓扑分层 —— 同层步骤互不依赖，层序满足依赖先后。
func TestLayers(t *testing.T) {
	t.Run("钻石依赖分层正确", func(t *testing.T) {
		steps := []Step{
			{Name: "d", Command: []string{"true"}, Deps: []string{"b", "c"}},
			{Name: "b", Command: []string{"true"}, Deps: []string{"a"}},
			{Name: "c", Command: []string{"true"}, Deps: []string{"a"}},
			{Name: "a", Command: []string{"true"}},
		}
		layers := Layers(steps)
		want := [][]string{{"a"}, {"b", "c"}, {"d"}}
		if len(layers) != len(want) {
			t.Fatalf("层数不符: 得到 %v", layers)
		}
		for i := range want {
			if strings.Join(layers[i], ",") != strings.Join(want[i], ",") {
				t.Fatalf("第 %d 层不符: 期望 %v, 得到 %v", i, want[i], layers[i])
			}
		}
		t.Logf("用例通过: 分层结果 %v", layers)
	})

	t.Run("分层结果与配置书写顺序无关", func(t *testing.T) {
		s1 := []Step{
			{Name: "b", Command: []string{"true"}},
			{Name: "a", Command: []string{"true"}},
		}
		s2 := []Step{
			{Name: "a", Command: []string{"true"}},
			{Name: "b", Command: []string{"true"}},
		}
		l1, l2 := Layers(s1), Layers(s2)
		if strings.Join(l1[0], ",") != strings.Join(l2[0], ",") {
			t.Fatalf("分层应稳定一致: %v vs %v", l1, l2)
		}
		t.Logf("用例通过: 两种书写顺序分层一致 %v", l1)
	})
}
