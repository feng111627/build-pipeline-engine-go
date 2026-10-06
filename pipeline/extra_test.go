package pipeline

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

func TestExtraBoundaries(t *testing.T) {
	t.Run("business=补充边界", func(t *testing.T) {
		t.Run("用例=配置文件缺失返回错误", func(t *testing.T) {
			if _, err := LoadConfig(filepath.Join(t.TempDir(), "nope.json")); err == nil {
				t.Fatal("missing config must error")
			} else {
				t.Logf("结果: %v", err)
			}
		})

		t.Run("用例=非法 duration 报错", func(t *testing.T) {
			var d Duration
			if err := json.Unmarshal([]byte(`"not-a-duration"`), &d); err == nil {
				t.Fatal("bad duration must error")
			}
			if err := json.Unmarshal([]byte(`123`), &d); err == nil {
				t.Fatal("non-string duration must error")
			}
			t.Log("结果: 两种非法 duration 形式均被拒绝")
		})

		t.Run("用例=Duration 可往返 JSON", func(t *testing.T) {
			var d Duration
			if err := json.Unmarshal([]byte(`"150ms"`), &d); err != nil {
				t.Fatal(err)
			}
			b, err := json.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != `"150ms"` {
				t.Fatalf("round trip = %s", b)
			}
			t.Logf("结果: 往返后=%s", b)
		})

		t.Run("用例=状态字符串覆盖所有终态", func(t *testing.T) {
			cases := map[StepStatus]string{
				StatusPending: "pending", StatusSuccess: "success", StatusCached: "cached",
				StatusFailed: "failed", StatusSkipped: "skipped", StatusCanceled: "canceled",
			}
			for st, want := range cases {
				if st.String() != want {
					t.Fatalf("%d -> %s, want %s", st, st.String(), want)
				}
			}
			t.Log("结果: 6 种状态字符串均正确")
		})

		t.Run("用例=Report 聚合按名称排序且 OK 正确", func(t *testing.T) {
			r := &Report{Results: map[string]*StepResult{
				"z": {Name: "z", Status: StatusFailed},
				"a": {Name: "a", Status: StatusSkipped},
				"m": {Name: "m", Status: StatusCanceled},
				"b": {Name: "b", Status: StatusSuccess},
			}}
			if f := r.Failed(); len(f) != 1 || f[0].Name != "z" {
				t.Fatalf("Failed = %+v", f)
			}
			if s := r.Skipped(); len(s) != 1 || s[0].Name != "a" {
				t.Fatalf("Skipped = %+v", s)
			}
			if c := r.Canceled(); len(c) != 1 || c[0].Name != "m" {
				t.Fatalf("Canceled = %+v", c)
			}
			if r.OK() {
				t.Fatal("report with failures must not be OK")
			}
			ok := &Report{Results: map[string]*StepResult{"x": {Name: "x", Status: StatusCached}}}
			if !ok.OK() {
				t.Fatal("all-cached report should be OK")
			}
			t.Log("结果: Failed/Skipped/Canceled 聚合与排序、OK 均正确")
		})

		t.Run("用例=Graph.Deps 返回依赖副本", func(t *testing.T) {
			g, err := BuildGraph(&Config{Steps: []StepSpec{
				{Name: "a", Command: "x"},
				{Name: "b", Command: "x", Depends: []string{"a"}},
			}})
			if err != nil {
				t.Fatal(err)
			}
			d := g.Deps("b")
			d[0] = "mutated"
			if g.Deps("b")[0] != "a" {
				t.Fatal("Deps must return a defensive copy")
			}
			t.Log("结果: Deps 返回防御性副本")
		})

		t.Run("用例=StepError 可 Unwrap 到根因", func(t *testing.T) {
			root := errors.New("root")
			se := &StepError{Step: "s", Attempts: 2, Cause: root}
			if !errors.Is(se, root) {
				t.Fatal("Unwrap chain broken")
			}
			t.Logf("结果: %v", se)
		})

		t.Run("用例=ValidateFile 文件不存在时报错", func(t *testing.T) {
			if err := ValidateFile(filepath.Join(t.TempDir(), "x.json")); err == nil {
				t.Fatal("expected error")
			}
			t.Log("结果: ValidateFile 对缺失文件报错")
		})
	})
}
