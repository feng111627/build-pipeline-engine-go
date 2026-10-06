package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCacheKey(t *testing.T) {
	t.Run("business=缓存键稳定性与失效", func(t *testing.T) {
		c := NewCache(t.TempDir())
		base := StepSpec{Name: "s", Command: "build.sh", Inputs: []InputSpec{
			{Name: "cfg", Value: "v1"},
			{Name: "mode", Value: "release"},
		}}

		t.Run("用例=同命令同输入得到稳定键", func(t *testing.T) {
			k1, err := c.Key(base, nil)
			if err != nil {
				t.Fatal(err)
			}
			k2, err := c.Key(base, nil)
			if err != nil {
				t.Fatal(err)
			}
			if k1 != k2 {
				t.Fatalf("keys not stable: %s vs %s", k1, k2)
			}
			if len(k1) != 64 {
				t.Fatalf("want sha256 hex (64 chars), got %d", len(k1))
			}
			t.Logf("结果: 稳定键=%s", k1)
		})

		t.Run("用例=输入声明顺序不同键相同", func(t *testing.T) {
			reordered := base
			reordered.Inputs = []InputSpec{
				{Name: "mode", Value: "release"},
				{Name: "cfg", Value: "v1"},
			}
			k1, _ := c.Key(base, nil)
			k2, _ := c.Key(reordered, nil)
			if k1 != k2 {
				t.Fatalf("key changed with input declaration order: %s vs %s", k1, k2)
			}
			t.Logf("结果: 顺序无关键=%s", k1)
		})

		t.Run("用例=输入值变化键失效", func(t *testing.T) {
			changed := base
			changed.Inputs = []InputSpec{
				{Name: "cfg", Value: "v2"},
				{Name: "mode", Value: "release"},
			}
			k1, _ := c.Key(base, nil)
			k2, _ := c.Key(changed, nil)
			if k1 == k2 {
				t.Fatal("cache key did not invalidate on input change")
			}
			t.Logf("结果: v1=%s -> v2=%s", k1[:12], k2[:12])
		})

		t.Run("用例=命令变化键失效", func(t *testing.T) {
			changed := base
			changed.Command = "build.sh --extra"
			k1, _ := c.Key(base, nil)
			k2, _ := c.Key(changed, nil)
			if k1 == k2 {
				t.Fatal("cache key did not invalidate on command change")
			}
			t.Logf("结果: 命令改变 -> 键不同")
		})

		t.Run("用例=依赖键变化导致本步骤键变化(传递失效)", func(t *testing.T) {
			kA, _ := c.Key(base, nil)
			down := StepSpec{Name: "down", Command: "package.sh"}
			k1, _ := c.Key(down, map[string]string{"s": kA})
			k2, _ := c.Key(down, map[string]string{"s": "0000000000000000000000000000000000000000000000000000000000000000"})
			if k1 == k2 {
				t.Fatal("downstream key must change when dependency key changes")
			}
			t.Logf("结果: 依赖变化时下游键级联变化")
		})

		t.Run("用例=依赖声明顺序不同键相同", func(t *testing.T) {
			down := StepSpec{Name: "down", Command: "package.sh"}
			k1, _ := c.Key(down, map[string]string{"a": "11", "b": "22"})
			k2, _ := c.Key(down, map[string]string{"b": "22", "a": "11"})
			if k1 != k2 {
				t.Fatalf("key must be independent of map iteration order: %s vs %s", k1, k2)
			}
			t.Logf("结果: 依赖顺序无关键=%s", k1)
		})

		t.Run("用例=文件内容哈希进键且内容变化即失效", func(t *testing.T) {
			dir := t.TempDir()
			f := filepath.Join(dir, "src.txt")
			if err := os.WriteFile(f, []byte("one"), 0o644); err != nil {
				t.Fatal(err)
			}
			spec := StepSpec{Name: "s", Command: "gen", Inputs: []InputSpec{{Name: "src", File: f}}}
			k1, err := c.Key(spec, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(f, []byte("two-different"), 0o644); err != nil {
				t.Fatal(err)
			}
			k2, err := c.Key(spec, nil)
			if err != nil {
				t.Fatal(err)
			}
			if k1 == k2 {
				t.Fatal("file content change must invalidate key")
			}
			// Rewriting identical content restores the key.
			if err := os.WriteFile(f, []byte("one"), 0o644); err != nil {
				t.Fatal(err)
			}
			k3, _ := c.Key(spec, nil)
			if k3 != k1 {
				t.Fatal("identical content must restore the same key")
			}
			t.Logf("结果: 文件键随内容变化且内容恢复后还原")
		})

		t.Run("用例=缺失文件输入报错而非静默", func(t *testing.T) {
			spec := StepSpec{Name: "s", Command: "gen", Inputs: []InputSpec{{Name: "src", File: filepath.Join(t.TempDir(), "missing")}}}
			if _, err := c.Key(spec, nil); err == nil {
				t.Fatal("missing input file must produce an error")
			} else {
				t.Logf("结果: 按预期报错 -> %v", err)
			}
		})
	})
}

func TestCacheStore(t *testing.T) {
	t.Run("business=缓存落盘与读取", func(t *testing.T) {
		c := NewCache(t.TempDir())
		t.Run("用例=未保存时 Has=false", func(t *testing.T) {
			if c.Has("0123456789abcdef") {
				t.Fatal("fresh cache must not contain key")
			}
			t.Log("结果: Has=false")
		})
		t.Run("用例=保存后可命中且记录可读回", func(t *testing.T) {
			spec := StepSpec{Name: "s", Command: "echo"}
			key, err := c.Key(spec, nil)
			if err != nil {
				t.Fatal(err)
			}
			rec := Record{Step: "s", Key: key, Entries: []RecordEntry{{Attempt: 1, Cmd: "sh", Output: "ok"}}}
			if err := c.Save(key, rec); err != nil {
				t.Fatal(err)
			}
			if !c.Has(key) {
				t.Fatal("Has must be true after Save")
			}
			got, err := c.Load(key)
			if err != nil {
				t.Fatal(err)
			}
			if got.Step != "s" || len(got.Entries) != 1 || got.Entries[0].Output != "ok" {
				t.Fatalf("record mismatch: %+v", got)
			}
			t.Logf("结果: 命中并读回 step=%s attempts=%d", got.Step, len(got.Entries))
		})
	})
}
