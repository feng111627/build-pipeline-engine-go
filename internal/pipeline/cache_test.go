package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// 业务：缓存键稳定性 —— 同样的输入与命令，键必须稳定且与声明顺序无关。
func TestCacheKeyStability(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.txt", "hello")
	writeFile(t, dir, "b.txt", "world")

	t.Run("输入声明顺序不影响键", func(t *testing.T) {
		s1 := Step{Name: "x", Command: []string{"cat"}, Inputs: []string{"a.txt", "b.txt"}}
		s2 := Step{Name: "x", Command: []string{"cat"}, Inputs: []string{"b.txt", "a.txt"}}
		k1, err1 := CacheKey(dir, s1)
		k2, err2 := CacheKey(dir, s2)
		if err1 != nil || err2 != nil {
			t.Fatalf("计算缓存键失败: %v %v", err1, err2)
		}
		if k1 != k2 {
			t.Fatalf("输入顺序不应影响缓存键: %s vs %s", k1, k2)
		}
		t.Logf("用例通过: 键稳定 %s", k1[:12])
	})

	t.Run("重复计算结果一致", func(t *testing.T) {
		s := Step{Name: "x", Command: []string{"cat"}, Inputs: []string{"a.txt"}}
		k1, _ := CacheKey(dir, s)
		k2, _ := CacheKey(dir, s)
		if k1 != k2 {
			t.Fatalf("同一输入重复计算键不一致: %s vs %s", k1, k2)
		}
		t.Log("用例通过: 重复计算键一致")
	})
}

// 业务：缓存失效 —— 输入内容、命令、输入文件集合任一变化，键必须变化。
func TestCacheKeyInvalidation(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.txt", "hello")
	base := Step{Name: "x", Command: []string{"cat", "a.txt"}, Inputs: []string{"a.txt"}}
	baseKey, err := CacheKey(dir, base)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("输入内容变化使键失效", func(t *testing.T) {
		writeFile(t, dir, "a.txt", "changed")
		k, _ := CacheKey(dir, base)
		if k == baseKey {
			t.Fatal("输入内容变化后缓存键未变化")
		}
		writeFile(t, dir, "a.txt", "hello") // 恢复
		t.Log("用例通过: 内容变化 -> 键变化")
	})

	t.Run("命令变化使键失效", func(t *testing.T) {
		s := base
		s.Command = []string{"cat", "-n", "a.txt"}
		k, _ := CacheKey(dir, s)
		if k == baseKey {
			t.Fatal("命令变化后缓存键未变化")
		}
		t.Log("用例通过: 命令变化 -> 键变化")
	})

	t.Run("输入文件出现或消失使键失效", func(t *testing.T) {
		s := base
		s.Inputs = []string{"a.txt", "ghost.txt"}
		k1, _ := CacheKey(dir, s) // ghost.txt 缺失
		writeFile(t, dir, "ghost.txt", "now exists")
		k2, _ := CacheKey(dir, s) // ghost.txt 出现
		if k1 == k2 {
			t.Fatal("输入文件出现后缓存键未变化")
		}
		t.Log("用例通过: 文件缺失/出现 -> 键变化")
	})
}

// 业务：缓存存取 —— Save 后同键 Hit，异键不 Hit。
func TestCacheStore(t *testing.T) {
	t.Run("保存后命中_键不同不命中", func(t *testing.T) {
		store := &CacheStore{Dir: t.TempDir()}
		if store.Hit("build", "key1") {
			t.Fatal("空缓存不应命中")
		}
		if err := store.Save("build", "key1"); err != nil {
			t.Fatal(err)
		}
		if !store.Hit("build", "key1") {
			t.Fatal("保存后应命中")
		}
		if store.Hit("build", "key2") {
			t.Fatal("不同的键不应命中")
		}
		if store.Hit("other", "key1") {
			t.Fatal("其他步骤不应命中")
		}
		t.Log("用例通过: Save/Hit 行为正确")
	})
}
