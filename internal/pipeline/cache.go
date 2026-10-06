package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// CacheKey 根据步骤的命令与输入文件内容计算稳定缓存键。
// 稳定性保证：
//   - 输入文件先按路径排序再哈希，与声明顺序、执行顺序无关；
//   - 哈希覆盖文件内容而非 mtime，内容不变则键不变；
//   - 输入文件缺失时记入特殊标记，文件出现/消失会使键变化。
func CacheKey(workDir string, s Step) (string, error) {
	h := sha256.New()
	// 命令逐元素写入，带长度前缀避免拼接歧义。
	for _, arg := range s.Command {
		fmt.Fprintf(h, "cmd:%d:%s\n", len(arg), arg)
	}
	inputs := append([]string{}, s.Inputs...)
	sort.Strings(inputs)
	for _, in := range inputs {
		fmt.Fprintf(h, "in:%s\n", in)
		p := filepath.Join(workDir, filepath.FromSlash(in))
		f, err := os.Open(p)
		if err != nil {
			if os.IsNotExist(err) {
				h.Write([]byte("missing\n"))
				continue
			}
			return "", fmt.Errorf("步骤 %q 读取输入 %s 失败: %w", s.Name, in, err)
		}
		_, copyErr := io.Copy(h, f)
		closeErr := f.Close()
		if copyErr != nil {
			return "", fmt.Errorf("步骤 %q 读取输入 %s 失败: %w", s.Name, in, copyErr)
		}
		if closeErr != nil {
			return "", fmt.Errorf("步骤 %q 关闭输入 %s 失败: %w", s.Name, in, closeErr)
		}
		h.Write([]byte("\n"))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// CacheStore 是基于目录的缓存记录存储：每个成功步骤一个记录文件。
type CacheStore struct {
	Dir string
}

func (c *CacheStore) recordPath(step string) string {
	// 步骤名中可能含路径分隔符，做安全转义。
	safe := strings.NewReplacer("/", "_", "\\", "_", "..", "_").Replace(step)
	return filepath.Join(c.Dir, safe+".key")
}

// Hit 判断指定步骤的缓存键是否命中。
func (c *CacheStore) Hit(step, key string) bool {
	data, err := os.ReadFile(c.recordPath(step))
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(data)) == key
}

// Save 记录步骤成功完成时的缓存键。
func (c *CacheStore) Save(step, key string) error {
	if err := os.MkdirAll(c.Dir, 0o755); err != nil {
		return fmt.Errorf("创建缓存目录失败: %w", err)
	}
	if err := os.WriteFile(c.recordPath(step), []byte(key+"\n"), 0o644); err != nil {
		return fmt.Errorf("写入缓存记录失败: %w", err)
	}
	return nil
}
