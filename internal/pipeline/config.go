// Package pipeline 提供本地构建流水线的配置解析、依赖调度、缓存与执行能力。
package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Step 描述流水线中的一个构建步骤。
type Step struct {
	Name    string   `json:"name"`           // 步骤名，全局唯一
	Command []string `json:"command"`        // 要执行的命令及参数
	Inputs  []string `json:"inputs"`         // 输入文件（相对工作目录），参与缓存键计算
	Deps    []string `json:"deps,omitempty"` // 依赖的其他步骤名
	Retries int      `json:"retries"`        // 失败后的重试次数（不含首次执行）
}

// Config 是流水线配置文件的顶层结构。
type Config struct {
	Concurrency int    `json:"concurrency"` // 并发上限，<=0 表示不限制
	WorkDir     string `json:"workdir"`     // 步骤执行的工作目录（相对配置文件所在目录）
	CacheDir    string `json:"cachedir"`    // 缓存目录（相对配置文件所在目录）
	Steps       []Step `json:"steps"`
}

// LoadConfig 从 JSON 文件加载配置。
// WorkDir 与 CacheDir 若为相对路径，会被解析为相对配置文件所在目录的绝对路径。
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件 %s 失败: %w", path, err)
	}
	if len(cfg.Steps) == 0 {
		return nil, fmt.Errorf("配置文件 %s 不包含任何步骤", path)
	}
	base := filepath.Dir(path)
	resolve := func(p, def string) (string, error) {
		if p == "" {
			p = def
		}
		if !filepath.IsAbs(p) {
			p = filepath.Join(base, p)
		}
		return filepath.Abs(p)
	}
	if cfg.WorkDir, err = resolve(cfg.WorkDir, "."); err != nil {
		return nil, fmt.Errorf("解析 workdir 失败: %w", err)
	}
	if cfg.CacheDir, err = resolve(cfg.CacheDir, ".pipeline-cache"); err != nil {
		return nil, fmt.Errorf("解析 cachedir 失败: %w", err)
	}
	for i, s := range cfg.Steps {
		if s.Name == "" {
			return nil, fmt.Errorf("第 %d 个步骤缺少 name", i+1)
		}
		if len(s.Command) == 0 {
			return nil, fmt.Errorf("步骤 %q 缺少 command", s.Name)
		}
		if s.Retries < 0 {
			return nil, fmt.Errorf("步骤 %q 的 retries 不能为负数", s.Name)
		}
	}
	return &cfg, nil
}
