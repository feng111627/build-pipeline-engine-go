package pipeline

import (
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	t.Run("business=配置加载", func(t *testing.T) {
		t.Run("用例=duration 字符串解析与默认并发", func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "pipeline.json")
			writeFile(p, `{
				"timeout": "250ms",
				"steps": [
					{"name":"a","command":"echo","timeout":"1s"}
				]
			}`)
			cfg, err := LoadConfig(p)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Timeout.Duration.String() != "250ms" {
				t.Fatalf("global timeout = %s", cfg.Timeout.Duration)
			}
			if cfg.Concurrency != 1 {
				t.Fatalf("default concurrency should be 1, got %d", cfg.Concurrency)
			}
			if cfg.Steps[0].Timeout.Duration.String() != "1s" {
				t.Fatalf("step timeout = %s", cfg.Steps[0].Timeout.Duration)
			}
			t.Logf("结果: timeout=%s 单步timeout=%s 默认并发=1",
				cfg.Timeout.Duration, cfg.Steps[0].Timeout.Duration)
		})

		t.Run("用例=未知字段被拒绝", func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "pipeline.json")
			writeFile(p, `{"steps":[{"name":"a","command":"echo","concurency":2}]}`)
			if _, err := LoadConfig(p); err == nil {
				t.Fatal("unknown field must be rejected")
			} else {
				t.Logf("结果: 按预期拒绝 -> %v", err)
			}
		})

		t.Run("用例=ValidateFile 区分好配置与坏配置", func(t *testing.T) {
			good := filepath.Join(t.TempDir(), "good.json")
			writeFile(good, `{"steps":[{"name":"a","command":"echo"}]}`)
			if err := ValidateFile(good); err != nil {
				t.Fatalf("good config rejected: %v", err)
			}
			bad := filepath.Join(t.TempDir(), "bad.json")
			writeFile(bad, `{"steps":[{"name":"a","command":"echo","depends":["x"]}]}`)
			if err := ValidateFile(bad); err == nil {
				t.Fatal("bad config accepted")
			} else {
				t.Logf("结果: 坏配置被拒绝 -> %v", err)
			}
		})
	})
}
