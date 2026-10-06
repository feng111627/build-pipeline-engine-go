package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
)

// Engine ties config parsing, graph validation, cache and scheduling.
type Engine struct {
	Runner Runner
}

// LoadConfig reads and parses a JSON pipeline config file and applies
// defaults. Structural errors are reported; semantic graph errors are
// reported separately by BuildGraph (called from Run/Validate).
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Config
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	return &cfg, nil
}

// Run validates the graph, then executes the pipeline and caches
// successful step runs under cacheDir.
func (e *Engine) Run(ctx context.Context, cfg *Config, cacheDir string) (*Report, error) {
	g, err := BuildGraph(cfg)
	if err != nil {
		return nil, err
	}
	runner := e.Runner
	if runner == nil {
		runner = &ShellRunner{Dir: ".", Stdout: os.Stdout, Stderr: os.Stderr}
	}
	cache := NewCache(cacheDir)
	exec := NewExecutor(cache, runner)
	sched, err := NewScheduler(cfg, g, exec)
	if err != nil {
		return nil, err
	}
	return sched.Run(ctx), nil
}

// ValidateFile parses a config and builds its graph, returning the
// first error if any. Useful as a pre-commit / CI check.
func ValidateFile(path string) error {
	cfg, err := LoadConfig(path)
	if err != nil {
		return err
	}
	_, err = BuildGraph(cfg)
	return err
}
