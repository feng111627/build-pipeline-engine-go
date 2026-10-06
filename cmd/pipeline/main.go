// pipeline 是本地构建流水线执行引擎的命令行入口。
//
// 用法:
//
//	pipeline -config pipeline.json [-timeout 10m]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/feng111627/build-pipeline-engine-go/internal/pipeline"
)

func main() {
	configPath := flag.String("config", "pipeline.json", "流水线配置文件路径 (JSON)")
	timeout := flag.Duration("timeout", 0, "整体超时时间，如 30s、5m；0 表示不限制")
	flag.Parse()

	cfg, err := pipeline.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "配置错误: %v\n", err)
		os.Exit(2)
	}
	if gerr := pipeline.Validate(cfg.Steps); gerr != nil {
		fmt.Fprintf(os.Stderr, "依赖图错误: %v\n", gerr)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	exec := &pipeline.Executor{
		Concurrency: cfg.Concurrency,
		WorkDir:     cfg.WorkDir,
		Cache:       &pipeline.CacheStore{Dir: cfg.CacheDir},
	}
	results := exec.Run(ctx, cfg.Steps)

	failed := false
	for _, r := range results {
		line := fmt.Sprintf("%-16s %-16s 尝试次数=%d", r.Step.Name, r.Status, r.Attempts)
		if r.Err != nil {
			line += fmt.Sprintf(" 错误=%v", r.Err)
		}
		fmt.Println(line)
		if r.Status == pipeline.StatusFailed || r.Status == pipeline.StatusCanceled {
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}
