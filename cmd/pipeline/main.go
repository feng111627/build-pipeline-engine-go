package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/feng111627/build-pipeline-engine-go/pipeline"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "run":
		os.Exit(runCmd(os.Args[2:]))
	case "validate":
		os.Exit(validateCmd(os.Args[2:]))
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: pipeline <run|validate> [-config pipeline.json] [-cache .pipeline-cache]")
}

func runCmd(args []string) int {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	configPath := fs.String("config", "pipeline.json", "path to the pipeline config")
	cacheDir := fs.String("cache", ".pipeline-cache", "cache directory")
	timeout := fs.Duration("timeout", 0, "optional global pipeline timeout (e.g. 2m)")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg, err := pipeline.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		return 2
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	if *timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	eng := &pipeline.Engine{}
	report, err := eng.Run(ctx, cfg, *cacheDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pipeline error:", err)
		return 2
	}

	printReport(cfg, report)
	if !report.OK() {
		return 1
	}
	return 0
}

func validateCmd(args []string) int {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	configPath := fs.String("config", "pipeline.json", "path to the pipeline config")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if err := pipeline.ValidateFile(*configPath); err != nil {
		fmt.Fprintln(os.Stderr, "invalid:", err)
		return 1
	}
	fmt.Println("config valid:", *configPath)
	return 0
}

func printReport(cfg *pipeline.Config, r *pipeline.Report) {
	fmt.Println()
	fmt.Println("=== pipeline report ===")
	// Emit in topological order when possible for readability.
	// cfg order is declaration order, which is fine and deterministic.
	for _, st := range cfg.Steps {
		res := r.Results[st.Name]
		if res == nil {
			continue
		}
		line := fmt.Sprintf("  %-16s %-9s attempts=%d duration=%s", res.Name, res.Status, res.Attempts, res.Duration.Round(time.Millisecond))
		if res.FromCache {
			line += " cache=hit"
		}
		if res.Err != nil {
			line += "\n                   -> " + res.Err.Error()
		}
		fmt.Println(line)
	}
	if failed := r.Failed(); len(failed) > 0 {
		fmt.Printf("summary: %d step(s) failed\n", len(failed))
	}
	if skipped := r.Skipped(); len(skipped) > 0 {
		fmt.Printf("summary: %d step(s) skipped due to upstream failure\n", len(skipped))
	}
	if canceled := r.Canceled(); len(canceled) > 0 {
		fmt.Printf("summary: %d step(s) canceled\n", len(canceled))
	}
}
