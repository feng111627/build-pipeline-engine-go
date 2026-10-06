package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// CommandResult is the raw outcome of one command attempt.
type CommandResult struct {
	Attempt  int
	Output   string
	ExitCode int
	Err      error
}

// Runner executes a single shell command. It is an interface so tests
// can script deterministic outcomes without spawning processes.
type Runner interface {
	Run(ctx context.Context, spec StepSpec, attempt int) CommandResult
}

// ShellRunner runs commands via "sh -c" on the local machine.
type ShellRunner struct {
	Dir    string
	Stdout io.Writer
	Stderr io.Writer
}

// Run executes spec.Command. When ctx is canceled or times out, the
// whole process group is killed so no child lingers.
func (r *ShellRunner) Run(ctx context.Context, spec StepSpec, attempt int) CommandResult {
	cmd := exec.CommandContext(ctx, "sh", "-c", spec.Command)
	cmd.Dir = r.Dir
	var combined bytes.Buffer
	cmd.Stdout = io.MultiWriter(&combined, r.Stdout)
	cmd.Stderr = io.MultiWriter(&combined, r.Stderr)
	// Kill the whole group (sh plus the processes it spawned) on cancel.
	cmd.Cancel = func() error { return killProcessGroup(cmd) }
	if runtime.GOOS != "windows" {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}

	err := cmd.Run()
	res := CommandResult{Attempt: attempt, Output: combined.String()}
	if cmd.ProcessState != nil {
		res.ExitCode = cmd.ProcessState.ExitCode()
	}
	if err != nil {
		if ctx.Err() != nil {
			res.Err = ctx.Err()
		} else {
			res.Err = fmt.Errorf("exit code %d", res.ExitCode)
		}
	}
	return res
}

// ScriptedRunner is a test double whose outcomes are pre-registered
// per step name. Every attempt returns the next scripted result; past
// the end, the last result is reused.
type ScriptedRunner struct {
	mu     sync.Mutex
	Calls  []ScriptedCall
	byStep map[string][]CommandResult
	delays map[string]time.Duration
	hooks  map[string]func(attempt int)
}

// ScriptedCall records one invocation of a scripted step.
type ScriptedCall struct {
	Step    string
	Attempt int
}

// NewScriptedRunner builds an empty ScriptedRunner.
func NewScriptedRunner() *ScriptedRunner {
	return &ScriptedRunner{
		byStep: make(map[string][]CommandResult),
		delays: make(map[string]time.Duration),
		hooks:  make(map[string]func(attempt int)),
	}
}

// ScriptStep registers per-attempt outcomes for a step.
func (r *ScriptedRunner) ScriptStep(name string, results ...CommandResult) {
	r.byStep[name] = results
}

// ScriptDelay makes a step wait before returning (used for concurrency
// and cancellation tests).
func (r *ScriptedRunner) ScriptDelay(name string, d time.Duration) {
	r.delays[name] = d
}

// ScriptHook registers a callback invoked at the start of each attempt.
func (r *ScriptedRunner) ScriptHook(name string, fn func(attempt int)) {
	r.hooks[name] = fn
}

// Run returns the scripted outcome for spec.Name at the given attempt.
func (r *ScriptedRunner) Run(ctx context.Context, spec StepSpec, attempt int) CommandResult {
	r.mu.Lock()
	r.Calls = append(r.Calls, ScriptedCall{Step: spec.Name, Attempt: attempt})
	results := r.byStep[spec.Name]
	delay := r.delays[spec.Name]
	hook := r.hooks[spec.Name]
	r.mu.Unlock()

	if hook != nil {
		hook(attempt)
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return CommandResult{Attempt: attempt, Err: ctx.Err(), ExitCode: -1}
		}
	}
	if len(results) == 0 {
		return CommandResult{Attempt: attempt}
	}
	idx := attempt - 1
	if idx >= len(results) {
		idx = len(results) - 1
	}
	res := results[idx]
	res.Attempt = attempt
	return res
}
