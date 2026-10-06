package pipeline

import (
	"context"
	"fmt"
	"os"
	"time"
)

// Executor runs a single step: cache lookup, command attempts with
// per-step timeout, retries, cache population and output cleanup.
type Executor struct {
	cache  *Cache
	runner Runner
}

// NewExecutor builds an Executor.
func NewExecutor(cache *Cache, runner Runner) *Executor {
	return &Executor{cache: cache, runner: runner}
}

// execOutcome is the internal result of executing one step.
type execOutcome struct {
	status   StepStatus
	attempts int
	err      error
	duration time.Duration
	key      string
}

// Execute performs cache lookup then (if needed) command execution with
// retries. depKeys contains the resolved cache key of every dependency.
// On failure/cancel, declared output artifacts are removed so no
// half-finished products remain.
//
// Retry semantics: retries is the number of retries after the first
// attempt (total attempts = retries + 1). A step timeout counts as a
// failed attempt and is retried; parent context cancellation aborts
// immediately and is not retried.
func (e *Executor) Execute(ctx context.Context, spec StepSpec, depKeys map[string]string, defaultTimeout time.Duration) execOutcome {
	start := time.Now()

	key, err := e.cache.Key(spec, depKeys)
	if err != nil {
		return execOutcome{status: StatusFailed, attempts: 0, duration: time.Since(start),
			err: &StepError{Step: spec.Name, Attempts: 0, Cause: err}}
	}

	if e.cache.Has(key) {
		return execOutcome{status: StatusCached, key: key, duration: time.Since(start)}
	}

	timeout := defaultTimeout
	if spec.Timeout != nil {
		timeout = spec.Timeout.Duration
	}
	totalAttempts := spec.Retries + 1

	var lastErr error
	var entries []RecordEntry
	for attempt := 1; attempt <= totalAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			removeOutputs(spec.Outputs)
			return execOutcome{status: StatusCanceled, attempts: attempt - 1, key: key,
				duration: time.Since(start), err: err}
		}

		attemptCtx := ctx
		var cancel context.CancelFunc
		if timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, timeout)
		}
		res := e.runner.Run(attemptCtx, spec, attempt)
		if cancel != nil {
			cancel()
		}

		entries = append(entries, RecordEntry{
			Attempt: attempt,
			Cmd:     "sh",
			Args:    []string{"-c", spec.Command},
			Output:  res.Output,
		})

		if res.Err == nil {
			// Success: persist cache record. A persistence error does
			// not turn the step into a failure, it just means the next
			// run cannot reuse the result.
			saveErr := e.cache.Save(key, Record{Step: spec.Name, Key: key, Entries: entries})
			out := execOutcome{status: StatusSuccess, attempts: attempt, key: key,
				duration: time.Since(start)}
			if saveErr != nil {
				out.err = fmt.Errorf("cache save failed for step %s: %w", spec.Name, saveErr)
			}
			return out
		}

		lastErr = res.Err

		// Parent cancellation/global timeout: stop immediately, no retry.
		if ctx.Err() != nil {
			removeOutputs(spec.Outputs)
			return execOutcome{status: StatusCanceled, attempts: attempt, key: key,
				duration: time.Since(start), err: ctx.Err()}
		}
		// Attempt-level timeout: treat like a normal failed attempt.
	}

	// Retries exhausted.
	removeOutputs(spec.Outputs)
	return execOutcome{status: StatusFailed, attempts: totalAttempts, key: key,
		duration: time.Since(start),
		err:      &StepError{Step: spec.Name, Attempts: totalAttempts, Cause: lastErr}}
}

// removeOutputs best-effort deletes declared output paths so a failed
// or canceled step never leaves half-finished products.
func removeOutputs(outputs []string) {
	for _, p := range outputs {
		_ = os.RemoveAll(p)
	}
}
