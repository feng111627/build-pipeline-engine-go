package pipeline

import (
	"context"
	"sync"
)

// job is one step handed to a worker.
type job struct {
	spec    StepSpec
	depKeys map[string]string
}

// completion is a finished step reported back to the coordinator.
type completion struct {
	name string
	out  execOutcome
}

// Scheduler runs a validated DAG respecting dependencies, a global
// concurrency limit, failure propagation and cancellation/timeout.
type Scheduler struct {
	cfg         *Config
	graph       *Graph
	exec        *Executor
	concurrency int
}

// NewScheduler builds a Scheduler. Concurrency defaults to 1 when not
// configured.
func NewScheduler(cfg *Config, graph *Graph, exec *Executor) (*Scheduler, error) {
	n := cfg.Concurrency
	if n <= 0 {
		n = 1
	}
	return &Scheduler{cfg: cfg, graph: graph, exec: exec, concurrency: n}, nil
}

// Run executes every step and returns the per-step report. It never
// deadlocks: configuration errors are rejected by BuildGraph before
// Run, and any failed/canceled step unblocks its successors as skipped.
//
// Scheduling model: one coordinator goroutine owns all bookkeeping and
// a fixed pool of workers performs execution. The coordinator only
// blocks on (a) a finished worker or (b) the pipeline context, so it
// always makes progress once a step finishes.
func (s *Scheduler) Run(ctx context.Context) *Report {
	// cfg.Timeout is a global pipeline deadline shared by every step;
	// when it fires, running steps are canceled and the remaining steps
	// are marked canceled/skipped. Per-attempt timeouts live on
	// StepSpec.Timeout.
	if s.cfg.Timeout != nil && s.cfg.Timeout.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.Timeout.Duration)
		defer cancel()
	}

	deps := s.graph.deps
	succ := s.graph.successors
	indeg := make(map[string]int)
	readyNames := []string{}
	stepByName := make(map[string]StepSpec, len(s.cfg.Steps))
	for _, st := range s.cfg.Steps {
		stepByName[st.Name] = st
	}
	// Seed ready set deterministically using the graph's topo order.
	for _, name := range s.graph.order {
		indeg[name] = len(deps[name])
		if indeg[name] == 0 {
			readyNames = append(readyNames, name)
		}
	}

	results := make(map[string]*StepResult)
	keys := make(map[string]string) // step -> resolved cache key
	remaining := len(s.cfg.Steps)
	inFlight := 0

	jobs := make(chan job)
	doneCh := make(chan completion)
	var wg sync.WaitGroup
	for i := 0; i < s.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for jb := range jobs {
				doneCh <- completion{name: jb.spec.Name, out: s.exec.Execute(ctx, jb.spec, jb.depKeys, 0)}
			}
		}()
	}

	dispatch := func(name string) {
		spec := stepByName[name]
		depKeys := make(map[string]string, len(deps[name]))
		for _, d := range deps[name] {
			if k, ok := keys[d]; ok {
				depKeys[d] = k
			}
		}
		inFlight++
		jobs <- job{spec: spec, depKeys: depKeys}
	}

	// Helper: cascade-skip every successor that can no longer run.
	// Already-finished or already-skipped successors are left alone.
	var cascade func(upstream string, reason StepStatus)
	cascade = func(upstream string, reason StepStatus) {
		for _, sn := range succ[upstream] {
			if results[sn] != nil {
				continue
			}
			readyNames = remove(readyNames, sn)
			results[sn] = &StepResult{
				Name:   sn,
				Status: StatusSkipped,
				Err:    &SkipError{Step: sn, Upstream: upstream, Reason: reason},
			}
			keys[sn] = ""
			remaining--
			cascade(sn, reason)
		}
	}

	for remaining > 0 {
		// Dispatch everything currently ready up to free worker slots.
		for len(readyNames) > 0 && inFlight < s.concurrency {
			name := readyNames[0]
			readyNames = readyNames[1:]
			dispatch(name)
		}

		if inFlight == 0 {
			// Nothing running and nothing ready: should only happen on
			// cancellation. Mark all unresolved steps as canceled.
			for _, name := range s.graph.order {
				if results[name] == nil {
					results[name] = &StepResult{Name: name, Status: StatusCanceled, Err: ctx.Err()}
					remaining--
				}
			}
			break
		}

		select {
		case <-ctx.Done():
			// Global cancellation/timeout: running steps stop on their
			// own (their ctx is canceled); drain their completions,
			// cascade-cancel the rest as skipped/canceled.
			for inFlight > 0 {
				c := <-doneCh
				inFlight--
				remaining--
				if results[c.name] == nil {
					results[c.name] = &StepResult{
						Name: c.name, Status: c.out.status, Attempts: c.out.attempts,
						Err: c.out.err, Duration: c.out.duration,
					}
					keys[c.name] = c.out.key
				}
			}
			for _, name := range s.graph.order {
				if results[name] == nil {
					results[name] = &StepResult{Name: name, Status: StatusCanceled, Err: context.Canceled}
					remaining--
				}
			}
		case c := <-doneCh:
			inFlight--
			if results[c.name] == nil {
				results[c.name] = &StepResult{
					Name:      c.name,
					Status:    c.out.status,
					Attempts:  c.out.attempts,
					Err:       c.out.err,
					FromCache: c.out.status == StatusCached,
					Duration:  c.out.duration,
				}
				keys[c.name] = c.out.key
			}
			remaining--
			if c.out.status == StatusSuccess || c.out.status == StatusCached {
				// Unblock successors whose deps are all done well.
				for _, sn := range succ[c.name] {
					indeg[sn]--
					if indeg[sn] == 0 && results[sn] == nil {
						readyNames = insertSorted(readyNames, sn)
					}
				}
			} else {
				// Failed or canceled: successors must not run. Branches
				// not connected to it remain ready/executing.
				cascade(c.name, c.out.status)
			}
		}
	}

	close(jobs)
	wg.Wait()
	return &Report{Results: results}
}
func contains(xs []string, x string) (int, bool) {
	for i, v := range xs {
		if v == x {
			return i, true
		}
	}
	return -1, false
}

func remove(xs []string, x string) []string {
	i, ok := contains(xs, x)
	if !ok {
		return xs
	}
	return append(xs[:i], xs[i+1:]...)
}

func insertSorted(xs []string, x string) []string {
	i := 0
	for i < len(xs) && xs[i] < x {
		i++
	}
	xs = append(xs, "")
	copy(xs[i+1:], xs[i:])
	xs[i] = x
	return xs
}
