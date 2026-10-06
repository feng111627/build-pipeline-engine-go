package pipeline

import (
	"sort"
	"time"
)

// Duration is a wrapper around time.Duration that can be unmarshaled
// from a JSON string like "5s".
type Duration struct {
	time.Duration
}

// MarshalJSON renders the duration as e.g. "5s".
func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(`"` + d.Duration.String() + `"`), nil
}

// UnmarshalJSON accepts a duration string ("500ms", "2m30s").
func (d *Duration) UnmarshalJSON(b []byte) error {
	if len(b) < 2 || b[0] != '"' || b[len(b)-1] != '"' {
		return &ConfigError{Kind: ErrEmptyName, Msg: "duration must be a string, e.g. \"5s\""}
	}
	v, err := time.ParseDuration(string(b[1 : len(b)-1]))
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

// InputSpec describes a single input of a step.
// Exactly one of Value or File should be set.
type InputSpec struct {
	Name  string `json:"name"`
	Value string `json:"value,omitempty"`
	File  string `json:"file,omitempty"`
}

// StepSpec is one build step as declared in the config file.
type StepSpec struct {
	Name    string      `json:"name"`
	Command string      `json:"command"`
	Inputs  []InputSpec `json:"inputs,omitempty"`
	Outputs []string    `json:"outputs,omitempty"`
	Depends []string    `json:"depends,omitempty"`
	Retries int         `json:"retries,omitempty"`
	Timeout *Duration   `json:"timeout,omitempty"`
}

// Config is the whole pipeline configuration.
type Config struct {
	Concurrency int        `json:"concurrency"`
	Timeout     *Duration  `json:"timeout,omitempty"`
	Steps       []StepSpec `json:"steps"`
}

// StepResult is the outcome of one step after a pipeline run.
type StepResult struct {
	Name      string
	Status    StepStatus
	Attempts  int
	Err       error
	FromCache bool
	Duration  time.Duration
}

// StepStatus is the terminal state of a step.
type StepStatus int

const (
	StatusPending  StepStatus = iota
	StatusSuccess             // ran and succeeded
	StatusCached              // skipped because its cache key hit
	StatusFailed              // ran and exhausted its retries
	StatusSkipped             // never ran because an upstream failed/canceled
	StatusCanceled            // was running (or ready) when the pipeline was canceled/timed out
)

// String returns a stable human-readable name for the status.
func (s StepStatus) String() string {
	switch s {
	case StatusSuccess:
		return "success"
	case StatusCached:
		return "cached"
	case StatusFailed:
		return "failed"
	case StatusSkipped:
		return "skipped"
	case StatusCanceled:
		return "canceled"
	default:
		return "pending"
	}
}

// Report is the full result of a pipeline run.
type Report struct {
	Results map[string]*StepResult
}

// Failed returns steps that ran and failed on their own, in
// topological order when available.
func (r *Report) Failed() []*StepResult { return r.byStatus(StatusFailed) }

// Skipped returns steps that never ran because an upstream failed.
func (r *Report) Skipped() []*StepResult { return r.byStatus(StatusSkipped) }

// Canceled returns steps that were interrupted by cancellation/timeout.
func (r *Report) Canceled() []*StepResult { return r.byStatus(StatusCanceled) }

// OK reports whether every step ended in success or cache hit.
func (r *Report) OK() bool {
	for _, res := range r.Results {
		if res.Status != StatusSuccess && res.Status != StatusCached {
			return false
		}
	}
	return true
}

func (r *Report) byStatus(s StepStatus) []*StepResult {
	var out []*StepResult
	for _, res := range r.Results {
		if res.Status == s {
			out = append(out, res)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
