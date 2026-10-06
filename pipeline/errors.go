package pipeline

import "strings"

// ConfigErrorKind lets callers distinguish configuration failure
// reasons programmatically (duplicate definition, dangling dependency,
// cycle, ...).
type ConfigErrorKind string

const (
	ErrDuplicateStep ConfigErrorKind = "duplicate_step"
	ErrUnknownDep    ConfigErrorKind = "unknown_dependency"
	ErrCycle         ConfigErrorKind = "cyclic_dependency"
	ErrEmptyName     ConfigErrorKind = "empty_step_name"
)

// ConfigError describes a configuration problem.
type ConfigError struct {
	Kind ConfigErrorKind
	// Steps involved, e.g. the duplicated name or the cycle path.
	Steps []string
	Msg   string
}

func (e *ConfigError) Error() string {
	var b strings.Builder
	switch e.Kind {
	case ErrDuplicateStep:
		b.WriteString("duplicate step definition")
	case ErrUnknownDep:
		b.WriteString("dependency points to a step that does not exist")
	case ErrCycle:
		b.WriteString("cyclic dependency detected")
	case ErrEmptyName:
		b.WriteString("step name must not be empty")
	default:
		b.WriteString("invalid configuration")
	}
	if len(e.Steps) > 0 {
		b.WriteString(": " + joinNames(e.Steps))
	}
	if e.Msg != "" {
		b.WriteString(" (" + e.Msg + ")")
	}
	return b.String()
}

// StepError records why a step failed on its own (as opposed to being
// skipped because an upstream failed).
type StepError struct {
	Step     string
	Attempts int
	Cause    error
}

func (e *StepError) Error() string {
	if e.Cause != nil {
		return "step " + e.Step + " failed after " + itoa(e.Attempts) + " attempt(s): " + e.Cause.Error()
	}
	return "step " + e.Step + " failed after " + itoa(e.Attempts) + " attempt(s)"
}

func (e *StepError) Unwrap() error { return e.Cause }

// SkipError marks a step that did not run because one of its transitive
// dependencies failed or was canceled — distinct from failing itself.
type SkipError struct {
	Step     string
	Upstream string
	Reason   StepStatus
}

func (e *SkipError) Error() string {
	return "step " + e.Step + " skipped: upstream " + e.Upstream + " " + e.Reason.String()
}

func joinNames(names []string) string { return strings.Join(names, " -> ") }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
