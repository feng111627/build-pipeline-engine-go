package pipeline

import "sort"

// Graph is the validated dependency DAG.
type Graph struct {
	order      []string
	deps       map[string][]string // direct dependencies (declared order)
	successors map[string][]string
}

// BuildGraph validates the config (empty names, duplicates, unknown
// deps, cycles) and returns a ready-to-schedule DAG. Errors are
// *ConfigError with a distinct Kind for every failure reason.
func BuildGraph(cfg *Config) (*Graph, error) {
	g := &Graph{
		deps:       make(map[string][]string),
		successors: make(map[string][]string),
	}

	seen := make(map[string]bool)
	for _, s := range cfg.Steps {
		if s.Name == "" {
			return nil, &ConfigError{Kind: ErrEmptyName}
		}
		if seen[s.Name] {
			return nil, &ConfigError{Kind: ErrDuplicateStep, Steps: []string{s.Name}}
		}
		seen[s.Name] = true
		g.deps[s.Name] = append([]string(nil), s.Depends...)
	}

	// Dangling dependencies must be reported (with both endpoints)
	// rather than silently ignored.
	for name, deps := range g.deps {
		for _, d := range deps {
			if !seen[d] {
				return nil, &ConfigError{
					Kind:  ErrUnknownDep,
					Steps: []string{name, d},
					Msg:   "step '" + name + "' depends on undefined step '" + d + "'",
				}
			}
			g.successors[d] = append(g.successors[d], name)
		}
	}
	for k := range g.successors {
		succ := g.successors[k]
		sort.Strings(succ)
		g.successors[k] = succ
	}

	if cyc := findCycle(g.deps); cyc != nil {
		return nil, &ConfigError{Kind: ErrCycle, Steps: cyc}
	}

	g.order = topoOrder(g.deps, g.successors)
	return g, nil
}

// findCycle returns a cycle path (e.g. ["a","b","a"]) or nil.
func findCycle(deps map[string][]string) []string {
	const white, gray, black = 0, 1, 2
	color := make(map[string]int)
	names := make([]string, 0, len(deps))
	for n := range deps {
		names = append(names, n)
	}
	sort.Strings(names)

	var stack []string
	var dfs func(n string) []string
	dfs = func(n string) []string {
		color[n] = gray
		stack = append(stack, n)
		ds := append([]string(nil), deps[n]...)
		sort.Strings(ds)
		for _, d := range ds {
			switch color[d] {
			case white:
				if cyc := dfs(d); cyc != nil {
					return cyc
				}
			case gray:
				// Extract the cycle from the recursion stack.
				start := 0
				for i, x := range stack {
					if x == d {
						start = i
						break
					}
				}
				cyc := append([]string(nil), stack[start:]...)
				return append(cyc, d)
			}
		}
		stack = stack[:len(stack)-1]
		color[n] = black
		return nil
	}
	for _, n := range names {
		if color[n] == white {
			if cyc := dfs(n); cyc != nil {
				return cyc
			}
		}
	}
	return nil
}

// topoOrder returns a deterministic topological ordering (Kahn's
// algorithm always picking the lexicographically smallest ready node).
func topoOrder(deps, successors map[string][]string) []string {
	indeg := make(map[string]int)
	ready := &strHeap{}
	for n, ds := range deps {
		indeg[n] = len(ds)
		if len(ds) == 0 {
			ready.push(n)
		}
	}
	var order []string
	for ready.len() > 0 {
		n := ready.pop()
		order = append(order, n)
		for _, s := range successors[n] {
			indeg[s]--
			if indeg[s] == 0 {
				ready.push(s)
			}
		}
	}
	return order
}

// Order returns step names in a deterministic topological order.
func (g *Graph) Order() []string { return append([]string(nil), g.order...) }

// Deps returns the direct dependencies of a step.
func (g *Graph) Deps(name string) []string {
	return append([]string(nil), g.deps[name]...)
}

// strHeap is a tiny sorted-array min-heap of strings.
type strHeap struct{ xs []string }

func (h *strHeap) push(x string) {
	h.xs = append(h.xs, x)
	sort.Strings(h.xs)
}
func (h *strHeap) pop() string {
	x := h.xs[0]
	h.xs = h.xs[1:]
	return x
}
func (h *strHeap) len() int { return len(h.xs) }
