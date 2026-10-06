package pipeline

import (
	"fmt"
	"sort"
	"strings"
)

// GraphErrorKind 区分依赖图校验失败的原因。
type GraphErrorKind int

const (
	ErrDuplicateStep GraphErrorKind = iota // 步骤重复定义
	ErrMissingDep                          // 依赖指向不存在的步骤
	ErrCycle                               // 存在环形依赖
)

// GraphError 是依赖图校验错误，Kind 可区分具体原因。
type GraphError struct {
	Kind  GraphErrorKind
	Steps []string // 涉及的步骤名
}

func (e *GraphError) Error() string {
	names := strings.Join(e.Steps, ", ")
	switch e.Kind {
	case ErrDuplicateStep:
		return fmt.Sprintf("步骤重复定义: %s", names)
	case ErrMissingDep:
		return fmt.Sprintf("依赖指向不存在的步骤: %s", names)
	case ErrCycle:
		return fmt.Sprintf("检测到环形依赖: %s", strings.Join(e.Steps, " -> "))
	default:
		return fmt.Sprintf("依赖图错误: %s", names)
	}
}

// Validate 校验步骤列表，返回第一个发现的图错误；无错误返回 nil。
// 校验顺序：重复定义 -> 缺失依赖 -> 环形依赖。
func Validate(steps []Step) *GraphError {
	seen := make(map[string]bool, len(steps))
	var dups []string
	for _, s := range steps {
		if seen[s.Name] {
			dups = append(dups, s.Name)
		}
		seen[s.Name] = true
	}
	if len(dups) > 0 {
		sort.Strings(dups)
		return &GraphError{Kind: ErrDuplicateStep, Steps: dups}
	}

	var missing []string
	for _, s := range steps {
		for _, d := range s.Deps {
			if !seen[d] {
				missing = append(missing, fmt.Sprintf("%s -> %s", s.Name, d))
			}
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return &GraphError{Kind: ErrMissingDep, Steps: missing}
	}

	if cycle := findCycle(steps); cycle != nil {
		return &GraphError{Kind: ErrCycle, Steps: cycle}
	}
	return nil
}

// findCycle 用三色 DFS 检测环，返回环上的步骤名（首尾相接），无环返回 nil。
func findCycle(steps []Step) []string {
	const (
		white = 0 // 未访问
		gray  = 1 // 在递归栈上
		black = 2 // 已完成
	)
	color := make(map[string]int, len(steps))
	deps := make(map[string][]string, len(steps))
	names := make([]string, 0, len(steps))
	for _, s := range steps {
		deps[s.Name] = s.Deps
		names = append(names, s.Name)
	}
	var stack []string
	var visit func(name string) []string
	visit = func(name string) []string {
		color[name] = gray
		stack = append(stack, name)
		for _, d := range deps[name] {
			switch color[d] {
			case gray:
				// 找到环：截取栈中从 d 开始的部分并闭合
				for i, n := range stack {
					if n == d {
						return append(append([]string{}, stack[i:]...), d)
					}
				}
			case white:
				if c := visit(d); c != nil {
					return c
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[name] = black
		return nil
	}
	for _, n := range names {
		if color[n] == white {
			if c := visit(n); c != nil {
				return c
			}
		}
	}
	return nil
}

// Layers 返回拓扑分层结果：同层步骤互不依赖可并发执行，
// 每层内的步骤名按字典序排序以保证结果稳定（与配置书写顺序无关）。
// 调用前必须先通过 Validate。
func Layers(steps []Step) [][]string {
	indeg := make(map[string]int, len(steps))
	dependents := make(map[string][]string, len(steps))
	for _, s := range steps {
		indeg[s.Name] = len(s.Deps)
		for _, d := range s.Deps {
			dependents[d] = append(dependents[d], s.Name)
		}
	}
	var ready []string
	for _, s := range steps {
		if indeg[s.Name] == 0 {
			ready = append(ready, s.Name)
		}
	}
	sort.Strings(ready)
	var layers [][]string
	for len(ready) > 0 {
		layers = append(layers, ready)
		var next []string
		for _, name := range ready {
			for _, dep := range dependents[name] {
				indeg[dep]--
				if indeg[dep] == 0 {
					next = append(next, dep)
				}
			}
		}
		sort.Strings(next)
		ready = next
	}
	return layers
}
