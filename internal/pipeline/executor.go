package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// Status 表示一个步骤的最终状态。
type Status int

const (
	StatusPending  Status = iota // 尚未执行
	StatusCached                 // 缓存命中，跳过执行
	StatusSuccess                // 执行成功
	StatusFailed                 // 自身执行失败（重试已耗尽）
	StatusSkipped                // 因上游失败/取消而未能执行
	StatusCanceled               // 因外部取消/超时而中断
)

func (s Status) String() string {
	switch s {
	case StatusPending:
		return "PENDING"
	case StatusCached:
		return "CACHED"
	case StatusSuccess:
		return "SUCCESS"
	case StatusFailed:
		return "FAILED"
	case StatusSkipped:
		return "SKIPPED_UPSTREAM"
	case StatusCanceled:
		return "CANCELED"
	default:
		return "UNKNOWN"
	}
}

// Result 记录单个步骤的执行结果。
type Result struct {
	Step     Step
	Status   Status
	Attempts int // 实际执行次数（含重试）；缓存命中或未执行为 0
	Err      error
}

// Executor 按依赖图调度并执行流水线。
type Executor struct {
	Concurrency int // 并发上限，<=0 表示不限制
	WorkDir     string
	Cache       *CacheStore // 为 nil 时禁用缓存
}

// Run 执行整个流水线，返回每个步骤的结果（按配置顺序）。
// 调用前 steps 必须通过 Validate。ctx 取消/超时会停止调度新步骤，
// 并杀掉正在运行的命令，未开始的步骤标记为 StatusCanceled。
func (e *Executor) Run(ctx context.Context, steps []Step) []Result {
	n := len(steps)
	results := make([]Result, n)
	byName := make(map[string]int, n)
	depsRemaining := make([]int, n)
	dependents := make(map[string][]int, n)
	for i, s := range steps {
		byName[s.Name] = i
		depsRemaining[i] = len(s.Deps)
		results[i].Step = s
		results[i].Status = StatusPending
	}
	for i, s := range steps {
		for _, d := range s.Deps {
			dependents[d] = append(dependents[d], i)
		}
	}

	conc := e.Concurrency
	if conc <= 0 {
		conc = n
	}
	sem := make(chan struct{}, conc)
	doneCh := make(chan Result, n)

	started := make([]bool, n)
	completed := 0
	running := 0
	cancelled := false

	propagate := func(i int) {
		for _, j := range dependents[steps[i].Name] {
			depsRemaining[j]--
		}
	}

	finish := func(i int, r Result) {
		results[i] = r
		started[i] = true
		completed++
		propagate(i)
	}

	for completed < n {
		// 1. 传播：依赖已全部resolve的步骤，若任一上游失败/跳过/取消，
		//    则标记为 Skipped（被上游拖累），不再执行。
		//    若外部已取消，所有未开始步骤标记为 Canceled。
		for i := range steps {
			if started[i] || depsRemaining[i] != 0 {
				continue
			}
			if cancelled {
				finish(i, Result{Step: steps[i], Status: StatusCanceled, Err: ctx.Err()})
				continue
			}
			blocked := false
			for _, d := range steps[i].Deps {
				dr := results[byName[d]]
				if dr.Status == StatusFailed || dr.Status == StatusSkipped || dr.Status == StatusCanceled {
					blocked = true
					break
				}
			}
			if blocked {
				finish(i, Result{Step: steps[i], Status: StatusSkipped,
					Err: errors.New("上游步骤失败，未执行")})
			}
		}
		if completed >= n {
			break
		}
		// 2. 启动就绪步骤（受并发上限约束）。
		launched := false
		if !cancelled {
			for i := range steps {
				if started[i] || depsRemaining[i] != 0 {
					continue
				}
				select {
				case sem <- struct{}{}:
					started[i] = true
					running++
					launched = true
					go func(idx int) {
						r := e.runStep(ctx, steps[idx])
						doneCh <- r
					}(i)
				default:
				}
			}
		}
		if completed >= n {
			break
		}
		// 3. 等待一个步骤完成或外部取消。
		if running == 0 && !launched {
			// 无可运行步骤且没有就绪步骤：只可能发生在取消场景，
			// 防御性处理，避免死锁。
			for i := range steps {
				if !started[i] {
					finish(i, Result{Step: steps[i], Status: StatusCanceled,
						Err: errors.New("调度器无法继续：存在未就绪步骤")})
				}
			}
			break
		}
		select {
		case r := <-doneCh:
			<-sem
			running--
			idx := byName[r.Step.Name]
			results[idx] = r
			completed++
			propagate(idx)
		case <-ctx.Done():
			cancelled = true
		}
	}
	return results
}

// runStep 执行单个步骤：先查缓存，未命中则按重试配置执行命令。
func (e *Executor) runStep(ctx context.Context, s Step) Result {
	r := Result{Step: s}
	if e.Cache != nil {
		key, err := CacheKey(e.WorkDir, s)
		if err != nil {
			r.Status = StatusFailed
			r.Err = err
			return r
		}
		if e.Cache.Hit(s.Name, key) {
			r.Status = StatusCached
			return r
		}
	}
	var lastErr error
	for attempt := 0; attempt <= s.Retries; attempt++ {
		if ctx.Err() != nil {
			r.Status = StatusCanceled
			r.Err = ctx.Err()
			return r
		}
		r.Attempts++
		lastErr = runCommand(ctx, e.WorkDir, s.Command)
		if lastErr == nil {
			r.Status = StatusSuccess
			if e.Cache != nil {
				// 只有完整成功才写缓存，失败/取消不会留下"半成品"缓存记录。
				if key, err := CacheKey(e.WorkDir, s); err == nil {
					_ = e.Cache.Save(s.Name, key)
				}
			}
			return r
		}
		if ctx.Err() != nil {
			r.Status = StatusCanceled
			r.Err = ctx.Err()
			return r
		}
	}
	r.Status = StatusFailed
	r.Err = lastErr
	return r
}

// runCommand 在工作目录下执行命令；ctx 取消时整个进程组会被杀掉，
// 确保子进程（如 sh 派生的 sleep）也被终止，不留下仍在写的半成品进程。
func runCommand(ctx context.Context, workDir string, argv []string) error {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = workDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		// 负 PID 表示向整个进程组发信号。
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 3 * time.Second // 防御：子进程占用管道时 Wait 不无限阻塞
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg != "" {
			return fmt.Errorf("命令 %q 失败: %w\n输出: %s", strings.Join(argv, " "), err, msg)
		}
		return fmt.Errorf("命令 %q 失败: %w", strings.Join(argv, " "), err)
	}
	return nil
}
