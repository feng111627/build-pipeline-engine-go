//go:build !windows

package pipeline

import (
	"os/exec"
	"syscall"
)

// killProcessGroup kills the shell and every process it spawned.
func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err == nil {
		return syscall.Kill(-pgid, syscall.SIGKILL)
	}
	return cmd.Process.Kill()
}
