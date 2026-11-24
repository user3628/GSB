//go:build unix

package verify

import (
	"os/exec"
	"syscall"
)

func killedBySigkill(err *exec.ExitError) bool {
	status, ok := err.ProcessState.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}
