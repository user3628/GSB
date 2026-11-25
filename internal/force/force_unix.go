//go:build !windows

package force

import (
	"os"
	"syscall"
)

// Terminate mimics an operator-forced stop. It does not run deferred cleanup.
func Terminate() {
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	os.Exit(137)
}
