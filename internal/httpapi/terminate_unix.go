//go:build !windows

package httpapi

import (
	"os"
	"syscall"
)

func terminateProcess() {
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		os.Exit(137)
	}
}
