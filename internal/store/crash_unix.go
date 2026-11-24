//go:build unix

package store

import (
	"os"
	"syscall"
)

func crashKill() {
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
}
