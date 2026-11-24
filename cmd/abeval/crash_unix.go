//go:build unix

package main

import (
	"os"
	"syscall"
)

func killSelfForFaultInjection() {
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
}
