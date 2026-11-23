//go:build !windows

package main

import (
	"os"
	"syscall"
)

func terminationSignals() []os.Signal {
	return []os.Signal{syscall.SIGINT, syscall.SIGTERM}
}
