//go:build windows

package store

import "os"

func crashKill() {
	p, _ := os.FindProcess(os.Getpid())
	if p != nil {
		_ = p.Kill()
	}
}
