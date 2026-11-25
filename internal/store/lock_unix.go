//go:build !windows

package store

import "syscall"

func platformLock(fd uintptr) error   { return syscall.Flock(int(fd), syscall.LOCK_EX) }
func platformUnlock(fd uintptr) error { return syscall.Flock(int(fd), syscall.LOCK_UN) }
