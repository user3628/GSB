//go:build windows

package store

import "golang.org/x/sys/windows"

func platformLock(fd uintptr) error {
	var ol windows.Overlapped
	return windows.LockFileEx(windows.Handle(fd), windows.LOCKFILE_EXCLUSIVE_LOCK, 0,
		1<<32-1, 1<<32-1, &ol)
}

func platformUnlock(fd uintptr) error {
	var ol windows.Overlapped
	return windows.UnlockFileEx(windows.Handle(fd), 0, 1<<32-1, 1<<32-1, &ol)
}
