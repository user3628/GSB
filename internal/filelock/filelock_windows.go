//go:build windows

package filelock

import (
	"os"

	"golang.org/x/sys/windows"
)

func lockFile(f *os.File) error {
	handle := windows.Handle(f.Fd())
	var ol windows.Overlapped
	flags := uint32(windows.LOCKFILE_EXCLUSIVE_LOCK | windows.LOCKFILE_FAIL_IMMEDIATELY)
	return windows.LockFileEx(handle, flags, 0, ^uint32(0), ^uint32(0), &ol)
}

func unlockFile(f *os.File) error {
	handle := windows.Handle(f.Fd())
	var ol windows.Overlapped
	return windows.UnlockFileEx(handle, 0, ^uint32(0), ^uint32(0), &ol)
}
