//go:build windows

package store

import (
	"os"
	"syscall"
)

// FileLock is a mandatory whole-file lock on Windows (LockFileEx exclusive).
type FileLock struct {
	f          *os.File
	overlapped syscall.Overlapped
}

var (
	kernel32     = syscall.NewLazyDLL("kernel32.dll")
	procLockFile = kernel32.NewProc("LockFileEx")
	procUnlock   = kernel32.NewProc("UnlockFileEx")
)

const lockfileExclusiveLock = 0x00000002

// AcquireFileLock opens and exclusively locks lockPath using LockFileEx.
func AcquireFileLock(lockPath string) (*FileLock, error) {
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	l := &FileLock{f: f}
	r1, _, e := procLockFile.Call(
		f.Fd(), uintptr(lockfileExclusiveLock), 0,
		^uintptr(0), ^uintptr(0), uintptr(0),
	)
	if r1 == 0 {
		f.Close()
		return nil, e
	}
	return l, nil
}

// Release unlocks and closes the lock file.
func (l *FileLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	procUnlock.Call(l.f.Fd(), 0, ^uintptr(0), ^uintptr(0), uintptr(0))
	return l.f.Close()
}
