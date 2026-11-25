//go:build windows

package pipeline

import (
	"os"

	"golang.org/x/sys/windows"
)

// FileLock is an exclusive whole-file lock implemented with LockFileEx.
type FileLock struct {
	f  *os.File
	ol windows.Overlapped
}

func AcquireFileLock(path string) (*FileLock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, bizError(CodeInternal, "open lock file: %v", err)
	}
	l := &FileLock{f: f}
	// Lock the maximum possible region (0xFFFFFFFFFFFFFFFF bytes), failing
	// immediately if another process already holds a lock.
	err = windows.LockFileEx(windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, ^uint32(0), ^uint32(0), &l.ol)
	if err != nil {
		f.Close()
		return nil, bizError(CodeLocked, "workspace lock already held by another process")
	}
	return l, nil
}

func (l *FileLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = windows.UnlockFileEx(windows.Handle(l.f.Fd()), 0, ^uint32(0), ^uint32(0), &l.ol)
	return l.f.Close()
}
