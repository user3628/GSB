//go:build windows

package filelock

import (
	"os"

	"golang.org/x/sys/windows"
)

// FileLock is a mandatory byte-range lock backed by LockFileEx.
type FileLock struct{ f *os.File }

// Acquire opens the lock file and takes an exclusive whole-file lock.
func Acquire(path string) (*FileLock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	var ol windows.Overlapped
	err = windows.LockFileEx(windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK, 0, ^uint32(0), ^uint32(0), &ol)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &FileLock{f: f}, nil
}

// Release releases the byte-range lock and closes the file.
func (l *FileLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	var ol windows.Overlapped
	_ = windows.UnlockFileEx(windows.Handle(l.f.Fd()), 0, ^uint32(0), ^uint32(0), &ol)
	return l.f.Close()
}
