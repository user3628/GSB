//go:build !windows

package store

import (
	"os"
	"syscall"
)

// FileLock is an advisory whole-file lock backed by flock(2) on unix.
type FileLock struct{ f *os.File }

// AcquireFileLock opens (creating if needed) and exclusively locks lockPath.
func AcquireFileLock(lockPath string) (*FileLock, error) {
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return &FileLock{f: f}, nil
}

// Release unlocks and closes the lock file.
func (l *FileLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	return l.f.Close()
}
