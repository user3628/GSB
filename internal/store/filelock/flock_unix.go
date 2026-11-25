//go:build !windows

package filelock

import (
	"os"

	"golang.org/x/sys/unix"
)

// FileLock is an advisory whole-file lock backed by flock(2).
type FileLock struct{ f *os.File }

// Acquire opens the lock file (creating it) and takes an exclusive lock.
func Acquire(path string) (*FileLock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &FileLock{f: f}, nil
}

// Release unlocks and closes the file.
func (l *FileLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = unix.Flock(int(l.f.Fd()), unix.LOCK_UN)
	return l.f.Close()
}
