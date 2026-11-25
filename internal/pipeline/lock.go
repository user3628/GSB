//go:build !windows

package pipeline

import (
	"os"

	"golang.org/x/sys/unix"
)

// FileLock is an advisory, whole-file lock implemented with a real OS lock.
// On Unix it is flock(2); the Windows implementation uses LockFileEx.
type FileLock struct {
	f *os.File
}

// AcquireFileLock opens (creating if needed) the lock file and takes an
// exclusive non-blocking lock. It returns E_LOCKED if another process holds it.
func AcquireFileLock(path string) (*FileLock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, bizError(CodeInternal, "open lock file: %v", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, bizError(CodeLocked, "workspace lock already held by another process")
	}
	return &FileLock{f: f}, nil
}

func (l *FileLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = unix.Flock(int(l.f.Fd()), unix.LOCK_UN)
	return l.f.Close()
}
