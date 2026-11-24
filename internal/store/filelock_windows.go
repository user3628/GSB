//go:build windows

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/windows"
)

type FileLock struct {
	f  *os.File
	ol windows.Overlapped
}

func AcquireFileLock(path string, timeout time.Duration) (*FileLock, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	lock := &FileLock{f: f}
	deadline := time.Now().Add(timeout)
	for {
		err = windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &lock.ol)
		if err == nil {
			return lock, nil
		}
		if time.Now().After(deadline) {
			_ = f.Close()
			return nil, fmt.Errorf("timed out acquiring file lock %s after %s: %w", path, timeout, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (l *FileLock) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = windows.UnlockFileEx(windows.Handle(l.f.Fd()), 0, 1, 0, &l.ol)
	return l.f.Close()
}
