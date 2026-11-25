//go:build windows

package filelock

import (
	"os"

	"golang.org/x/sys/windows"
)

type Lock struct {
	f  *os.File
	ol *windows.Overlapped
}

func Acquire(path string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	ol := new(windows.Overlapped)
	if err := windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, ol); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Lock{f: f, ol: ol}, nil
}

func (l *Lock) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	unlockErr := windows.UnlockFileEx(windows.Handle(l.f.Fd()), 0, 1, 0, l.ol)
	closeErr := l.f.Close()
	l.f = nil
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}
