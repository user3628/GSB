//go:build windows

package store

import (
	"os"

	"golang.org/x/sys/windows"
)

type fileLock struct {
	file *os.File
	over *windows.Overlapped
}

func acquireFileLock(path string) (*fileLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	over := new(windows.Overlapped)
	if err := windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, over); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &fileLock{file: file, over: over}, nil
}

func (l *fileLock) Release() {
	_ = windows.UnlockFileEx(windows.Handle(l.file.Fd()), 0, 1, 0, l.over)
	_ = l.file.Close()
}
