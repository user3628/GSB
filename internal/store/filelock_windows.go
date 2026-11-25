//go:build windows

package store

import (
	"context"
	"errors"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

func tryLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	overlapped := new(windows.Overlapped)
	err = windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, overlapped)
	if err != nil {
		_ = file.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, errLockBusy
		}
		return nil, err
	}
	return file, nil
}

func unlock(file *os.File) error {
	overlapped := new(windows.Overlapped)
	err := windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, overlapped)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func acquire(ctx context.Context, path string) (*FileLock, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		file, err := tryLock(path)
		if err == nil {
			return &FileLock{file: file, path: path}, nil
		}
		if !errors.Is(err, errLockBusy) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}
