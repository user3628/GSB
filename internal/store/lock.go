package store

import (
	"context"
	"errors"
	"os"
)

var errLockBusy = errors.New("sqlite publish file lock is busy")

type FileLock struct {
	file *os.File
	path string
}

func (l *FileLock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := unlock(l.file)
	l.file = nil
	return err
}

func AcquireFileLock(ctx context.Context, path string) (*FileLock, error) {
	return acquire(ctx, path)
}
