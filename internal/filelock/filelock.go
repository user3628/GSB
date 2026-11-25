// Package filelock 在 SQLite 事务之外提供真实的 OS 文件锁。
package filelock

import (
	"context"
	"os"
	"time"
)

type FileLock struct {
	path string
	f    *os.File
}

func Acquire(ctx context.Context, path string) (*FileLock, error) {
	for {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return nil, err
		}
		locked, err := tryLock(f)
		if err != nil {
			f.Close()
			return nil, err
		}
		if locked {
			return &FileLock{path: path, f: f}, nil
		}
		f.Close()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (l *FileLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	unlockErr := unlock(l.f)
	closeErr := l.f.Close()
	l.f = nil
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}

func (l *FileLock) Path() string { return l.path }
