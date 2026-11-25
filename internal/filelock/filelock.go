package filelock

import (
	"context"
	"os"
	"time"
)

type Lock struct {
	path string
	f    *os.File
}

func Acquire(ctx context.Context, path string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	deadline, ok := ctx.Deadline()
	for {
		err := lockFile(f)
		if err == nil {
			return &Lock{path: path, f: f}, nil
		}
		if ok && time.Now().After(deadline) {
			_ = f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (l *Lock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = unlockFile(l.f)
	err := l.f.Close()
	l.f = nil
	return err
}
