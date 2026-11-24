package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"motionlab/internal/model"
)

func (s *Store) withFileLock(ctx context.Context, scope string, fn func() error) error {
	name := model.DigestBytes([]byte(scope))[:24] + ".lock"
	path := filepath.Join(s.lockDir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open checkpoint file lock: %w", err)
	}
	defer f.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ok, err := tryLockFile(f)
		if err != nil {
			return fmt.Errorf("acquire checkpoint file lock: %w", err)
		}
		if ok {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if time.Now().After(deadline) {
			return model.NewError(model.CodeStaleCheckpoint, "timed out waiting for checkpoint file lock")
		}
		time.Sleep(4 * time.Millisecond)
	}
	defer unlockFile(f)
	if err := f.Truncate(0); err == nil {
		_, _ = f.Seek(0, 0)
		_, _ = fmt.Fprintf(f, "scope=%s pid=%d acquired=%s\n", scope, os.Getpid(), time.Now().UTC().Format(time.RFC3339Nano))
	}
	return fn()
}
