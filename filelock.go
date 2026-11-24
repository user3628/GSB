package rewardpack

import (
	"context"
	"errors"
	"os"
	"time"
)

// fileLock is a real OS advisory/exclusive lock held on a dedicated lock file.
// The SQLite conditional updates provide the durable sequence guard; this lock
// serializes competing builders before they reach publication.
type fileLock struct {
	file *os.File
}

var errLockBusy = errors.New("lock busy")

func acquireFileLock(ctx context.Context, path string) (*fileLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	for {
		locked, lockErr := tryLockFile(file)
		if lockErr != nil && !errors.Is(lockErr, errLockBusy) {
			_ = file.Close()
			return nil, lockErr
		}
		if locked {
			return &fileLock{file: file}, nil
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (l *fileLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	unlockErr := unlockFile(l.file)
	closeErr := l.file.Close()
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}

// WithProcessLock serializes a larger command or input-mutation sequence with
// other processes using the same database. Build itself already takes its own
// publication lock; this namespace lock lets CLI setup (config/shard sync) and
// verification run without racing another command's setup phase.
func (s *Store) WithProcessLock(ctx context.Context, namespace string, fn func() error) error {
	if s == nil {
		return &BusinessError{Code: CodeInternal, Message: "store is nil"}
	}
	path := s.lockPath
	if namespace != "" {
		path += "." + namespace
	}
	lock, err := acquireFileLock(ctx, path)
	if err != nil {
		if ctx.Err() != nil {
			return stoppedError("", "", "stop requested while waiting for process lock")
		}
		return &BusinessError{Code: CodeLockTimeout, Context: path, Message: err.Error()}
	}
	defer lock.Close()
	return fn()
}
