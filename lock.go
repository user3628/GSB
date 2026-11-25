package speakerdiar

import (
	"os"
	"path/filepath"
)

// FileLock is a cross-process exclusive lock backed by a real lock file.
type FileLock struct {
	path string
	file *os.File
}

// AcquireFileLock creates and locks path. The returned release must be called.
func AcquireFileLock(path string) (*FileLock, func() error, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, err
	}
	if err := platformLock(f); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	lock := &FileLock{path: path, file: f}
	release := func() error {
		if lock.file == nil {
			return nil
		}
		_ = platformUnlock(lock.file)
		err := lock.file.Close()
		lock.file = nil
		return err
	}
	return lock, release, nil
}
