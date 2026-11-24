package cockpitaudio

import (
	"os"
	"path/filepath"
)

type FileLock struct {
	path string
	file *os.File
}

func NewFileLock(path string) *FileLock {
	return &FileLock{path: path}
}

func (l *FileLock) Lock() error {
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if err := lockPlatform(f); err != nil {
		_ = f.Close()
		return err
	}
	l.file = f
	return nil
}

func (l *FileLock) Unlock() error {
	if l.file == nil {
		return nil
	}
	err := unlockPlatform(l.file)
	closeErr := l.file.Close()
	l.file = nil
	if err != nil {
		return err
	}
	return closeErr
}
