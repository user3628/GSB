//go:build unix

package checkpoint

import (
	"os"

	"golang.org/x/sys/unix"
)

type FileLock struct {
	file *os.File
	path string
}

func AcquireFileLock(path string) (*FileLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &FileLock{file: file, path: path}, nil
}

func (l *FileLock) Unlock() error {
	if l == nil || l.file == nil {
		return nil
	}
	if err := unix.Flock(int(l.file.Fd()), unix.LOCK_UN); err != nil {
		_ = l.file.Close()
		l.file = nil
		return err
	}
	err := l.file.Close()
	l.file = nil
	return err
}
