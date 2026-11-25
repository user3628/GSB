//go:build windows

package determ

import (
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

type FileLock struct {
	path string
	f    *os.File
}

func NewFileLock(dir, name string) (*FileLock, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileLock{path: path, f: f}, nil
}

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
)

const (
	lockfileExclusiveLock   = 0x00000002
	lockfileFailImmediately = 0x00000001
	// errorLockViolation is the Win32 ERROR_LOCK_VIOLATION (33) returned when a
	// non-blocking lock cannot be granted immediately.
	errorLockViolation syscall.Errno = 33
)

func (l *FileLock) lock(flags uint32) error {
	var ol syscall.Overlapped
	r1, _, e := procLockFileEx.Call(
		l.f.Fd(), uintptr(flags), 0, ^uintptr(0), 0, uintptr(unsafe.Pointer(&ol)),
	)
	if r1 == 0 {
		return e
	}
	return nil
}

func (l *FileLock) Lock() error { return l.lock(lockfileExclusiveLock) }

func (l *FileLock) TryLock() error {
	err := l.lock(lockfileExclusiveLock | lockfileFailImmediately)
	if errno, ok := err.(syscall.Errno); ok && errno == errorLockViolation {
		return ErrLocked
	}
	return err
}

func (l *FileLock) Unlock() error {
	r1, _, e := procUnlockFileEx.Call(l.f.Fd(), 0, ^uintptr(0), 0, 0)
	if r1 == 0 {
		return e
	}
	return nil
}

func (l *FileLock) Close() error {
	err1 := l.Unlock()
	err2 := l.f.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
