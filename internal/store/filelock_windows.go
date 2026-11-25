//go:build windows

package store

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32     = syscall.NewLazyDLL("kernel32.dll")
	lockFileEx   = kernel32.NewProc("LockFileEx")
	unlockFileEx = kernel32.NewProc("UnlockFileEx")
)

const (
	lockfileExclusiveLock = 0x00000002
)

func acquireFileLock(path string) (func() error, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	var overlapped syscall.Overlapped
	result, _, callErr := lockFileEx.Call(file.Fd(), lockfileExclusiveLock, 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if result == 0 {
		file.Close()
		return nil, callErr
	}
	return func() error {
		var unlockOverlapped syscall.Overlapped
		unlockFileEx.Call(file.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&unlockOverlapped)))
		return file.Close()
	}, nil
}
