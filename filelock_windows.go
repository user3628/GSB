//go:build windows

package rewardpack

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	lockfileExclusiveLock   = 0x00000002
	lockfileFailImmediately = 0x00000001
	errorLockViolation      = syscall.Errno(33)
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
)

func tryLockFile(file *os.File) (bool, error) {
	var overlapped syscall.Overlapped
	result, _, callErr := procLockFileEx.Call(
		file.Fd(), lockfileExclusiveLock|lockfileFailImmediately,
		0, 1, 0, uintptr(unsafe.Pointer(&overlapped)),
	)
	if result != 0 {
		return true, nil
	}
	if callErr == errorLockViolation {
		return false, errLockBusy
	}
	return false, callErr
}

func unlockFile(file *os.File) error {
	var overlapped syscall.Overlapped
	result, _, callErr := procUnlockFileEx.Call(
		file.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)),
	)
	if result == 0 {
		return callErr
	}
	return nil
}
