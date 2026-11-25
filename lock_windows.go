//go:build windows

package speakerdiar

import (
	"os"
	"syscall"
	"time"
)

var (
	kernel32       = syscall.NewLazyDLL("kernel32.dll")
	procLockFile   = kernel32.NewProc("LockFile")
	procUnlockFile = kernel32.NewProc("UnlockFile")
)

// platformLock takes an exclusive one-byte Windows lock. LockFile may return
// immediately with LOCK_VIOLATION, so retry with bounded local backoff.
func platformLock(f *os.File) error {
	handle := uintptr(syscall.Handle(f.Fd()))
	deadline := time.Now().Add(30 * time.Second)
	for {
		r1, _, callErr := procLockFile.Call(handle, 0, 0, 1, 0)
		if r1 != 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return callErr
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func platformUnlock(f *os.File) error {
	handle := uintptr(syscall.Handle(f.Fd()))
	r1, _, err := procUnlockFile.Call(handle, 0, 0, 1, 0)
	if r1 == 0 {
		return err
	}
	return nil
}
