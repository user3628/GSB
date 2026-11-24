//go:build !windows

package cockpitaudio

import (
	"os"
	"syscall"
)

func lockPlatform(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
}

func unlockPlatform(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
