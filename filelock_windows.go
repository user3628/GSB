//go:build windows

package cockpitaudio

import (
	"os"

	"golang.org/x/sys/windows"
)

func lockPlatform(f *os.File) error {
	var overlapped windows.Overlapped
	return windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK,
		0,
		^uint32(0),
		^uint32(0),
		&overlapped,
	)
}

func unlockPlatform(f *os.File) error {
	var overlapped windows.Overlapped
	return windows.UnlockFileEx(
		windows.Handle(f.Fd()),
		0,
		^uint32(0),
		^uint32(0),
		&overlapped,
	)
}
