//go:build unix

package store

import "golang.org/x/sys/unix"

func flock(fd uintptr, block bool) error {
	op := unix.LOCK_EX
	if !block {
		op |= unix.LOCK_NB
	}
	return unix.Flock(int(fd), op)
}

func tryFlock(fd uintptr) (bool, error) {
	if err := unix.Flock(int(fd), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if err == unix.EWOULDBLOCK {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func unflock(fd uintptr) error { return unix.Flock(int(fd), unix.LOCK_UN) }
