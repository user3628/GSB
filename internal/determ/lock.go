//go:build !windows

// Package determ contains the deterministic primitives used across the whole
// pipeline: a fixed-seed PRNG, canonical JSON serialization, content
// fingerprints and an advisory file lock. Nothing in here may depend on map
// iteration order, goroutine scheduling or wall-clock time.
package determ

import (
	"errors"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// FileLock is an advisory, whole-file lock backed by flock(2) on POSIX and
// LockFileEx on Windows. It is the "real file lock" required by the publish
// protocol; SQLite conditions provide the in-database ordering while this
// lock coordinates separate worker processes touching the same workspace.
type FileLock struct {
	path string
	f    *os.File
}

// NewFileLock opens (or creates) the lock file under dir.
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

// Lock acquires an exclusive lock, blocking until it is available.
func (l *FileLock) Lock() error {
	return unix.Flock(int(l.f.Fd()), unix.LOCK_EX)
}

// TryLock acquires the lock only if it is currently free.
func (l *FileLock) TryLock() error {
	err := unix.Flock(int(l.f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) {
		return ErrLocked
	}
	return err
}

// Unlock releases the lock.
func (l *FileLock) Unlock() error { return unix.Flock(int(l.f.Fd()), unix.LOCK_UN) }

// Close releases the lock and closes the descriptor.
func (l *FileLock) Close() error {
	err1 := l.Unlock()
	err2 := l.f.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
