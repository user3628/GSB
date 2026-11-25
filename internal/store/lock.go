package store

import (
	"fmt"
	"os"
	"sync"

	"qaudit/internal/model"
)

// FileLock is a real inter-process advisory lock backed by an OS flock on a
// dedicated lock file living next to the SQLite database.
type FileLock struct {
	f  *os.File
	mu sync.Mutex
}

// NewFileLock creates (but does not acquire) the lock file.
func NewFileLock(path string) (*FileLock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileLock{f: f}, nil
}

// Lock acquires an exclusive blocking lock.
func (l *FileLock) Lock() error {
	l.mu.Lock()
	return flock(l.f.Fd(), true)
}

// TryLock acquires the lock only if it is currently free.
func (l *FileLock) TryLock() (bool, error) {
	l.mu.Lock()
	ok, err := tryFlock(l.f.Fd())
	if err != nil || !ok {
		l.mu.Unlock()
	}
	return ok, err
}

// Unlock releases the lock.
func (l *FileLock) Unlock() error {
	err := unflock(l.f.Fd())
	l.mu.Unlock()
	return err
}

// Close unlocks and closes the underlying file.
func (l *FileLock) Close() error {
	_ = unflock(l.f.Fd())
	return l.f.Close()
}

// PublishGuard coordinates snapshot publication across worker processes.
type PublishGuard struct {
	store *Store
	lock  *FileLock
}

// NewPublishGuard builds a guard using the database's companion lock file.
func NewPublishGuard(st *Store) (*PublishGuard, error) {
	lock, err := NewFileLock(st.lockPath)
	if err != nil {
		return nil, err
	}
	return &PublishGuard{store: st, lock: lock}, nil
}

// Close releases the lock file.
func (g *PublishGuard) Close() error { return g.lock.Close() }

// PublishRequest is one conditional publication attempt.
type PublishRequest struct {
	Version     int64
	ExpectedSeq int64 // monotonic per-layer sequence the caller expects (1-based)
	LayerName   string
	InputFP     string
	PolicyFP    string
	Digest      string
	Artifact    *model.LayerCalibration
}

// Publish acquires the real file lock, verifies continuous progression with a
// conditional SQL check and commits the snapshot transactionally. Stale,
// duplicate or out-of-order attempts fail with E_STALE_VERSION.
func (g *PublishGuard) Publish(req PublishRequest) error {
	if err := g.lock.Lock(); err != nil {
		return model.NewBizError(model.CodeIO, "cannot acquire publish lock",
			map[string]string{"detail": err.Error()})
	}
	defer g.lock.Unlock()

	latest, err := g.store.LatestVersion()
	if err != nil {
		return err
	}
	// Versions must advance continuously: only the current latest version may be
	// extended; an older version is rejected as stale.
	if req.Version < latest {
		return model.NewBizError(model.CodeStaleVersion, "backward version publication rejected",
			map[string]string{
				"layer":   req.LayerName,
				"version": fmt.Sprintf("%d", req.Version),
				"latest":  fmt.Sprintf("%d", latest),
				"reason":  "behind",
			})
	}
	rows, err := g.store.ListSnapshots(req.Version)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if r.LayerName == req.LayerName {
			return model.NewBizError(model.CodeStaleVersion, "duplicate layer publication rejected",
				map[string]string{
					"layer":   req.LayerName,
					"version": fmt.Sprintf("%d", req.Version),
					"reason":  "duplicate",
				})
		}
	}
	if req.ExpectedSeq != int64(len(rows))+1 {
		return model.NewBizError(model.CodeStaleVersion, "out-of-order publication rejected",
			map[string]string{
				"layer":        req.LayerName,
				"version":      fmt.Sprintf("%d", req.Version),
				"expected_seq": fmt.Sprintf("%d", req.ExpectedSeq),
				"actual_seq":   fmt.Sprintf("%d", len(rows)+1),
				"reason":       "sequence_mismatch",
			})
	}
	return g.store.SaveSnapshot(req.Version, req.LayerName, req.InputFP, req.PolicyFP, req.Digest, req.Artifact)
}

// Lock exposes the file lock for whole-pipeline critical sections.
func (g *PublishGuard) Lock() *FileLock { return g.lock }
