package arbiter

import (
	"context"

	"leasearbiter/internal/store"
)

// RegisterSnapshot pins a reader snapshot so recycling preserves the versions
// it may read.
func (e *Engine) RegisterSnapshot(snapshotVer int64, holder string) error {
	if snapshotVer <= 0 {
		return apiError(CodeBadRequest, "positive snapshot_ver is required", nil)
	}
	return e.st.RegisterSnapshot(store.ActiveSnapshot{
		SnapshotVer: snapshotVer,
		Holder:      orDefault(holder, "reader"),
		OpenedAtMs:  store.Milli(e.now()),
	})
}

// ReleaseSnapshot removes a reader pin.
func (e *Engine) ReleaseSnapshot(snapshotVer int64, holder string) error {
	return e.st.ReleaseSnapshot(snapshotVer, orDefault(holder, "reader"))
}

// PruneOrphanedSnapshots removes registrations left by a killed process.
func (e *Engine) PruneOrphanedSnapshots() (int64, error) {
	return e.st.PruneOrphanedSnapshots()
}

// ListSnapshots returns reader pins for the snapshot-matrix diagnostics.
func (e *Engine) ListSnapshots(ctx context.Context) ([]store.ActiveSnapshot, error) {
	return e.st.ListSnapshots()
}

// RecoveryEvents returns the fact-only recovery audit trail.
func (e *Engine) RecoveryEvents(ctx context.Context) ([]store.RecoveryEvent, error) {
	return e.st.RecoveryEvents()
}
