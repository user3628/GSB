package arbiter

import (
	"context"
	"database/sql"

	"leasearbiter/internal/store"
)

// RecycleRequest declares the retention policy for one recycling pass.
type RecycleRequest struct {
	RowID              string `json:"row_id"`
	MinRetainSnapshots int64  `json:"min_retain_snapshots"`
	MinRetainVersions  int    `json:"min_retain_versions"`
	RequestID          string `json:"request_id,omitempty"`
}

// RecycleResult reports what one pass retired.
type RecycleResult struct {
	RowID              string  `json:"row_id"`
	ProtectedSnapshot  int64   `json:"protected_snapshot"`
	HadActiveSnapshot  bool    `json:"had_active_snapshot"`
	KeptRowVersions    []int64 `json:"kept_row_versions"`
	RetiredRowVersions []int64 `json:"retired_row_versions"`
}

// Recycle retires old row versions under a per-row single writer. A version is
// protected when:
//   - it is the latest visible version (never retired), or
//   - its commit_ver is >= the effective floor, where the floor is
//     max(declared min_retain_snapshots, lowest active reader snapshot), or
//   - retiring it would leave fewer than min_retain_versions non-retired rows.
//
// The intent is written before any row is marked retired and completed in the
// same transaction, so a forced kill always leaves either all changes visible
// or none.
func (e *Engine) Recycle(ctx context.Context, req RecycleRequest) (*RecycleResult, error) {
	if req.RowID == "" {
		return nil, apiError(CodeBadRequest, "row_id is required", nil)
	}
	if req.MinRetainVersions < 1 {
		req.MinRetainVersions = 1
	}
	g := e.gates.get(req.RowID)
	g.mu.Lock()
	defer g.mu.Unlock()

	activeSnap, hasActive, err := e.st.MinActiveSnapshot()
	if err != nil {
		return nil, err
	}
	floor := req.MinRetainSnapshots
	if hasActive && activeSnap > floor {
		floor = activeSnap
	}

	versions, err := store.RetainableVersions(e.st.DB, req.RowID)
	if err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		return &RecycleResult{RowID: req.RowID, ProtectedSnapshot: floor,
			HadActiveSnapshot: hasActive, KeptRowVersions: nil, RetiredRowVersions: nil}, nil
	}

	// versions is newest-first. The first element is the latest and always
	// kept; the rest are retirement candidates from oldest to newest.
	latest := versions[0]
	candidates := versions[1:]
	var keep, retire []int64
	keep = append(keep, latest.RowVersion)
	protectedCount := 1
	for i := len(candidates) - 1; i >= 0; i-- {
		v := candidates[i]
		if v.CommitVer >= floor || protectedCount < req.MinRetainVersions {
			keep = append(keep, v.RowVersion)
			protectedCount++
		} else {
			retire = append(retire, v.RowVersion)
		}
	}
	if len(retire) == 0 {
		return &RecycleResult{RowID: req.RowID, ProtectedSnapshot: floor,
			HadActiveSnapshot: hasActive, KeptRowVersions: keep}, nil
	}

	nowMs := store.Milli(e.now())
	payload := recyclePayload(floor, req.MinRetainVersions, retire)
	err = e.st.TxFunc(func(tx *sql.Tx) error {
		intentID, err := store.InsertIntent(tx, store.WriteIntent{
			Kind:        store.KindRecycle,
			RowID:       req.RowID,
			RequestID:   req.RequestID,
			Payload:     payload,
			CreatedAtMs: nowMs,
		})
		if err != nil {
			return err
		}
		for _, rv := range retire {
			if err := store.RetireVersion(tx, req.RowID, rv, nowMs); err != nil {
				return err
			}
		}
		return store.CompleteIntent(tx, intentID, nowMs)
	})
	if err != nil {
		return nil, err
	}
	e.invalidateCache(req.RowID)
	return &RecycleResult{
		RowID:              req.RowID,
		ProtectedSnapshot:  floor,
		HadActiveSnapshot:  hasActive,
		KeptRowVersions:    keep,
		RetiredRowVersions: retire,
	}, nil
}
