package arbiter

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"

	"leasearbiter/internal/store"
)

// ReadRequest pins one read snapshot for a single row lookup.
type ReadRequest struct {
	RowID       string `json:"row_id"`
	SnapshotVer int64  `json:"snapshot_ver"`
	Holder      string `json:"holder,omitempty"`
	Register    bool   `json:"register_snapshot,omitempty"`
}

// RowView is the wire representation of one visible row version.
type RowView struct {
	RowID       string `json:"row_id"`
	RowVersion  int64  `json:"row_version"`
	CommitVer   int64  `json:"commit_ver"`
	ContentHash string `json:"content_hash"`
	Content     string `json:"content"`
	Deleted     bool   `json:"deleted"`
	SnapshotVer int64  `json:"snapshot_ver"`
	CurrentVer  int64  `json:"current_commit_ver"`
}

// Read returns the row version visible at the given snapshot. It takes no row
// lock: SQLite WAL guarantees a consistent view of a fully committed version,
// so reads run concurrently with each other and with writers of other rows.
func (e *Engine) Read(ctx context.Context, req ReadRequest) (*RowView, error) {
	if req.RowID == "" || req.SnapshotVer <= 0 {
		return nil, apiError(CodeBadRequest, "row_id and positive snapshot_ver are required", nil)
	}
	current, err := e.st.CommitVer()
	if err != nil {
		return nil, err
	}
	if req.SnapshotVer > current {
		return nil, apiError(CodeSnapshotAhead, "snapshot is newer than any committed version", map[string]any{
			"requested_snapshot_ver": req.SnapshotVer,
			"current_commit_ver":     current,
		})
	}
	rv, err := store.RowVersionAt(e.st.DB, req.RowID, req.SnapshotVer)
	if errors.Is(err, store.ErrVersionRetired) {
		return nil, apiError(CodeVersionRetired, "the version visible to this snapshot was reclaimed; no newer version may be substituted", map[string]any{
			"row_id":       req.RowID,
			"snapshot_ver": req.SnapshotVer,
			"row_version":  rv.RowVersion,
			"commit_ver":   rv.CommitVer,
		})
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(CodeNotFound, "no committed row version exists at or below the snapshot", map[string]any{
			"row_id":       req.RowID,
			"snapshot_ver": req.SnapshotVer,
		})
	}
	if err != nil {
		return nil, err
	}
	if rv.Deleted {
		return nil, apiError(CodeRowDeleted, "the row is deleted at the requested snapshot", map[string]any{
			"row_id":       req.RowID,
			"row_version":  rv.RowVersion,
			"snapshot_ver": req.SnapshotVer,
		})
	}
	if req.Register {
		if err := e.st.RegisterSnapshot(store.ActiveSnapshot{
			SnapshotVer: req.SnapshotVer,
			Holder:      orDefault(req.Holder, "reader"),
			OpenedAtMs:  store.Milli(e.now()),
		}); err != nil {
			return nil, err
		}
	}
	return &RowView{
		RowID:       rv.RowID,
		RowVersion:  rv.RowVersion,
		CommitVer:   rv.CommitVer,
		ContentHash: rv.ContentHash,
		Content:     rv.Content,
		Deleted:     rv.Deleted,
		SnapshotVer: req.SnapshotVer,
		CurrentVer:  current,
	}, nil
}

// CommitRequest applies one edit. BaseRowVersion and LeaseVersion are both
// validated inside one transaction, so no commit can pass the row check with a
// dead lease.
type CommitRequest struct {
	RowID          string `json:"row_id"`
	Holder         string `json:"holder"`
	LeaseVersion   int64  `json:"lease_version"`
	BaseRowVersion int64  `json:"base_row_version"`
	Content        string `json:"content"`
	Deleted        bool   `json:"deleted"`
	RequestID      string `json:"request_id,omitempty"`
	PrecheckToken  string `json:"precheck_token,omitempty"`
}

// CommitResponse describes the newly committed row version.
type CommitResponse struct {
	RowID       string `json:"row_id"`
	RowVersion  int64  `json:"row_version"`
	CommitVer   int64  `json:"commit_ver"`
	ContentHash string `json:"content_hash"`
	Deleted     bool   `json:"deleted"`
	LeaseState  string `json:"lease_state"`
	Recheck     *Diff  `json:"recheck,omitempty"`
}

// Diff is the field-by-field reconciliation block required by requirement 8.
type Diff struct {
	SnapshotVer        int64  `json:"snapshot_ver"`
	ActualRowVersion   int64  `json:"actual_row_version"`
	ExpectedRowVersion int64  `json:"expected_row_version"`
	ActualHolder       string `json:"actual_holder"`
	PresentedHolder    string `json:"presented_holder"`
	ActualLeaseVer     int64  `json:"actual_lease_version"`
	PresentedLeaseVer  int64  `json:"presented_lease_version"`
	LeaseState         string `json:"lease_state"`
	ExpiresAtMs        int64  `json:"expires_at_ms"`
}

// Commit serializes on the row gate and applies the edit transactionally.
func (e *Engine) Commit(ctx context.Context, req CommitRequest) (*CommitResponse, error) {
	if req.RowID == "" || req.Holder == "" || req.LeaseVersion <= 0 || req.BaseRowVersion <= 0 {
		return nil, apiError(CodeBadRequest, "row_id, holder, positive lease_version and base_row_version are required", nil)
	}
	g := e.gates.get(req.RowID)
	g.mu.Lock()
	defer g.mu.Unlock()

	e.expireIfDueLocked(req.RowID)
	nowMs := store.Milli(e.now())
	var resp CommitResponse
	var leaseFail *APIError

	err := e.st.TxFunc(func(tx *sql.Tx) error {
		// Both checks run against the same transaction snapshot: SQLite takes
		// the write lock at BEGIN and no other process can interleave a commit
		// between the two reads.
		lease, lerr := store.ScanLease(tx, req.RowID)
		if lerr != nil {
			if errors.Is(lerr, sql.ErrNoRows) {
				leaseFail = e.leaseLost(req.RowID, "no_lease", "no write lease exists for the row")
				return leaseFail
			}
			return lerr
		}
		latest, rerr := store.LatestRowVersion(tx, req.RowID)
		if rerr != nil && !errors.Is(rerr, sql.ErrNoRows) {
			return rerr
		}
		if latest == nil {
			return apiError(CodeNotFound, "row has no committed version to edit", map[string]any{"row_id": req.RowID})
		}

		// Lease validity first; the diff still records actual row state.
		if lease.LeaseVersion != req.LeaseVersion || lease.Holder != req.Holder ||
			lease.State != store.LeaseGranted || lease.ExpiresAtMs <= nowMs {
			reason := "expired"
			if lease.State != store.LeaseGranted {
				reason = "state_" + string(lease.State)
			} else if lease.Holder != req.Holder {
				reason = "holder_mismatch"
			} else if lease.LeaseVersion != req.LeaseVersion {
				reason = "lease_version_stale"
			}
			leaseFail = apiError(CodeLeaseLost, "write lease is not valid for this commit", map[string]any{
				"row_id": req.RowID,
				"reason": reason,
				"diff":   buildDiff(req, lease, latest, 0),
			})
			return leaseFail
		}
		if latest.RowVersion != req.BaseRowVersion {
			return apiError(CodeEditStale, "base row version is no longer the latest visible version", map[string]any{
				"row_id":              req.RowID,
				"base_row_version":    req.BaseRowVersion,
				"current_row_version": latest.RowVersion,
				"current_commit_ver":  latest.CommitVer,
				"diff":                buildDiff(req, lease, latest, latest.CommitVer),
			})
		}

		intentID, ierr := store.InsertIntent(tx, store.WriteIntent{
			Kind:         store.KindCommit,
			RowID:        req.RowID,
			RequestID:    req.RequestID,
			LeaseVersion: req.LeaseVersion,
			BaseRowVer:   req.BaseRowVersion,
			Payload:      contentFingerprint(req.Content, req.Deleted),
			CreatedAtMs:  nowMs,
		})
		if ierr != nil {
			return ierr
		}
		commitVer, cerr := store.MetaTx(tx)
		if cerr != nil {
			return cerr
		}
		newRowVer := latest.RowVersion + 1
		hash := contentFingerprint(req.Content, req.Deleted)
		if ierr := store.InsertRowVersion(tx, store.RowVersion{
			RowID:       req.RowID,
			RowVersion:  newRowVer,
			CommitVer:   commitVer,
			ContentHash: hash,
			Content:     req.Content,
			Deleted:     req.Deleted,
			CreatedAtMs: nowMs,
			IntentID:    intentID,
		}); ierr != nil {
			return ierr
		}
		// A successful commit consumes the lease: the next editor must acquire
		// explicitly, eliminating "released but version invisible" windows
		// because release and the new version share one transaction.
		if _, uerr := tx.Exec(`UPDATE write_leases SET state=?, renewed_at=?
			WHERE row_id=? AND lease_version=?`,
			string(store.LeaseReleased), nowMs, req.RowID, req.LeaseVersion); uerr != nil {
			return uerr
		}
		if cerr := store.CompleteIntent(tx, intentID, nowMs); cerr != nil {
			return cerr
		}
		resp = CommitResponse{
			RowID:       req.RowID,
			RowVersion:  newRowVer,
			CommitVer:   commitVer,
			ContentHash: hash,
			Deleted:     req.Deleted,
			LeaseState:  string(store.LeaseReleased),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	e.invalidateCache(req.RowID)
	e.gates.get(req.RowID).wakeAll()
	return &resp, nil
}

func buildDiff(req CommitRequest, lease *store.Lease, latest *store.RowVersion, snapshot int64) Diff {
	d := Diff{
		ExpectedRowVersion: req.BaseRowVersion,
		PresentedHolder:    req.Holder,
		PresentedLeaseVer:  req.LeaseVersion,
		SnapshotVer:        snapshot,
	}
	if lease != nil {
		d.ActualHolder = lease.Holder
		d.ActualLeaseVer = lease.LeaseVersion
		d.LeaseState = string(lease.State)
		d.ExpiresAtMs = lease.ExpiresAtMs
	}
	if latest != nil {
		d.ActualRowVersion = latest.RowVersion
		if snapshot == 0 {
			d.SnapshotVer = latest.CommitVer
		}
	}
	return d
}

func contentFingerprint(content string, deleted bool) string {
	sum := sha256.Sum256([]byte(content))
	if deleted {
		return "deleted:" + hex.EncodeToString(sum[:])
	}
	return hex.EncodeToString(sum[:])
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
