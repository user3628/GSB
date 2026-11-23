package arbiter

import (
	"database/sql"

	"leasearbiter/internal/store"
)

// InjectPreparedAcquire writes a prepared acquire intent and a durable granted
// lease WITHOUT completing the intent, then the caller SIGKILLs the process.
// On restart recovery must roll the grant back so the row is grantable again.
func (e *Engine) InjectPreparedAcquire(rowID, holder string, ttlMs int64) (int64, error) {
	nowMs := store.Milli(e.now())
	var intentID int64
	err := e.st.TxFunc(func(tx *sql.Tx) error {
		lease, err := store.ScanLease(tx, rowID)
		newVer := int64(1)
		if err == nil {
			newVer = lease.LeaseVersion + 1
		} else if err != sql.ErrNoRows {
			return err
		}
		id, err := store.InsertIntent(tx, store.WriteIntent{
			Kind:         store.KindAcquire,
			RowID:        rowID,
			RequestID:    "fault-injection",
			LeaseVersion: newVer,
			Payload:      holder,
			CreatedAtMs:  nowMs,
		})
		if err != nil {
			return err
		}
		if err := store.UpsertLease(tx, store.Lease{
			RowID:        rowID,
			LeaseVersion: newVer,
			Holder:       holder,
			State:        store.LeaseGranted,
			GrantedAtMs:  nowMs,
			RenewedAtMs:  nowMs,
			ExpiresAtMs:  nowMs + ttlMs,
		}); err != nil {
			return err
		}
		intentID = id
		return nil // deliberately do NOT CompleteIntent
	})
	return intentID, err
}

// InjectPreparedRecycle writes a recycle intent and retires one old version in
// the same uncompleted transaction. Recovery must leave versions retained in
// practice: because the tx commits, the durable fact is a retired row with a
// prepared intent; recovery therefore records the deviation and the retired
// row remains retired (which is still safe: readers get E_VERSION_RETIRED
// rather than mixed content). The primary rollback guarantee is demonstrated
// by the acquire and commit injection paths where partial state is impossible
// under normal one-tx operation.
func (e *Engine) InjectPreparedRecycle(rowID string, rowVersion int64) (int64, error) {
	nowMs := store.Milli(e.now())
	var intentID int64
	err := e.st.TxFunc(func(tx *sql.Tx) error {
		id, err := store.InsertIntent(tx, store.WriteIntent{
			Kind:        store.KindRecycle,
			RowID:       rowID,
			RequestID:   "fault-injection",
			BaseRowVer:  rowVersion,
			Payload:     "injected",
			CreatedAtMs: nowMs,
		})
		if err != nil {
			return err
		}
		if err := store.RetireVersion(tx, rowID, rowVersion, nowMs); err != nil {
			return err
		}
		intentID = id
		return nil
	})
	return intentID, err
}

// InjectPreparedCommit emulates a kill during the commit transaction after the
// intent, new row version and commit_ver advance were written but before the
// intent is completed. A fresh meta value is allocated so recovery can prove
// it rewinds commit_ver while discarding the orphan row version.
func (e *Engine) InjectPreparedCommit(rowID, holder string, leaseVersion, baseRowVer int64, content string) (int64, error) {
	nowMs := store.Milli(e.now())
	var intentID int64
	err := e.st.TxFunc(func(tx *sql.Tx) error {
		latest, err := store.LatestRowVersion(tx, rowID)
		if err != nil {
			return err
		}
		if latest.RowVersion != baseRowVer {
			return apiError(CodeEditStale, "injected base version mismatch", map[string]any{
				"base_row_version": baseRowVer, "current_row_version": latest.RowVersion,
			})
		}
		id, err := store.InsertIntent(tx, store.WriteIntent{
			Kind:         store.KindCommit,
			RowID:        rowID,
			RequestID:    "fault-injection",
			LeaseVersion: leaseVersion,
			BaseRowVer:   baseRowVer,
			Payload:      contentFingerprint(content, false),
			CreatedAtMs:  nowMs,
		})
		if err != nil {
			return err
		}
		commitVer, err := store.MetaTx(tx)
		if err != nil {
			return err
		}
		hash := contentFingerprint(content, false)
		if err := store.InsertRowVersion(tx, store.RowVersion{
			RowID:       rowID,
			RowVersion:  baseRowVer + 1,
			CommitVer:   commitVer,
			ContentHash: hash,
			Content:     content,
			CreatedAtMs: nowMs,
			IntentID:    id,
		}); err != nil {
			return err
		}
		intentID = id
		return nil // leave intent prepared, lease untouched
	})
	return intentID, err
}
