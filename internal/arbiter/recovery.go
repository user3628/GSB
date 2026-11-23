package arbiter

import (
	"context"
	"database/sql"
	"errors"

	"leasearbiter/internal/store"
)

// RecoveryReport is built exclusively from durable rows. Nothing in this
// report is inferred from in-memory state, because the previous process was
// forcibly terminated and its memory is gone.
type RecoveryReport struct {
	RecoveredAtMs     int64                     `json:"recovered_at_ms"`
	PreparedIntents   int                       `json:"prepared_intents"`
	Actions           []RecoveryAction          `json:"actions"`
	AbortedRequests   int64                     `json:"aborted_requests"`
	OrphanedSnapshots int64                     `json:"orphaned_snapshots"`
	CurrentCommitVer  int64                     `json:"current_commit_ver"`
	LeaseStates       map[string]LeaseStateView `json:"lease_states"`
}

// RecoveryAction is one fact-based rollback step.
type RecoveryAction struct {
	IntentID int64  `json:"intent_id"`
	Kind     string `json:"kind"`
	RowID    string `json:"row_id"`
	Action   string `json:"action"`
	Detail   string `json:"detail"`
}

// LeaseStateView summarizes the durable lease after recovery.
type LeaseStateView struct {
	LeaseVersion int64  `json:"lease_version"`
	Holder       string `json:"holder"`
	State        string `json:"state"`
	ExpiresAtMs  int64  `json:"expires_at_ms"`
}

// Recover replays the intent log after a forced termination. The invariants:
//
//   - Acquire/renew intents: if the durable lease version matches the intent,
//     the applying transaction cannot have committed (it completes the intent
//     in the same tx), so the grant/renewal is reverted and the row becomes
//     grantable again.
//   - Commit intents: an uncommitted row version can never exist because the
//     row insert, meta advance, lease release and intent completion share one
//     transaction; this is verified, not assumed.
//   - Recycle intents: retired_at can only be visible with a completed intent,
//     so an unfinished recycle leaves every version retained.
//   - Running requests become aborted (retriable); open snapshots become
//     orphaned but keep protecting versions conservatively.
func (e *Engine) Recover(ctx context.Context) (*RecoveryReport, error) {
	nowMs := store.Milli(e.now())
	intents, err := e.st.PreparedIntents()
	if err != nil {
		return nil, err
	}
	report := &RecoveryReport{
		RecoveredAtMs:   nowMs,
		PreparedIntents: len(intents),
		LeaseStates:     map[string]LeaseStateView{},
	}

	for _, in := range intents {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		action, err := e.recoverIntent(in, nowMs)
		if err != nil {
			return nil, err
		}
		if action != nil {
			report.Actions = append(report.Actions, *action)
		}
	}

	aborted, err := e.st.AbortRunningRequests(nowMs)
	if err != nil {
		return nil, err
	}
	report.AbortedRequests = aborted
	orphans, err := e.st.MarkSnapshotsOrphaned()
	if err != nil {
		return nil, err
	}
	report.OrphanedSnapshots = orphans
	commitVer, err := e.st.CommitVer()
	if err != nil {
		return nil, err
	}
	report.CurrentCommitVer = commitVer

	leases, err := e.allLeaseStateViews()
	if err != nil {
		return nil, err
	}
	report.LeaseStates = leases
	return report, nil
}

func (e *Engine) recoverIntent(in store.WriteIntent, nowMs int64) (*RecoveryAction, error) {
	action := &RecoveryAction{IntentID: in.ID, Kind: string(in.Kind), RowID: in.RowID}
	err := e.st.TxFunc(func(tx *sql.Tx) error {
		switch in.Kind {
		case store.KindAcquire:
			lease, err := store.ScanLease(tx, in.RowID)
			if errors.Is(err, sql.ErrNoRows) {
				action.Action = "noop"
				action.Detail = "no durable lease row; grant was never committed"
				return markRolledBack(tx, in.ID, nowMs)
			}
			if err != nil {
				return err
			}
			if lease.LeaseVersion == in.LeaseVersion && lease.State == store.LeaseGranted {
				// Uncommitted grant: make the row grantable again. A newer
				// committed grant would carry a higher lease_version.
				if _, err := tx.Exec(`UPDATE write_leases SET state=? WHERE row_id=? AND lease_version=?`,
					string(store.LeaseExpired), in.RowID, in.LeaseVersion); err != nil {
					return err
				}
				action.Action = "rollback_acquire"
				action.Detail = "uncommitted lease grant reverted to expired; row is grantable"
			} else {
				action.Action = "verify_skip"
				action.Detail = "durable lease moved beyond intent version; grant had committed or been superseded"
			}
			return markRolledBack(tx, in.ID, nowMs)

		case store.KindRenew:
			lease, err := store.ScanLease(tx, in.RowID)
			if errors.Is(err, sql.ErrNoRows) {
				action.Action = "noop"
				action.Detail = "lease row absent; renewal was never committed"
				return markRolledBack(tx, in.ID, nowMs)
			}
			if err != nil {
				return err
			}
			if lease.LeaseVersion == in.LeaseVersion {
				action.Action = "rollback_renew"
				if lease.ExpiresAtMs <= nowMs {
					if _, err := tx.Exec(`UPDATE write_leases SET state=? WHERE row_id=? AND lease_version=?`,
						string(store.LeaseExpired), in.RowID, in.LeaseVersion); err != nil {
						return err
					}
					action.Detail = "uncommitted renewal discarded; TTL already elapsed, lease marked expired"
				} else {
					action.Detail = "uncommitted renewal discarded; original grant expiry remains authoritative"
				}
			} else {
				action.Action = "verify_skip"
				action.Detail = "lease version advanced after intent; renewal outcome superseded"
			}
			return markRolledBack(tx, in.ID, nowMs)

		case store.KindCommit:
			// Durable verification via the intent_id linkage: a normal commit
			// inserts the row version, advances commit_ver, releases the lease
			// and completes the intent in one transaction. A prepared intent
			// therefore means any linked row version is an orphan that must be
			// deleted, and the highest allocated commit number must rewind.
			orphan, oerr := store.RowVersionByIntent(tx, in.ID)
			switch {
			case errors.Is(oerr, sql.ErrNoRows):
				action.Action = "discard_commit"
				action.Detail = "no row version linked to intent; uncommitted write discarded"
			case oerr != nil:
				return oerr
			default:
				if err := store.DeleteRowVersion(tx, orphan.RowID, orphan.RowVersion); err != nil {
					return err
				}
				var maxCommit sql.NullInt64
				if err := tx.QueryRow(`SELECT MAX(commit_ver) FROM row_versions`).Scan(&maxCommit); err != nil {
					return err
				}
				floor := int64(0)
				if maxCommit.Valid {
					floor = maxCommit.Int64
				}
				if err := store.SetCommitVer(tx, floor); err != nil {
					return err
				}
				action.Action = "discard_commit"
				action.Detail = "orphan row version deleted and commit_ver rewound to last durable commit"
			}
			// Any lease still granted on this exact intent version cannot have
			// been consumed by the killed commit; release it so the row is
			// grantable, based only on the durable lease row.
			if lease, lerr := store.ScanLease(tx, in.RowID); lerr == nil &&
				lease.LeaseVersion == in.LeaseVersion && lease.State == store.LeaseGranted {
				if _, err := tx.Exec(`UPDATE write_leases SET state=? WHERE row_id=? AND lease_version=?`,
					string(store.LeaseExpired), in.RowID, in.LeaseVersion); err != nil {
					return err
				}
				action.Detail += "; dangling granted lease marked expired"
			} else if lerr != nil && !errors.Is(lerr, sql.ErrNoRows) {
				return lerr
			}
			return markRolledBack(tx, in.ID, nowMs)

		case store.KindRelease, store.KindExpire:
			lease, err := store.ScanLease(tx, in.RowID)
			if errors.Is(err, sql.ErrNoRows) {
				action.Action = "noop"
				action.Detail = "no lease row; transition was never committed"
				return markRolledBack(tx, in.ID, nowMs)
			}
			if err != nil {
				return err
			}
			if lease.LeaseVersion == in.LeaseVersion && lease.State == store.LeaseGranted {
				target := store.LeaseExpired
				if in.Kind == store.KindRelease {
					target = store.LeaseReleased
				}
				if _, err := tx.Exec(`UPDATE write_leases SET state=? WHERE row_id=? AND lease_version=?`,
					string(target), in.RowID, in.LeaseVersion); err != nil {
					return err
				}
				action.Action = "rollback_" + string(in.Kind)
				action.Detail = "uncommitted lease transition reverted to " + string(target)
			} else {
				action.Action = "verify_skip"
				action.Detail = "lease already in durable terminal/superseded state"
			}
			return markRolledBack(tx, in.ID, nowMs)

		case store.KindRecycle:
			// Normal one-transaction operation can never show a retired row
			// with a prepared intent. If fault injection produced one, the
			// fact-based response keeps the safe retired state (readers get
			// E_VERSION_RETIRED, never mixed content) and records the
			// deviation explicitly instead of claiming an impossible undo.
			var retired int64
			if err := tx.QueryRow(`SELECT COUNT(*) FROM row_versions
				WHERE row_id=? AND retired_at IS NOT NULL`, in.RowID).Scan(&retired); err != nil {
				return err
			}
			switch retired {
			case 0:
				action.Action = "rollback_recycle"
				action.Detail = "uncommitted recycle discarded; all row versions remain retained"
			default:
				action.Action = "recycle_deviation_recorded"
				action.Detail = "prepared recycle intent coexists with durable retired rows; safe retired state retained and deviation recorded"
			}
			return markRolledBack(tx, in.ID, nowMs)

		default:
			action.Action = "unknown_kind"
			action.Detail = "unrecognized intent kind left for operator review"
			return markRolledBack(tx, in.ID, nowMs)
		}
	},
	)
	if err != nil {
		return nil, err
	}
	return action, store.RecordRecoveryEvent(e.st.DB, store.RecoveryEvent{
		IntentID:  in.ID,
		Kind:      in.Kind,
		RowID:     in.RowID,
		Action:    action.Action,
		Detail:    action.Detail,
		CreatedMs: nowMs,
	})
}

func markRolledBack(tx *sql.Tx, intentID, nowMs int64) error {
	_, err := tx.Exec(`UPDATE write_intents SET state=?, completed_at=? WHERE id=?`,
		string(store.IntentRolledBack), nowMs, intentID)
	return err
}

func (e *Engine) allLeaseStateViews() (map[string]LeaseStateView, error) {
	rows, err := e.st.DB.Query(`SELECT row_id, lease_version, holder, state, expires_at
		FROM write_leases ORDER BY row_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]LeaseStateView{}
	for rows.Next() {
		var id, holder, state string
		var ver, expiry int64
		if err := rows.Scan(&id, &ver, &holder, &state, &expiry); err != nil {
			return nil, err
		}
		out[id] = LeaseStateView{LeaseVersion: ver, Holder: holder, State: state, ExpiresAtMs: expiry}
	}
	return out, rows.Err()
}
