package engine

import (
	"fmt"
	"os"

	"alarmengine/internal/model"
	"alarmengine/internal/storage"
)

// RecoveryReport contains only facts found on disk after a restart.
type RecoveryReport struct {
	RecoveredAtMS      int64    `json:"recovered_at_ms"`
	LastGoodVersion    int64    `json:"last_good_version"`
	RolledBackIntents  []string `json:"rolled_back_intents"`
	ResetToCandidate   []string `json:"reset_to_candidate"`
	RolledBackRebuilds []int64  `json:"rolled_back_rebuilds"`
	ReconciledRequests []string `json:"reconciled_requests"`
	Details            []string `json:"details"`
}

// Recover scans unfinished intents and restores consistency exclusively from
// durable facts:
//   - INGEST with no committed rebuild: its observations were never visible
//     (single transaction), so any candidate alerts belonging to the dangling
//     version are reset/removed and the version pointer rolls back.
//   - REPLACE_SNAPSHOT left mid-flight: the new snapshot stays on disk as a
//     stored row but the active pointer rolls back to the last complete
//     version; derived rows of the unfinished version are deleted.
//   - TRANSITION intents never commit partially (single tx), so closing the
//     intent is enough.
func (e *Engine) Recover() (RecoveryReport, error) {
	report := RecoveryReport{RecoveredAtMS: e.cfg.Now()}
	lastGood, err := storage.GetMetaInt(e.store.ReadDB(), storage.MetaLastCompleteVer, 0)
	if err != nil {
		return report, err
	}
	report.LastGoodVersion = lastGood

	pending, err := storage.ListUnfinishedIntents(e.store.ReadDB())
	if err != nil {
		return report, err
	}
	if len(pending) == 0 {
		return report, nil
	}

	tx, err := e.store.BeginWrite()
	if err != nil {
		return report, err
	}
	for _, intent := range pending {
		switch intent.Kind {
		case storage.IntentIngestObservations:
			// Domain rows and the intent completion live in one tx that did not
			// commit, so observations cannot exist. Defensively remove any rows
			// tagged with the dangling version and reset surviving alerts.
			dangling := intent.Version + 1
			alerts, _ := storage.LoadAlerts(tx, dangling)
			for _, a := range alerts {
				if a.State != model.StateCandidate {
					report.ResetToCandidate = append(report.ResetToCandidate, a.ID)
				}
			}
			if _, err := tx.Exec(`DELETE FROM observations WHERE commit_version=?`, dangling); err != nil {
				_ = tx.Rollback()
				return report, err
			}
			if err := storage.DeleteDerivedForVersion(tx, dangling); err != nil {
				_ = tx.Rollback()
				return report, err
			}
			report.RolledBackRebuilds = append(report.RolledBackRebuilds, dangling)
			report.Details = append(report.Details,
				fmt.Sprintf("ingest intent %s rolled back to version %d", intent.ID, lastGood))
		case storage.IntentReplaceSnapshot:
			dangling := intent.Version + 1
			if err := storage.DeleteDerivedForVersion(tx, dangling); err != nil {
				_ = tx.Rollback()
				return report, err
			}
			if _, err := tx.Exec(`DELETE FROM policy_versions WHERE version=?`, dangling); err != nil {
				_ = tx.Rollback()
				return report, err
			}
			report.RolledBackRebuilds = append(report.RolledBackRebuilds, dangling)
			report.Details = append(report.Details,
				fmt.Sprintf("snapshot intent %s rolled back; keeping version %d", intent.ID, lastGood))
		case storage.IntentRebuildDerived:
			// Bootstrap/services intent: services were in the same tx that did
			// not commit; simply close the intent.
			report.Details = append(report.Details, "rebuild intent "+intent.ID+" closed")
		case storage.IntentTransitionAlert:
			report.Details = append(report.Details, "transition intent "+intent.ID+" had no partial commit")
		}
		if err := storage.MarkIntentCommitted(tx, intent.ID); err != nil {
			_ = tx.Rollback()
			return report, err
		}
		report.RolledBackIntents = append(report.RolledBackIntents, intent.ID)
	}

	// Reconcile STARTED request ledger rows left behind by the crash.
	rows, qerr := tx.Query(`SELECT request_id FROM requests WHERE status=?`, storage.RequestStarted)
	if qerr != nil {
		_ = tx.Rollback()
		return report, qerr
	}
	var started []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			_ = tx.Rollback()
			return report, err
		}
		started = append(started, id)
	}
	rows.Close()
	for _, id := range started {
		// The operation never completed; remove the reservation so the same
		// request id can be re-submitted cleanly.
		if _, err := tx.Exec(`DELETE FROM requests WHERE request_id=? AND status=?`,
			id, storage.RequestStarted); err != nil {
			_ = tx.Rollback()
			return report, err
		}
		report.ReconciledRequests = append(report.ReconciledRequests, id)
	}

	if err := storage.SetMeta(tx, storage.MetaCommittedVersion, fmt.Sprintf("%d", lastGood)); err != nil {
		_ = tx.Rollback()
		return report, err
	}
	if err := tx.Commit(); err != nil {
		return report, err
	}

	e.mu.Lock()
	e.version = lastGood
	e.lastGood = lastGood
	e.mu.Unlock()
	return report, e.loadState()
}

// maybeCrash simulates SIGKILL at an externally configured crash point. It is
// driven purely by Config (external parameter), never by source edits.
func (e *Engine) maybeCrash(point CrashPoint, intentID string) {
	if e.cfg.CrashPoint == "" || e.cfg.CrashPoint != point {
		return
	}
	if e.cfg.CrashIntentID != "" && e.cfg.CrashIntentID != intentID {
		return
	}
	// Hard terminate while the open transaction is uncommitted. The WAL plus
	// the durable STARTED intent are the on-disk facts SQLite and the recovery
	// path rely on after restart; no checkpoint is attempted from inside a tx.
	_ = os.Stderr.Sync()
	os.Exit(97)
}
