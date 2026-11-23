package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"blocksnap/internal/apperr"
)

type pendingIntentFact struct {
	ID         string `json:"id"`
	RequestID  string `json:"request_id"`
	VolumeID   string `json:"volume_id"`
	Operation  string `json:"operation"`
	State      string `json:"observed_state"`
	Resolution string `json:"resolution"`
}

type recoveryDetails struct {
	PendingIntents        []pendingIntentFact `json:"pending_intents"`
	RepairedMetricVolumes []string            `json:"metrics_recomputed_volumes"`
	ObservedVolumes       []volumeFact        `json:"observed_volumes"`
	QuickCheck            []string            `json:"quick_check"`
	ForeignKeyViolations  int64               `json:"foreign_key_violations"`
	Note                  string              `json:"note"`
}

type volumeFact struct {
	VolumeID           string `json:"volume_id"`
	LastSeq            int64  `json:"last_seq"`
	Snapshots          int64  `json:"snapshots"`
	CommittedStateOnly bool   `json:"committed_state_only"`
}

func (s *Store) Recover(ctx context.Context) (RecoveryReport, error) {
	s.mutMu.Lock()
	defer s.mutMu.Unlock()

	started := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RecoveryReport{}, apperr.Internal(err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT id,request_id,volume_id,operation,state FROM intents WHERE state='pending' ORDER BY created_at,id`)
	if err != nil {
		return RecoveryReport{}, apperr.Internal(err)
	}
	pending := make([]pendingIntentFact, 0)
	for rows.Next() {
		var f pendingIntentFact
		if err := rows.Scan(&f.ID, &f.RequestID, &f.VolumeID, &f.Operation, &f.State); err != nil {
			rows.Close()
			return RecoveryReport{}, apperr.Internal(err)
		}
		pending = append(pending, f)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return RecoveryReport{}, apperr.Internal(err)
	}
	rows.Close()

	now := s.nowText()
	abortErr := apperr.WithContext(apperr.New(apperr.CodeOperationAborted, "operation was interrupted before its transaction committed", 409), "recovered", "true")
	abortBody, _ := marshalError(abortErr)
	for _, f := range pending {
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='aborted_recovered',updated_at=? WHERE id=? AND state='pending'`, now, f.ID); err != nil {
			return RecoveryReport{}, apperr.Internal(err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE requests SET status='completed',http_status=?,response_json=?,updated_at=? WHERE request_id=? AND status='pending'`,
			abortErr.HTTPStatus, string(abortBody), now, f.RequestID); err != nil {
			return RecoveryReport{}, apperr.Internal(err)
		}
	}

	volRows, err := tx.QueryContext(ctx, `SELECT id,last_seq FROM volumes ORDER BY id`)
	if err != nil {
		return RecoveryReport{}, apperr.Internal(err)
	}
	var volumeIDs []string
	for volRows.Next() {
		var id string
		var seq int64
		if err := volRows.Scan(&id, &seq); err != nil {
			volRows.Close()
			return RecoveryReport{}, apperr.Internal(err)
		}
		volumeIDs = append(volumeIDs, id)
	}
	if err := volRows.Err(); err != nil {
		volRows.Close()
		return RecoveryReport{}, apperr.Internal(err)
	}
	volRows.Close()

	details := recoveryDetails{
		PendingIntents:        pending,
		RepairedMetricVolumes: make([]string, 0, len(volumeIDs)),
		ObservedVolumes:       make([]volumeFact, 0, len(volumeIDs)),
		QuickCheck:            make([]string, 0, 1),
		Note:                  "Recovery reports only committed SQLite facts. Pending intents were marked aborted_recovered; any uncommitted operation transaction was discarded by SQLite.",
	}
	for _, id := range volumeIDs {
		if err := s.recomputeAllTx(ctx, tx, id); err != nil {
			return RecoveryReport{}, err
		}
		details.RepairedMetricVolumes = append(details.RepairedMetricVolumes, id)
		var seq, snapshots int64
		if err := tx.QueryRowContext(ctx, `SELECT last_seq FROM volumes WHERE id=?`, id).Scan(&seq); err != nil {
			return RecoveryReport{}, apperr.Internal(err)
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots WHERE volume_id=?`, id).Scan(&snapshots); err != nil {
			return RecoveryReport{}, apperr.Internal(err)
		}
		details.ObservedVolumes = append(details.ObservedVolumes, volumeFact{VolumeID: id, LastSeq: seq, Snapshots: snapshots, CommittedStateOnly: true})
	}
	for i := range details.PendingIntents {
		details.PendingIntents[i].Resolution = "aborted_recovered"
	}
	sort.Slice(details.ObservedVolumes, func(i, j int) bool { return details.ObservedVolumes[i].VolumeID < details.ObservedVolumes[j].VolumeID })

	checkRows, err := tx.QueryContext(ctx, `PRAGMA quick_check`)
	if err != nil {
		return RecoveryReport{}, apperr.Internal(err)
	}
	for checkRows.Next() {
		var line string
		if err := checkRows.Scan(&line); err != nil {
			checkRows.Close()
			return RecoveryReport{}, apperr.Internal(err)
		}
		details.QuickCheck = append(details.QuickCheck, line)
	}
	if err := checkRows.Err(); err != nil {
		checkRows.Close()
		return RecoveryReport{}, apperr.Internal(err)
	}
	checkRows.Close()

	fkRows, err := tx.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return RecoveryReport{}, apperr.Internal(err)
	}
	for fkRows.Next() {
		details.ForeignKeyViolations++
	}
	if err := fkRows.Err(); err != nil {
		fkRows.Close()
		return RecoveryReport{}, apperr.Internal(err)
	}
	fkRows.Close()

	status := "complete"
	for _, line := range details.QuickCheck {
		if strings.ToLower(line) != "ok" {
			status = "degraded"
		}
	}
	if details.ForeignKeyViolations > 0 {
		status = "degraded"
	}

	var committedCount int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM intents WHERE state='committed'`).Scan(&committedCount); err != nil {
		return RecoveryReport{}, apperr.Internal(err)
	}

	detailsRaw, err := json.Marshal(details)
	if err != nil {
		return RecoveryReport{}, apperr.Internal(err)
	}
	finished := s.now().UTC()
	res, err := tx.ExecContext(ctx, `INSERT INTO recovery_reports(started_at,finished_at,status,pending_found,aborted_count,committed_count,details_json) VALUES(?,?,?,?,?,?,?)`,
		started.Format(time.RFC3339Nano), finished.Format(time.RFC3339Nano), status, len(pending), len(pending), committedCount, string(detailsRaw))
	if err != nil {
		return RecoveryReport{}, apperr.Internal(err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return RecoveryReport{}, apperr.Internal(err)
	}
	if err := tx.Commit(); err != nil {
		return RecoveryReport{}, apperr.Internal(err)
	}
	return RecoveryReport{
		ID: id, StartedAt: started.Format(time.RFC3339Nano), FinishedAt: finished.Format(time.RFC3339Nano),
		Status: status, PendingFound: int64(len(pending)), AbortedCount: int64(len(pending)),
		CommittedCount: committedCount, Details: detailsRaw,
	}, nil
}

func (s *Store) LatestRecoveryReport(ctx context.Context) (RecoveryReport, error) {
	var report RecoveryReport
	var details string
	err := s.db.QueryRowContext(ctx, `SELECT id,started_at,finished_at,status,pending_found,aborted_count,committed_count,details_json
		FROM recovery_reports ORDER BY id DESC LIMIT 1`).
		Scan(&report.ID, &report.StartedAt, &report.FinishedAt, &report.Status, &report.PendingFound, &report.AbortedCount, &report.CommittedCount, &details)
	if errors.Is(err, sql.ErrNoRows) {
		return RecoveryReport{}, apperr.New(apperr.CodeRecoveryUnavailable, "no recovery report has been recorded", 404)
	}
	if err != nil {
		return RecoveryReport{}, apperr.Internal(err)
	}
	report.Details = json.RawMessage(details)
	return report, nil
}

type SimulateCrashRequest struct {
	VolumeID    string
	Operation   string
	ParentID    string
	ChildID     string
	RequestID   string
	RequestHash string
}

// SimulateCrash commits an intent, starts a transaction, performs a partial
// mutation, then calls crash(exitCode). With os.Exit the uncommitted SQLite
// transaction is discarded; the committed intent survives for recovery.
func (s *Store) SimulateCrash(ctx context.Context, req SimulateCrashRequest, crash func(exitCode int)) error {
	if crash == nil {
		return fmt.Errorf("crash callback is required")
	}
	if req.Operation != "merge" && req.Operation != "rollback" {
		return apperr.Invalid("crash operation must be merge or rollback")
	}
	if req.RequestID == "" {
		req.RequestID = fmt.Sprintf("crash-%d", s.now().UnixNano())
	}
	if req.RequestHash == "" {
		req.RequestHash = RequestHash(req.Operation, req.VolumeID, map[string]string{
			"parent_id": req.ParentID, "child_id": req.ChildID,
		})
	}
	s.mutMu.Lock()
	defer s.mutMu.Unlock()
	intent := intentID(req.RequestID, req.Operation)
	now := s.nowText()
	preTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return apperr.Internal(err)
	}
	if _, err := preTx.ExecContext(ctx, `INSERT INTO requests(request_id,request_hash,volume_id,operation,status,created_at,updated_at) VALUES(?,?,?,?, 'pending',?,?)`,
		req.RequestID, req.RequestHash, req.VolumeID, req.Operation, now, now); err != nil {
		preTx.Rollback()
		return apperr.Internal(err)
	}
	detail, _ := json.Marshal(map[string]string{"parent_id": req.ParentID, "child_id": req.ChildID, "simulated": "true"})
	if _, err := preTx.ExecContext(ctx, `INSERT INTO intents(id,request_id,volume_id,operation,state,detail_json,created_at,updated_at) VALUES(?,?,?,?, 'pending',?,?,?)`,
		intent, req.RequestID, req.VolumeID, req.Operation, string(detail), now, now); err != nil {
		preTx.Rollback()
		return apperr.Internal(err)
	}
	if err := preTx.Commit(); err != nil {
		return apperr.Internal(err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return apperr.Internal(err)
	}
	defer tx.Rollback()
	switch req.Operation {
	case "merge":
		var parent, child string
		err := tx.QueryRowContext(ctx, `SELECT COALESCE(parent_id,''),id FROM snapshots WHERE volume_id=? AND COALESCE(parent_id,'')<>'' 
			AND (?='' OR parent_id=?) AND (?='' OR id=?) ORDER BY ordinal LIMIT 1`,
			req.VolumeID, req.ParentID, req.ParentID, req.ChildID, req.ChildID).Scan(&parent, &child)
		if err != nil {
			return apperr.WithContext(apperr.New(apperr.CodeSnapshotParent, "no merge parent/child is available for crash simulation", 400), "volume_id", req.VolumeID)
		}
		// A deliberate partial rewrite of the parent's delta. It is never
		// visible because the process exits before COMMIT.
		if _, err := tx.ExecContext(ctx, `DELETE FROM snapshot_refs WHERE volume_id=? AND snapshot_id=?`, req.VolumeID, parent); err != nil {
			return apperr.Internal(err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE snapshots SET weight=weight+1 WHERE volume_id=? AND id=?`, req.VolumeID, parent); err != nil {
			return apperr.Internal(err)
		}
	case "rollback":
		if _, err := tx.ExecContext(ctx, `UPDATE blocks SET version=version+1 WHERE volume_id=? AND block_no=(SELECT MIN(block_no) FROM blocks WHERE volume_id=?)`, req.VolumeID, req.VolumeID); err != nil {
			return apperr.Internal(err)
		}
	}
	crash(86)
	return errors.New("crash callback returned; transaction was rolled back")
}
