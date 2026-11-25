package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type OrphanCleanup struct {
	Reservoirs    int64 `json:"reservoirs"`
	Hotspots      int64 `json:"hotspots"`
	Samples       int64 `json:"samples"`
	BatchItems    int64 `json:"batch_items"`
	SampleBatches int64 `json:"sample_batches"`
	Policies      int64 `json:"policies"`
	ChannelHits   int64 `json:"channel_hits"`
}

type RecoveryReport struct {
	RecoveredAt           time.Time     `json:"recovered_at"`
	LastCompleteCommitSeq int64         `json:"last_complete_commit_seq"`
	CurrentPolicyVersion  uint64        `json:"current_policy_version"`
	ScannedIntents        int           `json:"scanned_intents"`
	RolledBackIntents     int           `json:"rolled_back_intents"`
	RepairedCommitted     int           `json:"repaired_committed_intents"`
	OrphansRemoved        OrphanCleanup `json:"orphans_removed"`
	IntegrityCheck        string        `json:"integrity_check"`
	RollbackTarget        string        `json:"rollback_target"`
	Complete              bool          `json:"complete"`
}

type pendingIntent struct {
	ID     int64
	Kind   string
	Target string
}

func (s *Store) Recover(ctx context.Context) (RecoveryReport, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RecoveryReport{}, err
	}
	defer tx.Rollback()

	report := RecoveryReport{
		RecoveredAt:    time.Now().UTC(),
		RollbackTarget: "state_meta.commit_seq",
	}
	rows, err := tx.QueryContext(ctx, `SELECT id, kind, target FROM intents WHERE state='pending' ORDER BY id`)
	if err != nil {
		return RecoveryReport{}, err
	}
	var pending []pendingIntent
	for rows.Next() {
		var item pendingIntent
		if err := rows.Scan(&item.ID, &item.Kind, &item.Target); err != nil {
			rows.Close()
			return RecoveryReport{}, err
		}
		pending = append(pending, item)
	}
	if err := rows.Close(); err != nil {
		return RecoveryReport{}, err
	}
	if err := rows.Err(); err != nil {
		return RecoveryReport{}, err
	}
	report.ScannedIntents = len(pending)

	for _, item := range pending {
		committed, err := intentTargetCommitted(ctx, tx, item)
		if err != nil {
			return RecoveryReport{}, err
		}
		finishedAt := time.Now().UTC().Format(time.RFC3339Nano)
		if committed {
			if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='committed', finished_at=? WHERE id=?`, finishedAt, item.ID); err != nil {
				return RecoveryReport{}, err
			}
			report.RepairedCommitted++
		} else {
			if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='rolled_back', finished_at=? WHERE id=?`, finishedAt, item.ID); err != nil {
				return RecoveryReport{}, err
			}
			report.RolledBackIntents++
		}
	}

	state, err := currentStateTx(ctx, tx)
	if err != nil {
		return RecoveryReport{}, err
	}
	report.LastCompleteCommitSeq = state.CommitSeq
	report.CurrentPolicyVersion = state.CurrentPolicyVersion

	cleanup, err := cleanupOrphans(ctx, tx, state.CommitSeq)
	if err != nil {
		return RecoveryReport{}, err
	}
	report.OrphansRemoved = cleanup
	report.IntegrityCheck = "ok"
	var integrity string
	if err := tx.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&integrity); err != nil {
		return RecoveryReport{}, err
	}
	if integrity != "ok" {
		report.IntegrityCheck = integrity
		report.Complete = false
	} else {
		report.Complete = true
	}
	reportJSON, _ := json.Marshal(report)
	if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_reports(recovered_at, report_json) VALUES(?,?)`, report.RecoveredAt.Format(time.RFC3339Nano), string(reportJSON)); err != nil {
		return RecoveryReport{}, err
	}
	if err := tx.Commit(); err != nil {
		return RecoveryReport{}, err
	}
	if !report.Complete {
		return report, fmt.Errorf("sqlite integrity check failed: %s", report.IntegrityCheck)
	}
	return report, nil
}

func intentTargetCommitted(ctx context.Context, tx *sql.Tx, item pendingIntent) (bool, error) {
	switch item.Kind {
	case "sample_batch":
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sample_batches WHERE batch_id=?)`, item.Target).Scan(&exists); err != nil {
			return false, err
		}
		return exists == 1, nil
	case "policy_snapshot", "policy_increment":
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM policy_snapshots WHERE version=CAST(? AS INTEGER))`, item.Target).Scan(&exists); err != nil {
			return false, err
		}
		return exists == 1, nil
	default:
		return false, nil
	}
}

func cleanupOrphans(ctx context.Context, tx *sql.Tx, commitSeq int64) (OrphanCleanup, error) {
	var cleanup OrphanCleanup
	deletes := []struct {
		query string
		dest  *int64
	}{
		{`DELETE FROM reservoirs WHERE commit_seq > ?`, &cleanup.Reservoirs},
		{`DELETE FROM hotspot_runs WHERE commit_seq > ?`, &cleanup.Hotspots},
		{`DELETE FROM sample_batches WHERE commit_seq > ?`, &cleanup.SampleBatches},
		{`DELETE FROM samples WHERE first_commit_seq > ?`, &cleanup.Samples},
		{`DELETE FROM policy_snapshots WHERE commit_seq > ?`, &cleanup.Policies},
		{`DELETE FROM sample_batch_items WHERE batch_id NOT IN (SELECT batch_id FROM sample_batches)`, &cleanup.BatchItems},
		{`DELETE FROM sample_channel_hits WHERE sample_id NOT IN (SELECT sample_id FROM samples)`, &cleanup.ChannelHits},
	}
	for i, deletion := range deletes {
		var result sql.Result
		var err error
		if i == 5 || i == 6 {
			result, err = tx.ExecContext(ctx, deletion.query)
		} else {
			result, err = tx.ExecContext(ctx, deletion.query, commitSeq)
		}
		if err != nil {
			return cleanup, err
		}
		affected, _ := result.RowsAffected()
		*deletion.dest = affected
	}
	return cleanup, nil
}

func (s *Store) LatestRecoveryReport(ctx context.Context) (RecoveryReport, error) {
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT report_json FROM recovery_reports ORDER BY id DESC LIMIT 1`).Scan(&raw); err != nil {
		return RecoveryReport{}, err
	}
	var report RecoveryReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return RecoveryReport{}, err
	}
	return report, nil
}
