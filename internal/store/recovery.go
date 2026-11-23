package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type RecoveryIntent struct {
	IntentID      string `json:"intent_id"`
	Kind          string `json:"kind"`
	TargetVersion uint64 `json:"target_version"`
	PreviousState string `json:"previous_state"`
	Reason        string `json:"reason"`
}

type RecoveryReport struct {
	GeneratedAt      time.Time        `json:"generated_at"`
	PendingFound     int              `json:"pending_found"`
	Aborted          []RecoveryIntent `json:"aborted"`
	Completed        []RecoveryIntent `json:"completed"`
	CleanedVersions  []uint64         `json:"cleaned_versions"`
	CurrentVersion   uint64           `json:"current_version"`
	Consistent       bool             `json:"consistent"`
	Facts            map[string]any   `json:"facts"`
	RecoveryStrategy string           `json:"recovery_strategy"`
}

type pendingIntent struct {
	id     string
	kind   string
	target uint64
}

// Recover inspects only persisted SQLite state. A pending intent is either
// completed when its entire target version is already present, or aborted and
// left as an audit record. Incomplete version fragments are removed and the
// current pointer is moved to the newest complete version.
func (s *Store) Recover(ctx context.Context) (RecoveryReport, error) {
	report := RecoveryReport{
		GeneratedAt: time.Now().UTC(), Aborted: []RecoveryIntent{}, Completed: []RecoveryIntent{},
		CleanedVersions: []uint64{}, Facts: map[string]any{},
		RecoveryStrategy: "scan pending intents, then select newest fully materialized snapshot",
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT id, kind, target_version FROM intents WHERE status = 'pending' ORDER BY started_at, id`)
	if err != nil {
		return report, err
	}
	var pending []pendingIntent
	for rows.Next() {
		var p pendingIntent
		if err := rows.Scan(&p.id, &p.kind, &p.target); err != nil {
			rows.Close()
			return report, err
		}
		pending = append(pending, p)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return report, err
	}
	rows.Close()
	report.PendingFound = len(pending)

	for _, p := range pending {
		complete, err := completeVersionTx(ctx, tx, p.target)
		if err != nil {
			return report, err
		}
		current, err := currentVersionTx(ctx, tx)
		if err != nil {
			return report, err
		}
		item := RecoveryIntent{IntentID: p.id, Kind: p.kind, TargetVersion: p.target}
		if complete && current == p.target {
			item.PreviousState = "completed"
			item.Reason = "target version is fully materialized and referenced by the current-version pointer"
			report.Completed = append(report.Completed, item)
			if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='completed', error='', finished_at=? WHERE id=?`, formatTime(time.Now()), p.id); err != nil {
				return report, err
			}
		} else {
			item.PreviousState = "aborted"
			item.Reason = "process stopped after the durable intent was written but before a complete target version became current"
			report.Aborted = append(report.Aborted, item)
			if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='aborted', error=?, finished_at=? WHERE id=?`,
				item.Reason, formatTime(time.Now()), p.id); err != nil {
				return report, err
			}
		}
	}
	originalCurrent, err := currentVersionTx(ctx, tx)
	if err != nil {
		return report, err
	}

	orphanOps, orphanDerived, orphanDegr, err := orphanCountsTx(ctx, tx)
	if err != nil {
		return report, err
	}
	if orphanOps+orphanDerived+orphanDegr > 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM operators WHERE version NOT IN (SELECT version FROM snapshot_versions)`); err != nil {
			return report, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM derived_indexes WHERE version NOT IN (SELECT version FROM snapshot_versions)`); err != nil {
			return report, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM degradations WHERE version NOT IN (SELECT version FROM snapshot_versions)`); err != nil {
			return report, err
		}
	}

	allVersions, err := versionListTx(ctx, tx)
	if err != nil {
		return report, err
	}
	for _, version := range allVersions {
		complete, err := completeVersionTx(ctx, tx, version)
		if err != nil {
			return report, err
		}
		if complete {
			continue
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM snapshot_versions WHERE version = ?`, version); err != nil {
			return report, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM operators WHERE version = ?`, version); err != nil {
			return report, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM derived_indexes WHERE version = ?`, version); err != nil {
			return report, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM degradations WHERE version = ?`, version); err != nil {
			return report, err
		}
		report.CleanedVersions = append(report.CleanedVersions, version)
	}

	latest, err := latestCompleteVersionTx(ctx, tx)
	if err != nil {
		return report, err
	}
	report.CurrentVersion = latest
	pointerRepaired := originalCurrent != latest
	if latest == 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM meta WHERE key='current_version'`); err != nil {
			return report, err
		}
	} else {
		if _, err := tx.ExecContext(ctx, `INSERT INTO meta(key, value) VALUES('current_version', ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value`, fmt.Sprintf("%d", latest)); err != nil {
			return report, err
		}
	}

	committedVersions, err := versionListTx(ctx, tx)
	if err != nil {
		return report, err
	}
	var remainingPending int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM intents WHERE status='pending'`).Scan(&remainingPending); err != nil {
		return report, err
	}
	report.Facts = map[string]any{
		"pending_intents_found":           report.PendingFound,
		"pending_intents_remaining":       remainingPending,
		"committed_versions":              len(committedVersions),
		"committed_version_numbers":       committedVersions,
		"orphan_operator_rows_cleaned":    orphanOps,
		"orphan_derived_rows_cleaned":     orphanDerived,
		"orphan_degradation_rows_cleaned": orphanDegr,
		"current_pointer_repaired":        pointerRepaired,
		"current_pointer_before":          originalCurrent,
		"current_pointer_after":           latest,
	}
	report.Consistent = remainingPending == 0
	if latest > 0 {
		complete, err := completeVersionTx(ctx, tx, latest)
		if err != nil {
			return report, err
		}
		report.Consistent = report.Consistent && complete
	}

	if err := persistRecoveryFactsTx(ctx, tx, report); err != nil {
		return report, err
	}
	if pointerRepaired {
		payload, _ := json.Marshal(map[string]any{"event": "current_pointer_repaired", "from": originalCurrent, "to": latest})
		if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_events(event_json, created_at) VALUES (?, ?)`, string(payload), formatTime(report.GeneratedAt)); err != nil {
			return report, err
		}
	}
	if err := tx.Commit(); err != nil {
		return report, err
	}
	return report, nil
}

func (s *Store) LatestRecoveryReport(ctx context.Context) (*RecoveryReport, error) {
	var payload string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM recovery_reports ORDER BY id DESC LIMIT 1`).Scan(&payload)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var report RecoveryReport
	if err := json.Unmarshal([]byte(payload), &report); err != nil {
		return nil, err
	}
	return &report, nil
}

func persistRecoveryFactsTx(ctx context.Context, tx *sql.Tx, report RecoveryReport) error {
	for _, item := range report.Aborted {
		payload, _ := json.Marshal(map[string]any{"event": "intent_aborted", "detail": item})
		if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_events(event_json, created_at) VALUES (?, ?)`, string(payload), formatTime(report.GeneratedAt)); err != nil {
			return err
		}
	}
	for _, item := range report.Completed {
		payload, _ := json.Marshal(map[string]any{"event": "intent_completed", "detail": item})
		if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_events(event_json, created_at) VALUES (?, ?)`, string(payload), formatTime(report.GeneratedAt)); err != nil {
			return err
		}
	}
	for _, version := range report.CleanedVersions {
		payload, _ := json.Marshal(map[string]any{"event": "incomplete_version_removed", "version": version})
		if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_events(event_json, created_at) VALUES (?, ?)`, string(payload), formatTime(report.GeneratedAt)); err != nil {
			return err
		}
	}
	payload, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO recovery_reports(report_json, created_at) VALUES (?, ?)`,
		string(payload), formatTime(report.GeneratedAt))
	return err
}

func currentVersionTx(ctx context.Context, tx *sql.Tx) (uint64, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='current_version'`).Scan(&raw)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var version uint64
	_, err = fmt.Sscanf(raw, "%d", &version)
	return version, err
}

func completeVersionTx(ctx context.Context, tx *sql.Tx, version uint64) (bool, error) {
	if version == 0 {
		return false, nil
	}
	var nodeCount int
	var root, treeHash string
	err := tx.QueryRowContext(ctx, `SELECT node_count, root_id, tree_hash FROM snapshot_versions WHERE version=?`, version).Scan(&nodeCount, &root, &treeHash)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if nodeCount <= 0 || root == "" || treeHash == "" {
		return false, nil
	}
	var storedNodes int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operators WHERE version=?`, version).Scan(&storedNodes); err != nil {
		return false, err
	}
	if storedNodes != nodeCount {
		return false, nil
	}
	var kinds int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(DISTINCT kind) FROM derived_indexes WHERE version=? AND kind IN
		('tree_summary','column_index','predicate_index','cost_estimate')`, version).Scan(&kinds); err != nil {
		return false, err
	}
	return kinds == 4, nil
}

func versionListTx(ctx context.Context, tx *sql.Tx) ([]uint64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT version FROM snapshot_versions ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uint64
	for rows.Next() {
		var version uint64
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		out = append(out, version)
	}
	return out, rows.Err()
}

func latestCompleteVersionTx(ctx context.Context, tx *sql.Tx) (uint64, error) {
	versions, err := versionListTx(ctx, tx)
	if err != nil {
		return 0, err
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] > versions[j] })
	for _, version := range versions {
		complete, err := completeVersionTx(ctx, tx, version)
		if err != nil {
			return 0, err
		}
		if complete {
			return version, nil
		}
	}
	return 0, nil
}

func orphanCountsTx(ctx context.Context, tx *sql.Tx) (int, int, int, error) {
	count := func(table string) (int, error) {
		var n int
		err := tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE version NOT IN (SELECT version FROM snapshot_versions)`, table)).Scan(&n)
		return n, err
	}
	ops, err := count("operators")
	if err != nil {
		return 0, 0, 0, err
	}
	derived, err := count("derived_indexes")
	if err != nil {
		return 0, 0, 0, err
	}
	degr, err := count("degradations")
	if err != nil {
		return 0, 0, 0, err
	}
	return ops, derived, degr, nil
}
