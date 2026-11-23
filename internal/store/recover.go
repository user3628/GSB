package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type RecoveryResult struct {
	ReportID     string
	Report       map[string]any
	PendingCount int
}

// RecoverPending inspects only committed SQLite facts. It first persists a
// recovery intent, then atomically marks each pending batch aborted and writes
// the recovery report. It never guesses what an interrupted process intended.
func (s *Store) RecoverPending(ctx context.Context) (*RecoveryResult, error) {
	pending, err := s.ListPendingIntents(ctx)
	if err != nil {
		return nil, err
	}
	if len(pending) == 0 {
		return nil, nil
	}
	now := time.Now().UTC().UnixMilli()
	reportID := fmt.Sprintf("recovery-%d", now)
	activeVersion, err := s.ActiveVersion(ctx)
	if err != nil {
		return nil, err
	}
	intentFacts := make([]map[string]any, 0, len(pending))
	aborted := make([]map[string]any, 0, len(pending))
	for _, in := range pending {
		var snapshotCount int
		_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots WHERE snapshot_version = ?`, int64(in.SnapshotVersion)).Scan(&snapshotCount)
		facts := map[string]any{
			"intentId":                in.IntentID,
			"requestId":               in.RequestID,
			"kind":                    in.Kind,
			"snapshotVersion":         in.SnapshotVersion,
			"observedStatus":          in.Status,
			"createdAtMs":             in.CreatedAtMs,
			"persistedFacts":          in.Facts,
			"snapshotRowPresent":      snapshotCount > 0,
			"snapshotVersionIsActive": in.SnapshotVersion > 0 && in.SnapshotVersion == activeVersion,
		}
		intentFacts = append(intentFacts, facts)
		aborted = append(aborted, map[string]any{
			"intentId":        in.IntentID,
			"requestId":       in.RequestID,
			"snapshotVersion": in.SnapshotVersion,
			"action":          "mark_aborted",
			"reason":          "pending intent has no committed snapshot row and is not the active snapshot version",
		})
	}
	integrity, err := s.inspectActiveIntegrity(ctx, activeVersion)
	if err != nil {
		return nil, err
	}
	report := map[string]any{
		"reportId":                reportID,
		"recoveredAtMs":           now,
		"activeSnapshotVersion":   activeVersion,
		"activeSnapshotIntegrity": integrity,
		"pendingIntentCount":      len(pending),
		"pendingIntentsObserved":  intentFacts,
		"abortedBatches":          aborted,
		"factsSource":             "sqlite_committed_rows_only",
	}
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return nil, err
	}
	recoveryIntentID := "intent-" + reportID
	if err := s.CreateIntent(ctx, IntentRecord{
		IntentID: recoveryIntentID, RequestID: "", Kind: "recovery", SnapshotVersion: activeVersion,
		Status: "pending", CreatedAtMs: now, Facts: map[string]any{
			"recoveryReportId":   reportID,
			"pendingIntentCount": len(pending),
		},
	}); err != nil {
		return nil, fmt.Errorf("write recovery intent: %w", err)
	}
	tx, err := s.BeginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	for _, in := range pending {
		merged := make(map[string]any, len(in.Facts)+2)
		for k, v := range in.Facts {
			merged[k] = v
		}
		merged["recoveryReportId"] = reportID
		merged["recoveryAction"] = "aborted_pending_intent"
		mergedJSON, _ := json.Marshal(merged)
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status = 'aborted', completed_at_ms = ?, facts_json = ? WHERE intent_id = ? AND status = 'pending'`,
			now, string(mergedJSON), in.IntentID); err != nil {
			return nil, err
		}
		body, _ := json.Marshal(map[string]any{
			"error": map[string]any{
				"code":     "E_IMPORT_ABORTED",
				"message":  "import was interrupted before commit; recovery marked the batch aborted",
				"reportId": reportID,
			},
		})
		_, err := tx.ExecContext(ctx, `UPDATE request_registry SET status = 'aborted', http_status = 503, response_json = ?, report_id = ?, updated_at_ms = ?
			WHERE request_id = ? AND status = 'in_progress'`, string(body), reportID, now, in.RequestID)
		if err != nil {
			return nil, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_reports(report_id, created_at_ms, report_json) VALUES(?, ?, ?)`,
		reportID, now, string(reportJSON)); err != nil {
		return nil, err
	}
	recoveryFacts, _ := json.Marshal(map[string]any{
		"recoveryReportId": reportID, "abortedIntentCount": len(pending), "activeSnapshotVersion": activeVersion,
	})
	res, err := tx.ExecContext(ctx, `UPDATE intents SET status = 'completed', completed_at_ms = ?, facts_json = ? WHERE intent_id = ? AND status = 'pending'`,
		now, string(recoveryFacts), recoveryIntentID)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return nil, fmt.Errorf("recovery intent disappeared")
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &RecoveryResult{ReportID: reportID, Report: report, PendingCount: len(pending)}, nil
}

func (s *Store) inspectActiveIntegrity(ctx context.Context, activeVersion uint64) (map[string]any, error) {
	out := map[string]any{
		"activeSnapshotVersion": activeVersion,
		"complete":              true,
		"factsSource":           "sqlite_committed_rows",
	}
	if activeVersion == 0 {
		out["noActiveSnapshot"] = true
		return out, nil
	}
	var expectedPolicies, expectedRoutes, expectedWindows, expectedCircuits, expectedSummaries int
	err := s.db.QueryRowContext(ctx, `SELECT policy_count FROM snapshots WHERE snapshot_version = ?`, int64(activeVersion)).Scan(&expectedPolicies)
	if err == sql.ErrNoRows {
		out["complete"] = false
		out["snapshotRowPresent"] = false
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	out["snapshotRowPresent"] = true
	out["expectedPolicyCount"] = expectedPolicies
	var storedPolicies int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM policies WHERE snapshot_version = ?`, int64(activeVersion)).Scan(&storedPolicies); err != nil {
		return nil, err
	}
	out["storedPolicyCount"] = storedPolicies
	if storedPolicies != expectedPolicies {
		out["complete"] = false
	}
	err = s.db.QueryRowContext(ctx, `SELECT route_count, window_count, circuit_count, summary_count FROM snapshot_derivations WHERE snapshot_version = ?`, int64(activeVersion)).
		Scan(&expectedRoutes, &expectedWindows, &expectedCircuits, &expectedSummaries)
	if err == sql.ErrNoRows {
		out["derivationRowPresent"] = false
		out["complete"] = false
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	out["derivationRowPresent"] = true
	counts := []struct {
		table string
		want  int
		name  string
	}{
		{"route_index", expectedRoutes, "storedRouteCount"},
		{"rate_window_stats", expectedWindows, "storedRateWindowCount"},
		{"circuit_state", expectedCircuits, "storedCircuitCount"},
		{"service_summaries", expectedSummaries, "storedSummaryCount"},
	}
	for _, c := range counts {
		var got int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+c.table+` WHERE snapshot_version = ?`, int64(activeVersion)).Scan(&got); err != nil {
			return nil, err
		}
		out[c.name] = got
	}
	out["expectedRouteCount"] = expectedRoutes
	out["expectedRateWindowCount"] = expectedWindows
	out["expectedCircuitCount"] = expectedCircuits
	out["expectedSummaryCount"] = expectedSummaries
	if int(out["storedRouteCount"].(int)) != expectedRoutes ||
		int(out["storedRateWindowCount"].(int)) != expectedWindows ||
		int(out["storedCircuitCount"].(int)) != expectedCircuits ||
		int(out["storedSummaryCount"].(int)) != expectedSummaries {
		out["complete"] = false
	}
	return out, nil
}
