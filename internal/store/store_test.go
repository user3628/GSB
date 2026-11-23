package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecoveryRollsBackArtifactsAndReportsFacts(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "engine.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	requestID := "store-recovery-rollback"
	if _, err := st.BeginAttempt(ctx, requestID, "fingerprint", `{"request_id":"store-recovery-rollback"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO job_plans(request_id, partition_count, join_type, algorithm, path, merge_rule, tolerance, skew_threshold, fixed_scale_bits, route_json, skew_json, error_declaration_json)
		VALUES(?, 1, 'inner', 'naive', 'fast', 'partition_ascending', 1e-6, 0.35, 0, '{}', '{}', '{}')`, requestID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO partitions(request_id, partition, left_records, right_records, record_count, joined_records, bytes_estimate, record_share, skewed, affected_keys_json)
		VALUES(?, 0, 1, 1, 2, 1, 32, 1, 1, '[1]')`, requestID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO partial_sums(request_id, partition, group_key, partial_sum, compensation, fixed_point, group_count, value_count, abs_sum, compensation_digest)
		VALUES(?, 0, 1, 1, 0, '', 1, 1, 1, 'digest')`, requestID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO group_results(request_id, group_key, sum_value, avg_value, group_count, value_count, abs_sum, abs_error_bound, rel_error_bound, compensation_digest, max_relative_deviation)
		VALUES(?, 1, 1, 1, 1, 1, 1, 0.1, 0.1, 'digest', 0)`, requestID); err != nil {
		t.Fatal(err)
	}
	report, err := st.RecoverRunning(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.PendingIntentsRemaining != 0 {
		t.Fatalf("pending intents remain: %+v", report)
	}
	if len(report.AbortedRequests) != 1 || report.AbortedRequests[0] != requestID {
		t.Fatalf("aborted request not reconstructed from DB: %+v", report.AbortedRequests)
	}
	foundRollbackEvent := false
	for _, event := range report.Events {
		if event.RequestID == requestID && event.Action == "aborted_uncommitted_attempt" && strings.Contains(event.Detail, `"rollback_applied":true`) {
			foundRollbackEvent = true
		}
	}
	if !foundRollbackEvent {
		t.Fatalf("missing persisted rollback event: %+v", report.Events)
	}
	for _, table := range []string{"job_plans", "partitions", "partial_sums", "group_results"} {
		count, err := countRequestRowsForTest(ctx, st, table, requestID)
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("recovery left %d rows in %s", count, table)
		}
	}
}

func countRequestRowsForTest(ctx context.Context, st *Store, table, requestID string) (int64, error) {
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	return countRequestRows(ctx, tx, table, requestID)
}
