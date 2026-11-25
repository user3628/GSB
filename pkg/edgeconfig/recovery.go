package edgeconfig

import (
	"context"
	"encoding/json"
	"strconv"
)

func (s *Service) ListIntents(ctx context.Context, status string) ([]Intent, error) {
	query := `SELECT intent_id, kind, idempotency_key, request_hash, target_version, status,
	                 payload_json, recovery_action, created_at_ms, updated_at_ms FROM intents`
	args := make([]any, 0)
	if status != "" {
		query += ` WHERE status=?`
		args = append(args, status)
	}
	query += ` ORDER BY created_at_ms, intent_id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	intents := make([]Intent, 0)
	for rows.Next() {
		var intent Intent
		if err := rows.Scan(&intent.IntentID, &intent.Kind, &intent.IdempotencyKey, &intent.RequestHash,
			&intent.TargetVersion, &intent.Status, &intent.PayloadJSON, &intent.RecoveryAction,
			&intent.CreatedAtMS, &intent.UpdatedAtMS); err != nil {
			return nil, err
		}
		intents = append(intents, intent)
	}
	return intents, rows.Err()
}

func (s *Service) Recover(ctx context.Context) (*RecoveryReport, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	pending, err := s.ListIntents(ctx, IntentPending)
	if err != nil {
		return nil, err
	}
	var lastComplete int64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(commit_version), 0) FROM expected_views`).Scan(&lastComplete); err != nil {
		return nil, err
	}

	report := &RecoveryReport{
		RecoveredAtMS:       s.NowMS(),
		LastCompleteVersion: lastComplete,
		PendingIntents:      pending,
		AbortedIntentIDs:    make([]string, 0, len(pending)),
		Actions:             make([]string, 0),
		PersistedFactsOnly:  true,
	}
	if len(pending) == 0 {
		report.RolledBackToVersion = lastComplete
		report.Actions = append(report.Actions, "no pending intents; convergence view already at last complete version")
		raw, _ := json.Marshal(report)
		_, err := s.db.ExecContext(ctx, `INSERT INTO recovery_runs(report_json, created_at_ms) VALUES(?, ?)`, string(raw), report.RecoveredAtMS)
		return report, err
	}

	topology, err := s.LoadTopology(ctx)
	if err != nil {
		return nil, err
	}
	policy := withDefaultPolicy(topology.Policy)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	for _, intent := range pending {
		action := "abort_unfinished_intent"
		switch intent.Kind {
		case "increment", "snapshot":
			action = "commit_transaction_absent; marked_increment_or_snapshot_aborted"
		case "affected_recompute":
			action = "affected_recompute_interrupted; convergence_view_rolled_back"
			report.RolledBackToVersion = lastComplete
		case "probe_round":
			action = "probe_round_interrupted; atomic_round_not_visible; convergence_view_rolled_back"
			report.RolledBackToVersion = lastComplete
		default:
			action = "unfinished_write_marked_aborted; convergence_view_checked"
			report.RolledBackToVersion = lastComplete
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE intents SET status=?, recovery_action=?, updated_at_ms=? WHERE intent_id=? AND status=?`,
			IntentAborted, action, report.RecoveredAtMS, intent.IntentID, IntentPending); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		report.AbortedIntentIDs = append(report.AbortedIntentIDs, intent.IntentID)
		report.Actions = append(report.Actions, intent.IntentID+": "+action)
	}

	if lastComplete > 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO node_convergence(
				node_id, region, expected_version, expected_digest, updated_version, updated_at_ms
			)
			SELECT ev.node_id, ev.region, ev.expected_version, ev.expected_digest, ev.commit_version, ?
			FROM expected_views ev
			WHERE ev.commit_version=?
			ON CONFLICT(node_id) DO UPDATE SET
				region=excluded.region,
				expected_version=excluded.expected_version,
				expected_digest=excluded.expected_digest,
				updated_version=excluded.updated_version,
				updated_at_ms=excluded.updated_at_ms`,
			report.RecoveredAtMS, lastComplete); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		if _, err := refreshAndRecomputeTx(ctx, tx, lastComplete, "", topology.Nodes, policy); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		report.Actions = append(report.Actions, "rebuilt expected fields and consistency from complete version "+strconv.FormatInt(lastComplete, 10))
	} else {
		report.RolledBackToVersion = 0
		report.Actions = append(report.Actions, "no complete expected view exists; left only persisted node/report facts")
	}

	raw, err := json.Marshal(report)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_runs(report_json, created_at_ms) VALUES(?, ?)`, string(raw), report.RecoveredAtMS); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return report, nil
}

func (s *Service) CreatePendingIntent(ctx context.Context, kind, idempotencyKey string, targetVersion int64, payload any) (*Intent, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if idempotencyKey == "" {
		idempotencyKey = kind + ":" + newRandomID("crash")
	}
	hash, _ := RequestHash(payload)
	now := s.NowMS()
	intent := Intent{
		IntentID:       "intent_" + idempotencyKey,
		Kind:           kind,
		IdempotencyKey: idempotencyKey,
		RequestHash:    hash,
		TargetVersion:  targetVersion,
		Status:         IntentPending,
		PayloadJSON:    jsonString(payload),
		CreatedAtMS:    now,
		UpdatedAtMS:    now,
	}
	if err := insertIntent(ctx, s.db, intent); err != nil {
		return nil, err
	}
	return &intent, nil
}

func jsonString(value any) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}
