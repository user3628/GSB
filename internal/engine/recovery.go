package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"checkpointengine/internal/model"
)

type recoveryDetails struct {
	Pools           []model.PoolUsage    `json:"pools"`
	Consistency     []model.RoundSummary `json:"consistency"`
	Intents         []model.Intent       `json:"intents"`
	MissingBlobs    int                  `json:"missing_blobs"`
	InvalidBlobs    int                  `json:"invalid_blobs"`
	PendingWrites   int                  `json:"pending_writes"`
	CommittedWrites int                  `json:"committed_writes"`
	Note            string               `json:"note"`
}

func (e *Engine) Recover(ctx context.Context) (model.RecoveryReport, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	stateBefore, err := loadStateDB(ctx, e.store.DB)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	pending, err := e.pendingIntents(ctx, false)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	// A previous recovery may itself have been interrupted. It is intentionally
	// re-runnable and has no data mutation of its own.
	var actionable []model.Intent
	for _, in := range pending {
		if in.Kind == "RECOVERY" {
			continue
		}
		actionable = append(actionable, in)
	}
	intentIDs, err := e.insertIntents(ctx, []intentSpec{{Kind: "RECOVERY", SubmissionID: "startup",
		After: map[string]any{"interrupted_intents": len(actionable), "db_revision": stateBefore.Revision}}})
	if err != nil {
		return model.RecoveryReport{}, err
	}

	tx, err := e.store.DB.BeginTx(ctx, nil)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	defer tx.Rollback()
	now := e.nowText()
	rolledWrites, rolledEvictions, abortedSubmissions := 0, 0, 0
	for _, in := range actionable {
		switch in.Kind {
		case "WRITE":
			var after writeIntentJSON
			if err := json.Unmarshal([]byte(in.AfterJSON), &after); err != nil {
				return model.RecoveryReport{}, fmt.Errorf("decode write intent %d: %w", in.ID, err)
			}
			var durable int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM write_records WHERE round=? AND shard_id=? AND digest=?`,
				after.Round, after.ShardID, after.Digest).Scan(&durable); err != nil {
				return model.RecoveryReport{}, err
			}
			if durable == 0 {
				if _, err := tx.ExecContext(ctx, `UPDATE shards SET write_state='PENDING', updated_at=? WHERE round=? AND shard_id=?`,
					now, after.Round, after.ShardID); err != nil {
					return model.RecoveryReport{}, err
				}
			}
			rolledWrites++
		case "EVICT":
			var after evictionIntentJSON
			if err := json.Unmarshal([]byte(in.AfterJSON), &after); err != nil {
				return model.RecoveryReport{}, fmt.Errorf("decode eviction intent %d: %w", in.ID, err)
			}
			if after.Before.ShardID != "" {
				if _, err := tx.ExecContext(ctx, `UPDATE shards SET location=?, desired_pool=?, write_state=?, updated_at=?
					WHERE round=? AND shard_id=?`, after.Before.Location, after.Before.DesiredPool,
					after.Before.WriteState, now, after.Before.Round, after.Before.ShardID); err != nil {
					return model.RecoveryReport{}, err
				}
			}
			rolledEvictions++
		case "CHECKPOINT", "SNAPSHOT", "PRECISION_VERIFY":
			abortedSubmissions++
		}
	}
	if len(actionable) > 0 {
		_, err = tx.ExecContext(ctx, `UPDATE intents SET state='ABORTED', applied_at=?
			WHERE state='PENDING' AND kind IN ('WRITE','EVICT','CHECKPOINT','SNAPSHOT','PRECISION_VERIFY')`, now)
		if err != nil {
			return model.RecoveryReport{}, err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE intents SET state='ABORTED', applied_at=? WHERE state='PENDING' AND kind='RECOVERY' AND intent_id<>?`,
		now, intentIDs[len(intentIDs)-1])
	if err != nil {
		return model.RecoveryReport{}, err
	}

	missingBlobs, invalidBlobs, err := revokeInvalidWriteRecordsTx(ctx, tx, now)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE shards SET write_state='PENDING', updated_at=?
		WHERE NOT EXISTS (SELECT 1 FROM write_records w WHERE w.round=shards.round AND w.shard_id=shards.shard_id AND w.digest=shards.digest)`,
		now); err != nil {
		return model.RecoveryReport{}, err
	}
	state, err := loadStateTx(ctx, tx)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	var policy model.Snapshot
	if state.CurrentPolicyVersion > 0 {
		p, err := loadPolicyTx(ctx, tx, state.CurrentPolicyVersion)
		if err != nil {
			return model.RecoveryReport{}, err
		}
		policy = p.Snapshot
	}
	pools, err := rebuildPoolUsageTx(ctx, tx, state.Revision, now)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	if state.CurrentPolicyVersion > 0 {
		for i := range pools {
			if pools[i].Pool == model.PoolVRAM {
				pools[i].LimitBytes = policy.VRAMLimitBytes
			}
		}
	}
	summaries, err := rebuildAllDerivedTx(ctx, tx, policy, state.Revision, now)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.RecoveryReport{}, err
	}

	orphans, err := e.cleanupOrphanBlobs(ctx)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	durable, pendingCount, err := e.countDurability(ctx)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	details := recoveryDetails{
		Pools: pools, Consistency: summaries, Intents: actionable,
		MissingBlobs: missingBlobs, InvalidBlobs: invalidBlobs,
		PendingWrites: pendingCount, CommittedWrites: durable,
		Note: "Recovery is based only on committed SQLite transactions and verified durable blob manifests. Interrupted in-flight writes are never reported as completed.",
	}
	detailsJSON := encodeDetails(details)
	report := model.RecoveryReport{
		CreatedAt: now, DBRevision: state.Revision,
		FactsBasis:         "committed_sqlite_transactions_and_verified_blob_manifests",
		InterruptedIntents: len(actionable), RolledBackWrites: rolledWrites,
		RolledBackEvictions: rolledEvictions, AbortedSubmissions: abortedSubmissions,
		OrphanBlobsRemoved: orphans, PoolRebuilt: true, Pools: pools,
		Consistency: summaries, DurableShards: durable, PendingShards: pendingCount,
	}
	report.Details = map[string]any{
		"missing_blobs": missingBlobs, "invalid_blobs": invalidBlobs,
		"pending_writes": pendingCount, "committed_writes": durable,
		"note": details.Note, "intents": actionable,
		"report_scope":                       "restart_or_manual_recovery_scan",
		"in_flight_writes_reported_complete": false,
	}
	tx2, err := e.store.DB.BeginTx(ctx, nil)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	defer tx2.Rollback()
	res, err := tx2.ExecContext(ctx, `INSERT INTO recovery_reports(created_at, db_revision, facts_basis,
		interrupted_intents, rolled_back_writes, rolled_back_evictions, aborted_submissions,
		orphan_blobs_removed, pool_rebuilt, durable_shards, pending_shards, details_json)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, report.CreatedAt, report.DBRevision, report.FactsBasis,
		report.InterruptedIntents, report.RolledBackWrites, report.RolledBackEvictions,
		report.AbortedSubmissions, report.OrphanBlobsRemoved, 1, report.DurableShards,
		report.PendingShards, detailsJSON)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	report.ReportID, err = res.LastInsertId()
	if err != nil {
		return model.RecoveryReport{}, err
	}
	if err := markIntentsApplied(ctx, tx2, intentIDs, now); err != nil {
		return model.RecoveryReport{}, err
	}
	if err := tx2.Commit(); err != nil {
		return model.RecoveryReport{}, err
	}
	return report, nil
}

func (e *Engine) pendingIntents(ctx context.Context, includeRecovery bool) ([]model.Intent, error) {
	query := `SELECT intent_id, kind, submission_id, state, before_json, after_json, created_at, applied_at
		FROM intents WHERE state='PENDING'`
	if !includeRecovery {
		query += ` AND kind<>'RECOVERY'`
	}
	query += ` ORDER BY intent_id`
	rows, err := e.store.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Intent
	for rows.Next() {
		var in model.Intent
		if err := rows.Scan(&in.ID, &in.Kind, &in.SubmissionID, &in.State, &in.BeforeJSON,
			&in.AfterJSON, &in.CreatedAt, &in.AppliedAt); err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

func revokeInvalidWriteRecordsTx(ctx context.Context, tx *sql.Tx, now string) (int, int, error) {
	rows, err := tx.QueryContext(ctx, `SELECT record_id, round, shard_id, digest, blob_path FROM write_records ORDER BY record_id`)
	if err != nil {
		return 0, 0, err
	}
	type rec struct {
		id, round           int64
		shard, digest, path string
	}
	var records []rec
	for rows.Next() {
		var r rec
		if err := rows.Scan(&r.id, &r.round, &r.shard, &r.digest, &r.path); err != nil {
			rows.Close()
			return 0, 0, err
		}
		records = append(records, r)
	}
	if err := rows.Close(); err != nil {
		return 0, 0, err
	}
	missing, invalid := 0, 0
	for _, r := range records {
		data, err := os.ReadFile(r.path)
		if err != nil {
			missing++
			if _, err := tx.ExecContext(ctx, `DELETE FROM write_records WHERE record_id=?`, r.id); err != nil {
				return 0, 0, err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE shards SET write_state='PENDING', updated_at=? WHERE round=? AND shard_id=? AND digest=?`,
				now, r.round, r.shard, r.digest); err != nil {
				return 0, 0, err
			}
			continue
		}
		var manifest blobManifest
		if err := json.Unmarshal(data, &manifest); err != nil || manifest.Digest != r.digest || manifest.ShardID != r.shard || int64(manifest.Round) != r.round {
			invalid++
			if _, err := tx.ExecContext(ctx, `DELETE FROM write_records WHERE record_id=?`, r.id); err != nil {
				return 0, 0, err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE shards SET write_state='PENDING', updated_at=? WHERE round=? AND shard_id=? AND digest=?`,
				now, r.round, r.shard, r.digest); err != nil {
				return 0, 0, err
			}
		}
	}
	return missing, invalid, nil
}

func (e *Engine) cleanupOrphanBlobs(ctx context.Context) (int, error) {
	rows, err := e.store.DB.QueryContext(ctx, `SELECT blob_path FROM write_records`)
	if err != nil {
		return 0, err
	}
	referenced := make(map[string]bool)
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return 0, err
		}
		referenced[filepath.Clean(p)] = true
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(e.blobDir)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") && !strings.HasPrefix(name, ".write-") {
			continue
		}
		path := filepath.Join(e.blobDir, name)
		if referenced[filepath.Clean(path)] {
			continue
		}
		if err := os.Remove(path); err == nil {
			removed++
		}
	}
	return removed, nil
}

func (e *Engine) countDurability(ctx context.Context) (int, int, error) {
	var durable int
	if err := e.store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM write_records`).Scan(&durable); err != nil {
		return 0, 0, err
	}
	var pending int
	if err := e.store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM shards WHERE write_state='PENDING'`).Scan(&pending); err != nil {
		return 0, 0, err
	}
	return durable, pending, nil
}

func (e *Engine) RecoveryReport(ctx context.Context) (model.RecoveryReport, error) {
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return model.RecoveryReport{}, err
	}
	defer tx.Rollback()
	var report model.RecoveryReport
	var detailsJSON string
	err = tx.QueryRowContext(ctx, `SELECT report_id, created_at, db_revision, facts_basis,
		interrupted_intents, rolled_back_writes, rolled_back_evictions, aborted_submissions,
		orphan_blobs_removed, pool_rebuilt, durable_shards, pending_shards, details_json
		FROM recovery_reports ORDER BY report_id DESC LIMIT 1`).Scan(
		&report.ReportID, &report.CreatedAt, &report.DBRevision, &report.FactsBasis,
		&report.InterruptedIntents, &report.RolledBackWrites, &report.RolledBackEvictions,
		&report.AbortedSubmissions, &report.OrphanBlobsRemoved, &report.PoolRebuilt,
		&report.DurableShards, &report.PendingShards, &detailsJSON,
	)
	if isNoRows(err) {
		return report, model.NewAPIError(model.CodeNotFound, "no recovery report has been generated")
	}
	if err != nil {
		return report, err
	}
	var details recoveryDetails
	if err := json.Unmarshal([]byte(detailsJSON), &details); err != nil {
		return report, err
	}
	report.Pools = details.Pools
	report.Consistency = details.Consistency
	report.Details = map[string]any{
		"missing_blobs": details.MissingBlobs, "invalid_blobs": details.InvalidBlobs,
		"pending_writes": details.PendingWrites, "committed_writes": details.CommittedWrites,
		"note": details.Note, "intents": details.Intents,
		"report_scope":                       "restart_or_manual_recovery_scan",
		"in_flight_writes_reported_complete": false,
	}
	sort.Slice(report.Consistency, func(i, j int) bool { return report.Consistency[i].Round < report.Consistency[j].Round })
	return report, nil
}
