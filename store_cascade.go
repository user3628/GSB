package feedbackstream

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
)

type PolicySwitch struct {
	IntentID       string
	BaseVersion    int64
	NewPolicy      *PolicySnapshot
	AffectedGroups []string
	Rebuilds       []GroupCommit
}

func placeholders(n int) string {
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}

func (s *Store) CommitPolicySwitch(ctx context.Context, switchReq PolicySwitch) (int64, error) {
	payload, err := json.Marshal(switchReq.NewPolicy)
	if err != nil {
		return 0, err
	}
	var invalidated int64
	err = s.WithTx(ctx, func(tx *sql.Tx) error {
		ts := nowMS()
		oldVersion := switchReq.NewPolicy.Version - 1
		if switchReq.BaseVersion > 0 {
			oldVersion = switchReq.BaseVersion
		}
		if _, err := tx.ExecContext(ctx, `UPDATE policies SET status='SUPERSEDED' WHERE version=? AND status='ACTIVE'`, oldVersion); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO policies(version,kind,base_version,status,checksum,payload_json,created_at_ms,activated_at_ms)
			VALUES(?,?,?, 'ACTIVE', ?,?,?,?)
			ON CONFLICT(version) DO UPDATE SET status='ACTIVE', payload_json=excluded.payload_json, checksum=excluded.checksum, activated_at_ms=excluded.activated_at_ms`,
			switchReq.NewPolicy.Version, switchReq.NewPolicy.Kind, switchReq.BaseVersion,
			switchReq.NewPolicy.ChecksumSHA256, string(payload), ts, ts); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES('active_policy_version',?)
			ON CONFLICT(key) DO UPDATE SET value=excluded.value`, switchReq.NewPolicy.Version); err != nil {
			return err
		}

		if len(switchReq.AffectedGroups) > 0 {
			groupArgs := make([]any, 0, len(switchReq.AffectedGroups)+1)
			groupArgs = append(groupArgs, oldVersion)
			for _, group := range switchReq.AffectedGroups {
				groupArgs = append(groupArgs, group)
			}
			inClause := placeholders(len(switchReq.AffectedGroups))
			res, err := tx.ExecContext(ctx, `UPDATE window_results SET status='INVALIDATED'
				WHERE policy_version=? AND fusion_group_id IN (`+inClause+`) AND status='COMMITTED'`, groupArgs...)
			if err != nil {
				return err
			}
			invalidated, _ = res.RowsAffected()
			bufferArgs := append([]any{}, groupArgs...)
			if _, err := tx.ExecContext(ctx, `DELETE FROM buffer_inflight
				WHERE policy_version=? AND fusion_group_id IN (`+inClause+`)`, bufferArgs...); err != nil {
				return err
			}
			waitingArgs := append([]any{}, groupArgs...)
			if _, err := tx.ExecContext(ctx, `UPDATE waiting_batches SET status='CASCADE_INVALIDATED', updated_at_ms=?
				WHERE policy_version=? AND target_group_id IN (`+inClause+`) AND status='WAITING'`,
				append([]any{ts}, waitingArgs...)...); err != nil {
				return err
			}
		}

		rebuiltRows := 0
		for _, rebuild := range switchReq.Rebuilds {
			outputJSON, err := json.Marshal(rebuild.Output)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO group_outputs(batch_id,policy_version,fusion_group_id,signature_hash,output_json,created_at_ms)
				VALUES(?,?,?,?,?,?)
				ON CONFLICT(batch_id,policy_version,fusion_group_id) DO NOTHING`,
				rebuild.BatchID, switchReq.NewPolicy.Version, rebuild.Group.ID, rebuild.Group.SignatureHash, string(outputJSON), ts); err != nil {
				return err
			}
			sourceIDs, _ := json.Marshal([]string{rebuild.SourceBatchID})
			for _, agg := range rebuild.Output.Aggregates {
				if _, err := tx.ExecContext(ctx, `
					INSERT INTO window_results(policy_version,fusion_group_id,signature_hash,partition_id,window_start_ms,window_end_ms,group_key,count,sum_fixed,min_fixed,max_fixed,status,source_batch_ids,committed_intent_id,committed_at_ms)
					VALUES(?,?,?,?,?,?,?,?,?,?,?, 'COMMITTED',?,?,?)
					ON CONFLICT(policy_version,fusion_group_id,signature_hash,partition_id,window_start_ms,window_end_ms,group_key)
					DO NOTHING`,
					switchReq.NewPolicy.Version, rebuild.Group.ID, rebuild.Group.SignatureHash, agg.PartitionID,
					agg.WindowStartMS, agg.WindowEndMS, agg.GroupKey, agg.Count, agg.Sum, agg.Min, agg.Max,
					string(sourceIDs), switchReq.IntentID, ts); err != nil {
					return err
				}
				rebuiltRows++
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO group_completions(batch_id,policy_version,fusion_group_id,signature_hash,status,window_result_count,committed_intent_id,completed_at_ms)
				VALUES(?,?,?,?, 'COMPLETED',?,?,?)
				ON CONFLICT(batch_id,policy_version,fusion_group_id) DO NOTHING`,
				rebuild.BatchID, switchReq.NewPolicy.Version, rebuild.Group.ID, rebuild.Group.SignatureHash,
				len(rebuild.Output.Aggregates), switchReq.IntentID, ts); err != nil {
				return err
			}
		}

		affectedJSON, _ := json.Marshal(switchReq.AffectedGroups)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO cascade_events(intent_id,old_policy_version,new_policy_version,affected_groups,invalidated_window_rows,rebuilt_window_rows,created_at_ms)
			VALUES(?,?,?,?,?,?,?)`,
			switchReq.IntentID, oldVersion, switchReq.NewPolicy.Version, string(affectedJSON), invalidated, rebuiltRows, ts); err != nil {
			return err
		}
		return CompleteIntentTx(ctx, tx, switchReq.IntentID, IntentCommitted)
	})
	return invalidated, err
}
