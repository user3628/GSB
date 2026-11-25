package feedbackstream

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

func (s *Store) SaveRoundPlan(ctx context.Context, plan *RoundPlan, policyVersion int64) error {
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO rounds(round_id,policy_version,budget_ms,consumed_budget_ms,remaining_budget_ms,partition_concurrency,created_at_ms)
			VALUES(?,?,?,?,?,?,?) ON CONFLICT(round_id) DO NOTHING`,
			plan.RoundID, policyVersion, plan.BudgetMS, plan.ConsumedBudgetMS, plan.RemainingBudgetMS,
			plan.PartitionConcurrency, nowMS())
		if err != nil {
			return err
		}
		for _, decision := range plan.Shed {
			_, err := tx.ExecContext(ctx, `
				INSERT INTO shed_decisions(round_id,policy_version,batch_id,partition_id,window_start_ms,rank_no,estimated_cost_ms,consumed_budget_before_ms,remaining_budget_ms,reason_code,reason,created_at_ms)
				VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
				plan.RoundID, policyVersion, decision.BatchID, decision.PartitionID, decision.WindowStartMS,
				decision.Rank, decision.EstimatedCostMS, decision.ConsumedBudgetBefore, decision.RemainingBudgetMS,
				decision.ReasonCode, decision.Reason, nowMS())
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `UPDATE batches SET status='SHED', updated_at_ms=? WHERE batch_id=?`, nowMS(), decision.BatchID)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) RequeueShedBatches(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE batches SET status='QUEUED', updated_at_ms=? WHERE status='SHED'`, nowMS())
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return n, err
}

type GroupCommit struct {
	IntentID      string
	PolicyVersion int64
	BatchID       string
	Group         FusionGroup
	Output        GroupOutput
	FinalStatus   string
	SourceBatchID string
	Late          bool
	LateReason    string
}

func (s *Store) CommitGroupResult(ctx context.Context, commit GroupCommit) error {
	if commit.IntentID == "" {
		commit.IntentID = fmt.Sprintf("intent-window-%s-%s", commit.BatchID, commit.Group.ID)
	}
	payload, _ := json.Marshal(map[string]any{
		"batch_id": commit.BatchID, "group_id": commit.Group.ID,
		"signature_hash": commit.Group.SignatureHash, "aggregates": len(commit.Output.Aggregates),
	})
	if _, err := s.CreateIntent(ctx, IntentRecord{
		ID: commit.IntentID, Kind: IntentWindowConclusion, AggregateID: commit.BatchID,
		PayloadJSON: string(payload),
	}); err != nil {
		return err
	}

	outputJSON, err := json.Marshal(commit.Output)
	if err != nil {
		return err
	}
	sourceIDs, _ := json.Marshal([]string{commit.SourceBatchID})
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		ts := nowMS()
		_, err := tx.ExecContext(ctx, `
			INSERT INTO group_outputs(batch_id,policy_version,fusion_group_id,signature_hash,output_json,created_at_ms)
			VALUES(?,?,?,?,?,?) ON CONFLICT(batch_id,policy_version,fusion_group_id) DO NOTHING`,
			commit.BatchID, commit.PolicyVersion, commit.Group.ID, commit.Group.SignatureHash, string(outputJSON), ts)
		if err != nil {
			return err
		}
		for _, agg := range commit.Output.Aggregates {
			_, err := tx.ExecContext(ctx, `
				INSERT INTO window_results(policy_version,fusion_group_id,signature_hash,partition_id,window_start_ms,window_end_ms,group_key,count,sum_fixed,min_fixed,max_fixed,status,source_batch_ids,committed_intent_id,committed_at_ms)
				VALUES(?,?,?,?,?,?,?,?,?,?,?, 'COMMITTED',?,?,?)
				ON CONFLICT(policy_version,fusion_group_id,signature_hash,partition_id,window_start_ms,window_end_ms,group_key)
				DO NOTHING`,
				commit.PolicyVersion, commit.Group.ID, commit.Group.SignatureHash,
				agg.PartitionID,
				agg.WindowStartMS, agg.WindowEndMS, agg.GroupKey, agg.Count, agg.Sum, agg.Min, agg.Max,
				string(sourceIDs), commit.IntentID, ts)
			if err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO group_completions(batch_id,policy_version,fusion_group_id,signature_hash,status,window_result_count,committed_intent_id,completed_at_ms)
			VALUES(?,?,?,?, 'COMPLETED',?,?,?)
			ON CONFLICT(batch_id,policy_version,fusion_group_id) DO NOTHING`,
			commit.BatchID, commit.PolicyVersion, commit.Group.ID, commit.Group.SignatureHash,
			len(commit.Output.Aggregates), commit.IntentID, ts)
		if err != nil {
			return err
		}
		if err := s.RemoveInflight(ctx, tx, commit.PolicyVersion, commit.Group.ID, commit.BatchID); err != nil {
			return err
		}
		if commit.Late {
			batch, err := s.GetBatch(ctx, commit.BatchID)
			if err != nil {
				return err
			}
			if batch == nil || len(batch.Events) == 0 {
				return fmt.Errorf("late batch %s has no events", commit.BatchID)
			}
			last := batch.Events[len(batch.Events)-1]
			_, err = tx.ExecContext(ctx, `
				INSERT INTO late_events(policy_version,batch_id,partition_id,sequence,window_start_ms,window_end_ms,reason,recorded_at_ms)
				VALUES(?,?,?,?,?,?,?,?)`,
				commit.PolicyVersion, commit.BatchID, batch.PartitionID, last.Sequence,
				batch.WindowStart, batch.WindowEnd, commit.LateReason, ts)
			if err != nil {
				return err
			}
		}
		if commit.FinalStatus != "" {
			_, err = tx.ExecContext(ctx, `UPDATE batches SET status=?, current_group_id=?, updated_at_ms=? WHERE batch_id=?`,
				commit.FinalStatus, commit.Group.ID, ts, commit.BatchID)
			if err != nil {
				return err
			}
		}
		return CompleteIntentTx(ctx, tx, commit.IntentID, IntentCommitted)
	})
}

type WindowResultRecord struct {
	WindowAggregate
	PolicyVersion  int64  `json:"policy_version"`
	FusionGroupID  string `json:"fusion_group_id"`
	SignatureHash  string `json:"signature_hash"`
	Status         string `json:"status"`
	IntentID       string `json:"intent_id"`
	SourceBatchIDs string `json:"source_batch_ids"`
}

func (s *Store) ListWindowResults(ctx context.Context, policyVersion int64, includeInvalidated bool) ([]WindowResultRecord, error) {
	query := `
		SELECT policy_version,fusion_group_id,signature_hash,partition_id,window_start_ms,window_end_ms,group_key,
		       count,sum_fixed,min_fixed,max_fixed,status,committed_intent_id,source_batch_ids
		FROM window_results WHERE 1=1`
	args := []any{}
	if policyVersion > 0 {
		query += ` AND policy_version=?`
		args = append(args, policyVersion)
	}
	if !includeInvalidated {
		query += ` AND status='COMMITTED'`
	}
	query += ` ORDER BY policy_version,window_start_ms,group_key,fusion_group_id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []WindowResultRecord{}
	for rows.Next() {
		var rec WindowResultRecord
		if err := rows.Scan(&rec.PolicyVersion, &rec.FusionGroupID, &rec.SignatureHash,
			&rec.PartitionID, &rec.WindowStartMS, &rec.WindowEndMS, &rec.GroupKey, &rec.Count, &rec.Sum, &rec.Min, &rec.Max,
			&rec.Status, &rec.IntentID, &rec.SourceBatchIDs); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *Store) IsWindowCommitted(ctx context.Context, policyVersion int64, groupID, signatureHash string, partitionID int, windowStart int64) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM group_completions gc
		JOIN batches b ON b.batch_id=gc.batch_id
		WHERE gc.policy_version=? AND gc.fusion_group_id=? AND gc.signature_hash=?
		  AND b.partition_id=? AND b.window_start_ms=? AND gc.status='COMPLETED'`,
		policyVersion, groupID, signatureHash, partitionID, windowStart).Scan(&count)
	return count > 0, err
}

func (s *Store) RecordLateEvent(ctx context.Context, policyVersion int64, batch *Batch, reason string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO late_events(policy_version,batch_id,partition_id,sequence,window_start_ms,window_end_ms,reason,recorded_at_ms)
		SELECT ?,?,?,?,?,?,?,?`,
		policyVersion, batch.ID, batch.PartitionID, batch.Events[len(batch.Events)-1].Sequence,
		batch.WindowStart, batch.WindowEnd, reason, nowMS())
	return err
}

func (s *Store) ListShedDecisions(ctx context.Context, roundID string) ([]ShedDecision, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT batch_id,partition_id,window_start_ms,rank_no,estimated_cost_ms,consumed_budget_before_ms,
		       remaining_budget_ms,reason_code,reason
		FROM shed_decisions WHERE (?='' OR round_id=?) ORDER BY rank_no,id`, roundID, roundID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ShedDecision{}
	for rows.Next() {
		var d ShedDecision
		if err := rows.Scan(&d.BatchID, &d.PartitionID, &d.WindowStartMS, &d.Rank, &d.EstimatedCostMS,
			&d.ConsumedBudgetBefore, &d.RemainingBudgetMS, &d.ReasonCode, &d.Reason); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
