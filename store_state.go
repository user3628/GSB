package feedbackstream

import (
	"context"
	"encoding/json"
)

func (s *Store) MarkBatchRunning(ctx context.Context, batchID string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE batches SET status='RUNNING', attempts=attempts+1, updated_at_ms=? WHERE batch_id=?`,
		nowMS(), batchID)
	return err
}

func (s *Store) FinishBatchRequest(ctx context.Context, rec BatchRequestRecord) error {
	result := rec.ResultJSON
	if result == "" {
		result = "{}"
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE batch_requests
		SET status=?, result_json=?, error_code=?, error_json=?, completed_at_ms=?
		WHERE batch_id=?`,
		rec.Status, result, rec.ErrorCode, rec.ErrorJSON, nowMS(), rec.BatchID)
	return err
}

func (s *Store) GroupCompletionExists(ctx context.Context, policyVersion int64, batchID, groupID, signatureHash string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM group_completions
		WHERE policy_version=? AND batch_id=? AND fusion_group_id=? AND signature_hash=? AND status='COMPLETED'`,
		policyVersion, batchID, groupID, signatureHash).Scan(&count)
	return count > 0, err
}

func (s *Store) InflightExists(ctx context.Context, policyVersion int64, groupID, batchID string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM buffer_inflight
		WHERE policy_version=? AND fusion_group_id=? AND batch_id=?`,
		policyVersion, groupID, batchID).Scan(&count)
	return count > 0, err
}

func (s *Store) ListBatchesByStatus(ctx context.Context, statuses ...string) ([]*Batch, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	query := `SELECT batch_id FROM batches WHERE status IN (`
	args := make([]any, 0, len(statuses))
	for i, status := range statuses {
		if i > 0 {
			query += ","
		}
		query += "?"
		args = append(args, status)
	}
	query += `) ORDER BY partition_id,window_start_ms,batch_id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]*Batch, 0, len(ids))
	for _, id := range ids {
		batch, err := s.GetBatch(ctx, id)
		if err != nil {
			return nil, err
		}
		if batch != nil {
			out = append(out, batch)
		}
	}
	return out, nil
}

func errorJSON(err error) string {
	if err == nil {
		return ""
	}
	if coded, ok := AsError(err); ok {
		data, _ := json.Marshal(coded)
		return string(data)
	}
	data, _ := json.Marshal(map[string]string{"code": "E_INTERNAL", "message": err.Error()})
	return string(data)
}
