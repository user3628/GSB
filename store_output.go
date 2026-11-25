package feedbackstream

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

func (s *Store) GetGroupOutput(ctx context.Context, batchID string, policyVersion int64, groupID string) (*GroupOutput, error) {
	var payload string
	err := s.db.QueryRowContext(ctx, `
		SELECT output_json FROM group_outputs
		WHERE batch_id=? AND policy_version=? AND fusion_group_id=?`,
		batchID, policyVersion, groupID).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out GroupOutput
	if err := json.Unmarshal([]byte(payload), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Store) ListAllBatches(ctx context.Context) ([]*Batch, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT batch_id FROM batches ORDER BY partition_id,window_start_ms,batch_id`)
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

func (s *Store) ListBackpressureEvents(ctx context.Context, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT intent_id,policy_version,fusion_group_id,level,inflight_batches,inflight_bytes,
		       max_batches,max_bytes,batch_utilization,byte_utilization,blocked_upstream_batches,created_at_ms
		FROM backpressure_events ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var intentID, groupID, level string
		var policyVersion, inflightBatches, maxBatches, blocked, createdAt int64
		var inflightBytes, maxBytes int64
		var batchUtil, byteUtil float64
		if err := rows.Scan(&intentID, &policyVersion, &groupID, &level, &inflightBatches, &inflightBytes,
			&maxBatches, &maxBytes, &batchUtil, &byteUtil, &blocked, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"intent_id": intentID, "policy_version": policyVersion, "fusion_group_id": groupID,
			"level": level, "inflight_batches": inflightBatches, "inflight_bytes": inflightBytes,
			"max_batches": maxBatches, "max_bytes": maxBytes,
			"batch_utilization": batchUtil, "byte_utilization": byteUtil,
			"blocked_upstream_batches": blocked, "created_at_ms": createdAt,
		})
	}
	return out, rows.Err()
}
