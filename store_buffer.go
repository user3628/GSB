package feedbackstream

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

type PushBufferRequest struct {
	PolicyVersion   int64
	GroupID         string
	UpstreamGroupID string
	BatchID         string
	PartitionID     int
	Bytes           int64
	Limit           BufferLimit
	IntentID        string
}

type PushBufferResult struct {
	Admitted            bool      `json:"admitted"`
	Watermark           Watermark `json:"watermark"`
	BecameBackpressured bool      `json:"became_backpressured"`
	BlockedBatchCount   int       `json:"blocked_upstream_batches"`
	WaitingQueueCount   int       `json:"waiting_queue_count"`
}

type watermarkQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func getWatermark(ctx context.Context, q watermarkQuerier, policyVersion int64, groupID string, limit BufferLimit) (Watermark, error) {
	var count int
	var bytes sql.NullInt64
	err := q.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(SUM(bytes),0) FROM buffer_inflight
		WHERE policy_version=? AND fusion_group_id=?`, policyVersion, groupID).Scan(&count, &bytes)
	if err != nil {
		return Watermark{}, err
	}
	maxBatches := limit.MaxInflightBatches
	maxBytes := limit.MaxBytes
	ratio := limit.HighWatermarkRatio
	if ratio == 0 {
		ratio = 0.8
	}
	wm := Watermark{
		PolicyVersion: policyVersion, GroupID: groupID, InflightBatches: count,
		InflightBytes: bytes.Int64, MaxBatches: maxBatches, MaxBytes: maxBytes,
		WatermarkRatio: ratio,
	}
	if maxBatches > 0 {
		wm.BatchUtilization = float64(count) / float64(maxBatches)
	}
	if maxBytes > 0 {
		wm.ByteUtilization = float64(bytes.Int64) / float64(maxBytes)
	}
	wm.Backpressured = wm.BatchUtilization >= ratio || wm.ByteUtilization >= ratio ||
		count >= maxBatches || bytes.Int64 >= maxBytes
	return wm, nil
}

func (s *Store) Watermark(ctx context.Context, policyVersion int64, groupID string, limit BufferLimit) (Watermark, error) {
	return getWatermark(ctx, s.db, policyVersion, groupID, limit)
}

func (s *Store) PushToBuffer(ctx context.Context, req PushBufferRequest) (*PushBufferResult, error) {
	if req.IntentID == "" {
		req.IntentID = fmt.Sprintf("intent-bp-%s-%s-%d", req.BatchID, req.GroupID, nowMS())
	}
	payload, _ := json.Marshal(req)
	if _, err := s.CreateIntent(ctx, IntentRecord{
		ID: req.IntentID, Kind: IntentBackpressure, AggregateID: req.GroupID,
		PayloadJSON: string(payload),
	}); err != nil {
		return nil, err
	}

	result := &PushBufferResult{}
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		current, err := getWatermark(ctx, tx, req.PolicyVersion, req.GroupID, req.Limit)
		if err != nil {
			return err
		}
		hardCount := current.InflightBatches+1 > req.Limit.MaxInflightBatches
		hardBytes := current.InflightBytes+req.Bytes > req.Limit.MaxBytes
		block := current.Backpressured || hardCount || hardBytes

		if block {
			reason := "high_watermark_or_buffer_limit"
			if hardCount || hardBytes {
				reason = "hard_buffer_limit"
			}
			_, err = tx.ExecContext(ctx, `
				INSERT INTO waiting_batches(policy_version,batch_id,partition_id,upstream_group_id,target_group_id,bytes,status,reason,created_at_ms,updated_at_ms)
				VALUES(?,?,?,?,?,?,'WAITING',?,?,?)
				ON CONFLICT(batch_id,target_group_id) WHERE status='WAITING'
				DO UPDATE SET updated_at_ms=excluded.updated_at_ms, reason=excluded.reason, upstream_group_id=excluded.upstream_group_id`,
				req.PolicyVersion, req.BatchID, req.PartitionID, req.UpstreamGroupID, req.GroupID,
				req.Bytes, reason, nowMS(), nowMS())
			if err != nil {
				return err
			}
			var blockedForTarget int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM waiting_batches WHERE policy_version=? AND target_group_id=? AND status='WAITING'`,
				req.PolicyVersion, req.GroupID).Scan(&blockedForTarget); err != nil {
				return err
			}
			var totalWaiting int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM waiting_batches WHERE policy_version=? AND status='WAITING'`,
				req.PolicyVersion).Scan(&totalWaiting); err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `UPDATE batches SET status='BLOCKED', updated_at_ms=? WHERE batch_id=?`, nowMS(), req.BatchID)
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `
				INSERT INTO backpressure_events(intent_id,policy_version,fusion_group_id,level,inflight_batches,inflight_bytes,max_batches,max_bytes,batch_utilization,byte_utilization,watermark_ratio,blocked_upstream_batches,created_at_ms)
				VALUES(?,?,?,'BLOCKED',?,?,?,?,?,?,?,?,?)`,
				req.IntentID, req.PolicyVersion, req.GroupID, current.InflightBatches, current.InflightBytes,
				current.MaxBatches, current.MaxBytes, current.BatchUtilization, current.ByteUtilization,
				current.WatermarkRatio, blockedForTarget, nowMS())
			if err != nil {
				return err
			}
			if err := CompleteIntentTx(ctx, tx, req.IntentID, IntentCommitted); err != nil {
				return err
			}
			current.Backpressured = true
			result.Admitted = false
			result.Watermark = current
			result.BlockedBatchCount = blockedForTarget
			result.WaitingQueueCount = totalWaiting
			return nil
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO buffer_inflight(policy_version,fusion_group_id,batch_id,bytes,entered_at_ms)
			VALUES(?,?,?,?,?)`, req.PolicyVersion, req.GroupID, req.BatchID, req.Bytes, nowMS())
		if err != nil {
			return err
		}
		after, err := getWatermark(ctx, tx, req.PolicyVersion, req.GroupID, req.Limit)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE waiting_batches SET status='DONE', updated_at_ms=? WHERE batch_id=? AND target_group_id=? AND status='WAITING'`,
			nowMS(), req.BatchID, req.GroupID)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE batches SET current_group_id=?, status='RUNNING', updated_at_ms=? WHERE batch_id=?`,
			req.GroupID, nowMS(), req.BatchID)
		if err != nil {
			return err
		}
		if after.Backpressured {
			_, err = tx.ExecContext(ctx, `
				INSERT INTO backpressure_events(intent_id,policy_version,fusion_group_id,level,inflight_batches,inflight_bytes,max_batches,max_bytes,batch_utilization,byte_utilization,watermark_ratio,blocked_upstream_batches,created_at_ms)
				VALUES(?,?,?,'WATERMARK',?,?,?,?,?,?,?,0,?)`,
				req.IntentID, req.PolicyVersion, req.GroupID, after.InflightBatches, after.InflightBytes,
				after.MaxBatches, after.MaxBytes, after.BatchUtilization, after.ByteUtilization,
				after.WatermarkRatio, nowMS())
			if err != nil {
				return err
			}
		}
		if err := CompleteIntentTx(ctx, tx, req.IntentID, IntentCommitted); err != nil {
			return err
		}
		result.Admitted = true
		result.Watermark = after
		result.BecameBackpressured = after.Backpressured
		var waiting int
		_ = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM waiting_batches WHERE policy_version=? AND status='WAITING'`, req.PolicyVersion).Scan(&waiting)
		result.WaitingQueueCount = waiting
		return nil
	})
	return result, err
}

func (s *Store) RemoveInflight(ctx context.Context, tx *sql.Tx, policyVersion int64, groupID, batchID string) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM buffer_inflight WHERE policy_version=? AND fusion_group_id=? AND batch_id=?`,
		policyVersion, groupID, batchID)
	return err
}

func (s *Store) CountWaiting(ctx context.Context, policyVersion int64) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM waiting_batches WHERE policy_version=? AND status='WAITING'`, policyVersion).Scan(&count)
	return count, err
}

func (s *Store) AllWatermarks(ctx context.Context, policy *PolicySnapshot, plan *FusionPlan) ([]Watermark, bool, error) {
	out := make([]Watermark, 0, len(plan.Groups))
	allBP := len(plan.Groups) > 0
	for _, group := range plan.Groups {
		wm, err := s.Watermark(ctx, policy.Version, group.ID, policy.BufferFor(group.ID))
		if err != nil {
			return nil, false, err
		}
		out = append(out, wm)
		allBP = allBP && wm.Backpressured
	}
	return out, allBP, nil
}

func (s *Store) IsBackpressureStall(ctx context.Context, policy *PolicySnapshot, plan *FusionPlan) (bool, []Watermark, int, error) {
	watermarks, allBP, err := s.AllWatermarks(ctx, policy, plan)
	if err != nil {
		return false, nil, 0, err
	}
	waiting, err := s.CountWaiting(ctx, policy.Version)
	if err != nil {
		return false, nil, 0, err
	}
	return allBP && waiting >= policy.MaxWaitQueue, watermarks, waiting, nil
}
