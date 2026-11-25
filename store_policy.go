package feedbackstream

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

type PolicyRecord struct {
	Snapshot    *PolicySnapshot `json:"snapshot"`
	Status      string          `json:"status"`
	BaseVersion int64           `json:"base_version"`
	CreatedAtMS int64           `json:"created_at_ms"`
	ActivatedMS int64           `json:"activated_at_ms"`
}

func (s *Store) SaveInitialPolicy(ctx context.Context, policy *PolicySnapshot) error {
	payload, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		var activeVersion int64
		err := tx.QueryRowContext(ctx, `SELECT version FROM policies WHERE status='ACTIVE' ORDER BY version DESC LIMIT 1`).Scan(&activeVersion)
		if err == nil {
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		ts := nowMS()
		_, err = tx.ExecContext(ctx, `
			INSERT INTO policies(version,kind,base_version,status,checksum,payload_json,created_at_ms,activated_at_ms)
			VALUES(?,?,?,?,?,?,?,?)`,
			policy.Version, "FULL", 0, "ACTIVE", policy.ChecksumSHA256, string(payload), ts, ts)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES('active_policy_version',?)
			ON CONFLICT(key) DO UPDATE SET value=excluded.value`, policy.Version)
		return err
	})
}

func scanPolicy(row interface {
	Scan(...any) error
}) (*PolicyRecord, error) {
	var version, baseVersion, createdAt, activatedAt int64
	var kind, status, checksum, payload string
	if err := row.Scan(&version, &kind, &baseVersion, &status, &checksum, &payload, &createdAt, &activatedAt); err != nil {
		return nil, err
	}
	policy, err := ParsePolicySnapshot([]byte(payload))
	if err != nil {
		return nil, err
	}
	return &PolicyRecord{Snapshot: policy, Status: status, BaseVersion: baseVersion, CreatedAtMS: createdAt, ActivatedMS: activatedAt}, nil
}

const policySelectColumns = `version,kind,base_version,status,checksum,payload_json,created_at_ms,activated_at_ms`

func (s *Store) ActivePolicy(ctx context.Context) (*PolicyRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+policySelectColumns+` FROM policies WHERE status='ACTIVE' ORDER BY version DESC LIMIT 1`)
	rec, err := scanPolicy(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return rec, err
}

func (s *Store) PolicyVersion(ctx context.Context, version int64) (*PolicyRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+policySelectColumns+` FROM policies WHERE version=?`, version)
	rec, err := scanPolicy(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return rec, err
}

func (s *Store) GetBatchRequest(ctx context.Context, batchID string) (*BatchRequestRecord, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT batch_id,payload_hash,partition_id,status,result_json,error_code,error_json,created_at_ms,completed_at_ms
		FROM batch_requests WHERE batch_id=?`, batchID)
	var rec BatchRequestRecord
	err := row.Scan(&rec.BatchID, &rec.PayloadHash, &rec.PartitionID, &rec.Status, &rec.ResultJSON, &rec.ErrorCode, &rec.ErrorJSON, &rec.CreatedAtMS, &rec.CompletedMS)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *Store) InsertBatchRequestIfAbsent(ctx context.Context, rec BatchRequestRecord) (bool, error) {
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = nowMS()
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO batch_requests(batch_id,payload_hash,partition_id,status,created_at_ms)
		VALUES(?,?,?,?,?) ON CONFLICT(batch_id) DO NOTHING`,
		rec.BatchID, rec.PayloadHash, rec.PartitionID, rec.Status, rec.CreatedAtMS)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *Store) CompleteBatchRequest(ctx context.Context, batchID, status, resultJSON, errorCode string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE batch_requests SET status=?, result_json=?, error_code=?, completed_at_ms=?
		WHERE batch_id=?`, status, resultJSON, errorCode, nowMS(), batchID)
	return err
}

func (s *Store) SaveBatch(ctx context.Context, batch *Batch) error {
	ts := nowMS()
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO batches(batch_id,partition_id,window_start_ms,window_end_ms,event_count,bytes,payload_hash,status,created_at_ms,updated_at_ms)
			VALUES(?,?,?,?,?,?,?,'QUEUED',?,?)
			ON CONFLICT(batch_id) DO NOTHING`,
			batch.ID, batch.PartitionID, batch.WindowStart, batch.WindowEnd, batch.EventCount,
			batch.ByteSize, batch.PayloadHash, StatusQueued, ts, ts)
		if err != nil {
			return err
		}
		for _, event := range batch.Events {
			_, err := tx.ExecContext(ctx, `
				INSERT INTO events(partition_id,sequence,timestamp_ms,group_key,metric_fixed,batch_id)
				VALUES(?,?,?,?,?,?) ON CONFLICT(partition_id,sequence) DO NOTHING`,
				event.PartitionID, event.Sequence, event.TimestampMS, event.GroupKey, event.MetricFixed, batch.ID)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) GetBatch(ctx context.Context, batchID string) (*Batch, error) {
	var batch Batch
	err := s.db.QueryRowContext(ctx, `
		SELECT batch_id,partition_id,window_start_ms,window_end_ms,event_count,bytes,payload_hash,status,current_group_id
		FROM batches WHERE batch_id=?`, batchID).
		Scan(&batch.ID, &batch.PartitionID, &batch.WindowStart, &batch.WindowEnd,
			&batch.EventCount, &batch.ByteSize, &batch.PayloadHash, &batch.Status, &batch.CurrentGroupID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT timestamp_ms,partition_id,group_key,metric_fixed,sequence
		FROM events WHERE batch_id=? ORDER BY sequence`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.TimestampMS, &event.PartitionID, &event.GroupKey, &event.MetricFixed, &event.Sequence); err != nil {
			return nil, err
		}
		batch.Events = append(batch.Events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &batch, nil
}

func (s *Store) UpdateBatchStatus(ctx context.Context, batchID, status, currentGroup string, attempts int) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE batches SET status=?, current_group_id=?, attempts=?, updated_at_ms=? WHERE batch_id=?`,
		status, currentGroup, attempts, nowMS(), batchID)
	return err
}

func (s *Store) ListQueuedBatches(ctx context.Context) ([]*Batch, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT batch_id FROM batches WHERE status IN ('QUEUED','SHED') ORDER BY partition_id,window_start_ms,batch_id`)
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
