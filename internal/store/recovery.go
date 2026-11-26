package store

import (
	"context"
	"database/sql"
	"time"
)

type FactRef struct {
	IntentID     string `json:"intent_id"`
	Kind         string `json:"kind"`
	Status       string `json:"status"`
	SessionID    string `json:"session_id,omitempty"`
	BatchID      string `json:"batch_id,omitempty"`
	ShardID      string `json:"shard_id,omitempty"`
	Version      int64  `json:"version,omitempty"`
	BlockNo      int    `json:"block_no"`
	Partition    int    `json:"partition"`
	SegmentStart int    `json:"segment_start"`
	SegmentEnd   int    `json:"segment_end"`
	Fact         string `json:"fact"`
}

type RecomputeRef struct {
	SessionID    string `json:"session_id"`
	ShardID      string `json:"shard_id"`
	Version      int64  `json:"version"`
	Partition    int    `json:"partition"`
	BlockNo      int    `json:"block_no"`
	SegmentStart int    `json:"segment_start"`
	SegmentEnd   int    `json:"segment_end"`
	Reason       string `json:"reason"`
}

type PartialRef struct {
	SessionID      string `json:"session_id"`
	ShardID        string `json:"shard_id"`
	Version        int64  `json:"version"`
	Partition      int    `json:"partition"`
	BlockNo        int    `json:"block_no"`
	SegmentStart   int    `json:"segment_start"`
	SegmentEnd     int    `json:"segment_end"`
	DefinitionHash string `json:"definition_hash"`
}

type RecoveryReport struct {
	Committed           []FactRef      `json:"committed"`
	RolledBack          []FactRef      `json:"rolled_back"`
	NeedsRecompute      []RecomputeRef `json:"needs_recompute"`
	ReusablePartialSums []PartialRef   `json:"reusable_partial_sums"`
}

type scannedIntent struct {
	intent    Intent
	partition int
}

func (s *Store) Recover(ctx context.Context) (*RecoveryReport, error) {
	report := &RecoveryReport{
		Committed:           []FactRef{},
		RolledBack:          []FactRef{},
		NeedsRecompute:      []RecomputeRef{},
		ReusablePartialSums: []PartialRef{},
	}

	pending, err := s.scanPendingIntents(ctx)
	if err != nil {
		return nil, err
	}
	for _, item := range pending {
		intent := item.intent
		ref := FactRef{
			IntentID: intent.ID, Kind: intent.Kind, SessionID: intent.SessionID, BatchID: intent.BatchID,
			ShardID: intent.ShardID, Version: intent.Version, BlockNo: intent.BlockNo,
			Partition: item.partition, SegmentStart: intent.SegmentStart, SegmentEnd: intent.SegmentEnd,
		}
		committed, fact, err := s.hasCommitFact(ctx, intent)
		if err != nil {
			return nil, err
		}
		now := s.now().Format(time.RFC3339Nano)
		if committed {
			if _, err := s.db.ExecContext(ctx, `UPDATE intents SET status='committed',updated_at=? WHERE intent_id=? AND status='pending'`, now, intent.ID); err != nil {
				return nil, err
			}
			ref.Status = "committed"
			ref.Fact = fact
			report.Committed = append(report.Committed, ref)
			continue
		}

		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='rolled_back',updated_at=? WHERE intent_id=? AND status='pending'`, now, intent.ID); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		if intent.Kind == "segment_start" || intent.Kind == "partial_commit" {
			if _, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO segment_work
(session_id,shard_id,version,block_no,segment_start,segment_end,partition,status,updated_at)
VALUES(?,?,?,?,?,?,?, 'pending_decode', ?)`,
				intent.SessionID, intent.ShardID, intent.Version, intent.BlockNo,
				intent.SegmentStart, intent.SegmentEnd, item.partition, now); err != nil {
				_ = tx.Rollback()
				return nil, err
			}
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		ref.Status = "rolled_back"
		ref.Fact = "no atomic commit row found; pending intent aborted"
		report.RolledBack = append(report.RolledBack, ref)
	}

	workRows, err := s.db.QueryContext(ctx, `
SELECT session_id,shard_id,version,block_no,segment_start,segment_end,partition
FROM segment_work WHERE status='pending_decode'
ORDER BY partition, block_no, segment_start, session_id`)
	if err != nil {
		return nil, err
	}
	defer workRows.Close()
	for workRows.Next() {
		var ref RecomputeRef
		if err := workRows.Scan(&ref.SessionID, &ref.ShardID, &ref.Version, &ref.BlockNo, &ref.SegmentStart, &ref.SegmentEnd, &ref.Partition); err != nil {
			return nil, err
		}
		ref.Reason = "committed partial sum not found; segment restored to pending_decode"
		report.NeedsRecompute = append(report.NeedsRecompute, ref)
	}
	if err := workRows.Err(); err != nil {
		return nil, err
	}

	sumRows, err := s.db.QueryContext(ctx, `
SELECT session_id,shard_id,version,block_no,segment_start,segment_end,partition,definition_hash
FROM partial_sums ORDER BY session_id, partition, block_no, segment_start`)
	if err != nil {
		return nil, err
	}
	defer sumRows.Close()
	for sumRows.Next() {
		var ref PartialRef
		if err := sumRows.Scan(&ref.SessionID, &ref.ShardID, &ref.Version, &ref.BlockNo, &ref.SegmentStart, &ref.SegmentEnd, &ref.Partition, &ref.DefinitionHash); err != nil {
			return nil, err
		}
		report.ReusablePartialSums = append(report.ReusablePartialSums, ref)
	}
	if err := sumRows.Err(); err != nil {
		return nil, err
	}
	return report, nil
}

func (s *Store) scanPendingIntents(ctx context.Context) ([]scannedIntent, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT intent_id,kind,status,session_id,batch_id,shard_id,version,block_no,partition,segment_start,segment_end,payload_json
FROM intents WHERE status='pending' ORDER BY created_at,intent_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []scannedIntent{}
	for rows.Next() {
		var intent Intent
		var version, blockNo, partition, start, end sql.NullInt64
		if err := rows.Scan(&intent.ID, &intent.Kind, &intent.Status, &intent.SessionID, &intent.BatchID,
			&intent.ShardID, &version, &blockNo, &partition, &start, &end, &intent.PayloadJSON); err != nil {
			return nil, err
		}
		intent.Version = version.Int64
		intent.BlockNo = int(blockNo.Int64)
		intent.SegmentStart = int(start.Int64)
		intent.SegmentEnd = int(end.Int64)
		out = append(out, scannedIntent{intent: intent, partition: int(partition.Int64)})
	}
	return out, rows.Err()
}

func (s *Store) hasCommitFact(ctx context.Context, intent Intent) (bool, string, error) {
	switch intent.Kind {
	case "shard_switch":
		var version int64
		err := s.db.QueryRowContext(ctx, `SELECT version FROM current_shard WHERE shard_id=?`, intent.ShardID).Scan(&version)
		if err == sql.ErrNoRows {
			return false, "", nil
		}
		if err != nil {
			return false, "", err
		}
		if version != intent.Version {
			return false, "", nil
		}
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM shards WHERE shard_id=? AND version=?`, intent.ShardID, intent.Version).Scan(&count); err != nil {
			return false, "", err
		}
		return count == 1, "current_shard and shards row committed atomically", nil
	default:
		var count int
		err := s.db.QueryRowContext(ctx, `
SELECT COUNT(1) FROM partial_sums
WHERE session_id=? AND shard_id=? AND version=? AND block_no=? AND segment_start=? AND segment_end=?`,
			intent.SessionID, intent.ShardID, intent.Version, intent.BlockNo, intent.SegmentStart, intent.SegmentEnd).Scan(&count)
		if err != nil {
			return false, "", err
		}
		return count == 1, "partial_sums row committed atomically", nil
	}
}
