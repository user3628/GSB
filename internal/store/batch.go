package store

import (
	"context"
	"database/sql"

	"gpusched/internal/domain"
)

func (s *Store) CreateRoundTx(tx *sql.Tx, policyVersion, budgetMs int64, startedAt string) (int64, error) {
	res, err := tx.Exec(
		`INSERT INTO rounds(policy_version, status, budget_ms, started_at) VALUES(?,?,?,?)`,
		policyVersion, domain.RoundRunning, budgetMs, startedAt)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) FinishRound(ctx context.Context, id int64, status, finishedAt, reportJSON string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE rounds SET status=?, finished_at=?, report_json=? WHERE id=?`,
		status, finishedAt, reportJSON, id)
	return err
}

func (s *Store) FinishRoundTx(tx *sql.Tx, id int64, status, finishedAt, reportJSON string) error {
	_, err := tx.Exec(
		`UPDATE rounds SET status=?, finished_at=?, report_json=? WHERE id=?`,
		status, finishedAt, reportJSON, id)
	return err
}

func (s *Store) MarkRoundRecoveredTx(tx *sql.Tx, id int64, finishedAt, reportJSON string) error {
	return s.FinishRoundTx(tx, id, domain.RoundRecovered, finishedAt, reportJSON)
}

// InsertBatchTx inserts a batch header.
func (s *Store) InsertBatchTx(tx *sql.Tx, b domain.Batch) error {
	_, err := tx.Exec(
		`INSERT INTO batches(id, round_id, model_id, node_id, group_id, priority, status,
		   total_samples, total_memory, est_latency_ms, seq)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		b.ID, b.RoundID, b.ModelID, b.NodeID, b.GroupID, b.Priority, b.Status,
		b.TotalSamples, b.TotalMemory, b.EstLatencyMs, b.Seq)
	return err
}

func (s *Store) AddBatchMemberTx(tx *sql.Tx, batchID, requestID string, position int) error {
	_, err := tx.Exec(
		`INSERT INTO batch_members(batch_id, request_id, position) VALUES(?,?,?)`,
		batchID, requestID, position)
	return err
}

// BatchWithMembers joins a batch header with its member ids in position order.
type BatchWithMembers struct {
	domain.Batch
	Members []string `json:"members"`
}

func (s *Store) ListBatches(ctx context.Context) ([]BatchWithMembers, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, round_id, model_id, node_id, group_id, priority, status,
		   total_samples, total_memory, est_latency_ms, seq
		 FROM batches ORDER BY round_id, seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BatchWithMembers
	for rows.Next() {
		var b BatchWithMembers
		if err := rows.Scan(&b.ID, &b.RoundID, &b.ModelID, &b.NodeID, &b.GroupID,
			&b.Priority, &b.Status, &b.TotalSamples, &b.TotalMemory,
			&b.EstLatencyMs, &b.Seq); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		mrows, err := s.db.QueryContext(ctx,
			`SELECT request_id FROM batch_members WHERE batch_id=? ORDER BY position`, out[i].ID)
		if err != nil {
			return nil, err
		}
		for mrows.Next() {
			var id string
			if err := mrows.Scan(&id); err != nil {
				mrows.Close()
				return nil, err
			}
			out[i].Members = append(out[i].Members, id)
		}
		mrows.Close()
	}
	return out, nil
}

// ListBatchesByRoundTx returns batches belonging to a round ordered by seq.
func (s *Store) ListBatchesByRoundTx(tx *sql.Tx, roundID int64) ([]domain.Batch, error) {
	rows, err := tx.Query(
		`SELECT id, round_id, model_id, node_id, group_id, priority, status,
		   total_samples, total_memory, est_latency_ms, seq
		 FROM batches WHERE round_id=? ORDER BY seq`, roundID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Batch
	for rows.Next() {
		var b domain.Batch
		if err := rows.Scan(&b.ID, &b.RoundID, &b.ModelID, &b.NodeID, &b.GroupID,
			&b.Priority, &b.Status, &b.TotalSamples, &b.TotalMemory,
			&b.EstLatencyMs, &b.Seq); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) UpdateBatchStatus(ctx context.Context, batchID, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE batches SET status=? WHERE id=?`, status, batchID)
	return err
}

func UpdateBatchStatusTx(tx *sql.Tx, batchID, status string) error {
	_, err := tx.Exec(`UPDATE batches SET status=? WHERE id=?`, status, batchID)
	return err
}

// CountBatchesByStatusTx counts batches currently in a given status.
func CountBatchesByStatusTx(tx *sql.Tx, statuses []string) (int64, error) {
	if len(statuses) == 0 {
		return 0, nil
	}
	q := `SELECT COUNT(*) FROM batches WHERE status IN (` + placeholders(len(statuses)) + `)`
	args := make([]any, len(statuses))
	for i, st := range statuses {
		args[i] = st
	}
	var n int64
	err := tx.QueryRow(q, args...).Scan(&n)
	return n, err
}

// CountQueuedBatchesTx counts batches currently occupying the wait queue.
func CountQueuedBatchesTx(tx *sql.Tx) (int64, error) {
	var n int64
	err := tx.QueryRow(`SELECT COUNT(*) FROM batches WHERE status=?`, domain.BatchQueued).Scan(&n)
	return n, err
}

// MembersOfBatchTx returns member request ids in submission order.
func MembersOfBatchTx(tx *sql.Tx, batchID string) ([]string, error) {
	rows, err := tx.Query(
		`SELECT request_id FROM batch_members WHERE batch_id=? ORDER BY position`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// BatchOfRequestTx returns the batch id currently bound to a request, if any.
func BatchOfRequestTx(tx *sql.Tx, requestID string) (string, error) {
	var id string
	err := tx.QueryRow(
		`SELECT b.id FROM batches b JOIN batch_members m ON m.batch_id=b.id
		 WHERE m.request_id=? AND b.status NOT IN (?,?)
		 ORDER BY b.seq DESC LIMIT 1`,
		requestID, domain.BatchAborted, domain.BatchCompleted).Scan(&id)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return id, err
}

// AbortBatchesOfRoundTx flips every non-terminal batch of a round to ABORTED.
func AbortBatchesOfRoundTx(tx *sql.Tx, roundID int64) (int64, error) {
	res, err := tx.Exec(
		`UPDATE batches SET status=? WHERE round_id=? AND status NOT IN (?,?)`,
		domain.BatchAborted, roundID, domain.BatchCompleted, domain.BatchAborted)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// AbortIntentsOfRoundTx marks all pending intents of a round as aborted.
func AbortIntentsOfRoundTx(tx *sql.Tx, roundID int64) (int64, error) {
	res, err := tx.Exec(
		`UPDATE intents SET status=? WHERE round_id=? AND status=?`,
		domain.IntentAborted, roundID, domain.IntentPending)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// AbortAllPendingIntentsTx handles intents without a round (e.g. admission).
func AbortAllPendingIntentsTx(tx *sql.Tx) (int64, error) {
	res, err := tx.Exec(`UPDATE intents SET status=? WHERE status=?`,
		domain.IntentAborted, domain.IntentPending)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
