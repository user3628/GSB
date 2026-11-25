package store

import (
	"context"
	"database/sql"

	"gpusched/internal/domain"
)

// InsertRequestTx inserts a brand-new PENDING request. Duplicate ids fail.
func (s *Store) InsertRequestTx(tx *sql.Tx, r domain.Request) error {
	_, err := tx.Exec(
		`INSERT INTO requests(id, model_id, group_id, memory_need, compute_need_m,
		   samples_per_batch, latency_limit_ms, priority, state, seq, assigned_node_id, policy_version)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		r.ID, r.ModelID, r.GroupID, r.MemoryNeed, r.ComputeNeedM,
		r.SamplesPerBatch, r.LatencyLimitMs, r.Priority, r.State, r.Seq,
		r.AssignedNodeID, r.PolicyVersion)
	return err
}

func (s *Store) GetRequest(ctx context.Context, id string) (domain.Request, error) {
	return scanRequest(s.db.QueryRowContext(ctx, requestColumns+` FROM requests WHERE id=?`, id))
}

func (s *Store) GetRequestTx(tx *sql.Tx, id string) (domain.Request, error) {
	return scanRequest(tx.QueryRow(requestColumns+` FROM requests WHERE id=?`, id))
}

const requestColumns = `SELECT id, model_id, group_id, memory_need, compute_need_m,
	samples_per_batch, latency_limit_ms, priority, state, seq, assigned_node_id, policy_version`

func scanRequest(row *sql.Row) (domain.Request, error) {
	var r domain.Request
	err := row.Scan(&r.ID, &r.ModelID, &r.GroupID, &r.MemoryNeed, &r.ComputeNeedM,
		&r.SamplesPerBatch, &r.LatencyLimitMs, &r.Priority, &r.State, &r.Seq,
		&r.AssignedNodeID, &r.PolicyVersion)
	return r, err
}

func (s *Store) ListRequests(ctx context.Context) ([]domain.Request, error) {
	rows, err := s.db.QueryContext(ctx, requestColumns+` FROM requests ORDER BY seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRequestRows(rows)
}

// ListRequestsByStateTx returns requests in a state ordered by stable seq.
func (s *Store) ListRequestsByStateTx(tx *sql.Tx, states []string) ([]domain.Request, error) {
	if len(states) == 0 {
		return nil, nil
	}
	q := requestColumns + ` FROM requests WHERE state IN (` + placeholders(len(states)) + `) ORDER BY seq`
	args := make([]any, len(states))
	for i, st := range states {
		args[i] = st
	}
	rows, err := tx.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRequestRows(rows)
}

func scanRequestRows(rows *sql.Rows) ([]domain.Request, error) {
	var out []domain.Request
	for rows.Next() {
		var r domain.Request
		if err := rows.Scan(&r.ID, &r.ModelID, &r.GroupID, &r.MemoryNeed, &r.ComputeNeedM,
			&r.SamplesPerBatch, &r.LatencyLimitMs, &r.Priority, &r.State, &r.Seq,
			&r.AssignedNodeID, &r.PolicyVersion); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpdateRequestState performs a guarded state transition. The WHERE clause pins
// the expected previous state, so an illegal/duplicate transition changes zero
// rows and leaves the persisted state untouched.
func UpdateRequestState(tx *sql.Tx, id, from, to string) (bool, error) {
	res, err := tx.Exec(`UPDATE requests SET state=? WHERE id=? AND state=?`, to, id, from)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// AssignNodeTx records the node chosen during admission.
func AssignNodeTx(tx *sql.Tx, requestID, nodeID string, policyVersion int64) error {
	_, err := tx.Exec(
		`UPDATE requests SET assigned_node_id=?, policy_version=? WHERE id=?`,
		nodeID, policyVersion, requestID)
	return err
}

func placeholders(n int) string {
	out := ""
	for i := 0; i < n; i++ {
		if i > 0 {
			out += ","
		}
		out += "?"
	}
	return out
}
