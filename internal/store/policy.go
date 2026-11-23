package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"cdn-edge-consistency/internal/model"
	"cdn-edge-consistency/internal/policy"
)

func ValidateNewPolicyTx(ctx context.Context, tx *sql.Tx, version int64) error {
	var current int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM policy_snapshots`).Scan(&current); err != nil {
		return err
	}
	if version <= current {
		return model.WithDetails(model.NewBiz(model.CodeStaleSnapshot, "snapshot version must be strictly greater than the committed version"), map[string]any{
			"submitted_version": version,
			"committed_version": current,
		})
	}
	return nil
}

func SavePolicyTx(ctx context.Context, tx *sql.Tx, p *policy.Snapshot) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO policy_snapshots(version,checksum,generated_at,payload_json,committed_at_ms) VALUES(?,?,?,?,?)`,
		p.Version, p.Checksum, p.GeneratedAt, string(raw), p.CommittedAtMs)
	return err
}

func (s *Store) CurrentPolicy(ctx context.Context) (*policy.Snapshot, error) {
	return s.policyByQuery(ctx, s.db, `SELECT payload_json FROM policy_snapshots ORDER BY version DESC LIMIT 1`)
}

func (s *Store) PolicyByVersion(ctx context.Context, version int64) (*policy.Snapshot, error) {
	return s.policyByQuery(ctx, s.db, `SELECT payload_json FROM policy_snapshots WHERE version=?`, version)
}

func (s *Store) policyByQuery(ctx context.Context, db *sql.DB, query string, args ...any) (*policy.Snapshot, error) {
	var raw string
	err := db.QueryRowContext(ctx, query, args...).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var p policy.Snapshot
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Store) ListPolicies(ctx context.Context) ([]policy.Snapshot, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT payload_json FROM policy_snapshots ORDER BY version ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []policy.Snapshot
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var p policy.Snapshot
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
