package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"gpusched/internal/domain"
)

func (s *Store) InsertSnapshotTx(tx *sql.Tx, snap domain.PolicySnapshot) error {
	paramsJSON, err := json.Marshal(snap.Params)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		`INSERT INTO policy_snapshots(version, digest, prev_digest, params_json) VALUES(?,?,?,?)`,
		snap.Version, snap.Digest, snap.PrevDigest, string(paramsJSON))
	return err
}

func (s *Store) InsertFragmentTx(tx *sql.Tx, f domain.Fragment) error {
	fieldsJSON, err := json.Marshal(f.Fields)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		`INSERT INTO policy_fragments(version, base_digest, digest, fields_json) VALUES(?,?,?,?)`,
		f.Version, f.BaseDigest, f.Digest, string(fieldsJSON))
	return err
}

func (s *Store) LatestSnapshot(ctx context.Context) (domain.PolicySnapshot, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT version, digest, prev_digest, params_json FROM policy_snapshots ORDER BY version DESC LIMIT 1`)
	return scanSnapshot(row)
}

func (s *Store) GetSnapshot(ctx context.Context, version int64) (domain.PolicySnapshot, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT version, digest, prev_digest, params_json FROM policy_snapshots WHERE version=?`, version)
	return scanSnapshot(row)
}

func scanSnapshot(row *sql.Row) (domain.PolicySnapshot, error) {
	var snap domain.PolicySnapshot
	var paramsJSON string
	if err := row.Scan(&snap.Version, &snap.Digest, &snap.PrevDigest, &paramsJSON); err != nil {
		return snap, err
	}
	err := json.Unmarshal([]byte(paramsJSON), &snap.Params)
	return snap, err
}

func (s *Store) ListFragments(ctx context.Context) ([]domain.Fragment, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT version, base_digest, digest, fields_json FROM policy_fragments ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Fragment
	for rows.Next() {
		var f domain.Fragment
		var fieldsJSON string
		if err := rows.Scan(&f.Version, &f.BaseDigest, &f.Digest, &fieldsJSON); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(fieldsJSON), &f.Fields); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
