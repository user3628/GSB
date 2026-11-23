package snapshot

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sandboxpolicy/business"
)

// InjectVersionGap is a deterministic test hook. It inserts a valid-looking
// row beyond the next version, leaving a gap. Runtime application will refuse
// to advance past this gap.
func (s *Store) InjectVersionGap(ctx context.Context, namespace string, gapVersion int) error {
	current, _, err := s.Current(ctx, namespace)
	if err != nil {
		return err
	}
	if gapVersion <= current.Version+1 {
		return business.New(business.CodeSnapshotVersion, fmt.Sprintf("gap version %d must be greater than %d", gapVersion, current.Version+1))
	}
	policyBytes, err := rawSnapshotColumn(ctx, s.db, namespace, current.Version, "policy_json")
	if err != nil {
		return err
	}
	diagnosticsBytes, err := rawSnapshotColumn(ctx, s.db, namespace, current.Version, "diagnostics_json")
	if err != nil {
		return err
	}
	digest := digestSnapshot(policyBytes, diagnosticsBytes)
	_, err = s.db.ExecContext(ctx, `INSERT INTO snapshots(namespace, version, parent_version, digest, policy_json, diagnostics_json, created_at) VALUES(?,?,?,?,?,?,?)`, namespace, gapVersion, current.Version, digest, policyBytes, diagnosticsBytes, "injected-gap")
	if err != nil {
		return fmt.Errorf("inject version gap: %w", err)
	}
	return nil
}

// InjectDigestMismatch rewrites same-version content while retaining the old
// digest, allowing tests to prove mixed content is rejected.
func (s *Store) InjectDigestMismatch(ctx context.Context, namespace string, version int) error {
	policyBytes, err := rawSnapshotColumn(ctx, s.db, namespace, version, "policy_json")
	if err != nil {
		return err
	}
	if len(policyBytes) == 0 {
		return business.New(business.CodeSnapshotVersion, "snapshot content is empty")
	}
	replaced := append(append([]byte(nil), policyBytes...), '\n')
	_, err = s.db.ExecContext(ctx, `UPDATE snapshots SET policy_json = ? WHERE namespace = ? AND version = ?`, replaced, namespace, version)
	if err != nil {
		return fmt.Errorf("inject digest mismatch: %w", err)
	}
	return nil
}

func rawSnapshotColumn(ctx context.Context, db *sql.DB, namespace string, version int, column string) ([]byte, error) {
	if column != "policy_json" && column != "diagnostics_json" {
		return nil, fmt.Errorf("unsupported column %q", column)
	}
	var value []byte
	err := db.QueryRowContext(ctx, `SELECT `+column+` FROM snapshots WHERE namespace = ? AND version = ?`, namespace, version).Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &business.Error{Code: business.CodeSnapshotVersion, Version: version, Message: "snapshot version does not exist"}
		}
		return nil, err
	}
	return append([]byte(nil), value...), nil
}
