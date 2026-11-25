package storage

import "database/sql"

// PolicyRow is the persisted snapshot row.
type PolicyRow struct {
	Version           int64
	Checksum          string
	PayloadJSON       string
	SuppressionWindow int64
	ConcurrencyLimit  int
	RoundBudget       int64
	ToleranceMille    int64
	CreatedAtMS       int64
}

// InsertPolicyRow stores a complete snapshot row inside tx.
func InsertPolicyRow(tx *sql.Tx, r PolicyRow, nowMS int64) error {
	_, err := tx.Exec(
		`INSERT INTO policy_versions
		 (version,checksum,payload_json,suppression_window,concurrency_limit,round_budget,tolerance_mille,created_at_ms)
		 VALUES(?,?,?,?,?,?,?,?)`,
		r.Version, r.Checksum, r.PayloadJSON, r.SuppressionWindow,
		r.ConcurrencyLimit, r.RoundBudget, r.ToleranceMille, nowMS)
	return err
}

// GetPolicyRow loads one snapshot version.
func GetPolicyRow(q Querier, version int64) (PolicyRow, error) {
	var r PolicyRow
	err := q.QueryRow(
		`SELECT version,checksum,payload_json,suppression_window,concurrency_limit,round_budget,tolerance_mille,created_at_ms
		 FROM policy_versions WHERE version=?`, version).
		Scan(&r.Version, &r.Checksum, &r.PayloadJSON, &r.SuppressionWindow,
			&r.ConcurrencyLimit, &r.RoundBudget, &r.ToleranceMille, &r.CreatedAtMS)
	return r, err
}

// MaxPolicyVersion returns the highest stored snapshot version or 0.
func MaxPolicyVersion(q Querier) (int64, error) {
	var v int64
	err := q.QueryRow(`SELECT COALESCE(MAX(version),0) FROM policy_versions`).Scan(&v)
	return v, err
}

// GetPolicyRowTx loads a snapshot row inside an open transaction.
func GetPolicyRowTx(tx *sql.Tx, version int64) (PolicyRow, error) {
	var r PolicyRow
	err := tx.QueryRow(
		`SELECT version,checksum,payload_json,suppression_window,concurrency_limit,round_budget,tolerance_mille,created_at_ms
		 FROM policy_versions WHERE version=?`, version).
		Scan(&r.Version, &r.Checksum, &r.PayloadJSON, &r.SuppressionWindow,
			&r.ConcurrencyLimit, &r.RoundBudget, &r.ToleranceMille, &r.CreatedAtMS)
	return r, err
}
