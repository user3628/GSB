package store

import "database/sql"

// nextRequestSeqTx computes the next stable submission sequence inside the
// writer transaction so concurrent inserts cannot interleave.
func NextRequestSeqTx(tx *sql.Tx) (int64, error) {
	var seq int64
	err := tx.QueryRow(`SELECT COALESCE(MAX(seq),0)+1 FROM requests`).Scan(&seq)
	return seq, err
}
