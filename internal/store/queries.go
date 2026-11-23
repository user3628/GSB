package store

import (
	"database/sql"
	"fmt"
)

// ScanLease reads the single lease row for rowID. sql.ErrNoRows is returned
// untouched when no lease has ever been granted.
func ScanLease(q queryRower, rowID string) (*Lease, error) {
	row := q.QueryRow(`SELECT row_id, lease_version, holder, state, granted_at,
		renewed_at, expires_at FROM write_leases WHERE row_id=?`, rowID)
	var l Lease
	var state string
	if err := row.Scan(&l.RowID, &l.LeaseVersion, &l.Holder, &state,
		&l.GrantedAtMs, &l.RenewedAtMs, &l.ExpiresAtMs); err != nil {
		return nil, err
	}
	l.State = LeaseState(state)
	return &l, nil
}

// UpsertLease creates or replaces the lease row for rowID inside tx.
func UpsertLease(tx *sql.Tx, l Lease) error {
	_, err := tx.Exec(`INSERT INTO write_leases
		(row_id, lease_version, holder, state, granted_at, renewed_at, expires_at)
		VALUES(?,?,?,?,?,?,?)
		ON CONFLICT(row_id) DO UPDATE SET lease_version=excluded.lease_version,
		holder=excluded.holder, state=excluded.state, granted_at=excluded.granted_at,
		renewed_at=excluded.renewed_at, expires_at=excluded.expires_at`,
		l.RowID, l.LeaseVersion, l.Holder, string(l.State),
		l.GrantedAtMs, l.RenewedAtMs, l.ExpiresAtMs)
	return err
}

// UpdateLeaseState transitions a lease row, requiring the expected version so
// stale holders can never overwrite a newer grant.
func UpdateLeaseState(tx *sql.Tx, rowID string, leaseVer int64, state LeaseState, nowMs int64) (bool, error) {
	res, err := tx.Exec(`UPDATE write_leases SET state=?, renewed_at=?
		WHERE row_id=? AND lease_version=?`, string(state), nowMs, rowID, leaseVer)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// LatestRowVersion returns the newest (highest row_version) version of rowID
// regardless of deletion or retirement.
func LatestRowVersion(q queryRower, rowID string) (*RowVersion, error) {
	row := q.QueryRow(`SELECT row_id, row_version, commit_ver, content_hash, content,
		deleted, COALESCE(retired_at,0), created_at FROM row_versions
		WHERE row_id=? ORDER BY row_version DESC LIMIT 1`, rowID)
	return scanRow(row)
}

// RowVersionAt returns the visible row version for a read snapshot: the
// highest row_version whose commit_ver <= snapshot and which is not retired.
// Retired rows surface as a sentinel error so the caller can distinguish
// "never existed" from "existed but reclaimed".
func RowVersionAt(q queryRower, rowID string, snapshot int64) (*RowVersion, error) {
	rows, err := q.Query(`SELECT row_id, row_version, commit_ver, content_hash, content,
		deleted, COALESCE(retired_at,0), created_at FROM row_versions
		WHERE row_id=? AND commit_ver<=? ORDER BY row_version DESC LIMIT 1`, rowID, snapshot)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, sql.ErrNoRows
	}
	rv, err := scanRow(rows)
	if err != nil {
		return nil, err
	}
	if rv.RetiredAtMs != 0 {
		return rv, ErrVersionRetired
	}
	return rv, nil
}

// VersionAtOrRetired scans without the snapshot predicate for recycling logic.
func VersionByNumber(q queryRower, rowID string, rowVer int64) (*RowVersion, error) {
	row := q.QueryRow(`SELECT row_id, row_version, commit_ver, content_hash, content,
		deleted, COALESCE(retired_at,0), created_at FROM row_versions
		WHERE row_id=? AND row_version=?`, rowID, rowVer)
	return scanRow(row)
}

// RetainableVersions lists old (non-latest), non-retired versions of rowID
// ordered oldest first for retention decisions.
func RetainableVersions(q queryRower, rowID string) ([]RowVersion, error) {
	rows, err := q.Query(`SELECT row_id, row_version, commit_ver, content_hash, content,
		deleted, COALESCE(retired_at,0), created_at FROM row_versions
		WHERE row_id=? AND retired_at IS NULL
		ORDER BY row_version DESC`, rowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var all []RowVersion
	for rows.Next() {
		rv, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		all = append(all, *rv)
	}
	return all, rows.Err()
}

// RetireVersion marks one row version as reclaimed inside tx.
func RetireVersion(tx *sql.Tx, rowID string, rowVer, nowMs int64) error {
	_, err := tx.Exec(`UPDATE row_versions SET retired_at=?
		WHERE row_id=? AND row_version=? AND retired_at IS NULL`, nowMs, rowID, rowVer)
	return err
}

// InsertRowVersion appends a new immutable row version inside tx.
func InsertRowVersion(tx *sql.Tx, rv RowVersion) error {
	_, err := tx.Exec(`INSERT INTO row_versions
		(row_id, row_version, commit_ver, content_hash, content, deleted, retired_at, created_at, intent_id)
		VALUES(?,?,?,?,?,?,NULL,?,?)`,
		rv.RowID, rv.RowVersion, rv.CommitVer, rv.ContentHash, rv.Content,
		boolInt(rv.Deleted), rv.CreatedAtMs, rv.IntentID)
	return err
}

// RowVersionByIntent finds an orphan row version written by an uncompleted
// commit intent during crash recovery.
func RowVersionByIntent(tx *sql.Tx, intentID int64) (*RowVersion, error) {
	row := tx.QueryRow(`SELECT row_id, row_version, commit_ver, content_hash, content,
		deleted, COALESCE(retired_at,0), created_at FROM row_versions WHERE intent_id=?`, intentID)
	return scanRow(row)
}

// DeleteRowVersion removes an orphan row version during recovery.
func DeleteRowVersion(tx *sql.Tx, rowID string, rowVer int64) error {
	_, err := tx.Exec(`DELETE FROM row_versions WHERE row_id=? AND row_version=?`, rowID, rowVer)
	return err
}

// SetCommitVer rewinds the global commit version when the orphan version held
// the highest allocated commit number.
func SetCommitVer(tx *sql.Tx, v int64) error {
	_, err := tx.Exec(`UPDATE meta SET value=? WHERE key='commit_ver'`, fmt.Sprintf("%d", v))
	return err
}

// AllRowIDs returns the distinct set of seeded rows.
func AllRowIDs(q queryRower) ([]string, error) {
	rows, err := q.Query(`SELECT DISTINCT row_id FROM row_versions ORDER BY row_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

type rowScanner interface {
	Scan(dest ...any) error
}

type queryRower interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

func scanRow(sc rowScanner) (*RowVersion, error) {
	var rv RowVersion
	var deleted, retired int64
	if err := sc.Scan(&rv.RowID, &rv.RowVersion, &rv.CommitVer, &rv.ContentHash,
		&rv.Content, &deleted, &retired, &rv.CreatedAtMs); err != nil {
		return nil, err
	}
	rv.Deleted = deleted == 1
	rv.RetiredAtMs = retired
	return &rv, nil
}

// AllRowIDs enumerates rows directly on the store's database handle.
func (s *Store) AllRowIDs() ([]string, error) { return AllRowIDs(s.DB) }
