package snapshot

import (
	"database/sql"
	"fmt"
	"sort"

	_ "modernc.org/sqlite"

	"forensicworkbench/internal/codes"
	"forensicworkbench/internal/correlate"
)

// Store is a versioned snapshot repository backed by one SQLite file.
type Store struct {
	db *sql.DB
}

// Open opens (creating the schema if needed) the snapshot database at path.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS snapshots (
			version      INTEGER PRIMARY KEY,
			content_hash TEXT NOT NULL,
			payload      BLOB NOT NULL,
			created_seq  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS format_versions (
			version INTEGER NOT NULL,
			fmt     TEXT NOT NULL,
			PRIMARY KEY(version, fmt)
		);
		CREATE TABLE IF NOT EXISTS meta (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);`); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// LatestVersion returns the highest committed version, or 0 when empty.
func (s *Store) LatestVersion() (int64, error) {
	var v int64
	err := s.db.QueryRow(`SELECT COALESCE(MAX(version),0) FROM snapshots`).Scan(&v)
	return v, err
}

// supportedFormats is the set this build can load.
var supportedFormats = map[string]bool{
	"binary/v1": true, "binary/v2": true, "jsonl/v1": true, "text/v1": true,
}

// Commit persists s as the next replacement snapshot. It assigns the next
// version, records the format-version set and detects same-version content
// drift via the content hash. Snapshots are immutable once written.
//
// If s.Version is left zero it is assigned latest+1. A supplied version must be
// exactly latest+1 to keep the chain gap-free.
func (s *Store) Commit(sn *Snapshot) (*Snapshot, error) {
	sort.Strings(sn.FormatVersions)
	sort.Strings(sn.SourcePaths)

	payload, hash, err := canonicalPayload(sn)
	if err != nil {
		return nil, err
	}
	sn.ContentHash = hash

	latest, err := s.LatestVersion()
	if err != nil {
		return nil, err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Idempotent recommit: identical content already exists at any version.
	var existingVersion int64
	idemErr := tx.QueryRow(`SELECT version FROM snapshots WHERE content_hash=? ORDER BY version LIMIT 1`, hash).Scan(&existingVersion)
	if idemErr == nil {
		sn.Version = existingVersion
		return sn, tx.Commit()
	} else if idemErr != sql.ErrNoRows {
		return nil, idemErr
	}

	if sn.Version == 0 {
		sn.Version = latest + 1
	} else if sn.Version != latest+1 {
		return nil, newIncompat(fmt.Sprintf("version %d does not extend latest %d without a gap", sn.Version, latest))
	}

	// Detect replacement content that reuses an existing version number.
	var existingHash string
	lookupErr := tx.QueryRow(`SELECT content_hash FROM snapshots WHERE version=?`, sn.Version).Scan(&existingHash)
	if lookupErr == nil {
		if existingHash != hash {
			return nil, newIncompat(fmt.Sprintf("version %d already committed with different content (hash %s != %s)", sn.Version, existingHash, hash))
		}
		// Identical re-commit is an idempotent no-op.
		return sn, tx.Commit()
	} else if lookupErr != sql.ErrNoRows {
		return nil, lookupErr
	}

	if _, err := tx.Exec(
		`INSERT INTO snapshots(version,content_hash,payload,created_seq) VALUES(?,?,?,?)`,
		sn.Version, hash, payload, sn.Version,
	); err != nil {
		return nil, err
	}
	for _, fv := range sn.FormatVersions {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO format_versions(version,fmt) VALUES(?,?)`, sn.Version, fv); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return sn, nil
}

// LoadChain loads every snapshot from version 1 up to the latest, rejecting the
// whole chain if any of the following holds:
//   - a format version is unknown to the current parser;
//   - the version numbers are not a gap-free 1..N sequence;
//   - a stored payload's recomputed hash differs (tamper/content drift).
//
// On rejection nothing is returned (no partial load), matching the requirement
// to roll back to the most recent continuous chain.
func (s *Store) LoadChain() ([]*Snapshot, error) {
	rows, err := s.db.Query(`SELECT version, content_hash, payload FROM snapshots ORDER BY version ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var loaded []*Snapshot
	var expected int64 = 1
	for rows.Next() {
		var version int64
		var hash string
		var payload []byte
		if err := rows.Scan(&version, &hash, &payload); err != nil {
			return nil, err
		}
		if version != expected {
			return nil, newIncompat(fmt.Sprintf("version chain gap: expected %d found %d; rolling back to continuous prefix", expected, version))
		}
		fmts, err := s.formatsFor(version)
		if err != nil {
			return nil, err
		}
		for _, fv := range fmts {
			if !supportedFormats[fv] {
				return nil, newIncompat(fmt.Sprintf("snapshot %d depends on unsupported format version %q", version, fv))
			}
		}
		sn, err := decodePayload(version, hash, fmts, payload)
		if err != nil {
			return nil, err
		}
		loaded = append(loaded, sn)
		expected++
	}
	return loaded, rows.Err()
}

func (s *Store) formatsFor(version int64) ([]string, error) {
	rows, err := s.db.Query(`SELECT fmt FROM format_versions WHERE version=? ORDER BY fmt`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func decodePayload(version int64, hash string, fmts []string, payload []byte) (*Snapshot, error) {
	var body struct {
		FormatVersions []string            `json:"format_versions"`
		SourcePaths    []string            `json:"source_paths"`
		EventCount     int                 `json:"event_count"`
		Graph          *correlate.Graph    `json:"graph"`
		Errors         []codes.ErrorRecord `json:"errors"`
	}
	if err := jsonUnmarshal(payload, &body); err != nil {
		return nil, newIncompat("corrupt snapshot payload: " + err.Error())
	}
	sn := &Snapshot{
		Version:        version,
		ContentHash:    hash,
		FormatVersions: fmts,
		SourcePaths:    body.SourcePaths,
		EventCount:     body.EventCount,
		Graph:          body.Graph,
		Errors:         body.Errors,
	}
	recomputed, err := HashContent(sn)
	if err != nil {
		return nil, err
	}
	if recomputed != hash {
		return nil, newIncompat(fmt.Sprintf("snapshot %d content hash mismatch (stored %s got %s)", version, hash, recomputed))
	}
	return sn, nil
}
