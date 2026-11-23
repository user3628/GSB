package engine

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"

	_ "modernc.org/sqlite"
)

// Snapshot is one versioned forensic-conclusion snapshot. The prev_hash
// linkage plus the content hash make wholesale replacement of a snapshot
// detectable instead of only comparing version numbers.
type Snapshot struct {
	Version  int
	PrevHash string
	Hash     string
	ParamsFP string
	Content  string
}

type Store struct {
	db *sql.DB
}

func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS snapshots(
			version INTEGER PRIMARY KEY,
			prev_hash TEXT NOT NULL,
			hash TEXT NOT NULL,
			params_fingerprint TEXT NOT NULL,
			content TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS verdicts(
			window_id TEXT PRIMARY KEY,
			verdict TEXT NOT NULL,
			conclusion TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func contentHash(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// CommitSnapshot appends the next snapshot version inside a single SQLite
// transaction.
func (s *Store) CommitSnapshot(paramsFP, content string) (Snapshot, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Snapshot{}, err
	}
	defer tx.Rollback()

	version := 1
	prevHash := ""
	row := tx.QueryRow(`SELECT version, hash FROM snapshots ORDER BY version DESC LIMIT 1`)
	var lastVersion int
	var lastHash string
	switch err := row.Scan(&lastVersion, &lastHash); err {
	case nil:
		version = lastVersion + 1
		prevHash = lastHash
	case sql.ErrNoRows:
	default:
		return Snapshot{}, err
	}

	snap := Snapshot{
		Version:  version,
		PrevHash: prevHash,
		Hash:     contentHash(content),
		ParamsFP: paramsFP,
		Content:  content,
	}
	if _, err := tx.Exec(
		`INSERT INTO snapshots(version, prev_hash, hash, params_fingerprint, content) VALUES(?,?,?,?,?)`,
		snap.Version, snap.PrevHash, snap.Hash, snap.ParamsFP, snap.Content,
	); err != nil {
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

// LoadSnapshots validates the version chain starting at version 1: version
// continuity, prev_hash linkage and content-hash integrity. On any break it
// returns the longest valid prefix and broken=true (E_SNAPSHOT_VERSION),
// which rolls the loader back to the most recent continuous version.
func (s *Store) LoadSnapshots() (valid []Snapshot, broken bool, err error) {
	rows, err := s.db.Query(`SELECT version, prev_hash, hash, params_fingerprint, content FROM snapshots ORDER BY version`)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	valid = []Snapshot{}
	expectedVersion := 1
	expectedPrev := ""
	for rows.Next() {
		var snap Snapshot
		if err := rows.Scan(&snap.Version, &snap.PrevHash, &snap.Hash, &snap.ParamsFP, &snap.Content); err != nil {
			return nil, false, err
		}
		if snap.Version != expectedVersion || snap.PrevHash != expectedPrev || contentHash(snap.Content) != snap.Hash {
			return valid, true, nil
		}
		valid = append(valid, snap)
		expectedVersion++
		expectedPrev = snap.Hash
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return valid, false, nil
}

// SaveVerdict commits one window verdict in its own transaction. INSERT OR
// IGNORE guarantees that an already committed verdict never rolls back or is
// overwritten by a later recovery run.
func (s *Store) SaveVerdict(c Conclusion) error {
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT OR IGNORE INTO verdicts(window_id, verdict, conclusion) VALUES(?,?,?)`,
		c.WindowID, string(c.Verdict), string(data),
	)
	return err
}

func (s *Store) LoadVerdicts() (map[string]Conclusion, error) {
	rows, err := s.db.Query(`SELECT conclusion FROM verdicts`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]Conclusion{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var c Conclusion
		if err := json.Unmarshal([]byte(raw), &c); err != nil {
			return nil, err
		}
		out[c.WindowID] = c
	}
	return out, rows.Err()
}

// InsertSnapshotRow writes a raw snapshot row. It exists solely for the
// snapshot-version-skip fault injection path.
func (s *Store) InsertSnapshotRow(version int, prevHash, hash, paramsFP, content string) error {
	_, err := s.db.Exec(
		`INSERT INTO snapshots(version, prev_hash, hash, params_fingerprint, content) VALUES(?,?,?,?,?)`,
		version, prevHash, hash, paramsFP, content,
	)
	return err
}
