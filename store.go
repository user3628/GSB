package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// Store persists versioned repo snapshots in a single SQLite database.
type Store struct {
	db *sql.DB
}

func OpenStore(stateDir string) (*Store, error) {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", filepath.Join(stateDir, "bire-state.sqlite"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS snapshots (
  repo TEXT NOT NULL,
  version INTEGER NOT NULL,
  payload TEXT NOT NULL,
  digest TEXT NOT NULL,
  PRIMARY KEY (repo, version)
);
CREATE TABLE IF NOT EXISTS repo_state (
  repo TEXT PRIMARY KEY,
  current_version INTEGER NOT NULL,
  restore_generation INTEGER NOT NULL,
  codes TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  repo TEXT NOT NULL,
  status TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS batch_stats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  max_in_flight INTEGER NOT NULL,
  repos_total INTEGER NOT NULL,
  repos_failed INTEGER NOT NULL
);`)
	return err
}

// CommitSnapshot stores one inspection round as the next snapshot version
// for the repo, together with the repo state, in a single transaction.
// crashBeforeCommit simulates a hard kill between write and commit.
func (s *Store) CommitSnapshot(repo string, res *InspectResult, crashBeforeCommit bool) (int, error) {
	payload, err := json.Marshal(res)
	if err != nil {
		return 0, err
	}
	digest := sha256Hex(payload)
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var maxVersion sql.NullInt64
	if err := tx.QueryRow(`SELECT MAX(version) FROM snapshots WHERE repo = ?`, repo).Scan(&maxVersion); err != nil {
		return 0, err
	}
	version := int(maxVersion.Int64) + 1
	if _, err := tx.Exec(`INSERT INTO snapshots (repo, version, payload, digest) VALUES (?, ?, ?, ?)`,
		repo, version, string(payload), digest); err != nil {
		return 0, err
	}
	codes, _ := json.Marshal(res.Codes)
	if _, err := tx.Exec(`INSERT INTO repo_state (repo, current_version, restore_generation, codes)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(repo) DO UPDATE SET current_version = excluded.current_version,
		restore_generation = excluded.restore_generation, codes = excluded.codes`,
		repo, version, res.RestoreGen, string(codes)); err != nil {
		return 0, err
	}
	if crashBeforeCommit {
		crashExit()
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return version, nil
}

// LoadSnapshots returns the contiguous snapshot chain for a repo. A version
// gap or payload-digest mismatch stops the chain at the last consistent
// version; the fallback itself never advances any version number.
func (s *Store) LoadSnapshots(repo string) ([]Snapshot, string, error) {
	rows, err := s.db.Query(`SELECT version, payload, digest FROM snapshots WHERE repo = ? ORDER BY version`, repo)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var all []Snapshot
	for rows.Next() {
		var sn Snapshot
		sn.Repo = repo
		if err := rows.Scan(&sn.Version, &sn.Payload, &sn.Digest); err != nil {
			return nil, "", err
		}
		all = append(all, sn)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	var contiguous []Snapshot
	code := ""
	expected := 1
	for _, sn := range all {
		if sn.Version != expected {
			code = ESnapshotVersion
			break
		}
		if sha256Hex([]byte(sn.Payload)) != sn.Digest {
			code = ESnapshotDigest
			break
		}
		contiguous = append(contiguous, sn)
		expected++
	}
	return contiguous, code, nil
}

// LatestResult returns the newest contiguous inspection result for a repo.
func (s *Store) LatestResult(repo string) (*InspectResult, int, string, error) {
	snaps, code, err := s.LoadSnapshots(repo)
	if err != nil {
		return nil, 0, code, err
	}
	if len(snaps) == 0 {
		return nil, 0, code, fmt.Errorf("no snapshots for repo %q", repo)
	}
	latest := snaps[len(snaps)-1]
	var res InspectResult
	if err := json.Unmarshal([]byte(latest.Payload), &res); err != nil {
		return nil, 0, code, fmt.Errorf("snapshot payload corrupt: %v", err)
	}
	return &res, latest.Version, code, nil
}

// InjectSnapshotGap inserts a skipped-version snapshot to simulate external
// snapshot-chain corruption (injection via input only).
func (s *Store) InjectSnapshotGap(repo string) (int, error) {
	var maxVersion sql.NullInt64
	if err := s.db.QueryRow(`SELECT MAX(version) FROM snapshots WHERE repo = ?`, repo).Scan(&maxVersion); err != nil {
		return 0, err
	}
	gapVersion := int(maxVersion.Int64) + 2
	payload := `{"injected":true}`
	_, err := s.db.Exec(`INSERT INTO snapshots (repo, version, payload, digest) VALUES (?, ?, ?, ?)`,
		repo, gapVersion, payload, sha256Hex([]byte(payload)))
	return gapVersion, err
}

// Session tracking makes in-flight inspections observable.

func (s *Store) SessionStart(repo string) (int64, error) {
	r, err := s.db.Exec(`INSERT INTO sessions (repo, status) VALUES (?, 'running')`, repo)
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

func (s *Store) SessionFinish(id int64, status, detail string) error {
	_, err := s.db.Exec(`UPDATE sessions SET status = ?, detail = ? WHERE id = ?`, status, detail, id)
	return err
}

func (s *Store) RecordBatchStats(maxInFlight, total, failed int) error {
	_, err := s.db.Exec(`INSERT INTO batch_stats (max_in_flight, repos_total, repos_failed) VALUES (?, ?, ?)`,
		maxInFlight, total, failed)
	return err
}

// Stats summarises the most recent batch and session activity.
type Stats struct {
	MaxInFlight int          `json:"max_in_flight"`
	ReposTotal  int          `json:"repos_total"`
	ReposFailed int          `json:"repos_failed"`
	InFlight    int          `json:"in_flight"`
	Sessions    []SessionRow `json:"sessions"`
}

type SessionRow struct {
	Repo   string `json:"repo"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

func (s *Store) Stats() (*Stats, error) {
	st := &Stats{}
	row := s.db.QueryRow(`SELECT max_in_flight, repos_total, repos_failed FROM batch_stats ORDER BY id DESC LIMIT 1`)
	if err := row.Scan(&st.MaxInFlight, &st.ReposTotal, &st.ReposFailed); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE status = 'running'`).Scan(&st.InFlight); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`SELECT repo, status, detail FROM sessions ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var sr SessionRow
		if err := rows.Scan(&sr.Repo, &sr.Status, &sr.Detail); err != nil {
			return nil, err
		}
		st.Sessions = append(st.Sessions, sr)
	}
	return st, rows.Err()
}

// ListSnapshotRepos returns repos with at least one snapshot, sorted.
func (s *Store) ListSnapshotRepos() ([]string, error) {
	rows, err := s.db.Query(`SELECT DISTINCT repo FROM snapshots ORDER BY repo`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var repos []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return nil, err
		}
		repos = append(repos, r)
	}
	return repos, rows.Err()
}
