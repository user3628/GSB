// Package snapshot persists versioned tracing snapshots in SQLite. Each
// window settlement commits one snapshot with a strictly increasing version
// inside a single transaction. Loading verifies the version chain and the
// content-hash linkage so wholesale replacements are detected by content,
// not by version numbers alone.
package snapshot

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	_ "modernc.org/sqlite"

	"flowtrace/internal/codes"
	"flowtrace/internal/model"
)

// Snapshot is one committed tracing snapshot for a window.
type Snapshot struct {
	WindowID      string              `json:"window_id"`
	Version       int                 `json:"version"`
	PolicyVersion int                 `json:"policy_version"`
	Marks         map[string][]string `json:"marks"`
	Judgments     []model.Judgment    `json:"judgments"`
	ContentHash   string              `json:"content_hash"`
	PrevHash      string              `json:"prev_hash"`
}

// ChainError reports a broken version chain (gap or wholesale replacement).
type ChainError struct {
	Code     string
	WindowID string
	Detail   string
	// ValidPrefix is the latest continuous version that remains usable.
	ValidPrefix int
}

func (e *ChainError) Error() string {
	return fmt.Sprintf("%s: window %s: %s (valid prefix up to v%d)", e.Code, e.WindowID, e.Detail, e.ValidPrefix)
}

// Store wraps the SQLite database.
type Store struct{ db *sql.DB }

// Open opens (and lazily initializes) the snapshot database.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS snapshots(
			window_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			policy_version INTEGER NOT NULL,
			marks_json TEXT NOT NULL,
			judgments_json TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			prev_hash TEXT NOT NULL,
			PRIMARY KEY(window_id, version))`,
		`CREATE TABLE IF NOT EXISTS windows(
			window_id TEXT PRIMARY KEY,
			status TEXT NOT NULL,
			event_count INTEGER NOT NULL,
			judgment_version INTEGER NOT NULL,
			policy_version INTEGER NOT NULL)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

// HashContent computes the deterministic content hash of a snapshot body.
func HashContent(policyVersion int, marks map[string][]string, judgments []model.Judgment) string {
	fields := make([]string, 0, len(marks))
	for f := range marks {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	type body struct {
		PolicyVersion int                 `json:"p"`
		Fields        []string            `json:"f"`
		Marks         map[string][]string `json:"m"`
		Judgments     []model.Judgment    `json:"j"`
	}
	norm := make(map[string][]string, len(marks))
	for _, f := range fields {
		m := append([]string{}, marks[f]...)
		sort.Strings(m)
		norm[f] = m
	}
	js := append([]model.Judgment{}, judgments...)
	sort.Slice(js, func(a, b int) bool {
		if js[a].EventSeq != js[b].EventSeq {
			return js[a].EventSeq < js[b].EventSeq
		}
		return js[a].DstField < js[b].DstField
	})
	raw, _ := json.Marshal(body{policyVersion, fields, norm, js})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Commit stores one snapshot and the window progress row in a single
// transaction. The version is assigned as max(existing)+1 for the window.
func (s *Store) Commit(windowID string, policyVersion int, marks map[string][]string, judgments []model.Judgment, status string, eventCount, judgmentVersion int) (Snapshot, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Snapshot{}, err
	}
	defer tx.Rollback()

	var maxV sql.NullInt64
	var prevHash string
	row := tx.QueryRow(`SELECT MAX(version) FROM snapshots WHERE window_id=?`, windowID)
	if err := row.Scan(&maxV); err != nil {
		return Snapshot{}, err
	}
	if maxV.Valid {
		if err := tx.QueryRow(`SELECT content_hash FROM snapshots WHERE window_id=? AND version=?`, windowID, maxV.Int64).Scan(&prevHash); err != nil {
			return Snapshot{}, err
		}
	}
	version := int(maxV.Int64) + 1
	snap := Snapshot{
		WindowID: windowID, Version: version, PolicyVersion: policyVersion,
		Marks: marks, Judgments: judgments, PrevHash: prevHash,
	}
	snap.ContentHash = HashContent(policyVersion, marks, judgments)

	marksJSON, _ := json.Marshal(snap.Marks)
	judgmentsJSON, _ := json.Marshal(snap.Judgments)
	if _, err := tx.Exec(`INSERT INTO snapshots(window_id,version,policy_version,marks_json,judgments_json,content_hash,prev_hash)
		VALUES(?,?,?,?,?,?,?)`, windowID, version, policyVersion, string(marksJSON), string(judgmentsJSON), snap.ContentHash, prevHash); err != nil {
		return Snapshot{}, err
	}
	if _, err := tx.Exec(`INSERT INTO windows(window_id,status,event_count,judgment_version,policy_version)
		VALUES(?,?,?,?,?)
		ON CONFLICT(window_id) DO UPDATE SET status=excluded.status, event_count=excluded.event_count,
		judgment_version=excluded.judgment_version, policy_version=excluded.policy_version`,
		windowID, status, eventCount, judgmentVersion, policyVersion); err != nil {
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

// LoadChain loads the full snapshot chain of a window and verifies version
// continuity and content-hash linkage. A gap or a wholesale replacement
// (hash-link break with contiguous versions) yields a *ChainError with code
// E_SNAPSHOT_VERSION plus the usable continuous prefix.
func (s *Store) LoadChain(windowID string) ([]Snapshot, error) {
	rows, err := s.db.Query(`SELECT version,policy_version,marks_json,judgments_json,content_hash,prev_hash
		FROM snapshots WHERE window_id=? ORDER BY version`, windowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var chain []Snapshot
	for rows.Next() {
		var snap Snapshot
		var marksJSON, judgmentsJSON string
		if err := rows.Scan(&snap.Version, &snap.PolicyVersion, &marksJSON, &judgmentsJSON, &snap.ContentHash, &snap.PrevHash); err != nil {
			return nil, err
		}
		snap.WindowID = windowID
		if err := json.Unmarshal([]byte(marksJSON), &snap.Marks); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(judgmentsJSON), &snap.Judgments); err != nil {
			return nil, err
		}
		chain = append(chain, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i, snap := range chain {
		want := i + 1
		if snap.Version != want {
			return chain[:i], &ChainError{Code: codes.ESnapshotVersion, WindowID: windowID,
				Detail:      fmt.Sprintf("version gap: expected v%d, found v%d", want, snap.Version),
				ValidPrefix: i}
		}
		if i > 0 && snap.PrevHash != chain[i-1].ContentHash {
			return chain[:i], &ChainError{Code: codes.ESnapshotVersion, WindowID: windowID,
				Detail:      fmt.Sprintf("wholesale replacement at v%d: content-hash link broken", snap.Version),
				ValidPrefix: i}
		}
	}
	return chain, nil
}

// WindowRow is the committed progress of a window.
type WindowRow struct {
	WindowID        string
	Status          string
	EventCount      int
	JudgmentVersion int
	PolicyVersion   int
}

// CommittedWindows lists windows with committed progress (used on recovery;
// windows without a row were never committed and are re-traced wholesale).
func (s *Store) CommittedWindows() ([]WindowRow, error) {
	rows, err := s.db.Query(`SELECT window_id,status,event_count,judgment_version,policy_version FROM windows ORDER BY window_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WindowRow
	for rows.Next() {
		var w WindowRow
		if err := rows.Scan(&w.WindowID, &w.Status, &w.EventCount, &w.JudgmentVersion, &w.PolicyVersion); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// DeleteVersion removes one snapshot version. It exists solely so the
// snapshot-gap injection scenario (triggered by an external parameter) can
// simulate a broken version chain.
func (s *Store) DeleteVersion(windowID string, version int) error {
	_, err := s.db.Exec(`DELETE FROM snapshots WHERE window_id=? AND version=?`, windowID, version)
	return err
}
