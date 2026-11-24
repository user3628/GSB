// Package snapshot persists versioned detection snapshots in SQLite. Each
// round is committed in a single transaction; version-chain continuity and
// whole-bundle replacement are detected from content, not version numbers.
package snapshot

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"

	_ "modernc.org/sqlite"

	"privescworkbench/internal/domain"
)

// Store wraps a SQLite snapshot database.
type Store struct {
	db *sql.DB
}

// Open opens (creating if needed) a snapshot database.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_txlock=immediate&_busy_timeout=5000")
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;`); err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS snapshot_meta(
  version        INTEGER PRIMARY KEY,
  sample_version TEXT NOT NULL,
  sample_hash    TEXT NOT NULL,
  sampled_at     TEXT NOT NULL,
  elapsed_micros INTEGER NOT NULL,
  created_chain  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS proc_conclusion(
  version      INTEGER NOT NULL,
  pid          INTEGER NOT NULL,
  score_micro  INTEGER NOT NULL,
  bound_micro  INTEGER NOT NULL,
  low_micro    INTEGER NOT NULL,
  high_micro   INTEGER NOT NULL,
  code         TEXT NOT NULL,
  ledger_rem   INTEGER NOT NULL,
  ledger_items INTEGER NOT NULL,
  payload      TEXT NOT NULL,
  PRIMARY KEY(version, pid)
);
CREATE TABLE IF NOT EXISTS ledger_entry(
  version   INTEGER NOT NULL,
  pid       INTEGER NOT NULL,
  event_seq INTEGER NOT NULL,
  kind      TEXT NOT NULL,
  remainder INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS round_progress(
  session_id     TEXT PRIMARY KEY,
  sample_version TEXT NOT NULL,
  sample_hash    TEXT NOT NULL,
  started_at     TEXT NOT NULL
);
`)
	return err
}

// HashBundle returns the deterministic content hash of a sample bundle.
func HashBundle(b *domain.Bundle) string {
	procs, _ := json.Marshal(b.Processes)
	evs, _ := json.Marshal(b.Events)
	h := sha256.New()
	h.Write([]byte(b.Version))
	h.Write(procs)
	h.Write(evs)
	return hex.EncodeToString(h.Sum(nil))
}

// BeginProgress registers a running round so a hard kill leaves a resumable
// marker without advancing any snapshot version.
func (s *Store) BeginProgress(sessionID, sampleVersion, sampleHash string) error {
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO round_progress(session_id,sample_version,sample_hash,started_at)
		 VALUES(?,?,?,?)`, sessionID, sampleVersion, sampleHash, domain.FixedSampledAt)
	return err
}

// ClearProgress removes a finished round marker.
func (s *Store) ClearProgress(sessionID string) error {
	_, err := s.db.Exec(`DELETE FROM round_progress WHERE session_id=?`, sessionID)
	return err
}

// PendingProgress returns session ids left behind by a hard kill.
func (s *Store) PendingProgress() ([]string, error) {
	rows, err := s.db.Query(`SELECT session_id FROM round_progress ORDER BY session_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// Commit writes one full snapshot inside a single transaction and returns the
// new version number. The ledger is inserted as explicit rows so the error
// book remains auditable after reload.
func (s *Store) Commit(result *domain.RoundResult, ledger []domain.LedgerItem) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var next int64
	if err := tx.QueryRow(`SELECT COALESCE(MAX(version),0)+1 FROM snapshot_meta`).Scan(&next); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(`INSERT INTO snapshot_meta
		(version,sample_version,sample_hash,sampled_at,elapsed_micros,created_chain)
		VALUES(?,?,?,?,?,1)`,
		next, result.SampleVersion, result.SampleHash, result.SampledAt, result.ElapsedMicros); err != nil {
		return 0, err
	}
	for _, c := range result.Conclusions {
		payload, err := json.Marshal(c)
		if err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`INSERT INTO proc_conclusion
			(version,pid,score_micro,bound_micro,low_micro,high_micro,code,ledger_rem,ledger_items,payload)
			VALUES(?,?,?,?,?,?,?,?,?,?)`,
			next, c.PID, c.ScoreMicro, c.BoundMicro, c.Range.Low, c.Range.High,
			string(c.Code), c.Ledger.RemainderSum, c.Ledger.ItemCount, string(payload)); err != nil {
			return 0, err
		}
	}
	for _, item := range ledger {
		if _, err := tx.Exec(`INSERT INTO ledger_entry
			(version,pid,event_seq,kind,remainder) VALUES(?,?,?,?,?)`,
			next, item.ProcPID, item.EventSeq, item.Kind, item.Remainder); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

// LatestVersion returns the highest committed version or 0 when empty.
func (s *Store) LatestVersion() (int64, error) {
	var v int64
	err := s.db.QueryRow(`SELECT COALESCE(MAX(version),0) FROM snapshot_meta`).Scan(&v)
	return v, err
}

// LoadVersion reads one snapshot back into a RoundResult.
func (s *Store) LoadVersion(version int64) (*domain.RoundResult, []domain.LedgerItem, error) {
	var meta domain.RoundResult
	var chain int
	err := s.db.QueryRow(`SELECT sample_version,sample_hash,sampled_at,elapsed_micros,created_chain
		FROM snapshot_meta WHERE version=?`, version).
		Scan(&meta.SampleVersion, &meta.SampleHash, &meta.SampledAt, &meta.ElapsedMicros, &chain)
	if err == sql.ErrNoRows {
		return nil, nil, domain.NewError(domain.CodeSnapshotVersion,
			fmt.Sprintf("snapshot version %d not found", version))
	}
	if err != nil {
		return nil, nil, err
	}
	if chain == 0 {
		return nil, nil, domain.NewError(domain.CodeSnapshotVersion,
			fmt.Sprintf("snapshot version %d marked non-contiguous", version))
	}
	rows, err := s.db.Query(`SELECT payload FROM proc_conclusion WHERE version=? ORDER BY pid`, version)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, nil, err
		}
		var c domain.ProcConclusion
		if err := json.Unmarshal([]byte(payload), &c); err != nil {
			return nil, nil, err
		}
		meta.Conclusions = append(meta.Conclusions, c)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	lrows, err := s.db.Query(`SELECT pid,event_seq,kind,remainder FROM ledger_entry
		WHERE version=? ORDER BY pid,event_seq,kind`, version)
	if err != nil {
		return nil, nil, err
	}
	defer lrows.Close()
	var ledger []domain.LedgerItem
	for lrows.Next() {
		var item domain.LedgerItem
		if err := lrows.Scan(&item.ProcPID, &item.EventSeq, &item.Kind, &item.Remainder); err != nil {
			return nil, nil, err
		}
		ledger = append(ledger, item)
	}
	return &meta, ledger, lrows.Err()
}

// VerifyChain checks 1..N continuity and returns the last contiguous version.
// A gap yields E_SNAPSHOT_VERSION; callers fall back to the returned version.
func (s *Store) VerifyChain() (int64, error) {
	rows, err := s.db.Query(`SELECT version, created_chain FROM snapshot_meta ORDER BY version`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	expected := int64(1)
	var last int64
	for rows.Next() {
		var v, chain int64
		if err := rows.Scan(&v, &chain); err != nil {
			return 0, err
		}
		if v != expected || chain == 0 {
			return last, domain.NewError(domain.CodeSnapshotVersion,
				fmt.Sprintf("version chain gap at expected %d, found %d", expected, v))
		}
		last = v
		expected++
	}
	return last, rows.Err()
}

// MarkGap flags that a chain break was injected/observed at a version.
func (s *Store) MarkGap(version int64) error {
	_, err := s.db.Exec(`UPDATE snapshot_meta SET created_chain=0 WHERE version=?`, version)
	return err
}

// DeleteVersion removes a snapshot (used only by fault-injection tests).
func (s *Store) DeleteVersion(version int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM ledger_entry WHERE version=?`, version); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM proc_conclusion WHERE version=?`, version); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM snapshot_meta WHERE version=?`, version); err != nil {
		return err
	}
	return tx.Commit()
}

// ReplacementDetected reports whether the newest snapshot content hash differs
// from the previous one, detecting whole-bundle replacement across versions.
func (s *Store) ReplacementDetected() (replaced bool, fromHash, toHash string, err error) {
	rows, err := s.db.Query(`SELECT version, sample_hash FROM snapshot_meta ORDER BY version DESC LIMIT 2`)
	if err != nil {
		return false, "", "", err
	}
	defer rows.Close()
	var hashes []string
	for rows.Next() {
		var v int64
		var h string
		if err := rows.Scan(&v, &h); err != nil {
			return false, "", "", err
		}
		hashes = append(hashes, h)
	}
	if len(hashes) < 2 {
		return false, "", "", nil
	}
	// rows come newest first
	toHash, fromHash = hashes[0], hashes[1]
	return toHash != fromHash, fromHash, toHash, nil
}

// SortedVersions returns committed versions ascending.
func (s *Store) SortedVersions() ([]int64, error) {
	rows, err := s.db.Query(`SELECT version FROM snapshot_meta ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
