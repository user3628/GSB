package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"

	"queryrewrite/internal/spec"
)

// Store wraps a SQLite database and its cross-process file lock.
type Store struct {
	db   *sql.DB
	lock *FileLock
}

// Open opens (creating schema as needed) the SQLite database at dbPath.
func Open(dbPath, lockPath string) (*Store, error) {
	// DSN-level busy timeout plus WAL so concurrent processes/goroutines wait for
	// the write lock instead of raising SQLITE_BUSY immediately.
	dsn := "file:" + dbPath + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// Single connection keeps the file-lock/transaction pairing coherent within
	// a process; busy_timeout + WAL make concurrent writers wait for the write
	// lock instead of failing immediately with SQLITE_BUSY.
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA busy_timeout = 10000",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = FULL",
		"PRAGMA foreign_keys = ON",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, err
		}
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, err
	}
	lock, err := AcquireFileLock(lockPath)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, lock: lock}, nil
}

// Close releases the file lock and database handle.
func (s *Store) Close() error {
	err1 := s.db.Close()
	err2 := s.lock.Release()
	if err1 != nil {
		return err1
	}
	return err2
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS checkpoints (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  stage TEXT NOT NULL,
  session_id TEXT NOT NULL,
  turn INTEGER NOT NULL,
  input_digest TEXT NOT NULL,
  policy_version INTEGER NOT NULL,
  product_digest TEXT NOT NULL,
  payload TEXT NOT NULL,
  UNIQUE(stage, session_id, turn)
);
CREATE TABLE IF NOT EXISTS policy_versions (
  version INTEGER PRIMARY KEY,
  parent_version INTEGER NOT NULL,
  content_digest TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS slots (
  slot_id TEXT PRIMARY KEY,
  fingerprint TEXT NOT NULL UNIQUE,
  block_count INTEGER NOT NULL,
  owner_session TEXT NOT NULL,
  owner_turn INTEGER NOT NULL,
  birth_turn INTEGER NOT NULL,
  last_used_turn INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS turn_index (
  session_id TEXT NOT NULL,
  next_turn INTEGER NOT NULL,
  PRIMARY KEY(session_id)
);
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`

// CommitCheckpoint inserts one stage fact inside a transaction. The UNIQUE
// constraint guarantees a single effective record per session/turn/stage; a
// duplicate publish is reported as E_STALE_CHECKPOINT.
func (s *Store) CommitCheckpoint(cp spec.Checkpoint) (int64, error) {
	var seq int64
	err := withTx(s.db, func(tx *sql.Tx) error {
		var exists int
		row := tx.QueryRow(
			`SELECT COUNT(1) FROM checkpoints WHERE stage=? AND session_id=? AND turn=?`,
			cp.Stage, cp.SessionID, cp.Turn)
		if err := row.Scan(&exists); err != nil {
			return err
		}
		if exists > 0 {
			return spec.NewBizError(spec.CodeStaleCheckpoint,
				"checkpoint for stage already committed", cp.SessionID, cp.Turn, cp.Stage)
		}
		res, err := tx.Exec(
			`INSERT INTO checkpoints
			 (stage, session_id, turn, input_digest, policy_version, product_digest, payload)
			 VALUES (?,?,?,?,?,?,?)`,
			cp.Stage, cp.SessionID, cp.Turn, cp.InputDigest, cp.PolicyVersion,
			cp.ProductDigest, cp.Payload)
		if err != nil {
			return err
		}
		seq, err = res.LastInsertId()
		return err
	})
	return seq, err
}

// AdvanceTurn conditionally advances the per-session monotonic turn index.
// Only consecutive turns are accepted; lag/duplicate/cross-session misorder
// returns E_STALE_CHECKPOINT.
func (s *Store) AdvanceTurn(sessionID string, turn int) error {
	return withTx(s.db, func(tx *sql.Tx) error {
		var next int
		row := tx.QueryRow(`SELECT next_turn FROM turn_index WHERE session_id=?`, sessionID)
		err := row.Scan(&next)
		switch {
		case err == sql.ErrNoRows:
			if turn != 1 {
				return spec.NewBizError(spec.CodeStaleCheckpoint,
					"first turn for session must be 1", sessionID, turn, "")
			}
			if _, err := tx.Exec(
				`INSERT INTO turn_index(session_id, next_turn) VALUES(?,?)`,
				sessionID, 2); err != nil {
				return err
			}
		case err != nil:
			return err
		default:
			if turn < next {
				return spec.NewBizError(spec.CodeStaleCheckpoint,
					fmt.Sprintf("stale turn publish: turn=%d next_expected=%d", turn, next),
					sessionID, turn, "")
			}
			if turn > next {
				return spec.NewBizError(spec.CodeStaleCheckpoint,
					fmt.Sprintf("non-consecutive turn: turn=%d next_expected=%d", turn, next),
					sessionID, turn, "")
			}
			if _, err := tx.Exec(
				`UPDATE turn_index SET next_turn=? WHERE session_id=?`,
				next+1, sessionID); err != nil {
				return err
			}
		}
		return nil
	})
}

// CommitCheckpointWithSlots atomically inserts a stage fact and rewrites the
// durable slot table in one transaction. A crash either leaves both committed
// or neither, which keeps post-restart state identical to a continuous run.
func (s *Store) CommitCheckpointWithSlots(cp spec.Checkpoint, slots []*spec.Slot) (int64, error) {
	var seq int64
	err := withTx(s.db, func(tx *sql.Tx) error {
		var exists int
		row := tx.QueryRow(
			`SELECT COUNT(1) FROM checkpoints WHERE stage=? AND session_id=? AND turn=?`,
			cp.Stage, cp.SessionID, cp.Turn)
		if err := row.Scan(&exists); err != nil {
			return err
		}
		if exists > 0 {
			return spec.NewBizError(spec.CodeStaleCheckpoint,
				"checkpoint for stage already committed", cp.SessionID, cp.Turn, cp.Stage)
		}
		res, err := tx.Exec(
			`INSERT INTO checkpoints
			 (stage, session_id, turn, input_digest, policy_version, product_digest, payload)
			 VALUES (?,?,?,?,?,?,?)`,
			cp.Stage, cp.SessionID, cp.Turn, cp.InputDigest, cp.PolicyVersion,
			cp.ProductDigest, cp.Payload)
		if err != nil {
			return err
		}
		if seq, err = res.LastInsertId(); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM slots`); err != nil {
			return err
		}
		for _, sl := range slots {
			if _, err := tx.Exec(
				`INSERT INTO slots(slot_id, fingerprint, block_count, owner_session,
				                    owner_turn, birth_turn, last_used_turn)
				 VALUES(?,?,?,?,?,?,?)`,
				sl.SlotID, sl.Fingerprint, sl.BlockCount, sl.OwnerSession,
				sl.OwnerTurn, sl.BirthTurn, sl.LastUsedTurn); err != nil {
				return err
			}
		}
		return nil
	})
	return seq, err
}

// GetCheckpoint returns the committed fact for a stage, or nil without error.
func (s *Store) GetCheckpoint(stage, sessionID string, turn int) (*spec.Checkpoint, error) {
	row := s.db.QueryRow(
		`SELECT seq, stage, session_id, turn, input_digest, policy_version,
		        product_digest, payload
		 FROM checkpoints WHERE stage=? AND session_id=? AND turn=?`,
		stage, sessionID, turn)
	var cp spec.Checkpoint
	err := row.Scan(&cp.Seq, &cp.Stage, &cp.SessionID, &cp.Turn, &cp.InputDigest,
		&cp.PolicyVersion, &cp.ProductDigest, &cp.Payload)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &cp, nil
}

// Checkpoints returns all committed facts ordered by monotonic seq.
func (s *Store) Checkpoints() ([]spec.Checkpoint, error) {
	rows, err := s.db.Query(
		`SELECT seq, stage, session_id, turn, input_digest, policy_version,
		        product_digest, payload FROM checkpoints ORDER BY seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []spec.Checkpoint{}
	for rows.Next() {
		var cp spec.Checkpoint
		if err := rows.Scan(&cp.Seq, &cp.Stage, &cp.SessionID, &cp.Turn,
			&cp.InputDigest, &cp.PolicyVersion, &cp.ProductDigest, &cp.Payload); err != nil {
			return nil, err
		}
		out = append(out, cp)
	}
	return out, rows.Err()
}

// SavePolicy records a policy version (idempotent by version number).
func (s *Store) SavePolicy(p spec.Policy, kind string) error {
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT OR REPLACE INTO policy_versions
		 (version, parent_version, content_digest, kind, payload)
		 VALUES(?,?,?,?,?)`,
		p.Version, p.ParentVersion, p.ContentDigest, kind, string(b))
	return err
}

// CurrentPolicyVersion returns the highest committed policy version.
func (s *Store) CurrentPolicyVersion() (int, string, error) {
	row := s.db.QueryRow(
		`SELECT version, content_digest FROM policy_versions ORDER BY version DESC LIMIT 1`)
	var v int
	var d string
	err := row.Scan(&v, &d)
	if err == sql.ErrNoRows {
		return 0, "", nil
	}
	return v, d, err
}

// LoadPolicy returns a committed policy by version.
func (s *Store) LoadPolicy(version int) (*spec.Policy, error) {
	row := s.db.QueryRow(
		`SELECT payload FROM policy_versions WHERE version=?`, version)
	var payload string
	if err := row.Scan(&payload); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	var p spec.Policy
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// ReplaceSlots rewrites the whole slot table inside one transaction.
func (s *Store) ReplaceSlots(slots []*spec.Slot) error {
	return withTx(s.db, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM slots`); err != nil {
			return err
		}
		for _, sl := range slots {
			if _, err := tx.Exec(
				`INSERT INTO slots(slot_id, fingerprint, block_count, owner_session,
				                    owner_turn, birth_turn, last_used_turn)
				 VALUES(?,?,?,?,?,?,?)`,
				sl.SlotID, sl.Fingerprint, sl.BlockCount, sl.OwnerSession,
				sl.OwnerTurn, sl.BirthTurn, sl.LastUsedTurn); err != nil {
				return err
			}
		}
		return nil
	})
}

// LoadSlots reads all slots ordered by slot id for deterministic rebuild.
func (s *Store) LoadSlots() ([]*spec.Slot, error) {
	rows, err := s.db.Query(
		`SELECT slot_id, fingerprint, block_count, owner_session, owner_turn,
		        birth_turn, last_used_turn FROM slots ORDER BY slot_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*spec.Slot{}
	for rows.Next() {
		sl := &spec.Slot{}
		if err := rows.Scan(&sl.SlotID, &sl.Fingerprint, &sl.BlockCount,
			&sl.OwnerSession, &sl.OwnerTurn, &sl.BirthTurn, &sl.LastUsedTurn); err != nil {
			return nil, err
		}
		out = append(out, sl)
	}
	return out, rows.Err()
}

// SetMeta / GetMeta store small bookkeeping values.
func (s *Store) SetMeta(key, value string) error {
	_, err := s.db.Exec(
		`INSERT INTO meta(key,value) VALUES(?,?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

func (s *Store) GetMeta(key string) (string, error) {
	row := s.db.QueryRow(`SELECT value FROM meta WHERE key=?`, key)
	var v string
	err := row.Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return v, err
}

// DB exposes the handle for the engine's read-only reporting queries.
func (s *Store) DB() *sql.DB { return s.db }

// ExpectedNextTurn returns the next expected turn for a session (1 if unknown).
func (s *Store) ExpectedNextTurn(sessionID string) (int, error) {
	row := s.db.QueryRow(`SELECT next_turn FROM turn_index WHERE session_id=?`, sessionID)
	var next int
	err := row.Scan(&next)
	if err == sql.ErrNoRows {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	return next, nil
}

// HasCheckpoint reports whether any stage fact exists for a session/turn.
func (s *Store) HasCheckpoint(sessionID string, turn int) (bool, error) {
	row := s.db.QueryRow(
		`SELECT COUNT(1) FROM checkpoints WHERE session_id=? AND turn=?`, sessionID, turn)
	var n int
	if err := row.Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// withTx runs fn inside a transaction, retrying transparently on SQLITE_BUSY.
// busy_timeout already makes locks wait; this covers short WAL-checkpoint spikes.
func withTx(db *sql.DB, fn func(*sql.Tx) error) error {
	const attempts = 20
	var lastErr error
	for i := 0; i < attempts; i++ {
		tx, err := db.Begin()
		if err != nil {
			if isBusy(err) {
				lastErr = err
				sleepBeforeRetry(i)
				continue
			}
			return err
		}
		if err := fn(tx); err != nil {
			_ = tx.Rollback()
			if isBusy(err) {
				lastErr = err
				sleepBeforeRetry(i)
				continue
			}
			return err
		}
		if err := tx.Commit(); err != nil {
			if isBusy(err) {
				lastErr = err
				sleepBeforeRetry(i)
				continue
			}
			return err
		}
		return nil
	}
	return lastErr
}

func isBusy(err error) bool {
	return err != nil && strings.Contains(err.Error(), "SQLITE_BUSY")
}
