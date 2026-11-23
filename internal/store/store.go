// Package store owns all SQLite persistence for the lease/snapshot arbiter.
//
// The store is intentionally free of arbitration policy: the arbiter package
// decides which transactions to run; this package exposes the SQL building
// blocks and guarantees every mutation goes through write-ahead intents.
package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// Store wraps the single SQLite database file used by one engine process.
type Store struct {
	DB   *sql.DB
	Path string
}

// Open opens (creating if needed) the SQLite database and applies the
// durability-oriented pragmas. WAL gives readers a consistent snapshot without
// blocking the single writer; busy_timeout serializes multi-process access.
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(15000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_pragma=synchronous(FULL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// A single connection avoids SQLITE_BUSY inside one process; multi-process
	// contention is still handled by busy_timeout at the file level.
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(SchemaDDL); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{DB: db, Path: path}
	if err := s.bootstrapMeta(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.DB.Close() }

func (s *Store) bootstrapMeta() error {
	_, err := s.DB.Exec(`INSERT INTO meta(key, value) VALUES('commit_ver', '0')
		ON CONFLICT(key) DO NOTHING`)
	return err
}

// TxFunc runs fn inside a transaction. A non-nil error rolls back; nil commits.
func (s *Store) TxFunc(fn func(*sql.Tx) error) (err error) {
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// CommitVer returns the durable global commit version.
func (s *Store) CommitVer() (int64, error) {
	var v int64
	err := s.DB.QueryRow(`SELECT CAST(value AS INTEGER) FROM meta WHERE key='commit_ver'`).Scan(&v)
	return v, err
}

// MetaTx advances the global commit version inside tx and returns the new value.
func MetaTx(tx *sql.Tx) (int64, error) {
	var v int64
	if err := tx.QueryRow(`SELECT CAST(value AS INTEGER) FROM meta WHERE key='commit_ver'`).Scan(&v); err != nil {
		return 0, err
	}
	v++
	if _, err := tx.Exec(`UPDATE meta SET value=? WHERE key='commit_ver'`, fmt.Sprintf("%d", v)); err != nil {
		return 0, err
	}
	return v, nil
}

// InsertIntent writes a prepared write-ahead intent inside tx.
func InsertIntent(tx *sql.Tx, in WriteIntent) (int64, error) {
	res, err := tx.Exec(`INSERT INTO write_intents
		(kind, row_id, request_id, lease_version, base_row_ver, payload, state, created_at, completed_at)
		VALUES(?,?,?,?,?,?,?,?,NULL)`,
		string(in.Kind), in.RowID, in.RequestID, in.LeaseVersion, in.BaseRowVer,
		in.Payload, string(IntentPrepared), in.CreatedAtMs)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// CompleteIntent marks an intent as completed inside the same tx that applies
// its mutation, so commit of the tx makes intent completion atomic with the
// state change.
func CompleteIntent(tx *sql.Tx, intentID, nowMs int64) error {
	_, err := tx.Exec(`UPDATE write_intents SET state=?, completed_at=? WHERE id=?`,
		string(IntentCompleted), nowMs, intentID)
	return err
}

// PreparedIntents returns intents never confirmed as completed, ordered so
// recovery replay is deterministic.
func (s *Store) PreparedIntents() ([]WriteIntent, error) {
	rows, err := s.DB.Query(`SELECT id, kind, row_id, request_id, lease_version,
		base_row_ver, payload, state, created_at, COALESCE(completed_at,0)
		FROM write_intents WHERE state=? ORDER BY id`, string(IntentPrepared))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WriteIntent
	for rows.Next() {
		var in WriteIntent
		var kind, state string
		if err := rows.Scan(&in.ID, &kind, &in.RowID, &in.RequestID, &in.LeaseVersion,
			&in.BaseRowVer, &in.Payload, &state, &in.CreatedAtMs, &in.CompletedAtMs); err != nil {
			return nil, err
		}
		in.Kind = IntentKind(kind)
		in.State = IntentState(state)
		out = append(out, in)
	}
	return out, rows.Err()
}
