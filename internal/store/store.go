package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"gpusched/internal/domain"
)

// Store wraps the SQLite database. Only one goroutine (the service's writer)
// runs mutating transactions, while any number of readers may query committed
// snapshots concurrently (WAL mode).
type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	// modernc.org/sqlite registers as "sqlite".
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		return nil, err
	}
	// A single writer connection plus a small reader pool keeps the WAL
	// discipline explicit and avoids SQLITE_BUSY under concurrency.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)
	s := &Store{db: db}
	if _, err := db.Exec(schemaSQL); err != nil {
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

// Tx runs fn inside a single transaction; a non-nil error rolls everything back.
func (s *Store) Tx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// InsertIntent writes the durable intent record BEFORE any state change. It must
// be called within the same transaction that performs the change.
func InsertIntent(tx *sql.Tx, intentType string, roundID int64, requestID, batchID, payload string) (int64, error) {
	res, err := tx.Exec(
		`INSERT INTO intents(intent_type, round_id, request_id, batch_id, payload, status, created_at)
		 VALUES(?,?,?,?,?,?,?)`,
		intentType, roundID, requestID, batchID, payload, domain.IntentPending, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// MarkIntent flips an intent to its terminal status inside the same tx.
func MarkIntent(tx *sql.Tx, intentID int64, status string) error {
	_, err := tx.Exec(`UPDATE intents SET status=? WHERE id=?`, status, intentID)
	return err
}

// PendingIntent is a row returned during crash recovery scanning.
type PendingIntent struct {
	ID        int64
	Type      string
	RoundID   int64
	RequestID string
	BatchID   string
	Payload   string
	CreatedAt string
}

// ScanPendingIntents returns all intent rows never marked terminal, ordered by
// id so recovery replies them in submission order.
func (s *Store) ScanPendingIntents(ctx context.Context) ([]PendingIntent, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, intent_type, round_id, request_id, batch_id, payload, created_at
		 FROM intents WHERE status=? ORDER BY id`, domain.IntentPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingIntent
	for rows.Next() {
		var p PendingIntent
		if err := rows.Scan(&p.ID, &p.Type, &p.RoundID, &p.RequestID, &p.BatchID, &p.Payload, &p.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// RunningRound describes a round that never reached a terminal status.
type RunningRound struct {
	ID            int64
	PolicyVersion int64
	BudgetMs      int64
	StartedAt     string
}

func (s *Store) ScanRunningRounds(ctx context.Context) ([]RunningRound, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, policy_version, budget_ms, started_at FROM rounds WHERE status=? ORDER BY id`,
		domain.RoundRunning)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RunningRound
	for rows.Next() {
		var r RunningRound
		if err := rows.Scan(&r.ID, &r.PolicyVersion, &r.BudgetMs, &r.StartedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
