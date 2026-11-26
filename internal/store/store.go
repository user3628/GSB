package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"example.com/gsb/dnscache/internal/errs"
)

type Options struct {
	Clock func() time.Time
	// AfterIntent is a test-only durability hook invoked after an intent has
	// been committed in its own transaction. Production leaves it nil.
	AfterIntent func(kind, target string)
}

type Store struct {
	db       *sql.DB
	writeMu  sync.Mutex
	clock    func() time.Time
	options  Options
	recovery RecoveryReport
	idSeq    atomic.Uint64
}

func Open(path string, opts Options) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errs.New(errs.EInvalidRequest, "SQLite database path is empty")
	}
	if path != ":memory:" && !strings.HasPrefix(path, "file:") {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}
	dsn := sqliteDSN(path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(12)
	db.SetMaxIdleConns(8)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;`); err != nil {
		db.Close()
		return nil, fmt.Errorf("configure sqlite: %w", err)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}
	s := &Store{db: db, clock: opts.Clock, options: opts}
	if s.clock == nil {
		s.clock = time.Now
	}
	if err := s.recover(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("recover database: %w", err)
	}
	return s, nil
}

func sqliteDSN(path string) string {
	if path == ":memory:" || strings.Contains(path, "mode=memory") {
		return "file:dnscache-memory?mode=memory&cache=shared&_busy_timeout=5000&_foreign_keys=on"
	}
	if strings.HasPrefix(path, "file:") {
		if strings.Contains(path, "?") {
			return path + "&_busy_timeout=5000&_foreign_keys=on&_synchronous=FULL"
		}
		return path + "?_busy_timeout=5000&_foreign_keys=on&_synchronous=FULL"
	}
	return path + "?_busy_timeout=5000&_foreign_keys=on&_synchronous=FULL&_journal_mode=WAL"
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) nowMS() int64 { return s.clock().UnixMilli() }

func (s *Store) now() time.Time { return s.clock() }

func (s *Store) nextID(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, s.now().UTC().UnixNano(), s.idSeq.Add(1))
}

func (s *Store) RecoveryReport() RecoveryReport {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.recovery
}

type idemRecord struct {
	ContentHash string
	Status      string
	Code        string
	Message     string
	ResultJSON  string
}

func loadIdempotency(ctx context.Context, q queryer, kind, key string) (*idemRecord, error) {
	if key == "" {
		return nil, nil
	}
	var r idemRecord
	err := q.QueryRowContext(ctx, `SELECT content_hash, status, code, message, result_json FROM idempotency WHERE kind=? AND request_key=?`, kind, key).
		Scan(&r.ContentHash, &r.Status, &r.Code, &r.Message, &r.ResultJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func saveIdempotency(ctx context.Context, tx *sql.Tx, kind, key, hash, status, code, message string, result any, now int64) error {
	var resultJSON string
	if result != nil {
		b, err := json.Marshal(result)
		if err != nil {
			return err
		}
		resultJSON = string(b)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO idempotency(kind, request_key, content_hash, status, code, message, result_json, created_ms, updated_ms)
VALUES(?,?,?,?,?,?,?,?,?)
ON CONFLICT(kind, request_key) DO UPDATE SET content_hash=excluded.content_hash, status=excluded.status,
 code=excluded.code, message=excluded.message, result_json=excluded.result_json, updated_ms=excluded.updated_ms`,
		kind, key, hash, status, code, message, resultJSON, now, now)
	return err
}

func idempotentError(r *idemRecord) error {
	code := errs.Code(r.Code)
	if code == "" {
		code = errs.EInternal
	}
	return errs.New(code, "%s", r.Message)
}

func (s *Store) writeIntentPending(ctx context.Context, intentID, kind, target, hash string) error {
	now := s.nowMS()
	_, err := s.db.ExecContext(ctx, `INSERT INTO intention_log(intent_id, kind, target, payload_hash, status, detail, created_ms, updated_ms)
VALUES(?,?,?,?,'pending','',?,?)`, intentID, kind, target, hash, now, now)
	return err
}

func (s *Store) markIntent(ctx context.Context, intentID, status, detail string) error {
	now := s.nowMS()
	_, err := s.db.ExecContext(ctx, `UPDATE intention_log SET status=?, detail=?, updated_ms=? WHERE intent_id=?`, status, detail, now, intentID)
	return err
}

func markIntentTx(ctx context.Context, tx *sql.Tx, intentID, status, detail string, now int64) error {
	_, err := tx.ExecContext(ctx, `UPDATE intention_log SET status=?, detail=?, updated_ms=? WHERE intent_id=?`, status, detail, now, intentID)
	return err
}

func prepareBatch(ctx context.Context, db *sql.DB, intentID, batchID, zone, kind string, base, target int64, checksum string, payload any, now int64) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO intention_log(intent_id, kind, target, payload_hash, status, detail, created_ms, updated_ms)
VALUES(?,?,?,?,'pending','',?,?)`, intentID, kind, batchID, checksum, now, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO zone_batches(batch_id, zone, kind, base_version, target_version, checksum, status, code, message, result_json, payload_json, created_ms, updated_ms)
VALUES(?,?,?,?,?,?,'pending','','','',?,?,?)`, batchID, zone, kind, base, target, checksum, string(b), now, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Stats(ctx context.Context) (CacheStats, error) {
	var st CacheStats
	now := s.nowMS()
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cache_entries`).Scan(&st.Entries)
	if err != nil {
		return st, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM cache_entries GROUP BY status`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			rows.Close()
			return st, err
		}
		switch status {
		case "positive":
			st.Positive = n
		case "nxdomain":
			st.NXDomain = n
		case "nodata":
			st.NODATA = n
		}
	}
	if err := rows.Close(); err != nil {
		return st, err
	}
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cache_entries WHERE written_ms+ttl_ms<=?`, now).Scan(&st.Expired)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cache_deps`).Scan(&st.DependencyRows)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM zones`).Scan(&st.Zones)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM intention_log WHERE status='pending'`).Scan(&st.PendingIntents)
	return st, nil
}
