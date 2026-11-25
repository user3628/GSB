package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db      *sql.DB
	path    string
	writeMu sync.Mutex
}

func Open(ctx context.Context, path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("database path must not be empty")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, err
	}
	dsn := "file:" + abs + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	db.SetConnMaxLifetime(0)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	store := &Store{db: db, path: abs}
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		db.Close()
		return nil, err
	}
	if err := store.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := store.Recover(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Path() string { return s.path }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS state_meta (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			current_policy_version INTEGER NOT NULL,
			commit_seq INTEGER NOT NULL,
			next_sample_order INTEGER NOT NULL
		)`,
		`INSERT OR IGNORE INTO state_meta(id, current_policy_version, commit_seq, next_sample_order) VALUES (1, 0, 0, 1)`,
		`CREATE TABLE IF NOT EXISTS intents (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			request_key TEXT NOT NULL,
			kind TEXT NOT NULL,
			target TEXT NOT NULL,
			payload_hash TEXT NOT NULL,
			state TEXT NOT NULL,
			context_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			finished_at TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_state ON intents(state, id)`,
		`CREATE TABLE IF NOT EXISTS policy_snapshots (
			version INTEGER PRIMARY KEY,
			commit_seq INTEGER NOT NULL,
			generated_at TEXT NOT NULL,
			policy_json TEXT NOT NULL,
			rebuild_report_json TEXT NOT NULL,
			committed_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS sample_batches (
			batch_id TEXT PRIMARY KEY,
			payload_hash TEXT NOT NULL,
			policy_version INTEGER NOT NULL,
			commit_seq INTEGER NOT NULL,
			response_json TEXT NOT NULL,
			committed_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_sample_batches_commit ON sample_batches(commit_seq)`,
		`CREATE TABLE IF NOT EXISTS samples (
			sample_id TEXT PRIMARY KEY,
			content_hash TEXT NOT NULL,
			timestamp_ms INTEGER NOT NULL,
			service TEXT NOT NULL,
			first_channel TEXT NOT NULL,
			frames_json TEXT NOT NULL,
			weight INTEGER NOT NULL,
			seen_order INTEGER NOT NULL UNIQUE,
			first_batch_id TEXT NOT NULL,
			first_commit_seq INTEGER NOT NULL,
			duplicate_count INTEGER NOT NULL,
			received_count INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_samples_service_order ON samples(service, seen_order)`,
		`CREATE INDEX IF NOT EXISTS idx_samples_commit ON samples(first_commit_seq)`,
		`CREATE TABLE IF NOT EXISTS sample_channel_hits (
			sample_id TEXT NOT NULL,
			channel TEXT NOT NULL,
			observations INTEGER NOT NULL,
			PRIMARY KEY(sample_id, channel),
			FOREIGN KEY(sample_id) REFERENCES samples(sample_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS sample_batch_items (
			batch_id TEXT NOT NULL,
			sample_id TEXT NOT NULL,
			outcome TEXT NOT NULL,
			occurrences INTEGER NOT NULL,
			PRIMARY KEY(batch_id, sample_id),
			FOREIGN KEY(batch_id) REFERENCES sample_batches(batch_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS reservoirs (
			policy_version INTEGER NOT NULL,
			reservoir_id TEXT NOT NULL,
			commit_seq INTEGER NOT NULL,
			service TEXT NOT NULL,
			channel TEXT NOT NULL,
			layer_key TEXT NOT NULL,
			depth_prefixes_json TEXT NOT NULL,
			capacity INTEGER NOT NULL,
			seen_count INTEGER NOT NULL,
			duplicate_observation_count INTEGER NOT NULL,
			selected_count INTEGER NOT NULL,
			selected_json TEXT NOT NULL,
			replaced_ids_json TEXT NOT NULL,
			replaced_total INTEGER NOT NULL,
			rejected_total INTEGER NOT NULL,
			references_json TEXT NOT NULL,
			PRIMARY KEY(policy_version, reservoir_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_reservoirs_version_service ON reservoirs(policy_version, service, channel)`,
		`CREATE TABLE IF NOT EXISTS hotspot_runs (
			policy_version INTEGER PRIMARY KEY,
			commit_seq INTEGER NOT NULL,
			built_at TEXT NOT NULL,
			elapsed_ms INTEGER NOT NULL,
			result_json TEXT NOT NULL,
			mode_summary_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS rebuild_reports (
			policy_version INTEGER PRIMARY KEY,
			old_version INTEGER NOT NULL,
			commit_seq INTEGER NOT NULL,
			report_json TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS recovery_reports (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			recovered_at TEXT NOT NULL,
			report_json TEXT NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
	}
	return nil
}

type StateMeta struct {
	CurrentPolicyVersion uint64 `json:"current_policy_version"`
	CommitSeq            int64  `json:"commit_seq"`
	NextSampleOrder      int64  `json:"next_sample_order"`
}

func (s *Store) CurrentState(ctx context.Context) (StateMeta, error) {
	var state StateMeta
	err := s.db.QueryRowContext(ctx, `SELECT current_policy_version, commit_seq, next_sample_order FROM state_meta WHERE id=1`).
		Scan(&state.CurrentPolicyVersion, &state.CommitSeq, &state.NextSampleOrder)
	return state, err
}

func currentStateTx(ctx context.Context, tx *sql.Tx) (StateMeta, error) {
	var state StateMeta
	err := tx.QueryRowContext(ctx, `SELECT current_policy_version, commit_seq, next_sample_order FROM state_meta WHERE id=1`).
		Scan(&state.CurrentPolicyVersion, &state.CommitSeq, &state.NextSampleOrder)
	return state, err
}

func updateStateTx(ctx context.Context, tx *sql.Tx, policyVersion uint64, commitSeq, nextSampleOrder int64) error {
	_, err := tx.ExecContext(ctx, `UPDATE state_meta SET current_policy_version=?, commit_seq=?, next_sample_order=? WHERE id=1`, policyVersion, commitSeq, nextSampleOrder)
	return err
}

type IntentSpec struct {
	RequestKey  string
	Kind        string
	Target      string
	PayloadHash string
	Context     map[string]any
}

func crashAfterIntentEnabled() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("PROFILE_CRASH_AFTER_INTENT")))
	return value == "1" || value == "true" || value == "yes"
}

// MaybeCrash is a deterministic fault-injection hook used by the local demo and
// tests. It is inert unless PROFILE_CRASH_STAGE exactly matches stage. Calling
// os.Exit while a SQLite transaction is open deliberately simulates a forced
// process termination; the next Open performs WAL rollback and intent recovery.
func MaybeCrash(stage string) {
	if strings.TrimSpace(os.Getenv("PROFILE_CRASH_STAGE")) != stage {
		return
	}
	fmt.Fprintf(os.Stderr, "PROFILE_CRASH_STAGE=%s: exiting with status 137\n", stage)
	os.Exit(137)
}

// WithWrite serializes all writers, persists an intent in its own transaction,
// then applies state changes and marks the intent committed in the same SQLite
// transaction. A crash therefore leaves either no data mutation with a pending
// intent, or a complete data commit with its intent already marked committed.
func (s *Store) WithWrite(ctx context.Context, spec IntentSpec, fn func(*sql.Tx) error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	contextJSON, _ := json.Marshal(spec.Context)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := s.db.ExecContext(ctx, `INSERT INTO intents(request_key, kind, target, payload_hash, state, context_json, created_at) VALUES(?,?,?,?,?,?,?)`,
		spec.RequestKey, spec.Kind, spec.Target, spec.PayloadHash, "pending", string(contextJSON), now)
	if err != nil {
		return fmt.Errorf("persist intent: %w", err)
	}
	intentID, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("read intent id: %w", err)
	}
	if crashAfterIntentEnabled() {
		fmt.Fprintf(os.Stderr, "PROFILE_CRASH_AFTER_INTENT: exiting after intent %d (%s %s)\n", intentID, spec.Kind, spec.Target)
		os.Exit(137)
	}
	MaybeCrash("after_intent")

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.abortIntent(intentID, err)
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		s.abortIntent(intentID, err)
		return err
	}
	finishedAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='committed', finished_at=? WHERE id=?`, finishedAt, intentID); err != nil {
		_ = tx.Rollback()
		s.abortIntent(intentID, err)
		return err
	}
	if err := tx.Commit(); err != nil {
		s.abortIntent(intentID, err)
		return err
	}
	return nil
}

func (s *Store) abortIntent(intentID int64, cause error) {
	finishedAt := time.Now().UTC().Format(time.RFC3339Nano)
	_, _ = s.db.Exec(`UPDATE intents SET state='rolled_back', finished_at=? WHERE id=? AND state='pending'`, finishedAt, intentID)
}
