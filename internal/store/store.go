package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "modernc.org/sqlite"

	"mitmpipeline/internal/domain"
)

type Store struct {
	db               *sql.DB
	writeMu          sync.Mutex
	crashAfterIntent bool
}

func Open(ctx context.Context, path string) (*Store, error) {
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
	}
	dsn := path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	s := &Store{db: db}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) EnableCrashAfterIntent() {
	s.writeMu.Lock()
	s.crashAfterIntent = true
	s.writeMu.Unlock()
}

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS commits (
			version INTEGER PRIMARY KEY AUTOINCREMENT,
			kind TEXT NOT NULL,
			status TEXT NOT NULL,
			note TEXT,
			report_json TEXT,
			created_at_ms INTEGER NOT NULL,
			completed_at_ms INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			intent_id TEXT PRIMARY KEY,
			commit_version INTEGER NOT NULL,
			kind TEXT NOT NULL,
			status TEXT NOT NULL,
			batch_id TEXT,
			payload_json TEXT,
			crash_point TEXT,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS connections (
			canonical_id TEXT PRIMARY KEY,
			raw_id TEXT NOT NULL,
			sni TEXT,
			protocol_version TEXT,
			leaf_serial TEXT,
			state TEXT,
			conclusion TEXT,
			reason_code TEXT,
			latest_commit INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL,
			report_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS conclusion_versions (
			connection_id TEXT NOT NULL,
			commit_version INTEGER NOT NULL,
			conclusion TEXT NOT NULL,
			state TEXT NOT NULL,
			reason_code TEXT,
			policy_sequence_json TEXT NOT NULL,
			policy_versions_json TEXT NOT NULL,
			report_json TEXT NOT NULL,
			decided_at_ms INTEGER NOT NULL,
			PRIMARY KEY(connection_id, commit_version)
		)`,
		`CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			connection_id TEXT NOT NULL,
			commit_version INTEGER NOT NULL,
			seq INTEGER NOT NULL,
			type TEXT NOT NULL,
			raw_unix_ns INTEGER,
			timestamp_ms INTEGER NOT NULL,
			policy_version TEXT,
			rounding_json TEXT NOT NULL,
			judgment_json TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS evidence (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			connection_id TEXT NOT NULL,
			commit_version INTEGER NOT NULL,
			seq INTEGER NOT NULL,
			event_seq INTEGER,
			policy_version TEXT,
			kind TEXT NOT NULL,
			severity TEXT NOT NULL,
			code TEXT,
			message TEXT NOT NULL,
			details_json TEXT,
			observed_at_ms INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS policy_versions (
			version TEXT PRIMARY KEY,
			revision INTEGER NOT NULL,
			mode TEXT NOT NULL,
			document_json TEXT NOT NULL,
			status TEXT NOT NULL,
			available_from_ms INTEGER NOT NULL,
			retired_at_ms INTEGER,
			replaced_by TEXT,
			created_commit INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS connection_policy_switches (
			connection_id TEXT NOT NULL,
			version TEXT NOT NULL,
			mode TEXT NOT NULL,
			document_json TEXT NOT NULL,
			effective_at_ms INTEGER NOT NULL,
			commit_version INTEGER NOT NULL,
			created_at_ms INTEGER NOT NULL,
			PRIMARY KEY(connection_id, version)
		)`,
		`CREATE TABLE IF NOT EXISTS batches (
			batch_id TEXT PRIMARY KEY,
			commit_version INTEGER NOT NULL,
			format TEXT NOT NULL,
			negotiation_json TEXT,
			report_json TEXT,
			created_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS records (
			canonical_id TEXT PRIMARY KEY,
			format TEXT NOT NULL,
			format_version INTEGER NOT NULL,
			record_json TEXT NOT NULL,
			latest_commit INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS recovery_runs (
			recovery_id INTEGER PRIMARY KEY AUTOINCREMENT,
			started_at_ms INTEGER NOT NULL,
			finished_at_ms INTEGER NOT NULL,
			report_json TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_evidence_conn_commit ON evidence(connection_id, commit_version, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_events_conn_commit ON events(connection_id, commit_version, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_conclusion_commit ON conclusion_versions(commit_version)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

type IntentBody func(tx *sql.Tx, commitVersion int64) error

func (s *Store) Submit(ctx context.Context, kind, batchID, payloadJSON string, body IntentBody) (int64, string, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	now := domain.NowMS()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, "", domain.WrapError(domain.CodeInternal, err, "begin intent transaction")
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO commits(kind,status,note,created_at_ms) VALUES(?,?,?,?)`, kind, "PENDING", batchID, now)
	if err != nil {
		tx.Rollback()
		return 0, "", domain.WrapError(domain.CodeInternal, err, "insert commit intent")
	}
	version, err := result.LastInsertId()
	if err != nil {
		tx.Rollback()
		return 0, "", domain.WrapError(domain.CodeInternal, err, "read commit version")
	}
	intentID := fmt.Sprintf("intent-%d-%d", version, now)
	_, err = tx.ExecContext(ctx, `INSERT INTO intents(intent_id,commit_version,kind,status,batch_id,payload_json,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,?,?,?)`,
		intentID, version, kind, "started", batchID, payloadJSON, now, now)
	if err != nil {
		tx.Rollback()
		return 0, "", domain.WrapError(domain.CodeInternal, err, "insert intent")
	}
	if err := tx.Commit(); err != nil {
		return 0, "", domain.WrapError(domain.CodeInternal, err, "commit durable intent")
	}

	if s.crashAfterIntent {
		crashKill()
	}

	payloadTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.abortIntent(context.Background(), intentID, version, "begin payload transaction failed")
		return version, intentID, domain.WrapError(domain.CodeInternal, err, "begin payload transaction")
	}
	if err := body(payloadTx, version); err != nil {
		_ = payloadTx.Rollback()
		s.abortIntent(context.Background(), intentID, version, err.Error())
		return version, intentID, err
	}
	if _, err := payloadTx.ExecContext(ctx, `UPDATE intents SET status='committed', updated_at_ms=? WHERE intent_id=?`, domain.NowMS(), intentID); err != nil {
		_ = payloadTx.Rollback()
		s.abortIntent(context.Background(), intentID, version, "mark intent committed failed")
		return version, intentID, domain.WrapError(domain.CodeInternal, err, "commit intent state")
	}
	if _, err := payloadTx.ExecContext(ctx, `UPDATE commits SET status='COMPLETE', completed_at_ms=? WHERE version=?`, domain.NowMS(), version); err != nil {
		_ = payloadTx.Rollback()
		s.abortIntent(context.Background(), intentID, version, "mark commit complete failed")
		return version, intentID, domain.WrapError(domain.CodeInternal, err, "complete commit")
	}
	if err := payloadTx.Commit(); err != nil {
		s.abortIntent(context.Background(), intentID, version, "commit payload failed")
		return version, intentID, domain.WrapError(domain.CodeInternal, err, "commit payload transaction")
	}
	return version, intentID, nil
}

func (s *Store) abortIntent(ctx context.Context, intentID string, version int64, reason string) {
	now := domain.NowMS()
	_, _ = s.db.ExecContext(ctx, `UPDATE intents SET status='aborted', crash_point=?, updated_at_ms=? WHERE intent_id=?`, reason, now, intentID)
	_, _ = s.db.ExecContext(ctx, `UPDATE commits SET status='ABORTED', completed_at_ms=? WHERE version=?`, now, version)
}

func (s *Store) LatestCompleteVersion(ctx context.Context) (int64, error) {
	var version int64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM commits WHERE status='COMPLETE'`).Scan(&version)
	return version, err
}
