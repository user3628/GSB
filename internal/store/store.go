package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db   *sql.DB
	path string
}

func Open(path string) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, err
	}
	dsnPath := filepath.ToSlash(abs)
	dsn := "file:" + dsnPath + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(12)
	db.SetMaxIdleConns(12)
	db.SetConnMaxLifetime(0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	s := &Store{db: db, path: abs}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Path() string { return s.path }
func (s *Store) DB() *sql.DB  { return s.db }

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS snapshots (
			snapshot_version INTEGER PRIMARY KEY,
			snapshot_id TEXT NOT NULL,
			generated_at_ms INTEGER NOT NULL,
			source_format TEXT NOT NULL,
			format_version INTEGER NOT NULL,
			source_digest TEXT NOT NULL,
			content_digest TEXT NOT NULL,
			policy_count INTEGER NOT NULL,
			cumulative_rounding_error_us INTEGER NOT NULL,
			defaults_json TEXT NOT NULL,
			imported_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS active_snapshot (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			snapshot_version INTEGER NOT NULL REFERENCES snapshots(snapshot_version)
		)`,
		`CREATE TABLE IF NOT EXISTS policies (
			snapshot_version INTEGER NOT NULL REFERENCES snapshots(snapshot_version) ON DELETE CASCADE,
			policy_id TEXT NOT NULL,
			virtual_service_id TEXT NOT NULL,
			policy_revision INTEGER NOT NULL,
			last_write_ms INTEGER NOT NULL,
			policy_json TEXT NOT NULL,
			PRIMARY KEY (snapshot_version, policy_id)
		)`,
		`CREATE INDEX IF NOT EXISTS policies_service_idx ON policies(snapshot_version, virtual_service_id)`,
		`CREATE TABLE IF NOT EXISTS route_index (
			snapshot_version INTEGER NOT NULL REFERENCES snapshots(snapshot_version) ON DELETE CASCADE,
			virtual_service_id TEXT NOT NULL,
			route_key TEXT NOT NULL,
			policy_id TEXT NOT NULL,
			method TEXT NOT NULL,
			path_prefix TEXT NOT NULL,
			headers_json TEXT NOT NULL,
			PRIMARY KEY (snapshot_version, route_key, policy_id)
		)`,
		`CREATE TABLE IF NOT EXISTS rate_window_stats (
			snapshot_version INTEGER NOT NULL REFERENCES snapshots(snapshot_version) ON DELETE CASCADE,
			virtual_service_id TEXT NOT NULL,
			window_start_ms INTEGER NOT NULL,
			window_end_ms INTEGER NOT NULL,
			window_ms INTEGER NOT NULL,
			limit_bps TEXT NOT NULL,
			policy_count INTEGER NOT NULL,
			policy_ids_json TEXT NOT NULL,
			PRIMARY KEY (snapshot_version, virtual_service_id, window_start_ms, window_end_ms, window_ms)
		)`,
		`CREATE TABLE IF NOT EXISTS circuit_state (
			snapshot_version INTEGER NOT NULL REFERENCES snapshots(snapshot_version) ON DELETE CASCADE,
			virtual_service_id TEXT NOT NULL,
			state TEXT NOT NULL,
			window_ms INTEGER NOT NULL,
			policy_ids_json TEXT NOT NULL,
			PRIMARY KEY (snapshot_version, virtual_service_id)
		)`,
		`CREATE TABLE IF NOT EXISTS service_summaries (
			snapshot_version INTEGER NOT NULL REFERENCES snapshots(snapshot_version) ON DELETE CASCADE,
			virtual_service_id TEXT NOT NULL,
			policy_count INTEGER NOT NULL,
			route_count INTEGER NOT NULL,
			rate_window_count INTEGER NOT NULL,
			total_rate_bps TEXT NOT NULL,
			circuit_state TEXT NOT NULL,
			policy_ids_json TEXT NOT NULL,
			PRIMARY KEY (snapshot_version, virtual_service_id)
		)`,
		`CREATE TABLE IF NOT EXISTS snapshot_derivations (
			snapshot_version INTEGER PRIMARY KEY REFERENCES snapshots(snapshot_version) ON DELETE CASCADE,
			affected_services_json TEXT NOT NULL,
			rebuilt_at_ms INTEGER NOT NULL,
			route_count INTEGER NOT NULL,
			window_count INTEGER NOT NULL,
			circuit_count INTEGER NOT NULL,
			summary_count INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS import_reports (
			report_id TEXT PRIMARY KEY,
			request_id TEXT NOT NULL,
			snapshot_version INTEGER NOT NULL,
			status TEXT NOT NULL,
			decision TEXT NOT NULL,
			source_format TEXT NOT NULL,
			format_version INTEGER NOT NULL,
			source_digest TEXT NOT NULL,
			content_digest TEXT NOT NULL,
			report_json TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS import_report_items (
			report_id TEXT NOT NULL REFERENCES import_reports(report_id) ON DELETE CASCADE,
			seq INTEGER NOT NULL,
			policy_id TEXT NOT NULL,
			outcome TEXT NOT NULL,
			basis_json TEXT NOT NULL,
			detail_json TEXT NOT NULL,
			PRIMARY KEY (report_id, seq)
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			intent_id TEXT PRIMARY KEY,
			request_id TEXT NOT NULL,
			kind TEXT NOT NULL,
			snapshot_version INTEGER NOT NULL,
			status TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL,
			completed_at_ms INTEGER,
			facts_json TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS intents_status_idx ON intents(status)`,
		`CREATE TABLE IF NOT EXISTS request_registry (
			request_id TEXT PRIMARY KEY,
			request_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			http_status INTEGER NOT NULL,
			response_json TEXT NOT NULL,
			report_id TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS recovery_reports (
			report_id TEXT PRIMARY KEY,
			created_at_ms INTEGER NOT NULL,
			report_json TEXT NOT NULL
		)`,
	}
	for _, stmt := range statements {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate sqlite: %w\nstatement: %s", err, firstLine(stmt))
		}
	}
	return nil
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func (s *Store) BeginWrite(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
}

func (s *Store) BeginRead(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable})
}

func NullableVersion(v sql.NullInt64) uint64 {
	if !v.Valid || v.Int64 <= 0 {
		return 0
	}
	return uint64(v.Int64)
}
