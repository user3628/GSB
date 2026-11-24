package store

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	DB   *sql.DB
	Path string
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("database path is empty")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, err
	}
	u := &url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}
	if len(u.Path) >= 2 && u.Path[1] == ':' && u.Path[0] != '/' {
		// file:///C:/... is the portable SQLite URI form for Windows drive
		// letters. Linux absolute paths already begin with a slash.
		u.Path = "/" + u.Path
	}
	q := u.Query()
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "temp_store(MEMORY)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{DB: db, Path: abs}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.DB.Close() }

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS engine_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			revision INTEGER NOT NULL,
			current_policy_version INTEGER NOT NULL,
			loss_scale TEXT NOT NULL,
			loss_scale_growth TEXT NOT NULL
		)`,
		`INSERT OR IGNORE INTO engine_state(id, revision, current_policy_version, loss_scale, loss_scale_growth)
		 VALUES(1, 0, 0, '1', '1')`,
		`CREATE TABLE IF NOT EXISTS strategy_snapshots (
			version INTEGER PRIMARY KEY,
			generated_at TEXT NOT NULL,
			vram_limit_bytes INTEGER NOT NULL,
			write_budget_bytes_per_round INTEGER NOT NULL,
			time_budget_ms_per_round INTEGER NOT NULL,
			write_queue_capacity INTEGER NOT NULL,
			loss_scale_initial TEXT NOT NULL,
			loss_scale_growth TEXT NOT NULL,
			precision_abs_limit_nanos INTEGER NOT NULL,
			precision_rel_limit_ppm INTEGER NOT NULL,
			write_preference TEXT NOT NULL,
			checksum TEXT NOT NULL,
			canonical_json TEXT NOT NULL,
			submitted_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS snapshot_residency (
			policy_version INTEGER NOT NULL,
			shard_id TEXT NOT NULL,
			target_pool TEXT NOT NULL,
			PRIMARY KEY(policy_version, shard_id)
		)`,
		`CREATE TABLE IF NOT EXISTS shards (
			round INTEGER NOT NULL,
			shard_id TEXT NOT NULL,
			parameter_id TEXT NOT NULL,
			precision TEXT NOT NULL,
			bytes INTEGER NOT NULL CHECK(bytes >= 0),
			digest TEXT NOT NULL,
			momentum_digest TEXT NOT NULL DEFAULT '',
			restored_from_digest TEXT NOT NULL DEFAULT '',
			location TEXT NOT NULL CHECK(location IN ('VRAM','PERSISTED')),
			desired_pool TEXT NOT NULL CHECK(desired_pool IN ('VRAM','PERSISTED')),
			write_state TEXT NOT NULL CHECK(write_state IN ('PENDING','WRITTEN')),
			last_access_ms INTEGER NOT NULL,
			priority INTEGER NOT NULL,
			write_ms INTEGER NOT NULL,
			pinned INTEGER NOT NULL DEFAULT 0,
			quantization TEXT NOT NULL DEFAULT '',
			fp16_bits_json TEXT NOT NULL DEFAULT '',
			fp32_nanos_json TEXT NOT NULL DEFAULT '',
			payload_hex TEXT NOT NULL DEFAULT '',
			revision INTEGER NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY(round, shard_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_shards_location ON shards(location)`,
		`CREATE INDEX IF NOT EXISTS idx_shards_parameter ON shards(round, parameter_id, precision)`,
		`CREATE INDEX IF NOT EXISTS idx_shards_write_state ON shards(write_state)`,
		`CREATE TABLE IF NOT EXISTS pool_usage (
			pool TEXT PRIMARY KEY,
			used_bytes INTEGER NOT NULL,
			shard_count INTEGER NOT NULL,
			revision INTEGER NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`INSERT OR IGNORE INTO pool_usage(pool, used_bytes, shard_count, revision, updated_at)
		 VALUES('VRAM',0,0,0,''),('PERSISTED',0,0,0,'')`,
		`CREATE TABLE IF NOT EXISTS write_records (
			record_id INTEGER PRIMARY KEY AUTOINCREMENT,
			submission_id TEXT NOT NULL,
			round INTEGER NOT NULL,
			shard_id TEXT NOT NULL,
			precision TEXT NOT NULL,
			digest TEXT NOT NULL,
			bytes INTEGER NOT NULL,
			pool TEXT NOT NULL,
			committed_at TEXT NOT NULL,
			blob_path TEXT NOT NULL,
			UNIQUE(round, shard_id, digest)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_write_records_submission ON write_records(submission_id)`,
		`CREATE TABLE IF NOT EXISTS write_queue (
			queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
			round INTEGER NOT NULL,
			shard_id TEXT NOT NULL,
			precision TEXT NOT NULL,
			digest TEXT NOT NULL,
			bytes INTEGER NOT NULL,
			priority INTEGER NOT NULL,
			write_ms INTEGER NOT NULL,
			submission_id TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'WAITING',
			enqueued_ms INTEGER NOT NULL,
			reason TEXT NOT NULL,
			UNIQUE(round, shard_id, digest)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_write_queue_waiting ON write_queue(status, priority, round)`,
		`CREATE TABLE IF NOT EXISTS round_budget (
			round INTEGER PRIMARY KEY,
			policy_version INTEGER NOT NULL,
			bytes_limit INTEGER NOT NULL,
			bytes_used INTEGER NOT NULL,
			time_limit_ms INTEGER NOT NULL,
			time_used_ms INTEGER NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS submissions (
			submission_id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			request_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			result_json TEXT NOT NULL,
			revision INTEGER NOT NULL,
			created_at TEXT NOT NULL,
			completed_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			intent_id INTEGER PRIMARY KEY AUTOINCREMENT,
			kind TEXT NOT NULL,
			submission_id TEXT NOT NULL,
			state TEXT NOT NULL,
			before_json TEXT NOT NULL DEFAULT '',
			after_json TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			applied_at TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_state ON intents(state, intent_id)`,
		`CREATE TABLE IF NOT EXISTS shard_derivatives (
			round INTEGER NOT NULL,
			shard_id TEXT NOT NULL,
			master_digest TEXT NOT NULL,
			consistency_status TEXT NOT NULL,
			consistency_digest TEXT NOT NULL,
			vram_estimate_bytes INTEGER NOT NULL,
			recovery_baseline TEXT NOT NULL,
			source_fingerprint TEXT NOT NULL,
			rebuilt_at TEXT NOT NULL,
			PRIMARY KEY(round, shard_id)
		)`,
		`CREATE TABLE IF NOT EXISTS round_consistency (
			round INTEGER PRIMARY KEY,
			status TEXT NOT NULL,
			shard_count INTEGER NOT NULL,
			written_shards INTEGER NOT NULL,
			pending_shards INTEGER NOT NULL,
			failed_precision INTEGER NOT NULL,
			unverified_pairs INTEGER NOT NULL,
			source_fingerprint TEXT NOT NULL,
			decided_revision INTEGER NOT NULL,
			rebuilt_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS precision_results (
			round INTEGER NOT NULL,
			shard_id TEXT NOT NULL,
			parameter_id TEXT NOT NULL,
			fp16_shard_id TEXT NOT NULL,
			fp32_shard_id TEXT NOT NULL,
			fp16_digest TEXT NOT NULL,
			fp32_digest TEXT NOT NULL,
			loss_scale TEXT NOT NULL,
			rounding_mode TEXT NOT NULL,
			elements INTEGER NOT NULL,
			max_abs_nanos INTEGER NOT NULL,
			max_rel_ppm INTEGER NOT NULL,
			sum_abs_nanos INTEGER NOT NULL,
			cumulative_rounding_nanos INTEGER NOT NULL,
			abs_limit_nanos INTEGER NOT NULL,
			rel_limit_ppm INTEGER NOT NULL,
			check_mode TEXT NOT NULL,
			passed INTEGER NOT NULL,
			drift_reason TEXT NOT NULL,
			checked_at TEXT NOT NULL,
			result_digest TEXT NOT NULL,
			details_json TEXT NOT NULL,
			PRIMARY KEY(round, shard_id)
		)`,
		`CREATE TABLE IF NOT EXISTS recovery_reports (
			report_id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at TEXT NOT NULL,
			db_revision INTEGER NOT NULL,
			facts_basis TEXT NOT NULL,
			interrupted_intents INTEGER NOT NULL,
			rolled_back_writes INTEGER NOT NULL,
			rolled_back_evictions INTEGER NOT NULL,
			aborted_submissions INTEGER NOT NULL,
			orphan_blobs_removed INTEGER NOT NULL,
			pool_rebuilt INTEGER NOT NULL,
			durable_shards INTEGER NOT NULL,
			pending_shards INTEGER NOT NULL,
			details_json TEXT NOT NULL
		)`,
	}
	for _, stmt := range statements {
		if _, err := s.DB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w\nstatement: %s", err, stmt)
		}
	}
	return nil
}
