package engine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"ampcheckpoint/internal/domain"
	"ampcheckpoint/internal/sample"

	_ "modernc.org/sqlite"
)

type Engine struct {
	db      *sql.DB
	sample  *sample.Generator
	writeMu sync.Mutex
}

func Open(path string, gen *sample.Generator) (*Engine, error) {
	if path == "" {
		return nil, fmt.Errorf("database path is required")
	}
	if gen == nil {
		return nil, fmt.Errorf("sample generator is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && filepath.Dir(path) != "." {
		return nil, err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	uriPath := filepath.ToSlash(absPath)
	if runtime.GOOS == "windows" && len(uriPath) >= 2 && uriPath[1] == ':' {
		uriPath = "/" + uriPath
	}
	dsnURL := url.URL{Scheme: "file", Path: uriPath, RawQuery: "_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(FULL)"}
	dsn := dsnURL.String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	eng := &Engine{db: db, sample: gen}
	if err := eng.createSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := eng.Recover(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return eng, nil
}

func (e *Engine) Close() error { return e.db.Close() }

func (e *Engine) SampleDir() string               { return e.sample.Dir() }
func (e *Engine) SampleManifest() sample.Manifest { return e.sample.Manifest() }

func (e *Engine) createSchema(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS policy_versions (
			version INTEGER PRIMARY KEY,
			generated_at TEXT NOT NULL,
			vram_pool_bytes INTEGER NOT NULL,
			write_budget_bytes INTEGER NOT NULL,
			round_time_budget_ms INTEGER NOT NULL,
			write_queue_capacity INTEGER NOT NULL,
			task_time_ms INTEGER NOT NULL,
			loss_scale_fixed INTEGER NOT NULL,
			growth_factor_fixed INTEGER NOT NULL,
			overflow_threshold_fixed INTEGER NOT NULL,
			placements_json TEXT NOT NULL,
			checksum TEXT NOT NULL,
			source TEXT NOT NULL,
			base_version INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			version INTEGER PRIMARY KEY,
			policy_version INTEGER NOT NULL REFERENCES policy_versions(version),
			round INTEGER NOT NULL UNIQUE,
			committed_at TEXT NOT NULL,
			vram_limit_bytes INTEGER NOT NULL,
			write_budget_bytes INTEGER NOT NULL,
			write_used_bytes INTEGER NOT NULL,
			write_remaining_bytes INTEGER NOT NULL,
			time_budget_ms INTEGER NOT NULL,
			time_used_ms INTEGER NOT NULL,
			time_remaining_ms INTEGER NOT NULL,
			write_queue_capacity INTEGER NOT NULL,
			written_json TEXT NOT NULL,
			skipped_json TEXT NOT NULL,
			queued_json TEXT NOT NULL,
			evictions_json TEXT NOT NULL,
			checksum TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoint_copies (
			checkpoint_version INTEGER NOT NULL REFERENCES checkpoints(version),
			copy_id TEXT NOT NULL,
			shard_id TEXT NOT NULL,
			round INTEGER NOT NULL,
			precision TEXT NOT NULL,
			size_bytes INTEGER NOT NULL,
			digest TEXT NOT NULL,
			master_copy_id TEXT NOT NULL DEFAULT '',
			master_digest TEXT NOT NULL,
			momentum_digest TEXT NOT NULL,
			pool TEXT NOT NULL,
			status TEXT NOT NULL,
			last_access_ns INTEGER NOT NULL,
			priority INTEGER NOT NULL,
			loss_scale_fixed INTEGER NOT NULL,
			restoration TEXT NOT NULL,
			write_state TEXT NOT NULL,
			values_fixed_json TEXT NOT NULL,
			fp16_bits_json TEXT NOT NULL,
			PRIMARY KEY (checkpoint_version, copy_id)
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoint_derived (
			checkpoint_version INTEGER NOT NULL REFERENCES checkpoints(version),
			copy_id TEXT NOT NULL,
			master_derived_digest TEXT NOT NULL,
			consistency_ok INTEGER NOT NULL,
			vram_estimate_bytes INTEGER NOT NULL,
			recovery_baseline_json TEXT NOT NULL,
			dependency_refs_json TEXT NOT NULL,
			PRIMARY KEY (checkpoint_version, copy_id)
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoint_pools (
			checkpoint_version INTEGER NOT NULL REFERENCES checkpoints(version),
			pool TEXT NOT NULL,
			copy_count INTEGER NOT NULL,
			total_bytes INTEGER NOT NULL,
			limit_bytes INTEGER NOT NULL,
			remaining_bytes INTEGER NOT NULL,
			PRIMARY KEY (checkpoint_version, pool)
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoint_precision (
			checkpoint_version INTEGER NOT NULL REFERENCES checkpoints(version),
			shard_id TEXT NOT NULL,
			fp16_copy_id TEXT NOT NULL,
			master_copy_id TEXT NOT NULL,
			element_count INTEGER NOT NULL,
			loss_scale_fixed INTEGER NOT NULL,
			restoration TEXT NOT NULL,
			max_abs_deviation_fixed INTEGER NOT NULL,
			max_relative_deviation_ppm INTEGER NOT NULL,
			cumulative_abs_deviation_fixed INTEGER NOT NULL,
			consistency_ok INTEGER NOT NULL,
			elements_json TEXT NOT NULL,
			PRIMARY KEY (checkpoint_version, shard_id)
		)`,
		`CREATE TABLE IF NOT EXISTS operations (
			op_id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			status TEXT NOT NULL,
			phase TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			policy_version INTEGER NOT NULL DEFAULT 0,
			checkpoint_version INTEGER NOT NULL DEFAULT 0,
			base_checkpoint_version INTEGER NOT NULL DEFAULT 0,
			detail_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS operation_shards (
			op_id TEXT NOT NULL REFERENCES operations(op_id),
			copy_id TEXT NOT NULL,
			shard_id TEXT NOT NULL,
			target_pool TEXT NOT NULL,
			target_status TEXT NOT NULL,
			staging_state TEXT NOT NULL,
			detail_json TEXT NOT NULL,
			PRIMARY KEY (op_id, copy_id)
		)`,
		`CREATE TABLE IF NOT EXISTS operation_evictions (
			op_id TEXT NOT NULL REFERENCES operations(op_id),
			copy_id TEXT NOT NULL,
			from_pool TEXT NOT NULL,
			to_pool TEXT NOT NULL,
			state TEXT NOT NULL,
			detail_json TEXT NOT NULL,
			PRIMARY KEY (op_id, copy_id)
		)`,
		`CREATE TABLE IF NOT EXISTS recovery_log (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			op_id TEXT NOT NULL UNIQUE REFERENCES operations(op_id),
			base_checkpoint_version INTEGER NOT NULL,
			phase TEXT NOT NULL,
			recovered_at TEXT NOT NULL,
			pending_shards_json TEXT NOT NULL,
			aborted_evictions_json TEXT NOT NULL,
			detail_json TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_checkpoint_copies_shard ON checkpoint_copies(checkpoint_version, shard_id)`,
		`CREATE INDEX IF NOT EXISTS idx_operations_status ON operations(status)`,
		`CREATE INDEX IF NOT EXISTS idx_recovery_base ON recovery_log(base_checkpoint_version)`,
	}
	for _, statement := range statements {
		if _, err := e.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
	}
	return nil
}

type recoveredIntent struct {
	OperationID string
	Phase       string
	BaseVersion int64
	Pending     []string
	Evictions   []string
}

func (e *Engine) Recover(ctx context.Context) ([]recoveredIntent, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT op_id, phase, base_checkpoint_version FROM operations WHERE status='RUNNING' ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	type running struct {
		id          string
		phase       string
		baseVersion int64
	}
	var intents []running
	for rows.Next() {
		var r running
		if err := rows.Scan(&r.id, &r.phase, &r.baseVersion); err != nil {
			rows.Close()
			return nil, err
		}
		intents = append(intents, r)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	var out []recoveredIntent
	for _, intent := range intents {
		pending, err := stagedIDs(ctx, tx, `SELECT copy_id FROM operation_shards WHERE op_id=? ORDER BY copy_id`, intent.id)
		if err != nil {
			return nil, err
		}
		evictions, err := stagedIDs(ctx, tx, `SELECT copy_id FROM operation_evictions WHERE op_id=? ORDER BY copy_id`, intent.id)
		if err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE operation_shards SET staging_state='PENDING' WHERE op_id=?`, intent.id); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE operation_evictions SET state='ABORTED' WHERE op_id=?`, intent.id); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE operations SET status='ABORTED', phase='RECOVERED', updated_at=? WHERE op_id=?`, now.Format(time.RFC3339Nano), intent.id); err != nil {
			return nil, err
		}
		pendingJSON, _ := json.Marshal(pending)
		evictionsJSON, _ := json.Marshal(evictions)
		detail, _ := json.Marshal(map[string]any{"recovered_at": now, "phase": intent.phase, "note": "uncommitted intent aborted; committed checkpoint facts unchanged"})
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO recovery_log(op_id, base_checkpoint_version, phase, recovered_at, pending_shards_json, aborted_evictions_json, detail_json) VALUES(?,?,?,?,?,?,?)`,
			intent.id, intent.baseVersion, intent.phase, now.Format(time.RFC3339Nano), string(pendingJSON), string(evictionsJSON), string(detail)); err != nil {
			return nil, err
		}
		out = append(out, recoveredIntent{OperationID: intent.id, Phase: intent.phase, BaseVersion: intent.baseVersion, Pending: pending, Evictions: evictions})
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

func stagedIDs(ctx context.Context, tx *sql.Tx, query, opID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, opID)
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

func newOperationID(kind string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s-%d", kind, time.Now().UnixNano())
	}
	return kind + "-" + hex.EncodeToString(b[:])
}

func insertIntent(ctx context.Context, tx *sql.Tx, kind, phase string, baseCheckpoint, policyVersion int64, detail any) (string, error) {
	id := newOperationID(kind)
	raw, err := json.Marshal(detail)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = tx.ExecContext(ctx, `INSERT INTO operations(op_id, kind, status, phase, created_at, updated_at, policy_version, base_checkpoint_version, detail_json) VALUES(?,?,?,?,?,?,?,?,?)`,
		id, kind, "RUNNING", phase, now, now, policyVersion, baseCheckpoint, string(raw))
	return id, err
}

func finishIntent(ctx context.Context, tx *sql.Tx, opID string, status, phase string, checkpointVersion int64) error {
	_, err := tx.ExecContext(ctx, `UPDATE operations SET status=?, phase=?, checkpoint_version=?, updated_at=? WHERE op_id=?`,
		status, phase, checkpointVersion, time.Now().UTC().Format(time.RFC3339Nano), opID)
	return err
}

func (e *Engine) abortIntent(opID, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	detail, _ := json.Marshal(map[string]any{"abort_reason": reason})
	_, _ = tx.ExecContext(ctx, `UPDATE operations SET status='ABORTED', phase='ABORTED', updated_at=?, detail_json=? WHERE op_id=? AND status='RUNNING'`, now, string(detail), opID)
	_, _ = tx.ExecContext(ctx, `UPDATE operation_shards SET staging_state='PENDING' WHERE op_id=?`, opID)
	_, _ = tx.ExecContext(ctx, `UPDATE operation_evictions SET state='ABORTED' WHERE op_id=?`, opID)
	_ = tx.Commit()
}

func (e *Engine) LatestCheckpointVersion(ctx context.Context) (int64, error) {
	var version int64
	err := e.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM checkpoints`).Scan(&version)
	return version, err
}

func (e *Engine) LatestPolicyVersion(ctx context.Context) (int64, error) {
	var version int64
	err := e.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM policy_versions`).Scan(&version)
	return version, err
}

func parseTime(value string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, value)
	return t
}

func normalizeFaultMode(mode string) (string, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	switch mode {
	case "", "write", "evict":
		return mode, nil
	default:
		return "", domain.BizError(domain.ErrInvalidRequest, "fault_after_intent must be empty, write, or evict", map[string]any{"fault_after_intent": mode})
	}
}
