package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"localinfer/workbench/internal/domain"
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("database path is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)", filepath.ToSlash(abs))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS models (
  model_id TEXT PRIMARY KEY,
  vram_per_sample_bytes INTEGER NOT NULL,
  cost_per_sample_us INTEGER NOT NULL,
  max_batch_samples INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS catalog_meta (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  version TEXT NOT NULL,
  catalog_hash TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS traces (
  trace_id TEXT NOT NULL,
  version TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  trace_json BLOB NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (trace_id, version)
);
CREATE TABLE IF NOT EXISTS trace_requests (
  trace_id TEXT NOT NULL,
  version TEXT NOT NULL,
  seq INTEGER NOT NULL,
  request_id TEXT NOT NULL,
  arrival_ms INTEGER NOT NULL,
  model_id TEXT NOT NULL,
  samples INTEGER NOT NULL,
  latency_limit_us INTEGER NOT NULL,
  priority INTEGER NOT NULL,
  PRIMARY KEY (trace_id, version, request_id),
  FOREIGN KEY (trace_id, version) REFERENCES traces(trace_id, version) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  trace_id TEXT NOT NULL,
  version TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  catalog_hash TEXT NOT NULL,
  mode TEXT NOT NULL,
  mode_label TEXT NOT NULL,
  status TEXT NOT NULL,
  options_json TEXT NOT NULL,
  options_hash TEXT NOT NULL,
  plan_hash TEXT NOT NULL DEFAULT '',
  result_hash TEXT NOT NULL DEFAULT '',
  node_vram_bytes INTEGER NOT NULL,
  available_vram_bytes INTEGER NOT NULL,
  used_vram_bytes INTEGER NOT NULL DEFAULT 0,
  batch_count INTEGER NOT NULL DEFAULT 0,
  deferred_count INTEGER NOT NULL DEFAULT 0,
  deferred_reason_json TEXT NOT NULL DEFAULT '{}',
  selected_requests INTEGER NOT NULL DEFAULT 0,
  total_input_samples INTEGER NOT NULL DEFAULT 0,
  total_planned_samples INTEGER NOT NULL DEFAULT 0,
  sample_conservation_ok INTEGER NOT NULL DEFAULT 0,
  precision_ok INTEGER NOT NULL DEFAULT 1,
  latency_ok INTEGER NOT NULL DEFAULT 1,
  failure_code TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  completed_at TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (trace_id, version) REFERENCES traces(trace_id, version)
);
CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  batch_seq INTEGER NOT NULL,
  root_seq INTEGER NOT NULL,
  split_index INTEGER NOT NULL,
  split_reason TEXT NOT NULL,
  split_detail TEXT NOT NULL,
  status TEXT NOT NULL,
  mode TEXT NOT NULL,
  priority INTEGER NOT NULL,
  models_json TEXT NOT NULL,
  total_samples INTEGER NOT NULL,
  vram_bytes INTEGER NOT NULL,
  latency_us INTEGER NOT NULL,
  compensation_us INTEGER NOT NULL,
  latency_limit_us INTEGER NOT NULL,
  latency_ok INTEGER NOT NULL,
  sealed INTEGER NOT NULL,
  late_sealed INTEGER NOT NULL,
  seal_reason TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE,
  UNIQUE (run_id, batch_seq)
);
CREATE TABLE IF NOT EXISTS batch_members (
  batch_id TEXT NOT NULL,
  member_ord INTEGER NOT NULL,
  request_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  model_id TEXT NOT NULL,
  allocated_samples INTEGER NOT NULL,
  original_samples INTEGER NOT NULL,
  allocation_reason TEXT NOT NULL,
  PRIMARY KEY (batch_id, member_ord),
  FOREIGN KEY (batch_id) REFERENCES batches(batch_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS quantile_results (
  batch_id TEXT NOT NULL,
  quantile INTEGER NOT NULL,
  method TEXT NOT NULL,
  available INTEGER NOT NULL,
  latency_us INTEGER NOT NULL,
  latency_ms TEXT NOT NULL,
  rank INTEGER NOT NULL,
  sample_count INTEGER NOT NULL,
  min_samples INTEGER NOT NULL,
  error_code TEXT NOT NULL,
  reason TEXT NOT NULL,
  PRIMARY KEY (batch_id, quantile),
  FOREIGN KEY (batch_id) REFERENCES batches(batch_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS precision_results (
  batch_id TEXT PRIMARY KEY,
  truth_us INTEGER NOT NULL,
  reported_us INTEGER NOT NULL,
  reported_ms TEXT NOT NULL,
  rounding_place TEXT NOT NULL,
  internal_fixed_us INTEGER NOT NULL,
  compensation_us INTEGER NOT NULL,
  rounding_error_us INTEGER NOT NULL,
  cumulative_rounding_error_us INTEGER NOT NULL,
  abs_deviation_us INTEGER NOT NULL,
  relative_ppm INTEGER NOT NULL,
  max_abs_us INTEGER NOT NULL,
  max_rel_ppm INTEGER NOT NULL,
  precision_ok INTEGER NOT NULL,
  explanation TEXT NOT NULL,
  FOREIGN KEY (batch_id) REFERENCES batches(batch_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS deferred_requests (
  run_id TEXT NOT NULL,
  ord INTEGER NOT NULL,
  request_id TEXT NOT NULL,
  model_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  detail TEXT NOT NULL,
  PRIMARY KEY (run_id, ord),
  FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS mix_rejections (
  run_id TEXT NOT NULL,
  ord INTEGER NOT NULL,
  request_id TEXT NOT NULL,
  existing_models_json TEXT NOT NULL,
  candidate_model TEXT NOT NULL,
  reason TEXT NOT NULL,
  detail TEXT NOT NULL,
  PRIMARY KEY (run_id, ord),
  FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS intents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,
  trace_id TEXT NOT NULL DEFAULT '',
  run_id TEXT NOT NULL DEFAULT '',
  batch_id TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL,
  detail_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  finished_at TEXT NOT NULL DEFAULT '',
  error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_intents_state ON intents(state, id);
CREATE TABLE IF NOT EXISTS recovery_reports (
  report_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  clean INTEGER NOT NULL,
  payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_trace ON runs(trace_id, version, created_at);
CREATE INDEX IF NOT EXISTS idx_batches_run ON batches(run_id, batch_seq);
`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func (s *Store) withIntent(kind, traceID, runID, batchID string, detail any, fn func(*sql.Tx) error) error {
	detailJSON := "{}"
	if detail != nil {
		b, err := json.Marshal(detail)
		if err != nil {
			return err
		}
		detailJSON = string(b)
	}
	created := now()
	res, err := s.db.Exec(`INSERT INTO intents(kind,trace_id,run_id,batch_id,state,detail_json,created_at) VALUES(?,?,?,?, 'started', ?,?)`,
		kind, traceID, runID, batchID, detailJSON, created)
	if err != nil {
		return err
	}
	intentID, err := res.LastInsertId()
	if err != nil {
		return err
	}
	finish := func(state, errText string) {
		_, _ = s.db.Exec(`UPDATE intents SET state=?, finished_at=?, error=? WHERE id=?`, state, now(), errText, intentID)
	}
	tx, err := s.db.Begin()
	if err != nil {
		finish("aborted", err.Error())
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		finish("aborted", err.Error())
		return err
	}
	if err := tx.Commit(); err != nil {
		finish("aborted", err.Error())
		return err
	}
	finish("committed", "")
	return nil
}

// StartPendingIntent deliberately leaves a durable started intent. It is used
// only by the fault-injection path immediately before the process is killed.
func (s *Store) StartPendingIntent(kind, traceID, runID, batchID string, detail any) (int64, error) {
	detailJSON := "{}"
	if detail != nil {
		b, err := json.Marshal(detail)
		if err != nil {
			return 0, err
		}
		detailJSON = string(b)
	}
	res, err := s.db.Exec(`INSERT INTO intents(kind,trace_id,run_id,batch_id,state,detail_json,created_at) VALUES(?,?,?,?, 'started', ?,?)`,
		kind, traceID, runID, batchID, detailJSON, now())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func intToBool(v int64) bool { return v != 0 }

func (s *Store) InstallCatalog(catalog *domain.ModelCatalog) error {
	if catalog == nil {
		return domain.NewBizError(domain.CodeSpecInvalid, "catalog is required", nil)
	}
	return s.withIntent("INSTALL_MODELS", "", "", "", map[string]any{"catalog_hash": catalog.Hash, "version": catalog.Version, "models": len(catalog.Models)}, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM models`); err != nil {
			return err
		}
		for _, m := range catalog.Models {
			if _, err := tx.Exec(`INSERT INTO models(model_id,vram_per_sample_bytes,cost_per_sample_us,max_batch_samples) VALUES(?,?,?,?)`,
				m.ModelID, m.VRAMPerSample, m.CostPerSample, m.MaxBatchSample); err != nil {
				return err
			}
		}
		_, err := tx.Exec(`INSERT INTO catalog_meta(singleton,version,catalog_hash,updated_at) VALUES(1,?,?,?)
			ON CONFLICT(singleton) DO UPDATE SET version=excluded.version,catalog_hash=excluded.catalog_hash,updated_at=excluded.updated_at`,
			catalog.Version, catalog.Hash, now())
		return err
	})
}

func (s *Store) LoadCatalog() (*domain.ModelCatalog, error) {
	rows, err := s.db.Query(`SELECT model_id,vram_per_sample_bytes,cost_per_sample_us,max_batch_samples FROM models ORDER BY model_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	models := make([]domain.ModelSpec, 0)
	for rows.Next() {
		var m domain.ModelSpec
		if err := rows.Scan(&m.ModelID, &m.VRAMPerSample, &m.CostPerSample, &m.MaxBatchSample); err != nil {
			return nil, err
		}
		models = append(models, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(models) == 0 {
		return nil, domain.NewBizError(domain.CodeSpecInvalid, "model catalog is empty", nil)
	}
	var version, hash string
	err = s.db.QueryRow(`SELECT version,catalog_hash FROM catalog_meta WHERE singleton=1`).Scan(&version, &hash)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if version == "" {
		version = "catalog"
	}
	catalog := domain.MustCatalogFromModels(version, models)
	if hash != "" && catalog.Hash != hash {
		// The persisted hash is part of the fact stream; recomputing from rows
		// catches accidental out-of-band mutation.
		return nil, domain.NewBizError(domain.CodeSpecInvalid, "persisted model catalog hash mismatch", map[string]any{"expected": hash, "actual": catalog.Hash})
	}
	return catalog, nil
}
