package store

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"ropechunk/internal/model"
)

type Store struct {
	db       *sql.DB
	path     string
	lockPath string
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("database path is empty")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)&_txlock=immediate", filepath.ToSlash(abs))
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
	s := &Store{db: db, path: abs, lockPath: abs + ".checkpoint.lock"}
	if err := s.initSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func Clean(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	for _, p := range []string{abs, abs + "-wal", abs + "-shm", abs + ".checkpoint.lock"} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Path() string { return s.path }

func (s *Store) initSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS configs (
			version INTEGER PRIMARY KEY,
			digest TEXT NOT NULL,
			snapshot_json TEXT NOT NULL,
			imported_at TEXT NOT NULL,
			is_current INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS configs_one_current ON configs(is_current) WHERE is_current = 1`,
		`CREATE TABLE IF NOT EXISTS version_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			from_version INTEGER NOT NULL,
			to_version INTEGER NOT NULL,
			kind TEXT NOT NULL,
			from_digest TEXT NOT NULL DEFAULT '',
			to_digest TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS shards (
			id TEXT PRIMARY KEY,
			description TEXT NOT NULL DEFAULT '',
			shape_json TEXT NOT NULL,
			q_scale INTEGER NOT NULL,
			q_data BLOB NOT NULL,
			k_scale INTEGER NOT NULL,
			k_data BLOB NOT NULL,
			ref_q_scale INTEGER,
			ref_q_data BLOB,
			ref_k_scale INTEGER,
			ref_k_data BLOB,
			input_digest TEXT NOT NULL,
			imported_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS shape_declarations (
			shard_id TEXT PRIMARY KEY,
			shape_json TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			has_extremes INTEGER NOT NULL DEFAULT 0,
			FOREIGN KEY(shard_id) REFERENCES shards(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			shard_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			batch INTEGER NOT NULL,
			head INTEGER NOT NULL,
			row_block INTEGER NOT NULL,
			row_start INTEGER NOT NULL,
			row_end INTEGER NOT NULL,
			covered_key_start INTEGER NOT NULL,
			covered_key_end INTEGER NOT NULL,
			running_max REAL NOT NULL,
			exp_sum REAL NOT NULL,
			lse REAL NOT NULL,
			partial_output TEXT NOT NULL DEFAULT '[]',
			partial_digest TEXT NOT NULL DEFAULT '',
			input_fingerprint TEXT NOT NULL,
			version_fingerprint TEXT NOT NULL,
			sequence INTEGER NOT NULL,
			complete INTEGER NOT NULL,
			config_version INTEGER NOT NULL,
			run_id TEXT NOT NULL DEFAULT '',
			result_payload BLOB,
			updated_at TEXT NOT NULL,
			PRIMARY KEY(shard_id, stage, batch, head, row_block)
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS checkpoints_one_active ON checkpoints(shard_id, stage, batch, head, row_block)`,
		`CREATE INDEX IF NOT EXISTS checkpoints_stage ON checkpoints(shard_id, stage, complete)`,
		`CREATE TABLE IF NOT EXISTS invalidations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			config_version INTEGER NOT NULL,
			stage TEXT NOT NULL,
			shard_id TEXT NOT NULL,
			earliest_row_block INTEGER NOT NULL,
			col_block_start INTEGER NOT NULL,
			col_block_end INTEGER NOT NULL,
			reason TEXT NOT NULL,
			affected_rows INTEGER NOT NULL,
			reused_rows INTEGER NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS budget_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id TEXT NOT NULL,
			shard_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			batch INTEGER NOT NULL,
			head INTEGER NOT NULL,
			row_block INTEGER NOT NULL,
			status TEXT NOT NULL,
			reason TEXT NOT NULL DEFAULT '',
			estimated_us INTEGER NOT NULL,
			remaining_us INTEGER NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS budget_run ON budget_events(run_id)`,
		`CREATE TABLE IF NOT EXISTS memory_plans (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id TEXT NOT NULL,
			shard_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			budget_bytes INTEGER NOT NULL,
			full_estimate_json TEXT NOT NULL,
			mode TEXT NOT NULL,
			steps_json TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS runs (
			id TEXT PRIMARY KEY,
			config_version INTEGER NOT NULL,
			stage TEXT NOT NULL,
			status TEXT NOT NULL,
			started_at TEXT NOT NULL,
			finished_at TEXT NOT NULL DEFAULT '',
			note TEXT NOT NULL DEFAULT ''
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("schema: %w", err)
		}
	}
	return nil
}

func (s *Store) SaveBundle(ctx context.Context, bundle model.Bundle) (model.ConfigTransition, error) {
	if err := bundle.Validate(); err != nil {
		return model.ConfigTransition{}, &model.AppError{Code: model.CodeInvalidConfig, Message: err.Error()}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.ConfigTransition{}, err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, shard := range bundle.Shards {
		if err := saveShard(ctx, tx, shard, now); err != nil {
			return model.ConfigTransition{}, err
		}
	}
	for _, decl := range bundle.Declarations {
		shapeJSON, _ := json.Marshal(decl.Shape)
		if _, err := tx.ExecContext(ctx, `INSERT INTO shape_declarations(shard_id, shape_json, description, has_extremes)
			VALUES(?,?,?,?) ON CONFLICT(shard_id) DO UPDATE SET shape_json=excluded.shape_json,
			description=excluded.description, has_extremes=excluded.has_extremes`,
			decl.ShardID, string(shapeJSON), decl.Description, boolInt(decl.HasExtremes)); err != nil {
			return model.ConfigTransition{}, err
		}
	}
	cfg := bundle.Config
	cfgJSON, _ := json.Marshal(cfg)
	cfgDigest := model.ConfigDigest(cfg)
	var fromVersion int64
	var fromDigest string
	var fromJSON string
	err = tx.QueryRowContext(ctx, `SELECT version, digest, snapshot_json FROM configs WHERE is_current=1 LIMIT 1`).Scan(&fromVersion, &fromDigest, &fromJSON)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return model.ConfigTransition{}, err
	}
	hadCurrent := !errors.Is(err, sql.ErrNoRows)
	if _, err := tx.ExecContext(ctx, `INSERT INTO configs(version, digest, snapshot_json, imported_at, is_current)
		VALUES(?,?,?,?,0) ON CONFLICT(version) DO UPDATE SET digest=excluded.digest,
		snapshot_json=excluded.snapshot_json, imported_at=excluded.imported_at`,
		cfg.SnapshotVersion, cfgDigest, string(cfgJSON), now); err != nil {
		return model.ConfigTransition{}, err
	}
	transition := model.ConfigTransition{To: cfg}
	switch {
	case !hadCurrent:
		transition.Kind = "initial"
	case cfg.SnapshotVersion > fromVersion:
		transition.Kind = "advance"
	case cfg.SnapshotVersion < fromVersion:
		transition.Kind = "rollback"
	case cfgDigest == fromDigest:
		transition.Kind = "idempotent"
	default:
		transition.Kind = "replacement"
	}
	if hadCurrent {
		var fromCfg model.Config
		if json.Unmarshal([]byte(fromJSON), &fromCfg) == nil {
			transition.From = &fromCfg
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE configs SET is_current=0 WHERE is_current=1`); err != nil {
		return model.ConfigTransition{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE configs SET is_current=1 WHERE version=?`, cfg.SnapshotVersion); err != nil {
		return model.ConfigTransition{}, err
	}
	event := model.VersionEvent{FromVersion: fromVersion, ToVersion: cfg.SnapshotVersion, Kind: transition.Kind, FromDigest: fromDigest, ToDigest: cfgDigest, CreatedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO version_events(from_version,to_version,kind,from_digest,to_digest,created_at) VALUES(?,?,?,?,?,?)`,
		event.FromVersion, event.ToVersion, event.Kind, event.FromDigest, event.ToDigest, event.CreatedAt); err != nil {
		return model.ConfigTransition{}, err
	}
	transition.Event = event
	if err := tx.Commit(); err != nil {
		return model.ConfigTransition{}, err
	}
	return transition, nil
}

func saveShard(ctx context.Context, tx *sql.Tx, shard model.Shard, now string) error {
	shapeJSON, _ := json.Marshal(shard.Shape)
	inputDigest := model.InputDigest(shard)
	var refQScale, refKScale sql.NullInt64
	var refQData, refKData []byte
	if shard.ReferenceQ != nil {
		refQScale = sql.NullInt64{Int64: shard.ReferenceQ.Scale, Valid: true}
		refQData = encodeInts(shard.ReferenceQ.Data)
	}
	if shard.ReferenceK != nil {
		refKScale = sql.NullInt64{Int64: shard.ReferenceK.Scale, Valid: true}
		refKData = encodeInts(shard.ReferenceK.Data)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO shards(id,description,shape_json,q_scale,q_data,k_scale,k_data,
		ref_q_scale,ref_q_data,ref_k_scale,ref_k_data,input_digest,imported_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET description=excluded.description, shape_json=excluded.shape_json,
		q_scale=excluded.q_scale,q_data=excluded.q_data,k_scale=excluded.k_scale,k_data=excluded.k_data,
		ref_q_scale=excluded.ref_q_scale,ref_q_data=excluded.ref_q_data,
		ref_k_scale=excluded.ref_k_scale,ref_k_data=excluded.ref_k_data,
		input_digest=excluded.input_digest,imported_at=excluded.imported_at`,
		shard.ID, shard.Description, string(shapeJSON), shard.Q.Scale, encodeInts(shard.Q.Data),
		shard.K.Scale, encodeInts(shard.K.Data), nullableInt(refQScale), refQData,
		nullableInt(refKScale), refKData, inputDigest, now)
	return err
}

func (s *Store) CurrentConfig(ctx context.Context) (model.Config, error) {
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT snapshot_json FROM configs WHERE is_current=1 LIMIT 1`).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Config{}, &model.AppError{Code: model.CodeState, Message: "no current config snapshot"}
		}
		return model.Config{}, err
	}
	var cfg model.Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return model.Config{}, err
	}
	return cfg, nil
}

func (s *Store) LoadConfig(ctx context.Context, version int64) (model.Config, error) {
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT snapshot_json FROM configs WHERE version=?`, version).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Config{}, &model.AppError{Code: model.CodeNotFound, Message: fmt.Sprintf("config version %d not found", version)}
		}
		return model.Config{}, err
	}
	var cfg model.Config
	return cfg, json.Unmarshal([]byte(raw), &cfg)
}

func (s *Store) ListConfigs(ctx context.Context) ([]model.Config, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT snapshot_json FROM configs ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Config
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var cfg model.Config
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			return nil, err
		}
		out = append(out, cfg)
	}
	return out, rows.Err()
}

func (s *Store) LoadShard(ctx context.Context, id string) (model.Shard, error) {
	var shard model.Shard
	var shapeJSON string
	var qData, kData, refQData, refKData []byte
	var qScale, kScale int64
	var refQScale, refKScale sql.NullInt64
	var inputDigest string
	err := s.db.QueryRowContext(ctx, `SELECT id,description,shape_json,q_scale,q_data,k_scale,k_data,
		ref_q_scale,ref_q_data,ref_k_scale,ref_k_data,input_digest FROM shards WHERE id=?`, id).Scan(
		&shard.ID, &shard.Description, &shapeJSON, &qScale, &qData, &kScale, &kData,
		&refQScale, &refQData, &refKScale, &refKData, &inputDigest)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Shard{}, &model.AppError{Code: model.CodeNotFound, Message: fmt.Sprintf("shard %s not found", id)}
		}
		return model.Shard{}, err
	}
	if err := json.Unmarshal([]byte(shapeJSON), &shard.Shape); err != nil {
		return model.Shard{}, err
	}
	shard.Q = model.QuantTensor{Shape: shard.Shape, Scale: qScale, Data: decodeInts(qData)}
	shard.K = model.QuantTensor{Shape: shard.Shape, Scale: kScale, Data: decodeInts(kData)}
	if refQScale.Valid {
		shard.ReferenceQ = &model.QuantTensor{Shape: shard.Shape, Scale: refQScale.Int64, Data: decodeInts(refQData)}
	}
	if refKScale.Valid {
		shard.ReferenceK = &model.QuantTensor{Shape: shard.Shape, Scale: refKScale.Int64, Data: decodeInts(refKData)}
	}
	shard.InputDigest = inputDigest
	return shard, nil
}

func (s *Store) ListShards(ctx context.Context) ([]model.Shard, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM shards ORDER BY id`)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := make([]model.Shard, 0, len(ids))
	for _, id := range ids {
		shard, err := s.LoadShard(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, shard)
	}
	return out, nil
}

func (s *Store) ListDeclarations(ctx context.Context) ([]model.ShapeDeclaration, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT shard_id,shape_json,description,has_extremes FROM shape_declarations ORDER BY shard_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.ShapeDeclaration
	for rows.Next() {
		var d model.ShapeDeclaration
		var shapeJSON string
		var extreme int
		if err := rows.Scan(&d.ShardID, &shapeJSON, &d.Description, &extreme); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(shapeJSON), &d.Shape); err != nil {
			return nil, err
		}
		d.HasExtremes = extreme != 0
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) ListVersionEvents(ctx context.Context) ([]model.VersionEvent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT from_version,to_version,kind,from_digest,to_digest,created_at FROM version_events ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.VersionEvent
	for rows.Next() {
		var e model.VersionEvent
		if err := rows.Scan(&e.FromVersion, &e.ToVersion, &e.Kind, &e.FromDigest, &e.ToDigest, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func encodeInts(values []int64) []byte {
	out := make([]byte, len(values)*8)
	for i, v := range values {
		binary.LittleEndian.PutUint64(out[i*8:], uint64(v))
	}
	return out
}

func decodeInts(data []byte) []int64 {
	out := make([]int64, len(data)/8)
	for i := range out {
		out[i] = int64(binary.LittleEndian.Uint64(data[i*8:]))
	}
	return out
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func nullableInt(v sql.NullInt64) any {
	if !v.Valid {
		return nil
	}
	return v.Int64
}
