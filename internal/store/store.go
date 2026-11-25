package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/gofrs/flock"
	_ "modernc.org/sqlite"

	"featureplatform/internal/configx"
	"featureplatform/internal/model"
)

var ErrNoConfig = errors.New("no config snapshot")

type Store struct {
	db   *sql.DB
	path string
	mu   sync.Mutex
}

func Open(path string) (*Store, error) {
	if path == "" {
		path = "featureplatform.db"
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;`); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable sqlite pragmas: %w", err)
	}
	s := &Store{db: db, path: path}
	if err := s.migrateSchema(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DBPath() string { return s.path }

func (s *Store) withFileLock(fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	lockPath, err := filepath.Abs(s.path + ".lock")
	if err != nil {
		return err
	}
	fileLock := flock.New(lockPath)
	if err := fileLock.Lock(); err != nil {
		return fmt.Errorf("acquire file lock %s: %w", lockPath, err)
	}
	defer func() { _ = fileLock.Unlock() }()
	return fn()
}

func (s *Store) transact(fn func(tx *sql.Tx) error) error {
	return s.withFileLock(func() error {
		tx, err := s.db.Begin()
		if err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	})
}

func (s *Store) migrateSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`INSERT OR IGNORE INTO meta(key,value) VALUES
			('current_config_version','0'),
			('min_accepted_version','0'),
			('global_commit_cursor','0')`,
		`CREATE TABLE IF NOT EXISTS config_snapshots (
			config_version INTEGER PRIMARY KEY,
			fingerprint TEXT NOT NULL,
			canonical_json TEXT NOT NULL,
			fragment_id TEXT NOT NULL DEFAULT '',
			created_seq INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS config_fragments (
			fragment_id TEXT PRIMARY KEY,
			base_version INTEGER NOT NULL,
			new_version INTEGER NOT NULL,
			fragment_json TEXT NOT NULL,
			plan_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS invalidations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			config_version INTEGER NOT NULL,
			fragment_id TEXT NOT NULL DEFAULT '',
			source TEXT NOT NULL,
			plan_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS task_cursors (
			task TEXT PRIMARY KEY,
			last_sequence INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS task_version_state (
			task TEXT PRIMARY KEY,
			max_seen_version INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS samples (
			sample_id TEXT PRIMARY KEY,
			task TEXT NOT NULL,
			task_seq INTEGER NOT NULL,
			task_version INTEGER NOT NULL,
			content_fingerprint TEXT NOT NULL,
			canonical_json TEXT NOT NULL,
			import_batch TEXT NOT NULL DEFAULT '',
			UNIQUE(task, task_seq)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_samples_task_seq ON samples(task, task_seq)`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			stage TEXT NOT NULL,
			unit_id TEXT NOT NULL,
			sample_id TEXT NOT NULL DEFAULT '',
			content_fingerprint TEXT NOT NULL,
			config_version INTEGER NOT NULL,
			artifact_digest TEXT NOT NULL,
			stage_fingerprint TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'valid',
			superseded_by_seq INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_checkpoints_one_valid
			ON checkpoints(stage, unit_id) WHERE status = 'valid'`,
		`CREATE INDEX IF NOT EXISTS idx_checkpoints_sample ON checkpoints(sample_id)`,
		`CREATE INDEX IF NOT EXISTS idx_checkpoints_stage ON checkpoints(stage, seq)`,
		`CREATE TABLE IF NOT EXISTS record_versions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			sample_id TEXT NOT NULL,
			config_version INTEGER NOT NULL,
			task TEXT NOT NULL,
			unified_label TEXT NOT NULL,
			mapping_version TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			hit_bits INTEGER NOT NULL,
			denominator INTEGER NOT NULL DEFAULT 0,
			applicable INTEGER NOT NULL DEFAULT 0,
			canonical_json TEXT NOT NULL,
			digest TEXT NOT NULL,
			is_valid INTEGER NOT NULL DEFAULT 1
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_record_versions_one_valid
			ON record_versions(sample_id) WHERE is_valid = 1`,
		`CREATE INDEX IF NOT EXISTS idx_record_versions_valid_order
			ON record_versions(sample_id, id) WHERE is_valid = 1`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate schema: %w", err)
		}
	}
	return nil
}

func (s *Store) currentVersionTx(tx *sql.Tx) (int64, int64, error) {
	var current string
	if err := tx.QueryRow(`SELECT value FROM meta WHERE key='current_config_version'`).Scan(&current); err != nil {
		return 0, 0, err
	}
	var minAccepted string
	if err := tx.QueryRow(`SELECT value FROM meta WHERE key='min_accepted_version'`).Scan(&minAccepted); err != nil {
		return 0, 0, err
	}
	var cur, minV int64
	if _, err := fmt.Sscanf(current, "%d", &cur); err != nil {
		return 0, 0, err
	}
	if _, err := fmt.Sscanf(minAccepted, "%d", &minV); err != nil {
		return 0, 0, err
	}
	return cur, minV, nil
}

func (s *Store) CurrentVersion(ctx context.Context) (int64, int64, error) {
	var cur, minV int64
	err := s.db.QueryRowContext(ctx, `SELECT
		(SELECT value FROM meta WHERE key='current_config_version'),
		(SELECT value FROM meta WHERE key='min_accepted_version')`).Scan(&cur, &minV)
	if err != nil {
		return 0, 0, err
	}
	return cur, minV, nil
}

func (s *Store) LoadConfig(ctx context.Context) (configx.Config, error) {
	current, _, err := s.CurrentVersion(ctx)
	if err != nil {
		return configx.Config{}, err
	}
	if current == 0 {
		return configx.Config{}, ErrNoConfig
	}
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT canonical_json FROM config_snapshots WHERE config_version=?`, current).Scan(&raw); err != nil {
		return configx.Config{}, err
	}
	var cfg configx.Config
	if err := unmarshalCanonical(raw, &cfg); err != nil {
		return configx.Config{}, err
	}
	return cfg, nil
}

type ConfigCommit struct {
	Config   configx.Config
	Fragment *configx.Fragment
	Plan     *configx.InvalidationPlan
}

func (s *Store) CommitConfig(ctx context.Context, commit ConfigCommit) (string, error) {
	cfg := commit.Config
	fingerprint := cfg.Fingerprints().ConfigFingerprint
	canonical, err := model.CanonicalJSON(cfg)
	if err != nil {
		return "", err
	}
	err = s.transact(func(tx *sql.Tx) error {
		current, _, err := s.currentVersionTx(tx)
		if err != nil {
			return err
		}
		if cfg.ConfigVersion != current+1 {
			return &model.CodedError{
				Code: model.CodeStaleVersion, Message: "config version must be current+1",
				CurrentVersion: current, MinimumAcceptableVersion: current,
			}
		}
		cond, err := tx.Exec(`UPDATE meta SET value=? WHERE key='current_config_version' AND value=?`,
			fmt.Sprintf("%d", cfg.ConfigVersion), fmt.Sprintf("%d", current))
		if err != nil {
			return err
		}
		if n, _ := cond.RowsAffected(); n != 1 {
			fresh, _, _ := s.currentVersionTx(tx)
			return &model.CodedError{Code: model.CodeStaleVersion, Message: "conditional config version update lost", CurrentVersion: fresh}
		}
		if _, err := tx.Exec(`UPDATE meta SET value=? WHERE key='min_accepted_version'`, fmt.Sprintf("%d", cfg.ConfigVersion)); err != nil {
			return err
		}
		var maxSeq int64
		if err := tx.QueryRow(`SELECT COALESCE(MAX(created_seq),0)+1 FROM config_snapshots`).Scan(&maxSeq); err != nil {
			return err
		}
		fragmentID := ""
		if commit.Fragment != nil {
			fragmentID = commit.Fragment.FragmentID
			fragJSON, err := model.CanonicalJSON(commit.Fragment)
			if err != nil {
				return err
			}
			planJSON := []byte("{}")
			if commit.Plan != nil {
				planJSON, err = model.CanonicalJSON(commit.Plan)
				if err != nil {
					return err
				}
			}
			if _, err := tx.Exec(`INSERT INTO config_fragments(fragment_id,base_version,new_version,fragment_json,plan_json)
				VALUES(?,?,?,?,?)`, commit.Fragment.FragmentID, commit.Fragment.BaseVersion, commit.Fragment.NewVersion, string(fragJSON), string(planJSON)); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO invalidations(config_version,fragment_id,source,plan_json)
				VALUES(?,?,?,?)`, cfg.ConfigVersion, fragmentID, "fragment", string(planJSON)); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(`INSERT INTO config_snapshots(config_version,fingerprint,canonical_json,fragment_id,created_seq)
			VALUES(?,?,?,?,?)`, cfg.ConfigVersion, fingerprint, string(canonical), fragmentID, maxSeq); err != nil {
			return err
		}
		if commit.Plan != nil && commit.Plan.FullRebuild {
			if _, err := tx.Exec(`INSERT INTO meta(key,value) VALUES('pending_full_rebuild_version',?)
				ON CONFLICT(key) DO UPDATE SET value=excluded.value`, fmt.Sprintf("%d", cfg.ConfigVersion)); err != nil {
				return err
			}
		}
		return nil
	})
	return fingerprint, err
}

func (s *Store) GetMeta(ctx context.Context, key string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key=?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return value, err
}

func (s *Store) SetMeta(ctx context.Context, key, value string) error {
	return s.transact(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO meta(key,value) VALUES(?,?)
			ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
		return err
	})
}

type StoredSample struct {
	Sample             model.Sample
	TaskSequence       int64
	ContentFingerprint string
	ImportBatch        string
}

type ImportResult struct {
	InsertedOrUpdated   int      `json:"inserted_or_updated"`
	TaskVersionRollback []string `json:"task_version_rollback"`
}

func (s *Store) ImportShards(ctx context.Context, shards []model.Shard, batchID string) (ImportResult, error) {
	result := ImportResult{}
	err := s.transact(func(tx *sql.Tx) error {
		rollbackTasks := map[string]struct{}{}
		for _, shard := range shards {
			var maxExistingVersion int
			if err := tx.QueryRow(`SELECT COALESCE(MAX(max_seen_version),0) FROM task_version_state WHERE task=?`, shard.Task).Scan(&maxExistingVersion); err != nil {
				return err
			}
			var maxSeq int64
			if err := tx.QueryRow(`SELECT COALESCE(MAX(task_seq),0) FROM samples WHERE task=?`, shard.Task).Scan(&maxSeq); err != nil {
				return err
			}
			batchMaxVersion := 0
			for i := range shard.Samples {
				sample := shard.Samples[i]
				if sample.Task == "" {
					sample.Task = shard.Task
				}
				if sample.TaskVersion > batchMaxVersion {
					batchMaxVersion = sample.TaskVersion
				}
				if maxExistingVersion > 0 && sample.TaskVersion < maxExistingVersion {
					rollbackTasks[shard.Task] = struct{}{}
				}
				fp, err := sample.ContentFingerprint()
				if err != nil {
					return err
				}
				canonical, err := model.CanonicalJSON(sample)
				if err != nil {
					return err
				}
				taskSeq := maxSeq + int64(i) + 1
				if _, err := tx.Exec(`INSERT INTO samples(sample_id,task,task_seq,task_version,content_fingerprint,canonical_json,import_batch)
					VALUES(?,?,?,?,?,?,?)
					ON CONFLICT(sample_id) DO UPDATE SET
						task=excluded.task,
						task_version=excluded.task_version,
						content_fingerprint=excluded.content_fingerprint,
						canonical_json=excluded.canonical_json,
						import_batch=excluded.import_batch`,
					sample.SampleID, sample.Task, taskSeq, sample.TaskVersion, fp, string(canonical), batchID); err != nil {
					return err
				}
				result.InsertedOrUpdated++
			}
			if _, err := tx.Exec(`INSERT INTO task_version_state(task,max_seen_version) VALUES(?,?)
				ON CONFLICT(task) DO UPDATE SET max_seen_version=MAX(max_seen_version, excluded.max_seen_version)`,
				shard.Task, batchMaxVersion); err != nil {
				return err
			}
		}
		tasks := make([]string, 0, len(rollbackTasks))
		for task := range rollbackTasks {
			tasks = append(tasks, task)
		}
		result.TaskVersionRollback = tasks
		return nil
	})
	return result, err
}

func (s *Store) ListSamples(ctx context.Context) ([]StoredSample, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT sample_id,task,task_seq,task_version,content_fingerprint,canonical_json,import_batch
		FROM samples ORDER BY sample_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StoredSample{}
	for rows.Next() {
		var id, task, fp, raw, batch string
		var seq int64
		var version int
		if err := rows.Scan(&id, &task, &seq, &version, &fp, &raw, &batch); err != nil {
			return nil, err
		}
		var sample model.Sample
		if err := unmarshalCanonical(raw, &sample); err != nil {
			return nil, err
		}
		out = append(out, StoredSample{Sample: sample, TaskSequence: seq, ContentFingerprint: fp, ImportBatch: batch})
	}
	return out, rows.Err()
}

func unmarshalCanonical(raw string, target any) error {
	return modelJSONUnmarshal([]byte(raw), target)
}
