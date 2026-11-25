package checkpoint

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"searchlab/internal/apperr"
	"searchlab/internal/stage"
)

type Store struct {
	db       *sql.DB
	lockPath string
	lockFile *os.File
}

type Record struct {
	Seq            int64  `json:"seq"`
	Stage          string `json:"stage"`
	ShardDigest    string `json:"shard_digest"`
	InputDigest    string `json:"input_digest"`
	ConfigVersion  string `json:"config_version"`
	ArtifactDigest string `json:"artifact_digest"`
	PipelineID     string `json:"pipeline_id"`
	Active         bool   `json:"active"`
}

type PublishRequest struct {
	Seq            int64
	Stage          string
	ShardDigest    string
	InputDigest    string
	ConfigVersion  string
	ArtifactDigest string
	PipelineID     string
	Payload        []byte
}

type Loaded struct {
	Record  Record
	Payload []byte
}

func Open(dbPath, lockPath string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return nil, fmt.Errorf("create lock directory: %w", err)
	}
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open checkpoint lock: %w", err)
	}
	store := &Store{lockPath: lockPath, lockFile: lockFile}
	if err := store.lock(context.Background()); err != nil {
		lockFile.Close()
		return nil, err
	}
	defer store.unlock()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(8)
	for _, pragma := range []string{
		"PRAGMA busy_timeout=15000",
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			lockFile.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}
	store.db = db
	if err := store.initSchema(); err != nil {
		store.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) initSchema() error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS artifacts (
			digest TEXT PRIMARY KEY,
			stage TEXT NOT NULL,
			payload BLOB NOT NULL,
			created_seq INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			seq INTEGER NOT NULL UNIQUE,
			stage TEXT NOT NULL,
			shard_digest TEXT NOT NULL,
			input_digest TEXT NOT NULL,
			config_version TEXT NOT NULL,
			artifact_digest TEXT NOT NULL,
			pipeline_id TEXT NOT NULL,
			active INTEGER NOT NULL DEFAULT 1
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS checkpoints_one_active
			ON checkpoints(stage, shard_digest) WHERE active = 1`,
		`CREATE INDEX IF NOT EXISTS checkpoints_lookup
			ON checkpoints(stage, shard_digest, active)`,
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := s.db.Exec(statement); err != nil {
			return fmt.Errorf("initialize checkpoint schema: %w", err)
		}
	}
	return nil
}

func (s *Store) Close() error {
	var first error
	if s.lockFile != nil {
		if err := s.lockFile.Close(); err != nil && first == nil {
			first = err
		}
		s.lockFile = nil
	}
	if s.db != nil {
		if err := s.db.Close(); err != nil && first == nil {
			first = err
		}
		s.db = nil
	}
	return first
}

func (s *Store) lock(ctx context.Context) error {
	if s.lockFile == nil {
		return fmt.Errorf("checkpoint store is closed")
	}
	for {
		err := syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return fmt.Errorf("acquire checkpoint file lock: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (s *Store) unlock() {
	if s.lockFile != nil {
		_ = syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
	}
}

func stageIndex(stageName string) int {
	for i, candidate := range stage.Order {
		if candidate == stageName {
			return i
		}
	}
	return -1
}

func (s *Store) Publish(ctx context.Context, req PublishRequest) (Record, error) {
	if stageIndex(req.Stage) < 0 {
		return Record{}, apperr.E(apperr.CodeInput, "unknown checkpoint stage "+req.Stage, nil)
	}
	if req.Seq <= 0 {
		return Record{}, apperr.E(apperr.CodeInput, "invalid checkpoint request", nil)
	}
	if len(req.Payload) == 0 {
		return Record{}, apperr.E(apperr.CodeInput, "checkpoint payload is empty", nil)
	}
	actualDigest := digest(req.Payload)
	if req.ArtifactDigest == "" {
		req.ArtifactDigest = actualDigest
	} else if req.ArtifactDigest != actualDigest {
		return Record{}, apperr.E(apperr.CodeInput, "artifact digest does not match payload", nil)
	}
	if err := s.lock(ctx); err != nil {
		return Record{}, err
	}
	defer s.unlock()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return Record{}, fmt.Errorf("begin checkpoint transaction: %w", err)
	}
	defer tx.Rollback()
	var maxSeq int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM checkpoints`).Scan(&maxSeq); err != nil {
		return Record{}, fmt.Errorf("read checkpoint sequence: %w", err)
	}
	expected := maxSeq + 1
	if req.Seq != expected {
		return Record{}, &apperr.Error{Code: apperr.CodeStaleCheckpoint, Message: "checkpoint sequence is not the next monotonic value", Stage: req.Stage, ExpectedSeq: expected, ActualSeq: req.Seq}
	}
	idx := stageIndex(req.Stage)
	if idx > 0 {
		previousStage := stage.Order[idx-1]
		var previousSeq int64
		var previousPipeline string
		err := tx.QueryRowContext(ctx, `SELECT seq, pipeline_id FROM checkpoints WHERE stage=? AND shard_digest=? AND active=1`, previousStage, req.ShardDigest).Scan(&previousSeq, &previousPipeline)
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, &apperr.Error{Code: apperr.CodePrereqMissing, Message: "required predecessor checkpoint is missing", Stage: req.Stage, Predecessor: previousStage}
		}
		if err != nil {
			return Record{}, fmt.Errorf("read predecessor checkpoint: %w", err)
		}
		if previousPipeline != req.PipelineID || previousSeq != req.Seq-1 {
			return Record{}, &apperr.Error{Code: apperr.CodeStaleCheckpoint, Message: "checkpoint is cross-stage or cross-pipeline misaligned", Stage: req.Stage, Predecessor: previousStage, ExpectedSeq: req.Seq - 1, ActualSeq: previousSeq}
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE checkpoints SET active=0 WHERE stage=? AND active=1`, req.Stage); err != nil {
		return Record{}, fmt.Errorf("deactivate previous checkpoint: %w", err)
	}
	var artifactStage string
	err = tx.QueryRowContext(ctx, `SELECT stage FROM artifacts WHERE digest=?`, req.ArtifactDigest).Scan(&artifactStage)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO artifacts(digest, stage, payload, created_seq) VALUES(?,?,?,?)`, req.ArtifactDigest, req.Stage, req.Payload, req.Seq); err != nil {
			return Record{}, fmt.Errorf("insert artifact: %w", err)
		}
	} else if err != nil {
		return Record{}, fmt.Errorf("read artifact: %w", err)
	} else if artifactStage != req.Stage {
		return Record{}, apperr.E(apperr.CodeInput, "artifact digest is already associated with stage "+artifactStage, nil)
	}
	record := Record{
		Seq:            req.Seq,
		Stage:          req.Stage,
		ShardDigest:    req.ShardDigest,
		InputDigest:    req.InputDigest,
		ConfigVersion:  req.ConfigVersion,
		ArtifactDigest: req.ArtifactDigest,
		PipelineID:     req.PipelineID,
		Active:         true,
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO checkpoints(seq, stage, shard_digest, input_digest, config_version, artifact_digest, pipeline_id, active)
		 VALUES(?,?,?,?,?,?,?,1)`,
		record.Seq, record.Stage, record.ShardDigest, record.InputDigest, record.ConfigVersion, record.ArtifactDigest, record.PipelineID); err != nil {
		if isUniqueViolation(err) {
			return Record{}, &apperr.Error{Code: apperr.CodeStaleCheckpoint, Message: "duplicate checkpoint sequence", Stage: req.Stage, ExpectedSeq: expected, ActualSeq: req.Seq, Err: err}
		}
		return Record{}, fmt.Errorf("insert checkpoint: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Record{}, fmt.Errorf("commit checkpoint transaction: %w", err)
	}
	return record, nil
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	text := err.Error()
	return contains(text, "UNIQUE constraint failed") || contains(text, "constraint failed")
}

func contains(text, target string) bool {
	for i := 0; i+len(target) <= len(text); i++ {
		if text[i:i+len(target)] == target {
			return true
		}
	}
	return false
}

func (s *Store) NextSeq(ctx context.Context) (int64, error) {
	if err := s.lock(ctx); err != nil {
		return 0, err
	}
	defer s.unlock()
	var maxSeq int64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM checkpoints`).Scan(&maxSeq); err != nil {
		return 0, fmt.Errorf("read checkpoint sequence: %w", err)
	}
	return maxSeq + 1, nil
}

func (s *Store) CurrentSeq(ctx context.Context) (int64, error) {
	var maxSeq int64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM checkpoints`).Scan(&maxSeq); err != nil {
		return 0, fmt.Errorf("read checkpoint sequence: %w", err)
	}
	return maxSeq, nil
}

func (s *Store) Active(ctx context.Context, stage, shardDigest string) (*Loaded, error) {
	var record Record
	var payload []byte
	var active int
	err := s.db.QueryRowContext(ctx,
		`SELECT seq, stage, shard_digest, input_digest, config_version, artifact_digest, pipeline_id, active
		 FROM checkpoints WHERE stage=? AND shard_digest=? AND active=1`,
		stage, shardDigest).Scan(
		&record.Seq, &record.Stage, &record.ShardDigest, &record.InputDigest,
		&record.ConfigVersion, &record.ArtifactDigest, &record.PipelineID, &active,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read active checkpoint: %w", err)
	}
	record.Active = active == 1
	if err := s.db.QueryRowContext(ctx, `SELECT payload FROM artifacts WHERE digest=?`, record.ArtifactDigest).Scan(&payload); err != nil {
		return nil, &apperr.Error{Code: apperr.CodePrereqMissing, Message: "checkpoint artifact payload is missing", Stage: stage}
	}
	if digest(payload) != record.ArtifactDigest {
		return nil, &apperr.Error{Code: apperr.CodeInput, Message: "checkpoint artifact payload digest mismatch", Stage: stage}
	}
	return &Loaded{Record: record, Payload: payload}, nil
}

func (s *Store) LoadLatest(ctx context.Context, stage string) (*Loaded, error) {
	var record Record
	var payload []byte
	var active int
	err := s.db.QueryRowContext(ctx,
		`SELECT seq, stage, shard_digest, input_digest, config_version, artifact_digest, pipeline_id, active
		 FROM checkpoints WHERE stage=? AND active=1 ORDER BY seq DESC LIMIT 1`, stage).Scan(
		&record.Seq, &record.Stage, &record.ShardDigest, &record.InputDigest,
		&record.ConfigVersion, &record.ArtifactDigest, &record.PipelineID, &active,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, &apperr.Error{Code: apperr.CodePrereqMissing, Message: "required stage has no active checkpoint", Stage: stage}
	}
	if err != nil {
		return nil, fmt.Errorf("read latest checkpoint: %w", err)
	}
	record.Active = active == 1
	if err := s.db.QueryRowContext(ctx, `SELECT payload FROM artifacts WHERE digest=?`, record.ArtifactDigest).Scan(&payload); err != nil {
		return nil, &apperr.Error{Code: apperr.CodePrereqMissing, Message: "checkpoint artifact payload is missing", Stage: stage}
	}
	return &Loaded{Record: record, Payload: payload}, nil
}

func (s *Store) Records(ctx context.Context) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq, stage, shard_digest, input_digest, config_version, artifact_digest, pipeline_id, active
		 FROM checkpoints ORDER BY seq`)
	if err != nil {
		return nil, fmt.Errorf("read checkpoints: %w", err)
	}
	defer rows.Close()
	var records []Record
	for rows.Next() {
		var record Record
		var active int
		if err := rows.Scan(&record.Seq, &record.Stage, &record.ShardDigest, &record.InputDigest, &record.ConfigVersion, &record.ArtifactDigest, &record.PipelineID, &active); err != nil {
			return nil, fmt.Errorf("scan checkpoint: %w", err)
		}
		record.Active = active == 1
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func (s *Store) IndexVersion(ctx context.Context) (string, error) {
	records, err := s.Records(ctx)
	if err != nil {
		return "", err
	}
	activeByStage := make(map[string]Record)
	shard := ""
	for _, record := range records {
		if !record.Active {
			continue
		}
		activeByStage[record.Stage] = record
		if shard == "" {
			shard = record.ShardDigest
		} else if shard != record.ShardDigest {
			return "", apperr.E(apperr.CodePrereqMissing, "active checkpoints refer to different shard digests", nil)
		}
	}
	parts := make([]string, 0, len(stage.Order))
	for _, stageName := range stage.Order {
		record, ok := activeByStage[stageName]
		if !ok {
			return "", &apperr.Error{Code: apperr.CodePrereqMissing, Message: "required stage has no active checkpoint", Stage: stageName}
		}
		parts = append(parts, stageName+":"+record.ArtifactDigest)
	}
	sort.Strings(parts)
	canonical, _ := json.Marshal(parts)
	return digest(canonical), nil
}

func (s *Store) PutMeta(ctx context.Context, key string, value []byte) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, string(value))
	if err != nil {
		return fmt.Errorf("write meta %s: %w", key, err)
	}
	return nil
}

func (s *Store) GetMeta(ctx context.Context, key string) ([]byte, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key=?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, &apperr.Error{Code: apperr.CodePrereqMissing, Message: "metadata is missing", Stage: key}
	}
	if err != nil {
		return nil, fmt.Errorf("read meta %s: %w", key, err)
	}
	return []byte(value), nil
}

func digest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
