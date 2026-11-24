// Package store persists normalized snapshots, derived sequences and write
// intents in a local SQLite database using the pure-Go modernc driver.
package store

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
	"strconv"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"replicationlag/internal/apperr"
	"replicationlag/internal/domain"
)

type Store struct {
	db          *sql.DB
	writeMu     sync.Mutex
	afterIntent func()
	now         func() int64
	path        string
}

type OpenOptions struct {
	AfterIntent func()
	Now         func() int64
}

type WriteRequest struct {
	BatchID     string
	Operation   string
	RequestHash string
	BaseVersion *int64
}

type Mutation struct {
	Observations []domain.Observation
	Report       domain.MergeReport
	Response     map[string]any
}

type MutateFunc func(current []domain.Observation, currentVersion int64) (Mutation, error)

type Result struct {
	Success    bool
	Cached     bool
	Version    int64
	HTTPStatus int
	Body       []byte
	Code       string
}

type SnapshotData struct {
	Observations []domain.Observation `json:"observations"`
	Report       domain.MergeReport   `json:"report"`
}

func Open(ctx context.Context, path string, options OpenOptions) (*Store, error) {
	if path == "" {
		path = "lagengine.db"
	}
	if path != ":memory:" && !isURI(path) {
		dir := filepath.Dir(path)
		if dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("create database directory: %w", err)
			}
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	for _, pragma := range []string{
		"PRAGMA busy_timeout=5000",
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("configure sqlite (%s): %w", pragma, err)
		}
	}
	now := options.Now
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	s := &Store{db: db, afterIntent: options.AfterIntent, now: now, path: path}
	if err := s.initSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.RecoverPendingIntents(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func isURI(path string) bool {
	return len(path) >= 5 && (path[:5] == "file:" || path[:5] == "http:")
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) initSchema(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`INSERT OR IGNORE INTO meta(key, value) VALUES ('version', '0')`,
		`INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', '1')`,
		`CREATE TABLE IF NOT EXISTS observations (
			replica_id TEXT NOT NULL,
			sampled_at_ms INTEGER NOT NULL,
			gtid_json TEXT NOT NULL,
			gtid_text TEXT NOT NULL,
			position_name TEXT NOT NULL,
			lag_ms INTEGER NOT NULL,
			reported_version TEXT NOT NULL,
			format_version INTEGER NOT NULL,
			last_write_ms INTEGER NOT NULL,
			fingerprint TEXT NOT NULL,
			PRIMARY KEY (replica_id, sampled_at_ms)
		)`,
		`CREATE TABLE IF NOT EXISTS evolution_sequences (
			replica_id TEXT PRIMARY KEY,
			sequence_json TEXT NOT NULL,
			current_state TEXT NOT NULL,
			last_sampled_at_ms INTEGER NOT NULL,
			stall_count INTEGER NOT NULL,
			lag_level TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS evolution_steps (
			replica_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			sampled_at_ms INTEGER NOT NULL,
			step_json TEXT NOT NULL,
			PRIMARY KEY (replica_id, seq)
		)`,
		`CREATE TABLE IF NOT EXISTS replica_states (
			replica_id TEXT PRIMARY KEY,
			state TEXT NOT NULL,
			lag_level TEXT NOT NULL,
			suggested_action TEXT NOT NULL,
			updated_sampled_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			batch_id TEXT NOT NULL,
			operation TEXT NOT NULL,
			request_hash TEXT NOT NULL,
			base_version INTEGER NOT NULL,
			target_version INTEGER,
			status TEXT NOT NULL,
			detail_json TEXT NOT NULL,
			created_ms INTEGER NOT NULL,
			finished_ms INTEGER
		)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status)`,
		`CREATE TABLE IF NOT EXISTS batches (
			batch_id TEXT PRIMARY KEY,
			request_hash TEXT NOT NULL,
			operation TEXT NOT NULL,
			status TEXT NOT NULL,
			http_status INTEGER NOT NULL,
			response_json TEXT NOT NULL,
			version INTEGER NOT NULL,
			created_ms INTEGER NOT NULL,
			finished_ms INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS commits (
			version INTEGER PRIMARY KEY,
			operation TEXT NOT NULL,
			batch_id TEXT NOT NULL,
			observation_count INTEGER NOT NULL,
			snapshot_hash TEXT NOT NULL,
			created_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS version_snapshots (
			version INTEGER PRIMARY KEY,
			snapshot_json TEXT NOT NULL,
			created_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS recovery_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			intent_id INTEGER NOT NULL,
			batch_id TEXT NOT NULL,
			operation TEXT NOT NULL,
			base_version INTEGER NOT NULL,
			version_at_scan INTEGER NOT NULL,
			last_complete_version INTEGER NOT NULL,
			action TEXT NOT NULL,
			conclusion TEXT NOT NULL,
			scanned_ms INTEGER NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize schema: %w", err)
		}
	}
	return nil
}

// Submit serializes all writes, writes an intent before touching the data set,
// and commits observations, derived data, version and batch result atomically.
func (s *Store) Submit(ctx context.Context, request WriteRequest, mutate MutateFunc) (Result, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if request.Operation == "" {
		request.Operation = "unspecified"
	}
	currentVersion, err := s.Version(ctx)
	if err != nil {
		return Result{}, err
	}
	if request.BatchID != "" {
		if cached, found, cacheErr := s.cachedBatch(ctx, request); cacheErr != nil {
			return Result{}, cacheErr
		} else if found {
			return cached, nil
		}
	}
	if request.BaseVersion != nil && *request.BaseVersion != currentVersion {
		result := failureResult(apperr.New(apperr.CodeStaleBase, "base version is not current", map[string]any{
			"base_version":    *request.BaseVersion,
			"current_version": currentVersion,
		}), currentVersion)
		if request.BatchID != "" {
			if err := s.storeImmediateFailure(ctx, request, result); err != nil {
				return Result{}, err
			}
		}
		return result, nil
	}
	current, err := s.loadObservations(ctx)
	if err != nil {
		return Result{}, err
	}

	intentID, err := s.beginIntent(ctx, request, currentVersion)
	if err != nil {
		return Result{}, err
	}
	if s.afterIntent != nil {
		// Deliberate crash point for recovery verification. The callback is
		// supplied only by the executable's test flag/environment.
		s.afterIntent()
	}

	mutation, mutationErr := mutate(current, currentVersion)
	if mutationErr != nil {
		result := failureResult(mutationErr, currentVersion)
		if err := s.finishFailure(ctx, intentID, request, result); err != nil {
			return Result{}, err
		}
		return result, nil
	}
	nextVersion := currentVersion + 1
	if mutation.Response == nil {
		mutation.Response = map[string]any{}
	}
	mutation.Response["version"] = nextVersion
	body, err := json.Marshal(mutation.Response)
	if err != nil {
		internal := apperr.Wrap(apperr.CodeInternal, "cannot encode success response", map[string]any{"batch_id": request.BatchID}, err)
		result := failureResult(internal, currentVersion)
		if finishErr := s.finishFailure(ctx, intentID, request, result); finishErr != nil {
			return Result{}, finishErr
		}
		return result, nil
	}

	if err := s.commitMutation(ctx, intentID, request, nextVersion, mutation, body); err != nil {
		appErr := apperr.As(err)
		result := failureResult(appErr, currentVersion)
		if finishErr := s.finishFailure(ctx, intentID, request, result); finishErr != nil {
			return Result{}, finishErr
		}
		return result, nil
	}
	return Result{Success: true, Version: nextVersion, HTTPStatus: 200, Body: body}, nil
}

func (s *Store) beginIntent(ctx context.Context, request WriteRequest, currentVersion int64) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin intent transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := s.now()
	if request.BatchID != "" {
		_, err = tx.ExecContext(ctx, `INSERT INTO batches
			(batch_id, request_hash, operation, status, http_status, response_json, version, created_ms)
			VALUES (?, ?, ?, 'PENDING', 0, '', ?, ?)`,
			request.BatchID, request.RequestHash, request.Operation, currentVersion, now)
		if err != nil {
			return 0, apperr.Wrap(apperr.CodeInternal, "cannot reserve batch id", map[string]any{"batch_id": request.BatchID}, err)
		}
	}
	detail, _ := json.Marshal(map[string]any{"batch_id": request.BatchID, "operation": request.Operation})
	result, err := tx.ExecContext(ctx, `INSERT INTO intents
		(batch_id, operation, request_hash, base_version, status, detail_json, created_ms)
		VALUES (?, ?, ?, ?, 'PENDING', ?, ?)`,
		request.BatchID, request.Operation, request.RequestHash, currentVersion, string(detail), now)
	if err != nil {
		return 0, apperr.Wrap(apperr.CodeInternal, "cannot write intent", map[string]any{"batch_id": request.BatchID}, err)
	}
	intentID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("read intent id: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit intent: %w", err)
	}
	return intentID, nil
}

func (s *Store) commitMutation(ctx context.Context, intentID int64, request WriteRequest, nextVersion int64, mutation Mutation, body []byte) error {
	if err := validateObservationSet(mutation.Observations); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin write transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := replaceObservations(ctx, tx, mutation.Observations); err != nil {
		return err
	}
	if err := persistReport(ctx, tx, mutation.Report); err != nil {
		return err
	}
	snapshot := SnapshotData{Observations: mutation.Observations, Report: mutation.Report}
	snapshotJSON, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode version snapshot: %w", err)
	}
	hash := sha256.Sum256(snapshotJSON)
	now := s.now()
	if err := setVersion(ctx, tx, nextVersion); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO version_snapshots(version, snapshot_json, created_ms) VALUES (?, ?, ?)`,
		nextVersion, string(snapshotJSON), now); err != nil {
		return fmt.Errorf("write version snapshot: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO commits
		(version, operation, batch_id, observation_count, snapshot_hash, created_ms) VALUES (?, ?, ?, ?, ?, ?)`,
		nextVersion, request.Operation, request.BatchID, len(mutation.Observations), hex.EncodeToString(hash[:]), now); err != nil {
		return fmt.Errorf("write commit record: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='COMPLETED', target_version=?, finished_ms=?, detail_json=?
		WHERE id=?`, nextVersion, now, `{"outcome":"committed"}`, intentID); err != nil {
		return fmt.Errorf("complete intent: %w", err)
	}
	if request.BatchID != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE batches SET status='COMPLETE', http_status=200,
			response_json=?, version=?, finished_ms=? WHERE batch_id=?`,
			string(body), nextVersion, now, request.BatchID); err != nil {
			return fmt.Errorf("complete batch: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit observation transaction: %w", err)
	}
	return nil
}

func (s *Store) finishFailure(ctx context.Context, intentID int64, request WriteRequest, result Result) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin failure transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := s.now()
	detail, _ := json.Marshal(map[string]any{"code": result.Code, "http_status": result.HTTPStatus})
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='ABORTED', finished_ms=?, detail_json=? WHERE id=?`,
		now, string(detail), intentID); err != nil {
		return fmt.Errorf("abort intent: %w", err)
	}
	if request.BatchID != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE batches SET status='FAILED', http_status=?, response_json=?, finished_ms=? WHERE batch_id=?`,
			result.HTTPStatus, string(result.Body), now, request.BatchID); err != nil {
			return fmt.Errorf("record failed batch: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failure record: %w", err)
	}
	return nil
}

func (s *Store) storeImmediateFailure(ctx context.Context, request WriteRequest, result Result) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	now := s.now()
	_, err = tx.ExecContext(ctx, `INSERT INTO batches
		(batch_id, request_hash, operation, status, http_status, response_json, version, created_ms, finished_ms)
		VALUES (?, ?, ?, 'FAILED', ?, ?, ?, ?, ?)
		ON CONFLICT(batch_id) DO NOTHING`,
		request.BatchID, request.RequestHash, request.Operation, result.HTTPStatus, string(result.Body), result.Version, now, now)
	if err != nil {
		return fmt.Errorf("store immediate failure: %w", err)
	}
	return tx.Commit()
}

func failureResult(err error, currentVersion int64) Result {
	appErr := apperr.As(err)
	body, _ := json.Marshal(appErr.Public())
	return Result{
		Success:    false,
		Version:    currentVersion,
		HTTPStatus: apperr.HTTPStatus(appErr.Code),
		Body:       append(body, '\n'),
		Code:       appErr.Code,
	}
}

func (s *Store) cachedBatch(ctx context.Context, request WriteRequest) (Result, bool, error) {
	var hash, status, body string
	var httpStatus int
	var version int64
	err := s.db.QueryRowContext(ctx, `SELECT request_hash, status, http_status, response_json, version FROM batches WHERE batch_id=?`, request.BatchID).
		Scan(&hash, &status, &httpStatus, &body, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return Result{}, false, nil
	}
	if err != nil {
		return Result{}, false, fmt.Errorf("read batch: %w", err)
	}
	if hash != request.RequestHash {
		result := failureResult(apperr.New(apperr.CodeConflictingBatch, "batch id was already used with different content", map[string]any{
			"batch_id": request.BatchID,
		}), version)
		return result, true, nil
	}
	switch status {
	case "COMPLETE":
		return Result{Success: true, Cached: true, Version: version, HTTPStatus: httpStatus, Body: []byte(body)}, true, nil
	case "FAILED", "ABORTED":
		code := ""
		var envelope struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal([]byte(body), &envelope)
		code = envelope.Code
		return Result{Success: false, Cached: true, Version: version, HTTPStatus: httpStatus, Body: []byte(body), Code: code}, true, nil
	case "PENDING":
		// Within one process the write mutex guarantees this branch is only
		// reachable from another process still working on the same batch.
		return Result{}, false, apperr.New(apperr.CodeConflictingBatch, "batch id is currently being processed", map[string]any{
			"batch_id": request.BatchID, "status": status,
		})
	default:
		return Result{}, false, apperr.New(apperr.CodeInternal, "batch has unknown status", map[string]any{"batch_id": request.BatchID, "status": status})
	}
}

func validateObservationSet(observations []domain.Observation) error {
	seen := map[string]bool{}
	for _, observation := range observations {
		if err := observation.Validate(); err != nil {
			return err
		}
		key := observation.ReplicaID + "\x00" + strconv.FormatInt(observation.SampledAtMS, 10)
		if seen[key] {
			return apperr.New(apperr.CodeObservationConflict, "duplicate replica and sample time in observation set", map[string]any{
				"replica_id": observation.ReplicaID, "sampled_at_ms": observation.SampledAtMS,
			})
		}
		seen[key] = true
	}
	return nil
}

func (s *Store) Version(ctx context.Context) (int64, error) {
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='version'`).Scan(&raw); err != nil {
		return 0, fmt.Errorf("read store version: %w", err)
	}
	version, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse store version: %w", err)
	}
	return version, nil
}

func setVersion(ctx context.Context, tx *sql.Tx, version int64) error {
	if _, err := tx.ExecContext(ctx, `UPDATE meta SET value=? WHERE key='version'`, strconv.FormatInt(version, 10)); err != nil {
		return fmt.Errorf("set store version: %w", err)
	}
	return nil
}
