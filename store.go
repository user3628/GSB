package prefixcache

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS cache_meta (
    key TEXT PRIMARY KEY,
    current_version INTEGER NOT NULL,
    current_policy_digest TEXT NOT NULL,
    allocation_sequence INTEGER NOT NULL
);
INSERT OR IGNORE INTO cache_meta(key, current_version, current_policy_digest, allocation_sequence)
VALUES('head', 0, '', 0);

CREATE TABLE IF NOT EXISTS policy_versions (
    version INTEGER PRIMARY KEY,
    policy_json TEXT NOT NULL,
    policy_digest TEXT NOT NULL,
    parent_version INTEGER NOT NULL,
    parent_digest TEXT NOT NULL,
    stage_normalization TEXT NOT NULL,
    stage_fingerprint TEXT NOT NULL,
    stage_slotting TEXT NOT NULL,
    stage_hit TEXT NOT NULL,
    version_fingerprint TEXT NOT NULL,
    slot_digest TEXT NOT NULL,
    allocation_sequence INTEGER NOT NULL,
    kind TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS requests (
    id TEXT PRIMARY KEY,
    template_id TEXT NOT NULL,
    body_json TEXT NOT NULL,
    updated_version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS slots (
    version INTEGER NOT NULL,
    slot_no INTEGER NOT NULL,
    prefix_key TEXT NOT NULL,
    prefix_chars INTEGER NOT NULL,
    block_count INTEGER NOT NULL,
    occupied_at INTEGER NOT NULL,
    reused_from INTEGER NOT NULL,
    owners_json TEXT NOT NULL,
    fingerprints_json TEXT NOT NULL,
    PRIMARY KEY(version, slot_no),
    UNIQUE(version, prefix_key)
);

CREATE TABLE IF NOT EXISTS request_states (
    version INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    state_json TEXT NOT NULL,
    PRIMARY KEY(version, request_id)
);

CREATE TABLE IF NOT EXISTS invalidations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL,
    of_version INTEGER NOT NULL,
    field TEXT NOT NULL,
    stage TEXT NOT NULL,
    request_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    old_value TEXT NOT NULL,
    new_value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS evictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    slot_no INTEGER NOT NULL,
    prefix_key TEXT NOT NULL,
    prefix_chars INTEGER NOT NULL,
    reason TEXT NOT NULL,
    winner_prefix_key TEXT NOT NULL,
    winner_request_id TEXT NOT NULL,
    similarity REAL NOT NULL,
    threshold REAL NOT NULL,
    threshold_margin REAL NOT NULL,
    occupied_at INTEGER NOT NULL,
    winner_occupied_at INTEGER NOT NULL,
    note TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_invalidations_version ON invalidations(version, request_id);
CREATE INDEX IF NOT EXISTS idx_evictions_version ON evictions(version, request_id);
PRAGMA user_version = 1;
`

// Option configures an Engine.
type Option func(*engineOptions)

type engineOptions struct {
	busyTimeout time.Duration
	lockTimeout time.Duration
	now         func() time.Time
}

// WithBusyTimeout sets SQLite's busy timeout.
func WithBusyTimeout(timeout time.Duration) Option {
	return func(options *engineOptions) { options.busyTimeout = timeout }
}

// WithLockTimeout bounds acquisition of the cross-process file lock.
func WithLockTimeout(timeout time.Duration) Option {
	return func(options *engineOptions) { options.lockTimeout = timeout }
}

// Engine is a SQLite-backed prompt prefix cache. It is safe for concurrent use.
type Engine struct {
	db       *sql.DB
	dbPath   string
	lockPath string
	options  engineOptions

	writeMu        sync.Mutex
	beforeCommit   func(commitInfo) error
	beforeCommitMu sync.Mutex
}

type commitInfo struct {
	Version int64
	Kind    string
}

type headState struct {
	Version            int64
	PolicyDigest       string
	AllocationSequence int64
}

// Open opens or creates a cache database and installs the schema.
func Open(ctx context.Context, dbPath string, options ...Option) (*Engine, error) {
	if strings.TrimSpace(dbPath) == "" {
		return nil, newError(CodeInternal, "database path must not be empty")
	}
	absolute, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}

	optionsState := engineOptions{
		busyTimeout: 5 * time.Second,
		lockTimeout: 10 * time.Second,
		now:         time.Now,
	}
	for _, option := range options {
		option(&optionsState)
	}

	fileURL := &url.URL{Scheme: "file", Path: filepath.ToSlash(absolute)}
	dsn := fileURL.String() + "?_txlock=immediate" +
		"&_pragma=busy_timeout(" + strconv.FormatInt(optionsState.busyTimeout.Milliseconds(), 10) + ")" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(FULL)" +
		"&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(0)

	engine := &Engine{
		db:       db,
		dbPath:   absolute,
		lockPath: absolute + ".lock",
		options:  optionsState,
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("install schema: %w", err)
	}
	return engine, nil
}

// Close closes the SQLite pool.
func (e *Engine) Close() error {
	if e == nil || e.db == nil {
		return nil
	}
	return e.db.Close()
}

// DatabasePath returns the absolute SQLite path.
func (e *Engine) DatabasePath() string { return e.dbPath }

func (e *Engine) acquireWriteLock(ctx context.Context) (func() error, error) {
	lockContext := ctx
	cancel := func() {}
	if _, ok := ctx.Deadline(); !ok {
		lockContext, cancel = context.WithTimeout(ctx, e.options.lockTimeout)
	}
	unlock, err := lockFile(lockContext, e.lockPath)
	if err != nil {
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &Error{Code: CodeLockTimeout, Message: "timed out acquiring the cache database file lock", Cause: err}
		}
		return nil, err
	}
	return func() error {
		cancel()
		return unlock()
	}, nil
}

func (e *Engine) withWriteTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	unlock, err := e.acquireWriteLock(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = unlock() }()

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin immediate transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	committed = true
	return nil
}

func (e *Engine) withReadTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	return fn(tx)
}

func loadHead(tx *sql.Tx) (headState, error) {
	var head headState
	err := tx.QueryRow(
		`SELECT current_version, current_policy_digest, allocation_sequence FROM cache_meta WHERE key='head'`,
	).Scan(&head.Version, &head.PolicyDigest, &head.AllocationSequence)
	if err != nil {
		return headState{}, fmt.Errorf("load cache head: %w", err)
	}
	return head, nil
}

type policyRow struct {
	Version            int64
	Policy             Policy
	PolicyDigest       string
	ParentVersion      int64
	ParentDigest       string
	StageDigests       StageDigests
	SlotDigest         string
	AllocationSequence int64
	Kind               string
	CreatedAt          time.Time
}

func loadPolicyRow(tx *sql.Tx, version int64) (policyRow, error) {
	var row policyRow
	var policyJSON, createdAt string
	err := tx.QueryRow(
		`SELECT version, policy_json, policy_digest, parent_version, parent_digest,
		        stage_normalization, stage_fingerprint, stage_slotting, stage_hit,
		        version_fingerprint, slot_digest, allocation_sequence, kind, created_at
		   FROM policy_versions WHERE version=?`, version,
	).Scan(
		&row.Version, &policyJSON, &row.PolicyDigest, &row.ParentVersion, &row.ParentDigest,
		&row.StageDigests.Normalization, &row.StageDigests.Fingerprint, &row.StageDigests.Slotting, &row.StageDigests.Hit,
		&row.StageDigests.Version, &row.SlotDigest, &row.AllocationSequence, &row.Kind, &createdAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return policyRow{}, &Error{Code: CodeNotFound, Message: fmt.Sprintf("version %d does not exist", version)}
	}
	if err != nil {
		return policyRow{}, fmt.Errorf("load policy version %d: %w", version, err)
	}
	if err := json.Unmarshal([]byte(policyJSON), &row.Policy); err != nil {
		return policyRow{}, fmt.Errorf("decode policy version %d: %w", version, err)
	}
	parsed, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return policyRow{}, fmt.Errorf("parse version timestamp: %w", err)
	}
	row.CreatedAt = parsed
	return row, nil
}

func loadRequests(tx *sql.Tx) (map[string]Request, error) {
	rows, err := tx.Query(`SELECT id, body_json FROM requests ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("query requests: %w", err)
	}
	defer rows.Close()
	requests := make(map[string]Request)
	for rows.Next() {
		var id, body string
		if err := rows.Scan(&id, &body); err != nil {
			return nil, err
		}
		var request Request
		if err := json.Unmarshal([]byte(body), &request); err != nil {
			return nil, fmt.Errorf("decode request %s: %w", id, err)
		}
		requests[id] = request
	}
	return requests, rows.Err()
}

func loadSlots(tx *sql.Tx, version int64) ([]Slot, error) {
	rows, err := tx.Query(
		`SELECT version, slot_no, prefix_key, prefix_chars, block_count, occupied_at,
		        reused_from, owners_json, fingerprints_json
		   FROM slots WHERE version=? ORDER BY slot_no`, version,
	)
	if err != nil {
		return nil, fmt.Errorf("query slots: %w", err)
	}
	defer rows.Close()
	slots := make([]Slot, 0)
	for rows.Next() {
		var slot Slot
		var ownersJSON, fingerprintsJSON string
		if err := rows.Scan(
			&slot.Version, &slot.SlotNo, &slot.PrefixKey, &slot.PrefixChars, &slot.BlockCount,
			&slot.OccupiedAt, &slot.ReusedFrom, &ownersJSON, &fingerprintsJSON,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(ownersJSON), &slot.Owners); err != nil {
			return nil, fmt.Errorf("decode slot owners: %w", err)
		}
		if err := json.Unmarshal([]byte(fingerprintsJSON), &slot.BlockFingerprints); err != nil {
			return nil, fmt.Errorf("decode slot fingerprints: %w", err)
		}
		slots = append(slots, slot)
	}
	return slots, rows.Err()
}

func loadRequest(tx *sql.Tx, requestID string) (Request, error) {
	var body string
	err := tx.QueryRow(`SELECT body_json FROM requests WHERE id=?`, requestID).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return Request{}, &Error{Code: CodeNotFound, Message: fmt.Sprintf("request %s does not exist", requestID), RequestID: requestID}
	}
	if err != nil {
		return Request{}, fmt.Errorf("load request %s: %w", requestID, err)
	}
	var request Request
	if err := json.Unmarshal([]byte(body), &request); err != nil {
		return Request{}, fmt.Errorf("decode request %s: %w", requestID, err)
	}
	return request, nil
}

func loadStates(tx *sql.Tx, version int64) ([]RequestState, error) {
	rows, err := tx.Query(`SELECT state_json FROM request_states WHERE version=? ORDER BY request_id`, version)
	if err != nil {
		return nil, fmt.Errorf("query request states: %w", err)
	}
	defer rows.Close()
	states := make([]RequestState, 0)
	for rows.Next() {
		var body string
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var state RequestState
		if err := json.Unmarshal([]byte(body), &state); err != nil {
			return nil, fmt.Errorf("decode request state: %w", err)
		}
		states = append(states, state)
	}
	return states, rows.Err()
}

func loadInvalidations(tx *sql.Tx, version int64, requestID string) ([]Invalidation, error) {
	query := `SELECT version, of_version, field, stage, request_id, reason, old_value, new_value
	            FROM invalidations WHERE version=?`
	args := []any{version}
	if requestID != "" {
		query += ` AND request_id=?`
		args = append(args, requestID)
	}
	query += ` ORDER BY id`
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query invalidations: %w", err)
	}
	defer rows.Close()
	out := make([]Invalidation, 0)
	for rows.Next() {
		var invalidation Invalidation
		if err := rows.Scan(
			&invalidation.Version, &invalidation.OfVersion, &invalidation.Field, &invalidation.Stage,
			&invalidation.RequestID, &invalidation.Reason, &invalidation.OldValue, &invalidation.NewValue,
		); err != nil {
			return nil, err
		}
		out = append(out, invalidation)
	}
	return out, rows.Err()
}

func loadEvictions(tx *sql.Tx, version int64, requestID string) ([]Eviction, error) {
	query := `SELECT version, request_id, slot_no, prefix_key, prefix_chars, reason,
	                 winner_prefix_key, winner_request_id, similarity, threshold,
	                 threshold_margin, occupied_at, winner_occupied_at, note
	            FROM evictions WHERE version=?`
	args := []any{version}
	if requestID != "" {
		query += ` AND request_id=?`
		args = append(args, requestID)
	}
	query += ` ORDER BY id`
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query evictions: %w", err)
	}
	defer rows.Close()
	out := make([]Eviction, 0)
	for rows.Next() {
		var eviction Eviction
		if err := rows.Scan(
			&eviction.Version, &eviction.RequestID, &eviction.SlotNo, &eviction.PrefixKey, &eviction.PrefixChars,
			&eviction.Reason, &eviction.WinnerPrefixKey, &eviction.WinnerRequestID, &eviction.Similarity,
			&eviction.Threshold, &eviction.ThresholdMargin, &eviction.OccupiedAt, &eviction.WinnerOccupiedAt, &eviction.Note,
		); err != nil {
			return nil, err
		}
		out = append(out, eviction)
	}
	return out, rows.Err()
}
