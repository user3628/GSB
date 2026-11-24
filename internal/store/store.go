package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"local/sqlitepool/internal/domain"

	_ "modernc.org/sqlite"
)

type Store struct {
	db   *sql.DB
	Path string
}

func Open(ctx context.Context, path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("metadata path is empty")
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// The durable ledger intentionally serializes all reads/writes. Pool
	// operations are already ordered by the single-writer actor.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db, Path: path}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS pool_config (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			max_open INTEGER NOT NULL,
			max_idle INTEGER NOT NULL,
			max_lifetime_ms INTEGER NOT NULL,
			max_idle_time_ms INTEGER NOT NULL,
			acquire_timeout_ms INTEGER NOT NULL,
			max_waiters INTEGER NOT NULL,
			version INTEGER NOT NULL,
			updated_at_ns INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS connections (
			connection_id TEXT PRIMARY KEY,
			state TEXT NOT NULL,
			created_at_ns INTEGER NOT NULL,
			last_used_at_ns INTEGER NOT NULL,
			use_count INTEGER NOT NULL DEFAULT 0,
			close_reason TEXT NOT NULL DEFAULT '',
			replaced_by TEXT NOT NULL DEFAULT '',
			replaced_connection_id TEXT NOT NULL DEFAULT '',
			replacement_reason TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_connections_state ON connections(state)`,
		`CREATE TABLE IF NOT EXISTS intents (
			intent_id INTEGER PRIMARY KEY AUTOINCREMENT,
			kind TEXT NOT NULL,
			connection_id TEXT NOT NULL DEFAULT '',
			request_id TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			created_at_ns INTEGER NOT NULL,
			committed_at_ns INTEGER,
			recovered_at_ns INTEGER
		)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status)`,
		`CREATE TABLE IF NOT EXISTS recovery_reports (
			report_id TEXT PRIMARY KEY,
			created_at_ns INTEGER NOT NULL,
			report_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS injections (
			injection_id TEXT PRIMARY KEY,
			type TEXT NOT NULL,
			params_json TEXT NOT NULL,
			business_code TEXT NOT NULL DEFAULT '',
			observation_json TEXT NOT NULL DEFAULT '{}',
			status TEXT NOT NULL,
			created_at_ns INTEGER NOT NULL,
			completed_at_ns INTEGER
		)`,
		`CREATE INDEX IF NOT EXISTS idx_injections_type ON injections(type)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

func (s *Store) SeedConfig(ctx context.Context, cfg domain.Config) (bool, error) {
	if err := cfg.Validate(); err != nil {
		return false, err
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pool_config`).Scan(&count); err != nil {
		return false, err
	}
	if count > 0 {
		return false, nil
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO pool_config
		(id, max_open, max_idle, max_lifetime_ms, max_idle_time_ms, acquire_timeout_ms, max_waiters, version, updated_at_ns)
		VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)`,
		cfg.MaxOpen, cfg.MaxIdle, cfg.MaxLifetimeMS, cfg.MaxIdleTimeMS,
		cfg.AcquireTimeoutMS, cfg.MaxWaiters, cfg.Version, time.Now().UnixNano())
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) LoadConfig(ctx context.Context) (domain.Config, error) {
	var cfg domain.Config
	err := s.db.QueryRowContext(ctx, `SELECT max_open, max_idle, max_lifetime_ms, max_idle_time_ms,
		acquire_timeout_ms, max_waiters, version FROM pool_config WHERE id = 1`).
		Scan(&cfg.MaxOpen, &cfg.MaxIdle, &cfg.MaxLifetimeMS, &cfg.MaxIdleTimeMS,
			&cfg.AcquireTimeoutMS, &cfg.MaxWaiters, &cfg.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return cfg, fmt.Errorf("pool config is not initialized")
	}
	return cfg, err
}

func (s *Store) BeginConfigIntent(ctx context.Context, requestID string, cfg domain.Config, expectedVersion int64) (int64, error) {
	payload, _ := json.Marshal(map[string]any{
		"expected_version": expectedVersion,
		"new_config":       cfg,
	})
	res, err := s.db.ExecContext(ctx, `INSERT INTO intents
		(kind, connection_id, request_id, status, payload_json, created_at_ns)
		VALUES (?, '', ?, ?, ?, ?)`,
		domain.IntentConfig, requestID, domain.IntentPending, string(payload), time.Now().UnixNano())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// CommitConfig applies the new version and marks the intent committed in one
// transaction. The WHERE clause is a second monotonic-version guard in addition
// to the actor's serialized check.
func (s *Store) CommitConfig(ctx context.Context, intentID int64, cfg domain.Config, expectedVersion int64) (bool, error) {
	if err := cfg.Validate(); err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE pool_config SET
		max_open=?, max_idle=?, max_lifetime_ms=?, max_idle_time_ms=?, acquire_timeout_ms=?,
		max_waiters=?, version=?, updated_at_ns=?
		WHERE id=1 AND version=? AND version < ?`,
		cfg.MaxOpen, cfg.MaxIdle, cfg.MaxLifetimeMS, cfg.MaxIdleTimeMS, cfg.AcquireTimeoutMS,
		cfg.MaxWaiters, cfg.Version, time.Now().UnixNano(), expectedVersion, cfg.Version)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n != 1 {
		if err := tx.Rollback(); err != nil {
			return false, err
		}
		_, _ = s.db.ExecContext(ctx, `UPDATE intents SET status=?, recovered_at_ns=? WHERE intent_id=?`,
			domain.IntentAborted, time.Now().UnixNano(), intentID)
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status=?, committed_at_ns=? WHERE intent_id=?`,
		domain.IntentCommitted, time.Now().UnixNano(), intentID); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) AbortIntent(ctx context.Context, intentID int64, detail string) error {
	payload, _ := json.Marshal(map[string]any{"abort_detail": detail})
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET status=?, payload_json=?, recovered_at_ns=? WHERE intent_id=?`,
		domain.IntentAborted, string(payload), time.Now().UnixNano(), intentID)
	return err
}

func (s *Store) InsertConnection(ctx context.Context, rec domain.ConnectionRecord) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO connections
		(connection_id, state, created_at_ns, last_used_at_ns, use_count, close_reason,
		 replaced_by, replaced_connection_id, replacement_reason)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(connection_id) DO UPDATE SET
			state=excluded.state, last_used_at_ns=excluded.last_used_at_ns,
			close_reason=excluded.close_reason, replaced_by=excluded.replaced_by,
			replaced_connection_id=excluded.replaced_connection_id,
			replacement_reason=excluded.replacement_reason`,
		rec.ConnectionID, rec.State, rec.CreatedAt.UnixNano(), rec.LastUsedAt.UnixNano(),
		rec.UseCount, rec.CloseReason, rec.ReplacedBy, rec.ReplacedConnID, rec.ReplacementReason)
	return err
}

// PrepareAcquire is phase one of an acquisition intent: the pending intent is
// inserted before the durable state flips to in_use, then both become visible in
// one transaction. Phase two calls MarkIntentCommitted. A kill in between leaves
// exactly the facts recovery needs.
func (s *Store) PrepareAcquire(ctx context.Context, connectionID, requestID string) (int64, domain.ConnectionRecord, error) {
	now := time.Now()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, domain.ConnectionRecord{}, err
	}
	defer tx.Rollback()
	payload, _ := json.Marshal(map[string]any{"previous_state": domain.StateAvailable, "request_id": requestID})
	res, err := tx.ExecContext(ctx, `INSERT INTO intents
		(kind, connection_id, request_id, status, payload_json, created_at_ns)
		VALUES (?, ?, ?, ?, ?, ?)`,
		domain.IntentAcquire, connectionID, requestID, domain.IntentPending, string(payload), now.UnixNano())
	if err != nil {
		return 0, domain.ConnectionRecord{}, err
	}
	intentID, err := res.LastInsertId()
	if err != nil {
		return 0, domain.ConnectionRecord{}, err
	}
	res, err = tx.ExecContext(ctx, `UPDATE connections SET state=?, last_used_at_ns=?, use_count=use_count+1
		WHERE connection_id=? AND state=?`,
		domain.StateInUse, now.UnixNano(), connectionID, domain.StateAvailable)
	if err != nil {
		return 0, domain.ConnectionRecord{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, domain.ConnectionRecord{}, err
	}
	if n != 1 {
		return 0, domain.ConnectionRecord{}, fmt.Errorf("connection %s is not available", connectionID)
	}
	rec, err := queryConnection(ctx, tx, connectionID)
	if err != nil {
		return 0, domain.ConnectionRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return 0, domain.ConnectionRecord{}, err
	}
	return intentID, rec, nil
}

func (s *Store) PrepareRelease(ctx context.Context, connectionID, requestID, targetState, reason string) (int64, error) {
	if targetState != domain.StateAvailable && targetState != domain.StateClosed && targetState != domain.StateInvalid {
		return 0, fmt.Errorf("invalid release target state %q", targetState)
	}
	now := time.Now()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	before, err := queryConnection(ctx, tx, connectionID)
	if err != nil {
		return 0, err
	}
	payload, _ := json.Marshal(map[string]any{
		"previous_state": before.State,
		"target_state":   targetState,
		"reason":         reason,
	})
	res, err := tx.ExecContext(ctx, `INSERT INTO intents
		(kind, connection_id, request_id, status, payload_json, created_at_ns)
		VALUES (?, ?, ?, ?, ?, ?)`,
		domain.IntentRelease, connectionID, requestID, domain.IntentPending, string(payload), now.UnixNano())
	if err != nil {
		return 0, err
	}
	intentID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE connections SET state=?, last_used_at_ns=?, close_reason=?
		WHERE connection_id=?`,
		targetState, now.UnixNano(), reason, connectionID)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return intentID, nil
}

func (s *Store) PrepareInvalidate(ctx context.Context, connectionID, requestID, reason string) (int64, error) {
	now := time.Now()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	before, err := queryConnection(ctx, tx, connectionID)
	if err != nil {
		return 0, err
	}
	payload, _ := json.Marshal(map[string]any{
		"previous_state": before.State,
		"reason":         reason,
	})
	res, err := tx.ExecContext(ctx, `INSERT INTO intents
		(kind, connection_id, request_id, status, payload_json, created_at_ns)
		VALUES (?, ?, ?, ?, ?, ?)`,
		domain.IntentInvalidate, connectionID, requestID, domain.IntentPending, string(payload), now.UnixNano())
	if err != nil {
		return 0, err
	}
	intentID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE connections SET state=?, close_reason=?, last_used_at_ns=?
		WHERE connection_id=?`, domain.StateInvalid, reason, now.UnixNano(), connectionID)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return intentID, nil
}

func (s *Store) MarkIntentCommitted(ctx context.Context, intentID int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET status=?, committed_at_ns=? WHERE intent_id=? AND status=?`,
		domain.IntentCommitted, time.Now().UnixNano(), intentID, domain.IntentPending)
	return err
}

func (s *Store) MarkReplacement(ctx context.Context, oldID, newID, reason string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE connections SET replaced_by=?, replacement_reason=? WHERE connection_id=?`,
		newID, reason, oldID)
	return err
}

func (s *Store) MarkConnectionState(ctx context.Context, connectionID, state, reason string) error {
	now := time.Now().UnixNano()
	_, err := s.db.ExecContext(ctx, `UPDATE connections SET state=?, close_reason=?, last_used_at_ns=? WHERE connection_id=?`,
		state, reason, now, connectionID)
	return err
}

func (s *Store) TouchConnection(ctx context.Context, connectionID string, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE connections SET last_used_at_ns=? WHERE connection_id=?`,
		at.UnixNano(), connectionID)
	return err
}

func (s *Store) LoadAvailableConnections(ctx context.Context) ([]domain.ConnectionRecord, error) {
	rows, err := s.db.QueryContext(ctx, connectionSelect+` WHERE state=? ORDER BY last_used_at_ns ASC`, domain.StateAvailable)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanConnections(rows)
}

func (s *Store) LoadAllConnections(ctx context.Context) ([]domain.ConnectionRecord, error) {
	rows, err := s.db.QueryContext(ctx, connectionSelect+` ORDER BY created_at_ns ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanConnections(rows)
}

func (s *Store) GetConnection(ctx context.Context, connectionID string) (domain.ConnectionRecord, error) {
	row := s.db.QueryRowContext(ctx, connectionSelect+` WHERE connection_id=?`, connectionID)
	return scanConnection(row)
}

const connectionSelect = `SELECT connection_id, state, created_at_ns, last_used_at_ns, use_count,
	close_reason, replaced_by, replaced_connection_id, replacement_reason FROM connections`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanConnection(scanner rowScanner) (domain.ConnectionRecord, error) {
	var rec domain.ConnectionRecord
	var created, lastUsed int64
	err := scanner.Scan(&rec.ConnectionID, &rec.State, &created, &lastUsed, &rec.UseCount,
		&rec.CloseReason, &rec.ReplacedBy, &rec.ReplacedConnID, &rec.ReplacementReason)
	if err != nil {
		return rec, err
	}
	rec.CreatedAt = time.Unix(0, created)
	rec.LastUsedAt = time.Unix(0, lastUsed)
	return rec, nil
}

func scanConnections(rows *sql.Rows) ([]domain.ConnectionRecord, error) {
	defer rows.Close()
	var out []domain.ConnectionRecord
	for rows.Next() {
		rec, err := scanConnection(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func queryConnection(ctx context.Context, tx *sql.Tx, connectionID string) (domain.ConnectionRecord, error) {
	return scanConnection(tx.QueryRowContext(ctx, connectionSelect+` WHERE connection_id=?`, connectionID))
}

func NewInjection(ctx context.Context, s *Store, injectionType string, params map[string]any) (domain.InjectionRecord, error) {
	now := time.Now()
	id := newID("inj")
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return domain.InjectionRecord{}, err
	}
	rec := domain.InjectionRecord{
		ID:        id,
		Type:      injectionType,
		Params:    params,
		Status:    "running",
		CreatedAt: now,
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO injections
		(injection_id, type, params_json, business_code, observation_json, status, created_at_ns)
		VALUES (?, ?, ?, '', '{}', ?, ?)`, id, injectionType, string(paramsJSON), rec.Status, now.UnixNano())
	if err != nil {
		return domain.InjectionRecord{}, err
	}
	return rec, nil
}

func (s *Store) CompleteInjection(ctx context.Context, rec domain.InjectionRecord) error {
	now := time.Now()
	observationJSON, err := json.Marshal(rec.Observation)
	if err != nil {
		return err
	}
	rec.CompletedAt = &now
	_, err = s.db.ExecContext(ctx, `UPDATE injections SET business_code=?, observation_json=?, status=?, completed_at_ns=?
		WHERE injection_id=?`, rec.BusinessCode, string(observationJSON), rec.Status, now.UnixNano(), rec.ID)
	return err
}

func (s *Store) GetInjection(ctx context.Context, id string) (domain.InjectionRecord, error) {
	var rec domain.InjectionRecord
	var paramsJSON, observationJSON string
	var created, completed sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT injection_id, type, params_json, business_code, observation_json,
		status, created_at_ns, completed_at_ns FROM injections WHERE injection_id=?`, id).
		Scan(&rec.ID, &rec.Type, &paramsJSON, &rec.BusinessCode, &observationJSON, &rec.Status, &created, &completed)
	if err != nil {
		return rec, err
	}
	_ = json.Unmarshal([]byte(paramsJSON), &rec.Params)
	_ = json.Unmarshal([]byte(observationJSON), &rec.Observation)
	rec.CreatedAt = time.Unix(0, created.Int64)
	if completed.Valid {
		t := time.Unix(0, completed.Int64)
		rec.CompletedAt = &t
	}
	return rec, nil
}

func (s *Store) ListInjections(ctx context.Context, limit int) ([]domain.InjectionRecord, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT injection_id FROM injections ORDER BY created_at_ns DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]domain.InjectionRecord, 0, len(ids))
	for _, id := range ids {
		rec, err := s.GetInjection(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

func newID(prefix string) string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%d-%s", prefix, time.Now().UnixNano(), hex.EncodeToString(buf))
}
