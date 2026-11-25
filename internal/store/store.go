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
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"leasearbiter/internal/core"
)

const (
	IntentPrepared  = "PREPARED"
	IntentCommitted = "COMMITTED"
	IntentAborted   = "ABORTED"
)

type Intent struct {
	ID            string `json:"id"`
	Kind          string `json:"kind"`
	BaseVersion   int64  `json:"base_version"`
	TargetVersion int64  `json:"target_version"`
	Payload       string `json:"payload"`
	State         string `json:"state"`
	CreatedAtMS   int64  `json:"created_at_ms"`
	CompletedAtMS int64  `json:"completed_at_ms,omitempty"`
	Error         string `json:"error,omitempty"`
}

type VersionDiff struct {
	BaseVersion          int64    `json:"base_version"`
	LastCommittedVersion int64    `json:"last_committed_version"`
	IntendedTarget       int64    `json:"intended_target"`
	Changed              bool     `json:"changed"`
	Details              []string `json:"details"`
}

type RecoveredIntent struct {
	Intent
	Outcome     string      `json:"outcome"`
	VersionDiff VersionDiff `json:"version_diff"`
}

type RecoveryReport struct {
	RecoveredAtMS        int64             `json:"recovered_at_ms"`
	LastCommittedVersion int64             `json:"last_committed_version"`
	PreparedIntents      []RecoveredIntent `json:"prepared_intents"`
	VersionDiff          VersionDiff       `json:"version_diff"`
	Facts                []string          `json:"facts"`
}

type Store struct {
	db       *sql.DB
	lockFile *os.File
	writeMu  sync.Mutex
	now      func() time.Time
}

func Open(path string) (*Store, error) {
	lockFile, err := acquireProcessLock(path)
	if err != nil {
		return nil, err
	}
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		releaseProcessLock(lockFile)
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)
	store := &Store{db: db, lockFile: lockFile, now: time.Now}
	if err := store.migrate(context.Background()); err != nil {
		_ = db.Close()
		releaseProcessLock(lockFile)
		return nil, err
	}
	return store, nil
}

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS committed_states (
			version INTEGER PRIMARY KEY,
			snapshot_version INTEGER NOT NULL,
			payload BLOB NOT NULL,
			created_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			base_version INTEGER NOT NULL,
			target_version INTEGER NOT NULL,
			payload TEXT NOT NULL,
			state TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL,
			completed_at_ms INTEGER,
			error TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_state ON intents(state)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_target ON intents(target_version)`,
		`CREATE TABLE IF NOT EXISTS recovery_reports (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			recovered_at_ms INTEGER NOT NULL,
			payload BLOB NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM committed_states`).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		state := core.NewState()
		payload, _ := json.Marshal(state)
		if _, err := s.db.ExecContext(ctx, `INSERT INTO committed_states(version, snapshot_version, payload, created_at_ms) VALUES(0, 0, ?, ?)`, payload, s.now().UnixMilli()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	err := s.db.Close()
	releaseProcessLock(s.lockFile)
	s.lockFile = nil
	return err
}

func (s *Store) CurrentState(ctx context.Context) (core.State, error) {
	var version int64
	var payload []byte
	err := s.db.QueryRowContext(ctx, `SELECT version, payload FROM committed_states ORDER BY version DESC LIMIT 1`).Scan(&version, &payload)
	if err != nil {
		return core.State{}, err
	}
	return decodeState(version, payload)
}

func (s *Store) StateAtVersion(ctx context.Context, version int64) (core.State, error) {
	var payload []byte
	err := s.db.QueryRowContext(ctx, `SELECT payload FROM committed_states WHERE version = ?`, version).Scan(&payload)
	if err != nil {
		return core.State{}, err
	}
	return decodeState(version, payload)
}

func (s *Store) BeginIntent(ctx context.Context, kind string, payload any) (Intent, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	current, err := s.currentStateLocked(ctx)
	if err != nil {
		return Intent{}, err
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return Intent{}, err
	}
	intent := Intent{
		ID:            newIntentID(),
		Kind:          kind,
		BaseVersion:   current.Version,
		TargetVersion: current.Version + 1,
		Payload:       string(payloadBytes),
		State:         IntentPrepared,
		CreatedAtMS:   s.now().UnixMilli(),
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO intents(id, kind, base_version, target_version, payload, state, created_at_ms) VALUES(?, ?, ?, ?, ?, ?, ?)`,
		intent.ID, intent.Kind, intent.BaseVersion, intent.TargetVersion, intent.Payload, intent.State, intent.CreatedAtMS)
	if err != nil {
		return Intent{}, err
	}
	return intent, nil
}

func (s *Store) CommitIntent(ctx context.Context, intentID string, state core.State) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	intent, err := loadIntentTx(ctx, tx, intentID)
	if err != nil {
		return err
	}
	if intent.State != IntentPrepared {
		return fmt.Errorf("intent %s is %s, not PREPARED", intentID, intent.State)
	}
	if state.Version != intent.TargetVersion {
		return fmt.Errorf("state version %d does not match intent target %d", state.Version, intent.TargetVersion)
	}
	var current int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM committed_states`).Scan(&current); err != nil {
		return err
	}
	if current != intent.BaseVersion {
		return fmt.Errorf("current version %d does not match intent base %d", current, intent.BaseVersion)
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO committed_states(version, snapshot_version, payload, created_at_ms) VALUES(?, ?, ?, ?)`,
		state.Version, state.SnapshotVersion, payload, s.now().UnixMilli()); err != nil {
		return err
	}
	completed := s.now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET state = ?, completed_at_ms = ? WHERE id = ?`, IntentCommitted, completed, intentID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) AbortIntent(ctx context.Context, intentID, reason string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET state = ?, completed_at_ms = ?, error = ? WHERE id = ? AND state = ?`,
		IntentAborted, s.now().UnixMilli(), reason, intentID, IntentPrepared)
	return err
}

// UpdateIntentPayload persists the fully materialized action plan after the
// intent row itself is durable and before any external heal is dispatched.
func (s *Store) UpdateIntentPayload(ctx context.Context, intentID string, payload any) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE intents SET payload = ? WHERE id = ? AND state = ?`, string(payloadBytes), intentID, IntentPrepared)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("intent %s is not PREPARED", intentID)
	}
	return nil
}

func (s *Store) Intent(ctx context.Context, id string) (Intent, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, kind, base_version, target_version, payload, state, created_at_ms, COALESCE(completed_at_ms,0), COALESCE(error,'') FROM intents WHERE id = ?`, id)
	return scanIntent(row)
}

func (s *Store) Intents(ctx context.Context) ([]Intent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, kind, base_version, target_version, payload, state, created_at_ms, COALESCE(completed_at_ms,0), COALESCE(error,'') FROM intents ORDER BY created_at_ms, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var intents []Intent
	for rows.Next() {
		intent, err := scanIntent(rows)
		if err != nil {
			return nil, err
		}
		intents = append(intents, intent)
	}
	return intents, rows.Err()
}

// Recover marks every durable PREPARED intent ABORTED and leaves the last
// complete committed row as the authoritative state. The report is derived
// only from rows that survived the forced termination.
func (s *Store) Recover(ctx context.Context) (RecoveryReport, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RecoveryReport{}, err
	}
	defer tx.Rollback()
	var lastCommitted int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM committed_states`).Scan(&lastCommitted); err != nil {
		return RecoveryReport{}, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id, kind, base_version, target_version, payload, state, created_at_ms, COALESCE(completed_at_ms,0), COALESCE(error,'') FROM intents WHERE state = ? ORDER BY created_at_ms, id`, IntentPrepared)
	if err != nil {
		return RecoveryReport{}, err
	}
	var prepared []Intent
	for rows.Next() {
		intent, err := scanIntent(rows)
		if err != nil {
			rows.Close()
			return RecoveryReport{}, err
		}
		prepared = append(prepared, intent)
	}
	if err := rows.Close(); err != nil {
		return RecoveryReport{}, err
	}
	now := s.now().UnixMilli()
	report := RecoveryReport{
		RecoveredAtMS:        now,
		LastCommittedVersion: lastCommitted,
		Facts: []string{
			fmt.Sprintf("last complete committed version: %d", lastCommitted),
			fmt.Sprintf("durable PREPARED intents discovered: %d", len(prepared)),
		},
	}
	for _, intent := range prepared {
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET state = ?, completed_at_ms = ?, error = ? WHERE id = ? AND state = ?`,
			IntentAborted, now, "process terminated before commit; state retained at last complete version", intent.ID, IntentPrepared); err != nil {
			return RecoveryReport{}, err
		}
		intent.State = IntentAborted
		intent.CompletedAtMS = now
		intent.Error = "process terminated before commit; state retained at last complete version"
		diff := VersionDiff{
			BaseVersion:          intent.BaseVersion,
			LastCommittedVersion: lastCommitted,
			IntendedTarget:       intent.TargetVersion,
			Details: []string{
				fmt.Sprintf("intent %s (%s) targeted version %d", intent.ID, intent.Kind, intent.TargetVersion),
				fmt.Sprintf("committed version remained %d", lastCommitted),
				"intent payload retained on disk for postmortem review",
			},
		}
		diff.Changed = lastCommitted != intent.BaseVersion
		report.PreparedIntents = append(report.PreparedIntents, RecoveredIntent{Intent: intent, Outcome: "ABORTED", VersionDiff: diff})
		report.VersionDiff = diff
	}
	if len(prepared) == 0 {
		report.VersionDiff = VersionDiff{BaseVersion: lastCommitted, LastCommittedVersion: lastCommitted, Details: []string{"no prepared intent required rollback"}}
	}
	payload, err := json.Marshal(report)
	if err != nil {
		return RecoveryReport{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_reports(recovered_at_ms, payload) VALUES(?, ?)`, now, payload); err != nil {
		return RecoveryReport{}, err
	}
	if err := tx.Commit(); err != nil {
		return RecoveryReport{}, err
	}
	return report, nil
}

func (s *Store) currentStateLocked(ctx context.Context) (core.State, error) {
	var version int64
	var payload []byte
	if err := s.db.QueryRowContext(ctx, `SELECT version, payload FROM committed_states ORDER BY version DESC LIMIT 1`).Scan(&version, &payload); err != nil {
		return core.State{}, err
	}
	return decodeState(version, payload)
}

func decodeState(expectedVersion int64, payload []byte) (core.State, error) {
	var state core.State
	if err := json.Unmarshal(payload, &state); err != nil {
		return core.State{}, err
	}
	if state.Version != expectedVersion {
		return core.State{}, fmt.Errorf("state payload version %d does not match row %d", state.Version, expectedVersion)
	}
	if state.Results == nil {
		state.Results = make(map[string]core.RebuildResult)
	}
	if state.Heals == nil {
		state.Heals = make(map[string]core.HealExecution)
	}
	return state, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanIntent(row scanner) (Intent, error) {
	var intent Intent
	err := row.Scan(&intent.ID, &intent.Kind, &intent.BaseVersion, &intent.TargetVersion, &intent.Payload, &intent.State, &intent.CreatedAtMS, &intent.CompletedAtMS, &intent.Error)
	return intent, err
}

func loadIntentTx(ctx context.Context, tx *sql.Tx, id string) (Intent, error) {
	row := tx.QueryRowContext(ctx, `SELECT id, kind, base_version, target_version, payload, state, created_at_ms, COALESCE(completed_at_ms,0), COALESCE(error,'') FROM intents WHERE id = ?`, id)
	return scanIntent(row)
}

func newIntentID() string {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("intent-%d", time.Now().UnixNano())
	}
	return "intent-" + hex.EncodeToString(value[:])
}

func IsNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

func acquireProcessLock(path string) (*os.File, error) {
	if path == "" || path == ":memory:" {
		return nil, nil
	}
	file, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open writer lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("database %s already has an active writer", path)
		}
		return nil, fmt.Errorf("acquire writer lock: %w", err)
	}
	return file, nil
}

func releaseProcessLock(file *os.File) {
	if file == nil {
		return
	}
	_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	_ = file.Close()
}
