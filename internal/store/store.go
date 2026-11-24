package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	_ "modernc.org/sqlite"

	"keylife/internal/model"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS subjects (
  id TEXT PRIMARY KEY,
  min_strength_bits INTEGER NOT NULL,
  rotation_interval_ticks INTEGER NOT NULL,
  default_lifetime_ticks INTEGER NOT NULL,
  max_pending_requests INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS keys (
  id TEXT PRIMARY KEY,
  subject TEXT NOT NULL,
  algorithm TEXT NOT NULL,
  family TEXT NOT NULL,
  strength_bits INTEGER NOT NULL,
  created_tick INTEGER NOT NULL,
  expires_tick INTEGER NOT NULL,
  rotation_interval_ticks INTEGER NOT NULL,
  phase TEXT NOT NULL,
  parent_id TEXT NOT NULL DEFAULT '',
  successor_id TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS audits (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  subject TEXT NOT NULL,
  key_id TEXT NOT NULL,
  action TEXT NOT NULL,
  from_phase TEXT NOT NULL,
  to_phase TEXT NOT NULL,
  code TEXT NOT NULL,
  at_tick INTEGER NOT NULL,
  snapshot_version INTEGER NOT NULL,
  detail TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS snapshots (
  version INTEGER PRIMARY KEY,
  parent_hash TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  chain_hash TEXT NOT NULL,
  payload BLOB NOT NULL,
  created_tick INTEGER NOT NULL,
  time_sample TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_keys_subject ON keys(subject, id);
CREATE INDEX IF NOT EXISTS idx_audits_subject_key ON audits(subject, key_id, seq);
`

type Store struct {
	db      *sql.DB
	writeMu sync.Mutex
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, model.NewError(model.CodeConfig, "database path is required")
	}
	if path != ":memory:" {
		abs, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
		path = abs
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("apply %s: %w", pragma, err)
		}
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

type Tx struct {
	tx *sql.Tx
}

func (s *Store) Write(ctx context.Context, fn func(*Tx) error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	wrapped := &Tx{tx: tx}
	if _, err := loadChain(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := fn(wrapped); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// DebugWrite bypasses chain validation. It is used only by the explicit
// external snapshot-anomaly injector after a chain has been built.
func (s *Store) DebugWrite(ctx context.Context, fn func(*Tx) error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(&Tx{tx: tx}); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (t *Tx) GetSubject(ctx context.Context, id string) (model.SubjectPolicy, error) {
	var p model.SubjectPolicy
	var rotation, lifetime int64
	err := t.tx.QueryRowContext(ctx, `SELECT id, min_strength_bits, rotation_interval_ticks, default_lifetime_ticks, max_pending_requests FROM subjects WHERE id = ?`, id).
		Scan(&p.Subject, &p.MinStrengthBits, &rotation, &lifetime, &p.MaxPendingRequests)
	if errors.Is(err, sql.ErrNoRows) {
		return model.SubjectPolicy{}, model.NewError(model.CodePolicyNotFound, "subject policy %q not found", id)
	}
	if err != nil {
		return model.SubjectPolicy{}, err
	}
	p.RotationInterval = model.MonoDuration(rotation)
	p.DefaultLifetime = model.MonoDuration(lifetime)
	return p, nil
}

func (t *Tx) PutSubject(ctx context.Context, p model.SubjectPolicy) error {
	_, err := t.tx.ExecContext(ctx, `INSERT INTO subjects(id, min_strength_bits, rotation_interval_ticks, default_lifetime_ticks, max_pending_requests)
VALUES(?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET min_strength_bits=excluded.min_strength_bits,
rotation_interval_ticks=excluded.rotation_interval_ticks,
default_lifetime_ticks=excluded.default_lifetime_ticks,
max_pending_requests=excluded.max_pending_requests`,
		p.Subject, p.MinStrengthBits, int64(p.RotationInterval), int64(p.DefaultLifetime), p.MaxPendingRequests)
	return err
}

func (t *Tx) GetKey(ctx context.Context, id string) (model.Key, error) {
	return scanKey(t.tx.QueryRowContext(ctx, keySelect+` WHERE id = ?`, id))
}

func (t *Tx) ListKeys(ctx context.Context) ([]model.Key, error) {
	rows, err := t.tx.QueryContext(ctx, keySelect+` ORDER BY subject, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []model.Key
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

func (t *Tx) ListPolicies(ctx context.Context) ([]model.SubjectPolicy, error) {
	rows, err := t.tx.QueryContext(ctx, `SELECT id, min_strength_bits, rotation_interval_ticks, default_lifetime_ticks, max_pending_requests FROM subjects ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var policies []model.SubjectPolicy
	for rows.Next() {
		var p model.SubjectPolicy
		var rotation, lifetime int64
		if err := rows.Scan(&p.Subject, &p.MinStrengthBits, &rotation, &lifetime, &p.MaxPendingRequests); err != nil {
			return nil, err
		}
		p.RotationInterval = model.MonoDuration(rotation)
		p.DefaultLifetime = model.MonoDuration(lifetime)
		policies = append(policies, p)
	}
	return policies, rows.Err()
}

func (t *Tx) ListAudits(ctx context.Context) ([]model.Audit, error) {
	rows, err := t.tx.QueryContext(ctx, `SELECT seq, event_id, subject, key_id, action, from_phase, to_phase, code, at_tick, snapshot_version, detail FROM audits ORDER BY seq, event_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var audits []model.Audit
	for rows.Next() {
		var a model.Audit
		var at int64
		if err := rows.Scan(&a.Seq, &a.EventID, &a.Subject, &a.KeyID, &a.Action, &a.FromPhase, &a.ToPhase, &a.Code, &at, &a.SnapshotVersion, &a.Detail); err != nil {
			return nil, err
		}
		a.At = model.T(at)
		audits = append(audits, a)
	}
	return audits, rows.Err()
}

func (t *Tx) PutKey(ctx context.Context, key model.Key) error {
	_, err := t.tx.ExecContext(ctx, `INSERT INTO keys(id, subject, algorithm, family, strength_bits, created_tick, expires_tick, rotation_interval_ticks, phase, parent_id, successor_id)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET subject=excluded.subject, algorithm=excluded.algorithm, family=excluded.family,
strength_bits=excluded.strength_bits, created_tick=excluded.created_tick, expires_tick=excluded.expires_tick,
rotation_interval_ticks=excluded.rotation_interval_ticks, phase=excluded.phase, parent_id=excluded.parent_id, successor_id=excluded.successor_id`,
		key.ID, key.Subject, key.Algorithm, key.Family, key.StrengthBits, key.CreatedAt.Tick, key.ExpiresAt.Tick,
		int64(key.RotationInterval), key.Phase, key.ParentID, key.SuccessorID)
	return err
}

func (t *Tx) InsertAudit(ctx context.Context, a model.Audit) (int64, error) {
	result, err := t.tx.ExecContext(ctx, `INSERT INTO audits(event_id, subject, key_id, action, from_phase, to_phase, code, at_tick, snapshot_version, detail)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, a.EventID, a.Subject, a.KeyID, a.Action, a.FromPhase, a.ToPhase, a.Code, a.At.Tick, a.SnapshotVersion, a.Detail)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (t *Tx) NextSnapshotVersion(ctx context.Context) (int64, error) {
	var version int64
	if err := t.tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM snapshots`).Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func GenesisHash() string {
	return model.SHA256Hex([]byte("keylife:snapshot-genesis:v1"))
}

func SnapshotHashes(parentHash string, payload []byte) (contentHash, chainHash string) {
	contentHash = model.SHA256Hex(payload)
	chainHash = model.SHA256Hex([]byte(parentHash + "\n" + contentHash))
	return contentHash, chainHash
}

func (t *Tx) InsertSnapshot(ctx context.Context, version int64, payload []byte, createdTick int64) (contentHash, chainHash string, err error) {
	var parentHash string
	if version == 1 {
		parentHash = GenesisHash()
	} else {
		if err := t.tx.QueryRowContext(ctx, `SELECT chain_hash FROM snapshots WHERE version = ?`, version-1).Scan(&parentHash); err != nil {
			return "", "", err
		}
	}
	contentHash, chainHash = SnapshotHashes(parentHash, payload)
	_, err = t.tx.ExecContext(ctx, `INSERT INTO snapshots(version, parent_hash, content_hash, chain_hash, payload, created_tick, time_sample)
VALUES(?, ?, ?, ?, ?, ?, ?)`, version, parentHash, contentHash, chainHash, payload, createdTick, model.TimeSample)
	if err != nil {
		return "", "", err
	}
	_, err = t.tx.ExecContext(ctx, `INSERT INTO meta(key, value) VALUES('snapshot_head', ?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value`, fmt.Sprintf("%d:%s", version, chainHash))
	if err != nil {
		return "", "", err
	}
	return contentHash, chainHash, nil
}

func (s *Store) ListPolicies(ctx context.Context) ([]model.SubjectPolicy, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, min_strength_bits, rotation_interval_ticks, default_lifetime_ticks, max_pending_requests FROM subjects ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var policies []model.SubjectPolicy
	for rows.Next() {
		var p model.SubjectPolicy
		var rotation, lifetime int64
		if err := rows.Scan(&p.Subject, &p.MinStrengthBits, &rotation, &lifetime, &p.MaxPendingRequests); err != nil {
			return nil, err
		}
		p.RotationInterval = model.MonoDuration(rotation)
		p.DefaultLifetime = model.MonoDuration(lifetime)
		policies = append(policies, p)
	}
	return policies, rows.Err()
}

func (s *Store) GetSubject(ctx context.Context, id string) (model.SubjectPolicy, error) {
	var p model.SubjectPolicy
	var rotation, lifetime int64
	err := s.db.QueryRowContext(ctx, `SELECT id, min_strength_bits, rotation_interval_ticks, default_lifetime_ticks, max_pending_requests FROM subjects WHERE id = ?`, id).
		Scan(&p.Subject, &p.MinStrengthBits, &rotation, &lifetime, &p.MaxPendingRequests)
	if errors.Is(err, sql.ErrNoRows) {
		return model.SubjectPolicy{}, model.NewError(model.CodePolicyNotFound, "subject policy %q not found", id)
	}
	if err != nil {
		return model.SubjectPolicy{}, err
	}
	p.RotationInterval = model.MonoDuration(rotation)
	p.DefaultLifetime = model.MonoDuration(lifetime)
	return p, nil
}

func (s *Store) UpsertSubject(ctx context.Context, p model.SubjectPolicy) error {
	return s.Write(ctx, func(tx *Tx) error { return tx.PutSubject(ctx, p) })
}

func (s *Store) GetKey(ctx context.Context, id string) (model.Key, error) {
	return scanKey(s.db.QueryRowContext(ctx, keySelect+` WHERE id = ?`, id))
}

func (s *Store) ListKeys(ctx context.Context, subject string) ([]model.Key, error) {
	query := keySelect
	var args []any
	if subject != "" {
		query += ` WHERE subject = ?`
		args = append(args, subject)
	}
	query += ` ORDER BY subject, id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []model.Key
	for rows.Next() {
		key, err := scanKey(rows)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func (s *Store) ListAudits(ctx context.Context) ([]model.Audit, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT seq, event_id, subject, key_id, action, from_phase, to_phase, code, at_tick, snapshot_version, detail FROM audits ORDER BY seq, event_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var audits []model.Audit
	for rows.Next() {
		var a model.Audit
		var at int64
		if err := rows.Scan(&a.Seq, &a.EventID, &a.Subject, &a.KeyID, &a.Action, &a.FromPhase, &a.ToPhase, &a.Code, &at, &a.SnapshotVersion, &a.Detail); err != nil {
			return nil, err
		}
		a.At = model.T(at)
		audits = append(audits, a)
	}
	return audits, rows.Err()
}

type SnapshotEntry struct {
	Version     int64                `json:"version"`
	ParentHash  string               `json:"parent_hash"`
	ContentHash string               `json:"content_hash"`
	ChainHash   string               `json:"chain_hash"`
	Payload     json.RawMessage      `json:"payload,omitempty"`
	State       *model.StateSnapshot `json:"-"`
	CreatedAt   model.MonoTime       `json:"created_at"`
	TimeSample  string               `json:"time_sample"`
}

type ChainResult struct {
	Entries            []SnapshotEntry `json:"entries"`
	RecoveredThrough   int64           `json:"recovered_through"`
	PhysicalMaxVersion int64           `json:"physical_max_version"`
}

func (s *Store) LoadChain(ctx context.Context) (ChainResult, error) {
	return loadChain(ctx, s.db)
}

func (s *Store) PhysicalMaxVersion(ctx context.Context) (int64, error) {
	var version int64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM snapshots`).Scan(&version)
	return version, err
}

func (s *Store) GetSnapshot(ctx context.Context, version int64) (SnapshotEntry, error) {
	row := s.db.QueryRowContext(ctx, `SELECT version, parent_hash, content_hash, chain_hash, payload, created_tick, time_sample FROM snapshots WHERE version = ?`, version)
	return scanSnapshot(row)
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (t *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

func (t *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

func (t *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func loadChain(ctx context.Context, q queryer) (ChainResult, error) {
	result := ChainResult{}
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM snapshots`).Scan(&result.PhysicalMaxVersion); err != nil {
		return result, err
	}
	rows, err := q.QueryContext(ctx, `SELECT version, parent_hash, content_hash, chain_hash, payload, created_tick, time_sample FROM snapshots ORDER BY version`)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	expected := int64(1)
	parent := GenesisHash()
	for rows.Next() {
		entry, err := scanSnapshot(rows)
		if err != nil {
			return result, err
		}
		if entry.Version != expected {
			return result, chainError(model.CodeSnapshotVersion, result.RecoveredThrough, "snapshot version jump: expected %d, found %d", expected, entry.Version)
		}
		if entry.State.Version != entry.Version {
			return result, chainError(model.CodeSnapshotReplaced, result.RecoveredThrough, "snapshot %d contains payload version %d", entry.Version, entry.State.Version)
		}
		contentHash, chainHash := SnapshotHashes(entry.ParentHash, entry.Payload)
		if contentHash != entry.ContentHash || chainHash != entry.ChainHash {
			return result, chainError(model.CodeSnapshotReplaced, result.RecoveredThrough, "snapshot %d content was replaced", entry.Version)
		}
		if entry.ParentHash != parent {
			return result, chainError(model.CodeSnapshotReplaced, result.RecoveredThrough, "snapshot %d parent hash mismatch", entry.Version)
		}
		result.Entries = append(result.Entries, entry)
		result.RecoveredThrough = entry.Version
		parent = entry.ChainHash
		expected++
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	if result.PhysicalMaxVersion != result.RecoveredThrough {
		return result, chainError(model.CodeSnapshotVersion, result.RecoveredThrough, "snapshot chain has unreadable tail")
	}
	if result.RecoveredThrough > 0 {
		var head string
		err := q.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'snapshot_head'`).Scan(&head)
		if errors.Is(err, sql.ErrNoRows) {
			return result, chainError(model.CodeSnapshotReplaced, result.RecoveredThrough, "snapshot head anchor is missing")
		}
		if err != nil {
			return result, err
		}
		want := fmt.Sprintf("%d:%s", result.RecoveredThrough, parent)
		if head != want {
			return result, chainError(model.CodeSnapshotReplaced, result.RecoveredThrough, "snapshot head anchor mismatch")
		}
	}
	return result, nil
}

func chainError(code model.Code, recovered int64, format string, args ...any) error {
	return &model.BizError{Code: code, Message: fmt.Sprintf(format, args...), RecoveredThrough: recovered}
}

func scanSnapshot(scanner interface{ Scan(...any) error }) (SnapshotEntry, error) {
	var entry SnapshotEntry
	var payload []byte
	var createdTick int64
	if err := scanner.Scan(&entry.Version, &entry.ParentHash, &entry.ContentHash, &entry.ChainHash, &payload, &createdTick, &entry.TimeSample); err != nil {
		return SnapshotEntry{}, err
	}
	entry.Payload = append(json.RawMessage(nil), payload...)
	entry.CreatedAt = model.T(createdTick)
	var state model.StateSnapshot
	if err := json.Unmarshal(payload, &state); err != nil {
		return SnapshotEntry{}, chainError(model.CodeSnapshotIntegrity, entry.Version-1, "snapshot %d payload is not valid JSON: %v", entry.Version, err)
	}
	entry.State = &state
	return entry, nil
}

func scanKey(scanner interface{ Scan(...any) error }) (model.Key, error) {
	var key model.Key
	var created, expires, rotation int64
	err := scanner.Scan(&key.ID, &key.Subject, &key.Algorithm, &key.Family, &key.StrengthBits, &created, &expires, &rotation, &key.Phase, &key.ParentID, &key.SuccessorID)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Key{}, model.NewError(model.CodeKeyNotFound, "key not found")
	}
	if err != nil {
		return model.Key{}, err
	}
	key.CreatedAt = model.T(created)
	key.ExpiresAt = model.T(expires)
	key.RotationInterval = model.MonoDuration(rotation)
	return key, nil
}

const keySelect = `SELECT id, subject, algorithm, family, strength_bits, created_tick, expires_tick, rotation_interval_ticks, phase, parent_id, successor_id FROM keys`

// InjectSnapshotGap creates a physical version jump. The injected row is a
// copy of an existing valid payload; chain validation rejects the gap first.
func (s *Store) InjectSnapshotGap(ctx context.Context, fromVersion, toVersion int64, atTick int64) error {
	return s.DebugWrite(ctx, func(tx *Tx) error {
		entry, err := tx.getSnapshot(ctx, fromVersion)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO snapshots(version, parent_hash, content_hash, chain_hash, payload, created_tick, time_sample)
VALUES(?, ?, ?, ?, ?, ?, ?)`, toVersion, entry.ParentHash, entry.ContentHash, entry.ChainHash, []byte(entry.Payload), atTick, entry.TimeSample)
		return err
	})
}

// InjectSnapshotReplacement changes content while retaining the version and
// stored hash columns, emulating a whole-snapshot replacement attack.
func (s *Store) InjectSnapshotReplacement(ctx context.Context, version int64, replacement []byte) error {
	return s.DebugWrite(ctx, func(tx *Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE snapshots SET payload = ? WHERE version = ?`, replacement, version)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return model.NewError(model.CodeSnapshotVersion, "snapshot version %d not found", version)
		}
		return nil
	})
}

func (t *Tx) getSnapshot(ctx context.Context, version int64) (SnapshotEntry, error) {
	row := t.QueryRowContext(ctx, `SELECT version, parent_hash, content_hash, chain_hash, payload, created_tick, time_sample FROM snapshots WHERE version = ?`, version)
	entry, err := scanSnapshot(row)
	if errors.Is(err, sql.ErrNoRows) {
		return SnapshotEntry{}, model.NewError(model.CodeSnapshotVersion, "snapshot version %d not found", version)
	}
	return entry, err
}

// SnapshotChainVersions returns the complete physical version list, including
// injected gaps, for diagnostics.
func (s *Store) SnapshotChainVersions(ctx context.Context) ([]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT version FROM snapshots ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var versions []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		versions = append(versions, v)
	}
	return versions, rows.Err()
}

func SortedVersions(entries []SnapshotEntry) []int64 {
	versions := make([]int64, 0, len(entries))
	for _, entry := range entries {
		versions = append(versions, entry.Version)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	return versions
}
