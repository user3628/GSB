package authlab

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// SnapshotMutation is executed against a tenant's latest validated state inside
// a SQLite transaction. Returning changed=false performs no write.
type SnapshotMutation func(state *TenantState) (changed bool, err error)

// SessionStore is the host-integration boundary. Every state operation is
// tenant-scoped; implementations must not expose cross-tenant rows.
type SessionStore interface {
	Init(ctx context.Context, fixture Fixture) error
	FindUserByUsername(ctx context.Context, tenantID, username string) (UserRecord, error)
	FindUserByID(ctx context.Context, tenantID, userID string) (UserRecord, error)
	UpdateAuthorization(ctx context.Context, tenantID, userID string, roles, permissions []string) error

	// LoadSnapshot validates the complete version chain. On integrity failure it
	// returns the most recent continuous snapshot together with E_SNAPSHOT_VERSION.
	LoadSnapshot(ctx context.Context, tenantID string) (Snapshot, error)
	SnapshotAt(ctx context.Context, tenantID string, version int64) (Snapshot, error)
	MutateSnapshot(ctx context.Context, tenantID string, mutate SnapshotMutation) (Snapshot, bool, error)
	SnapshotHistory(ctx context.Context, tenantID string) ([]SnapshotMeta, error)
	AuditSnapshots(ctx context.Context, tenantID string) (SnapshotAudit, error)
	RepairSnapshots(ctx context.Context, tenantID string) (Snapshot, bool, error)
	Close() error
}

// SQLiteStore is a pure-Go SQLite implementation. Session state lives only in
// immutable snapshots; there is no secondary mutable session table that could
// drift from the snapshot chain.
type SQLiteStore struct {
	db  *sql.DB
	now func() time.Time
}

func OpenSQLiteStore(ctx context.Context, path string, now func() time.Time) (*SQLiteStore, error) {
	if path == "" {
		return nil, newError(CodeInternal, "open_store", "empty database path")
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("sqlite pragma: %w", err)
		}
	}
	store := &SQLiteStore{db: db, now: now}
	if err := store.createSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStore) Close() error { return s.db.Close() }

func (s *SQLiteStore) createSchema(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS users (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    password_salt TEXT NOT NULL,
    iterations INTEGER NOT NULL,
    roles_json TEXT NOT NULL,
    permissions_json TEXT NOT NULL,
    PRIMARY KEY (tenant_id, user_id),
    UNIQUE (tenant_id, username),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
CREATE TABLE IF NOT EXISTS snapshots (
    tenant_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    previous_digest TEXT NOT NULL,
    digest TEXT NOT NULL,
    content BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, version),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
CREATE TABLE IF NOT EXISTS snapshot_heads (
    tenant_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    digest TEXT NOT NULL,
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	return nil
}

func (s *SQLiteStore) Init(ctx context.Context, fixture Fixture) error {
	if err := ValidateFixture(fixture); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin fixture transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, tenant := range fixture.Tenants {
		if _, err := tx.ExecContext(ctx, `INSERT INTO tenants(id, name) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET name=excluded.name`, tenant.ID, tenant.Name); err != nil {
			return fmt.Errorf("insert tenant: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO snapshot_heads(tenant_id, version, digest) VALUES(?, 0, ?) ON CONFLICT(tenant_id) DO NOTHING`, tenant.ID, GenesisDigest); err != nil {
			return fmt.Errorf("insert snapshot head: %w", err)
		}
	}
	for _, user := range fixture.Users {
		roles, _ := json.Marshal(normalizeStrings(user.Roles))
		permissions, _ := json.Marshal(normalizeStrings(user.Permissions))
		_, err := tx.ExecContext(ctx, `
INSERT INTO users(tenant_id, user_id, username, password_hash, password_salt, iterations, roles_json, permissions_json)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(tenant_id, user_id) DO UPDATE SET
    username=excluded.username,
    password_hash=excluded.password_hash,
    password_salt=excluded.password_salt,
    iterations=excluded.iterations,
    roles_json=excluded.roles_json,
    permissions_json=excluded.permissions_json`,
			user.TenantID, user.UserID, user.Username, user.PasswordHash, user.PasswordSalt,
			user.Iterations, string(roles), string(permissions))
		if err != nil {
			return fmt.Errorf("insert user: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit fixture: %w", err)
	}
	return nil
}

func (s *SQLiteStore) FindUserByUsername(ctx context.Context, tenantID, username string) (UserRecord, error) {
	return s.findUser(ctx, tenantID, "username", username)
}

func (s *SQLiteStore) FindUserByID(ctx context.Context, tenantID, userID string) (UserRecord, error) {
	return s.findUser(ctx, tenantID, "user_id", userID)
}

func (s *SQLiteStore) findUser(ctx context.Context, tenantID, column, value string) (UserRecord, error) {
	var user UserRecord
	var rolesJSON, permissionsJSON string
	query := `SELECT tenant_id, user_id, username, password_hash, password_salt, iterations, roles_json, permissions_json FROM users WHERE tenant_id=? AND ` + column + `=?`
	err := s.db.QueryRowContext(ctx, query, tenantID, value).Scan(
		&user.TenantID, &user.UserID, &user.Username, &user.PasswordHash,
		&user.PasswordSalt, &user.Iterations, &rolesJSON, &permissionsJSON,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return UserRecord{}, ErrNotFound
	}
	if err != nil {
		return UserRecord{}, fmt.Errorf("find user: %w", err)
	}
	if err := json.Unmarshal([]byte(rolesJSON), &user.Roles); err != nil {
		return UserRecord{}, fmt.Errorf("decode roles: %w", err)
	}
	if err := json.Unmarshal([]byte(permissionsJSON), &user.Permissions); err != nil {
		return UserRecord{}, fmt.Errorf("decode permissions: %w", err)
	}
	user.Roles = normalizeStrings(user.Roles)
	user.Permissions = normalizeStrings(user.Permissions)
	return user, nil
}

func (s *SQLiteStore) UpdateAuthorization(ctx context.Context, tenantID, userID string, roles, permissions []string) error {
	rolesJSON, _ := json.Marshal(normalizeStrings(roles))
	permissionsJSON, _ := json.Marshal(normalizeStrings(permissions))
	res, err := s.db.ExecContext(ctx, `UPDATE users SET roles_json=?, permissions_json=? WHERE tenant_id=? AND user_id=?`, string(rolesJSON), string(permissionsJSON), tenantID, userID)
	if err != nil {
		return fmt.Errorf("update authorization: %w", err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update authorization rows: %w", err)
	}
	if changed != 1 {
		return ErrNotFound
	}
	return nil
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type storedSnapshot struct {
	Version        int64
	PreviousDigest string
	Digest         string
	Content        []byte
	CreatedAt      int64
}

func loadChain(ctx context.Context, q queryer, tenantID string) (Snapshot, SnapshotAudit, string, error) {
	empty := emptySnapshot(tenantID)
	audit := SnapshotAudit{TenantID: tenantID, Versions: []int64{}, IntegrityOK: true}
	var headVersion int64
	var headDigest string
	if err := q.QueryRowContext(ctx, `SELECT version, digest FROM snapshot_heads WHERE tenant_id=?`, tenantID).Scan(&headVersion, &headDigest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			audit.ValidVersion = 0
			audit.Problem = "missing_head"
			audit.IntegrityOK = false
			return empty, audit, audit.Problem, nil
		}
		return Snapshot{}, audit, "", fmt.Errorf("read snapshot head: %w", err)
	}
	audit.HeadVersion = headVersion

	rows, err := q.QueryContext(ctx, `SELECT version, previous_digest, digest, content, created_at FROM snapshots WHERE tenant_id=? ORDER BY version ASC`, tenantID)
	if err != nil {
		return Snapshot{}, audit, "", fmt.Errorf("read snapshots: %w", err)
	}
	defer rows.Close()

	valid := empty
	validByVersion := map[int64]Snapshot{0: empty}
	expected := int64(1)
	previous := GenesisDigest
	problem := ""
	var lastRawVersion int64
	for rows.Next() {
		var stored storedSnapshot
		if err := rows.Scan(&stored.Version, &stored.PreviousDigest, &stored.Digest, &stored.Content, &stored.CreatedAt); err != nil {
			return Snapshot{}, audit, "", fmt.Errorf("scan snapshot: %w", err)
		}
		audit.Versions = append(audit.Versions, stored.Version)
		lastRawVersion = stored.Version
		if problem != "" {
			continue
		}
		if stored.Version != expected {
			problem = "version_gap"
			continue
		}
		content, err := decodeSnapshotContent(stored.Content)
		if err != nil {
			problem = "malformed_snapshot"
			continue
		}
		if content.Schema != SnapshotSchema || content.TenantID != tenantID || content.Version != stored.Version {
			problem = "content_identity_mismatch"
			continue
		}
		if stored.PreviousDigest != previous || content.PreviousDigest != previous {
			problem = "previous_digest_mismatch"
			continue
		}
		_, computed, err := marshalSnapshotContent(content.TenantID, content.Version, content.PreviousDigest, content.State)
		if err != nil || computed != stored.Digest {
			problem = "content_digest_mismatch"
			continue
		}
		snapshot := Snapshot{
			TenantID:       tenantID,
			Version:        stored.Version,
			PreviousDigest: stored.PreviousDigest,
			Digest:         stored.Digest,
			State:          content.State.normalized(),
		}
		valid = snapshot
		validByVersion[stored.Version] = snapshot
		previous = stored.Digest
		expected++
	}
	if err := rows.Err(); err != nil {
		return Snapshot{}, audit, "", fmt.Errorf("iterate snapshots: %w", err)
	}

	lastValidVersion := expected - 1
	if problem == "" {
		if headVersion == 0 {
			if lastRawVersion != 0 {
				problem = "snapshots_beyond_head"
			}
		} else {
			headSnapshot, ok := validByVersion[headVersion]
			switch {
			case !ok || headSnapshot.Digest != headDigest:
				problem = "head_digest_mismatch"
			case lastRawVersion > headVersion:
				problem = "snapshots_beyond_head"
			case lastValidVersion < headVersion:
				problem = "head_version_mismatch"
			}
		}
	}
	audit.ValidVersion = valid.Version
	if problem != "" {
		audit.IntegrityOK = false
		audit.Problem = problem
		switch {
		case problem == "head_digest_mismatch":
			// The trusted head digest disagreed with a same-version replacement,
			// so the replaced head must not be loaded. Fall back one step.
			if fallback, ok := validByVersion[headVersion-1]; ok {
				valid = fallback
			} else {
				valid = empty
			}
		case problem == "snapshots_beyond_head" && headVersion > 0:
			if trustedHead, ok := validByVersion[headVersion]; ok {
				valid = trustedHead
			} else {
				valid = empty
			}
		case valid.Version > headVersion:
			valid = empty
			if headVersion > 0 {
				if headSnapshot, ok := validByVersion[headVersion]; ok {
					valid = headSnapshot
				}
			}
		}
		audit.ValidVersion = valid.Version
	}
	return valid, audit, problem, nil
}

func (s *SQLiteStore) LoadSnapshot(ctx context.Context, tenantID string) (Snapshot, error) {
	snapshot, audit, problem, err := loadChain(ctx, s.db, tenantID)
	if err != nil {
		return Snapshot{}, err
	}
	if problem != "" {
		_ = audit
		return snapshot, newError(CodeSnapshotVersion, "load_snapshot", problem)
	}
	return snapshot, nil
}

func (s *SQLiteStore) SnapshotAt(ctx context.Context, tenantID string, version int64) (Snapshot, error) {
	latest, _, problem, err := loadChain(ctx, s.db, tenantID)
	if err != nil {
		return Snapshot{}, err
	}
	if problem != "" {
		return Snapshot{}, newError(CodeSnapshotVersion, "snapshot_at", problem)
	}
	if version < 0 || version > latest.Version {
		return Snapshot{}, newError(CodeSnapshotVersion, "snapshot_at", "version unavailable")
	}
	if version == 0 {
		return emptySnapshot(tenantID), nil
	}
	var stored storedSnapshot
	err = s.db.QueryRowContext(ctx, `SELECT version, previous_digest, digest, content, created_at FROM snapshots WHERE tenant_id=? AND version=?`, tenantID, version).
		Scan(&stored.Version, &stored.PreviousDigest, &stored.Digest, &stored.Content, &stored.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, newError(CodeSnapshotVersion, "snapshot_at", "version unavailable")
	}
	if err != nil {
		return Snapshot{}, fmt.Errorf("read snapshot version: %w", err)
	}
	content, err := decodeSnapshotContent(stored.Content)
	if err != nil {
		return Snapshot{}, newError(CodeSnapshotVersion, "snapshot_at", "malformed snapshot")
	}
	return Snapshot{
		TenantID:       tenantID,
		Version:        stored.Version,
		PreviousDigest: stored.PreviousDigest,
		Digest:         stored.Digest,
		State:          content.State.normalized(),
	}, nil
}

func (s *SQLiteStore) MutateSnapshot(ctx context.Context, tenantID string, mutate SnapshotMutation) (Snapshot, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("begin snapshot transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, _, problem, err := loadChain(ctx, tx, tenantID)
	if err != nil {
		return Snapshot{}, false, err
	}
	if problem != "" {
		return current, false, newError(CodeSnapshotVersion, "mutate_snapshot", problem)
	}
	current.State = current.State.normalized()
	changed, err := mutate(&current.State)
	if err != nil {
		return Snapshot{}, false, err
	}
	if !changed {
		return current, false, nil
	}
	nextVersion := current.Version + 1
	previous := current.Digest
	payload, digest, err := marshalSnapshotContent(tenantID, nextVersion, previous, current.State)
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("marshal snapshot: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO snapshots(tenant_id, version, previous_digest, digest, content, created_at) VALUES(?, ?, ?, ?, ?, ?)`,
		tenantID, nextVersion, previous, digest, payload, s.now().Unix())
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("insert snapshot: %w", err)
	}
	res, err := tx.ExecContext(ctx, `UPDATE snapshot_heads SET version=?, digest=? WHERE tenant_id=? AND version=? AND digest=?`,
		nextVersion, digest, tenantID, current.Version, current.Digest)
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("advance snapshot head: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil || affected != 1 {
		return Snapshot{}, false, newError(CodeSnapshotVersion, "mutate_snapshot", "optimistic head conflict")
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, false, fmt.Errorf("commit snapshot: %w", err)
	}
	return Snapshot{
		TenantID:       tenantID,
		Version:        nextVersion,
		PreviousDigest: previous,
		Digest:         digest,
		State:          current.State.normalized(),
	}, true, nil
}

func (s *SQLiteStore) SnapshotHistory(ctx context.Context, tenantID string) ([]SnapshotMeta, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT tenant_id, version, previous_digest, digest FROM snapshots WHERE tenant_id=? ORDER BY version ASC`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("snapshot history: %w", err)
	}
	defer rows.Close()
	out := []SnapshotMeta{}
	for rows.Next() {
		var meta SnapshotMeta
		if err := rows.Scan(&meta.TenantID, &meta.Version, &meta.PreviousDigest, &meta.Digest); err != nil {
			return nil, fmt.Errorf("scan snapshot history: %w", err)
		}
		out = append(out, meta)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) AuditSnapshots(ctx context.Context, tenantID string) (SnapshotAudit, error) {
	_, audit, _, err := loadChain(ctx, s.db, tenantID)
	return audit, err
}

func (s *SQLiteStore) RepairSnapshots(ctx context.Context, tenantID string) (Snapshot, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("begin repair transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	valid, _, problem, err := loadChain(ctx, tx, tenantID)
	if err != nil {
		return Snapshot{}, false, err
	}
	if problem == "" {
		return valid, false, nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM snapshots WHERE tenant_id=? AND version>?`, tenantID, valid.Version); err != nil {
		return Snapshot{}, false, fmt.Errorf("truncate invalid snapshots: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE snapshot_heads SET version=?, digest=? WHERE tenant_id=?`, valid.Version, valid.Digest, tenantID); err != nil {
		return Snapshot{}, false, fmt.Errorf("repair snapshot head: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, false, fmt.Errorf("commit repair: %w", err)
	}
	return valid, true, nil
}
