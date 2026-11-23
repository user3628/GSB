package authquota

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY,
    quota_remaining INTEGER NOT NULL CHECK (quota_remaining >= 0),
    ledger_version INTEGER NOT NULL DEFAULT 0,
    snapshot_version INTEGER NOT NULL DEFAULT 1,
    unit_cost INTEGER NOT NULL,
    rate_num INTEGER NOT NULL,
    rate_den INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS clients (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    redirect_uri TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS auth_codes (
    code_hash TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    client_id TEXT NOT NULL REFERENCES clients(id),
    redirect_uri TEXT NOT NULL,
    code_challenge TEXT NOT NULL,
    nonce TEXT NOT NULL,
    issued_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    consumed_at INTEGER,
    UNIQUE (tenant_id, client_id, nonce)
);
CREATE TABLE IF NOT EXISTS token_families (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    client_id TEXT NOT NULL REFERENCES clients(id),
    created_at INTEGER NOT NULL,
    revoked_at INTEGER NOT NULL DEFAULT 0,
    revoke_reason TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS refresh_tokens (
    token_hash TEXT PRIMARY KEY,
    family_id TEXT NOT NULL REFERENCES token_families(id),
    sequence INTEGER NOT NULL,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    client_id TEXT NOT NULL REFERENCES clients(id),
    issued_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    revoked_at INTEGER NOT NULL DEFAULT 0,
    revoke_reason TEXT NOT NULL DEFAULT '',
    replaced_by_hash TEXT NOT NULL DEFAULT '',
    UNIQUE (family_id, sequence)
);
CREATE TABLE IF NOT EXISTS access_tokens (
    token_hash TEXT PRIMARY KEY,
    family_id TEXT NOT NULL REFERENCES token_families(id),
    sequence INTEGER NOT NULL,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    client_id TEXT NOT NULL REFERENCES clients(id),
    issued_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    revoked_at INTEGER NOT NULL DEFAULT 0,
    revoke_reason TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS billing_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    ledger_version INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    requests INTEGER NOT NULL,
    unit_cost INTEGER NOT NULL,
    rate_num INTEGER NOT NULL,
    rate_den INTEGER NOT NULL,
    charged INTEGER NOT NULL,
    trunc_num INTEGER NOT NULL,
    trunc_den INTEGER NOT NULL,
    remaining_after INTEGER NOT NULL,
    snapshot_version INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE (tenant_id, ledger_version)
);
CREATE TABLE IF NOT EXISTS error_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL DEFAULT '',
    request_id TEXT NOT NULL DEFAULT '',
    code TEXT NOT NULL,
    detail TEXT NOT NULL,
    ledger_version INTEGER NOT NULL,
    snapshot_version INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS truncation_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    ledger_version INTEGER NOT NULL,
    snapshot_version INTEGER NOT NULL,
    charged_nano INTEGER NOT NULL,
    trunc_num INTEGER NOT NULL,
    trunc_den INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE (tenant_id, ledger_version)
);
CREATE TABLE IF NOT EXISTS tenant_snapshots (
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    version INTEGER NOT NULL,
    kind TEXT NOT NULL,
    state_json BLOB NOT NULL,
    state_hash TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, version)
);
CREATE TABLE IF NOT EXISTS idempotency_records (
    tenant_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    idem_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_payload BLOB NOT NULL,
    ledger_version INTEGER NOT NULL,
    snapshot_version INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, client_id, operation, idem_key)
);
CREATE INDEX IF NOT EXISTS idx_error_tenant_ledger ON error_events(tenant_id, ledger_version);
CREATE INDEX IF NOT EXISTS idx_refresh_family ON refresh_tokens(family_id, sequence);
CREATE INDEX IF NOT EXISTS idx_access_family ON access_tokens(family_id);
CREATE INDEX IF NOT EXISTS idx_billing_tenant ON billing_entries(tenant_id, ledger_version);
CREATE INDEX IF NOT EXISTS idx_truncation_tenant ON truncation_entries(tenant_id, ledger_version);
`

func openStore(ctx context.Context, cfg Config) (*sql.DB, error) {
	path := cfg.DBPath
	if path == "" {
		return nil, bizError(CodeInvalidRequest, "DBPath is required", nil)
	}
	var dsn string
	if path == ":memory:" {
		dsn = "file:openplatform-memory?mode=memory&cache=shared&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	} else {
		absolutePath, err := filepath.Abs(path)
		if err != nil {
			return nil, bizError(CodeInternal, "resolve database path", err)
		}
		dir := filepath.Dir(absolutePath)
		if dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, bizError(CodeInternal, "create database directory", err)
			}
		}
		uriPath := filepath.ToSlash(absolutePath)
		if filepath.VolumeName(absolutePath) != "" && !strings.HasPrefix(uriPath, "/") {
			uriPath = "/" + uriPath
		}
		uri := &url.URL{Scheme: "file", Path: uriPath}
		query := uri.Query()
		query.Set("_txlock", "immediate")
		query.Add("_pragma", "busy_timeout(5000)")
		query.Add("_pragma", "foreign_keys(1)")
		query.Add("_pragma", "journal_mode(WAL)")
		query.Add("_pragma", "synchronous(FULL)")
		uri.RawQuery = query.Encode()
		dsn = uri.String()
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, bizError(CodeInternal, "open sqlite", err)
	}
	db.SetMaxOpenConns(maxInt(4, cfg.MaxInFlight+4))
	db.SetMaxIdleConns(maxInt(2, cfg.MaxInFlight))
	db.SetConnMaxIdleTime(5 * time.Minute)

	for _, stmt := range splitStatements(schemaSQL) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			return nil, bizError(CodeInternal, "initialize schema", err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO schema_meta(key, value) VALUES('schema_version', '1') ON CONFLICT(key) DO NOTHING`); err != nil {
		db.Close()
		return nil, bizError(CodeInternal, "write schema version", err)
	}
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
		db.Close()
		return nil, bizError(CodeInternal, "enable foreign keys", err)
	}
	return db, nil
}

func splitStatements(sqlText string) []string {
	parts := strings.Split(sqlText, ";")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		stmt := strings.TrimSpace(part)
		if stmt != "" {
			result = append(result, stmt)
		}
	}
	return result
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func nowNano(cfg Config) int64 { return cfg.Clock().UTC().UnixNano() }

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func requestFingerprint(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte(fmt.Sprintf("%d:", len(part))))
		h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

type idempotencyRecord struct {
	RequestHash     string
	ResponsePayload []byte
	LedgerVersion   int64
	SnapshotVersion int64
}

func loadIdempotency(ctx context.Context, tx *sql.Tx, tenantID, clientID, operation, key string) (*idempotencyRecord, error) {
	if key == "" {
		return nil, nil
	}
	var rec idempotencyRecord
	err := tx.QueryRowContext(ctx, `
SELECT request_hash, response_payload, ledger_version, snapshot_version
FROM idempotency_records
WHERE tenant_id=? AND client_id=? AND operation=? AND idem_key=?`,
		tenantID, clientID, operation, key).Scan(&rec.RequestHash, &rec.ResponsePayload, &rec.LedgerVersion, &rec.SnapshotVersion)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func saveIdempotency(ctx context.Context, tx *sql.Tx, secret []byte, tenantID, clientID, operation, key, requestHash string, response any, ledgerVersion, snapshotVersion, createdAt int64) error {
	if key == "" {
		return nil
	}
	body, err := CanonicalJSON(response)
	if err != nil {
		return err
	}
	payload, err := sealIdempotencyPayload(secret, body)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO idempotency_records(tenant_id, client_id, operation, idem_key, request_hash, response_payload, ledger_version, snapshot_version, created_at)
VALUES(?,?,?,?,?,?,?,?,?)`,
		tenantID, clientID, operation, key, requestHash, payload, ledgerVersion, snapshotVersion, createdAt)
	return err
}
