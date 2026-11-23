package store

// SchemaDDL creates every durable object used by the engine.
//
// Design notes:
//   - row_versions is append-only; recycling sets retired_at instead of
//     deleting rows, so retired reads deterministically return E_VERSION_RETIRED.
//   - write_leases keeps one row per business row; lease_version grows on every
//     (re)grant and drives E_LEASE_STALE / E_LEASE_LOST checks.
//   - write_intents is the write-ahead log referenced by crash recovery.
//   - inflight_requests gives cross-process idempotency on request_id.
//   - active_snapshots protects retained versions from aggressive recycling.
const SchemaDDL = `
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS row_versions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    row_id       TEXT NOT NULL,
    row_version  INTEGER NOT NULL,
    commit_ver   INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    content      TEXT NOT NULL,
    deleted      INTEGER NOT NULL DEFAULT 0,
    retired_at   INTEGER,
    created_at   INTEGER NOT NULL,
    intent_id    INTEGER,
    UNIQUE(row_id, row_version)
);
CREATE INDEX IF NOT EXISTS idx_row_versions_lookup
    ON row_versions(row_id, commit_ver);
CREATE INDEX IF NOT EXISTS idx_row_versions_retired
    ON row_versions(row_id, retired_at);

CREATE TABLE IF NOT EXISTS write_leases (
    row_id        TEXT PRIMARY KEY,
    lease_version INTEGER NOT NULL,
    holder        TEXT NOT NULL,
    state         TEXT NOT NULL,
    granted_at    INTEGER NOT NULL,
    renewed_at    INTEGER NOT NULL,
    expires_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS write_intents (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    kind          TEXT NOT NULL,
    row_id        TEXT NOT NULL,
    request_id    TEXT NOT NULL,
    lease_version INTEGER NOT NULL,
    base_row_ver  INTEGER NOT NULL,
    payload       TEXT NOT NULL,
    state         TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    completed_at  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_intents_state
    ON write_intents(state, id);

CREATE TABLE IF NOT EXISTS inflight_requests (
    request_id    TEXT PRIMARY KEY,
    kind          TEXT NOT NULL,
    request_key   TEXT NOT NULL,
    payload_hash  TEXT NOT NULL,
    status        TEXT NOT NULL,
    response      TEXT NOT NULL DEFAULT '',
    created_at    INTEGER NOT NULL,
    completed_at  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_requests_status
    ON inflight_requests(status);

CREATE TABLE IF NOT EXISTS active_snapshots (
    snapshot_ver INTEGER NOT NULL,
    holder       TEXT NOT NULL,
    opened_at    INTEGER NOT NULL,
    orphaned     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(snapshot_ver, holder)
);

CREATE TABLE IF NOT EXISTS recovery_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id  INTEGER NOT NULL,
    kind       TEXT NOT NULL,
    row_id     TEXT NOT NULL,
    action     TEXT NOT NULL,
    detail     TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
`
