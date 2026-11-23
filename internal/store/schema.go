package store

const schemaSQL = `
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;
PRAGMA synchronous=FULL;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS instances (
    id         TEXT PRIMARY KEY,
    port       INTEGER NOT NULL,
    weight     INTEGER NOT NULL,
    zone       TEXT NOT NULL,
    probe_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS requests (
    seq              INTEGER PRIMARY KEY,
    timestamp_millis INTEGER NOT NULL,
    client_id        TEXT NOT NULL,
    request_weight   INTEGER NOT NULL,
    request_bytes    INTEGER NOT NULL,
    status           TEXT NOT NULL,
    round_id         TEXT,
    decision_json    TEXT
);

CREATE TABLE IF NOT EXISTS instance_state (
    instance_id    TEXT PRIMARY KEY,
    phase          TEXT NOT NULL,
    success_streak INTEGER NOT NULL,
    failure_streak INTEGER NOT NULL,
    reason         TEXT NOT NULL DEFAULT '',
    updated_at     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    version            INTEGER PRIMARY KEY AUTOINCREMENT,
    reason             TEXT NOT NULL,
    created_at         INTEGER NOT NULL,
    state_json         TEXT NOT NULL,
    zone_weights_json  TEXT NOT NULL,
    zone_ratios_json   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rounds (
    id          TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    status      TEXT NOT NULL,
    started_at  INTEGER NOT NULL,
    finished_at INTEGER,
    detail_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS probes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id    TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    success     INTEGER NOT NULL,
    status_code INTEGER NOT NULL DEFAULT 0,
    err_code    TEXT NOT NULL DEFAULT '',
    err_msg     TEXT NOT NULL DEFAULT '',
    elapsed_ms  INTEGER NOT NULL,
    created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dispatches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id    TEXT NOT NULL,
    request_seq INTEGER NOT NULL,
    instance_id TEXT NOT NULL,
    status      TEXT NOT NULL,
    http_status INTEGER NOT NULL DEFAULT 0,
    err_code    TEXT NOT NULL DEFAULT '',
    err_msg     TEXT NOT NULL DEFAULT '',
    started_at  INTEGER NOT NULL,
    finished_at INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dispatch_round_request
    ON dispatches(round_id, request_seq);

-- 意图日志：OPEN 为已预备未完成，COMMITTED 为完整提交边界，ABORTED 为恢复时裁决中止。
CREATE TABLE IF NOT EXISTS intents (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id     TEXT NOT NULL DEFAULT '',
    kind         TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    status       TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    resolved_at  INTEGER
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at  INTEGER NOT NULL,
    detail_json TEXT NOT NULL
);
`
