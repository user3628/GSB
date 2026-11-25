package store

const schema = `
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS snapshots (
  version      INTEGER PRIMARY KEY,
  created_at   TEXT NOT NULL,
  checksum     TEXT NOT NULL,
  status       TEXT NOT NULL,             -- complete | pending
  payload      BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS audits (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id       TEXT NOT NULL UNIQUE,
  base_version     INTEGER NOT NULL,
  snapshot_version INTEGER NOT NULL,
  status           TEXT NOT NULL,         -- running | complete | aborted
  kind             TEXT NOT NULL,         -- full | incremental
  request_hash     TEXT NOT NULL,
  result           BLOB,
  error_code       TEXT,
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS error_records (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_version INTEGER NOT NULL,
  item_id          TEXT NOT NULL,
  node_id          TEXT NOT NULL,
  key_path         TEXT NOT NULL,
  channel          TEXT NOT NULL,
  abs_deviation    TEXT NOT NULL,
  rel_deviation    TEXT NOT NULL,
  error_bound      TEXT NOT NULL,
  within_bound     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  intent_type      TEXT NOT NULL,         -- audit | snapshot_replace | incremental
  request_id       TEXT,
  snapshot_version INTEGER,
  base_version     INTEGER,
  status           TEXT NOT NULL,         -- pending | committed | aborted
  detail           BLOB,
  created_at       TEXT NOT NULL,
  resolved_at      TEXT
);

CREATE INDEX IF NOT EXISTS idx_audits_version ON audits(snapshot_version);
CREATE INDEX IF NOT EXISTS idx_errors_version ON error_records(snapshot_version);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
`
