package engine

const schemaSQL = `
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS commits (
  commit_id    TEXT PRIMARY KEY,
  kind         TEXT NOT NULL,             -- snapshot | delta
  version      INTEGER NOT NULL,
  base_version INTEGER NOT NULL,
  checksum     TEXT NOT NULL,
  status       TEXT NOT NULL,             -- committed | aborted
  created_at   TEXT NOT NULL,
  payload      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
  intent_id    TEXT PRIMARY KEY,
  commit_id    TEXT NOT NULL,
  kind         TEXT NOT NULL,
  target_version INTEGER NOT NULL,
  base_version INTEGER NOT NULL,
  status       TEXT NOT NULL,             -- prepared | completed | aborted
  phase        TEXT NOT NULL,             -- data_written | derived_built
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  pending_keys TEXT NOT NULL,             -- JSON array
  payload      TEXT NOT NULL
);

-- Current key-path values, versioned row-wise.
CREATE TABLE IF NOT EXISTS config_items (
  key_path TEXT PRIMARY KEY,
  type     TEXT NOT NULL,
  value    TEXT NOT NULL,
  ord      INTEGER NOT NULL,
  version  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS components (
  component_id TEXT PRIMARY KEY,
  wildcard     INTEGER NOT NULL,
  spec_json    TEXT NOT NULL,
  version      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriptions (
  component_id TEXT NOT NULL,
  prefix       TEXT NOT NULL,
  version      INTEGER NOT NULL,
  PRIMARY KEY (component_id, prefix)
);

CREATE TABLE IF NOT EXISTS dependencies (
  component_id TEXT NOT NULL,
  upstream_id  TEXT NOT NULL,
  version      INTEGER NOT NULL,
  PRIMARY KEY (component_id, upstream_id)
);

-- Derived structures: effective views.
CREATE TABLE IF NOT EXISTS views (
  component_id TEXT PRIMARY KEY,
  version      INTEGER NOT NULL,
  digest       TEXT NOT NULL,
  view_json    TEXT NOT NULL
);

-- Derived structures: reference relations used for cascade invalidation.
CREATE TABLE IF NOT EXISTS view_refs (
  component_id TEXT NOT NULL,
  ref_kind     TEXT NOT NULL,   -- key | prefix | dep_edge
  ref_key      TEXT NOT NULL,
  version      INTEGER NOT NULL,
  PRIMARY KEY (component_id, ref_kind, ref_key)
);

-- Derived structures: affected sets, one row set per committed version.
CREATE TABLE IF NOT EXISTS affected_sets (
  version       INTEGER PRIMARY KEY,
  trigger_keys  TEXT NOT NULL,
  unconverged   INTEGER NOT NULL,
  set_json      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at  TEXT NOT NULL,
  report_json TEXT NOT NULL
);

-- Idempotency records: request body plus the cached commit result.
CREATE TABLE IF NOT EXISTS idempotency (
  commit_id   TEXT PRIMARY KEY,
  request     TEXT NOT NULL,
  result_json TEXT NOT NULL,
  created_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
CREATE INDEX IF NOT EXISTS idx_commits_version ON commits(version);
CREATE INDEX IF NOT EXISTS idx_view_refs_lookup ON view_refs(ref_kind, ref_key);
`
