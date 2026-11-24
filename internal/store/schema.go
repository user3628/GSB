package store

const schemaSQL = `
CREATE TABLE IF NOT EXISTS snapshots (
  version INTEGER PRIMARY KEY,
  generated_at TEXT NOT NULL,
  row_count INTEGER NOT NULL,
  payload TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  loaded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chains (
  chain_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  base_snapshot_version INTEGER NOT NULL,
  payload TEXT NOT NULL,
  state TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (chain_id, version)
);

CREATE TABLE IF NOT EXISTS executions (
  execution_id TEXT PRIMARY KEY,
  chain_id TEXT NOT NULL,
  chain_version INTEGER NOT NULL,
  snapshot_version INTEGER NOT NULL,
  bitmap_form TEXT NOT NULL,
  bitmap_json TEXT NOT NULL,
  batch_size INTEGER NOT NULL,
  large_threshold INTEGER NOT NULL,
  selected_rows INTEGER NOT NULL,
  path TEXT NOT NULL,
  decision TEXT NOT NULL,
  batch_count INTEGER NOT NULL,
  status TEXT NOT NULL,
  error_code TEXT NOT NULL DEFAULT '',
  error_context TEXT NOT NULL DEFAULT '{}',
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_executions_chain_version
  ON executions(chain_id, chain_version, started_at);

CREATE TABLE IF NOT EXISTS batches (
  execution_id TEXT NOT NULL,
  batch_no INTEGER NOT NULL,
  row_start INTEGER NOT NULL DEFAULT 0,
  row_end INTEGER NOT NULL DEFAULT 0,
  selected_count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  trace_json TEXT NOT NULL DEFAULT '',
  aggregate_json TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (execution_id, batch_no)
);

CREATE TABLE IF NOT EXISTS aggregate_results (
  execution_id TEXT PRIMARY KEY,
  aggregate_json TEXT NOT NULL,
  committed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  execution_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  chain_version INTEGER NOT NULL,
  kind TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  applied_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intents_execution ON intents(execution_id, id);

CREATE TABLE IF NOT EXISTS recovery_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  execution_id TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
`
