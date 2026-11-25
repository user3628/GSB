package store

const schemaSQL = `
CREATE TABLE IF NOT EXISTS schema_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('schema_version', '1');

CREATE TABLE IF NOT EXISTS plan_versions (
  plan_key TEXT NOT NULL,
  version_seq INTEGER NOT NULL CHECK (version_seq > 0),
  shape_fingerprint TEXT NOT NULL,
  structure_fingerprint TEXT NOT NULL,
  dense_fingerprint TEXT NOT NULL,
  sum_strategy TEXT NOT NULL,
  cache_budget INTEGER NOT NULL,
  peak_memory INTEGER NOT NULL,
  row_block_size INTEGER NOT NULL,
  col_block_size INTEGER NOT NULL,
  payload_hash TEXT NOT NULL,
  plan_digest TEXT NOT NULL,
  prev_digest TEXT NOT NULL,
  plan_json BLOB NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (plan_key, version_seq),
  UNIQUE (plan_key, plan_digest)
);

CREATE TABLE IF NOT EXISTS plan_heads (
  plan_key TEXT PRIMARY KEY,
  version_seq INTEGER NOT NULL,
  plan_digest TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  shape_fingerprint TEXT NOT NULL,
  structure_fingerprint TEXT NOT NULL,
  dense_fingerprint TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS block_results (
  plan_key TEXT NOT NULL,
  version_seq INTEGER NOT NULL,
  plan_digest TEXT NOT NULL,
  execution_row_index INTEGER NOT NULL,
  plan_row_block_index INTEGER NOT NULL,
  row_start INTEGER NOT NULL,
  row_end INTEGER NOT NULL,
  col_start INTEGER NOT NULL,
  col_end INTEGER NOT NULL,
  input_fingerprint TEXT NOT NULL,
  output_digest TEXT NOT NULL,
  trace_digest TEXT NOT NULL,
  error_digest TEXT NOT NULL,
  output_json BLOB NOT NULL,
  trace_json BLOB NOT NULL,
  error_json BLOB NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (plan_key, version_seq, execution_row_index)
);

CREATE TABLE IF NOT EXISTS error_reports (
  plan_key TEXT NOT NULL,
  version_seq INTEGER NOT NULL,
  execution_row_index INTEGER NOT NULL,
  report_digest TEXT NOT NULL,
  report_json BLOB NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (plan_key, version_seq, execution_row_index)
);

CREATE TABLE IF NOT EXISTS operator_state (
  plan_key TEXT PRIMARY KEY,
  version_seq INTEGER NOT NULL,
  plan_digest TEXT NOT NULL,
  last_execution_row_index INTEGER NOT NULL DEFAULT -1,
  last_row_start INTEGER NOT NULL DEFAULT 0,
  last_row_end INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plan_invalidations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  plan_key TEXT NOT NULL,
  status TEXT NOT NULL,
  reasons TEXT NOT NULL,
  invalidated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_plan_versions_key_seq ON plan_versions(plan_key, version_seq);
CREATE INDEX IF NOT EXISTS idx_block_results_version ON block_results(plan_key, version_seq, execution_row_index);
CREATE INDEX IF NOT EXISTS idx_error_reports_version ON error_reports(plan_key, version_seq, execution_row_index);
`
