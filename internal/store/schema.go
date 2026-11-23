package store

const schemaSQL = `
CREATE TABLE IF NOT EXISTS schema_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS server_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  last_committed_version INTEGER NOT NULL,
  last_reference_time_ms INTEGER NOT NULL,
  last_config_version TEXT NOT NULL,
  last_committed_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS config_versions (
  version TEXT PRIMARY KEY,
  snapshot_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL,
  planned_window_version INTEGER NOT NULL,
  config_version TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  mode TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending','committed','aborted')),
  detail_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  UNIQUE(batch_id, planned_window_version)
);
CREATE INDEX IF NOT EXISTS idx_intents_state ON intents(state);
CREATE INDEX IF NOT EXISTS idx_intents_batch ON intents(batch_id);

CREATE TABLE IF NOT EXISTS intent_nonce_refs (
  intent_id INTEGER NOT NULL REFERENCES intents(id),
  client_id TEXT NOT NULL,
  nonce TEXT NOT NULL,
  PRIMARY KEY (intent_id, client_id, nonce)
);
CREATE INDEX IF NOT EXISTS idx_intent_nonce_refs_nonce ON intent_nonce_refs(client_id, nonce);

CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  content_hash TEXT NOT NULL,
  mode TEXT NOT NULL,
  reference_time_ms INTEGER NOT NULL,
  reference_source TEXT NOT NULL,
  window_version INTEGER NOT NULL UNIQUE,
  config_version TEXT NOT NULL,
  outcome TEXT NOT NULL,
  code TEXT NOT NULL,
  message TEXT NOT NULL,
  request_count INTEGER NOT NULL,
  prechecked_count INTEGER NOT NULL,
  precheck_limit INTEGER NOT NULL,
  total_weight INTEGER NOT NULL,
  sample_rate REAL NOT NULL,
  precheck_method TEXT NOT NULL,
  confidence TEXT NOT NULL,
  confidence_note TEXT NOT NULL,
  boundary_rule TEXT NOT NULL,
  committed_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_version ON batches(window_version);
CREATE INDEX IF NOT EXISTS idx_batches_content ON batches(content_hash);

CREATE TABLE IF NOT EXISTS batch_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL REFERENCES batches(batch_id),
  ordinal INTEGER NOT NULL,
  request_id TEXT NOT NULL,
  client_id TEXT NOT NULL,
  nonce TEXT NOT NULL,
  result TEXT NOT NULL,
  code TEXT NOT NULL,
  window_version INTEGER NOT NULL,
  config_version TEXT NOT NULL,
  item_json TEXT NOT NULL,
  UNIQUE(batch_id, ordinal)
);
CREATE INDEX IF NOT EXISTS idx_batch_items_batch ON batch_items(batch_id, ordinal);
CREATE INDEX IF NOT EXISTS idx_batch_items_nonce ON batch_items(client_id, nonce);

CREATE TABLE IF NOT EXISTS nonces (
  client_id TEXT NOT NULL,
  nonce TEXT NOT NULL,
  first_occupied_at_ms INTEGER NOT NULL,
  window_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL,
  weight INTEGER NOT NULL,
  first_batch_id TEXT NOT NULL,
  first_request_id TEXT NOT NULL,
  window_version INTEGER NOT NULL,
  config_version TEXT NOT NULL,
  PRIMARY KEY (client_id, nonce)
);
CREATE INDEX IF NOT EXISTS idx_nonces_expiry ON nonces(expires_at_ms);

CREATE TABLE IF NOT EXISTS nonce_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id TEXT NOT NULL,
  nonce TEXT NOT NULL,
  event_type TEXT NOT NULL CHECK (event_type IN ('occupy','reclaim')),
  at_ms INTEGER NOT NULL,
  first_occupied_at_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL,
  window_ms INTEGER NOT NULL,
  weight INTEGER NOT NULL,
  batch_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  window_version INTEGER NOT NULL,
  config_version TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_nonce_events_version ON nonce_events(window_version, id);
CREATE INDEX IF NOT EXISTS idx_nonce_events_key ON nonce_events(client_id, nonce, id);

CREATE TABLE IF NOT EXISTS audit_trail (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL,
  window_version INTEGER NOT NULL,
  config_version TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  request_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  outcome TEXT NOT NULL,
  code TEXT NOT NULL,
  at_ms INTEGER NOT NULL,
  details_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_batch ON audit_trail(batch_id, id);
CREATE INDEX IF NOT EXISTS idx_audit_version ON audit_trail(window_version, id);

CREATE TABLE IF NOT EXISTS recovery_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER NOT NULL,
  pending_intents INTEGER NOT NULL,
  confirmed_committed INTEGER NOT NULL,
  aborted INTEGER NOT NULL,
  facts_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_recovery_finished ON recovery_reports(finished_at_ms DESC);
`
