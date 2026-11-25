// Package storage owns the SQLite persistence facts: schema, versioned
// snapshots, write-ahead intent records and all durable tables used by the
// engine. It uses the pure-Go modernc.org/sqlite driver (no cgo).
package storage

const schemaSQL = `
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS meta(
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- Durable input batches; whole batch is rejected before any row is inserted.
CREATE TABLE IF NOT EXISTS services(
  id              TEXT PRIMARY KEY,
  zone            TEXT NOT NULL,
  weight_mille    INTEGER NOT NULL,
  upstreams       TEXT NOT NULL,
  sensitivity     INTEGER NOT NULL,
  silence_window  INTEGER NOT NULL,
  version         INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS observations(
  seq           INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp_ms  INTEGER NOT NULL,
  service_id    TEXT NOT NULL,
  metric        TEXT NOT NULL,
  value         INTEGER NOT NULL,
  labels_json   TEXT NOT NULL,
  batch_id      TEXT NOT NULL,
  base_version  INTEGER NOT NULL,
  commit_version INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_obs_commit ON observations(commit_version);

CREATE TABLE IF NOT EXISTS policy_versions(
  version              INTEGER PRIMARY KEY,
  checksum             TEXT NOT NULL,
  payload_json         TEXT NOT NULL,
  suppression_window   INTEGER NOT NULL,
  concurrency_limit    INTEGER NOT NULL,
  round_budget         INTEGER NOT NULL,
  tolerance_mille      INTEGER NOT NULL,
  created_at_ms        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts(
  id                TEXT PRIMARY KEY,
  version           INTEGER NOT NULL,
  fingerprint       TEXT NOT NULL,
  service_id        TEXT NOT NULL,
  zone              TEXT NOT NULL,
  metric            TEXT NOT NULL,
  labels_json       TEXT NOT NULL,
  first_seen_ms     INTEGER NOT NULL,
  last_seen_ms      INTEGER NOT NULL,
  window_start_ms   INTEGER NOT NULL,
  window_end_ms     INTEGER NOT NULL,
  state             TEXT NOT NULL,
  merged_count      INTEGER NOT NULL,
  suppressed_by_json TEXT NOT NULL,
  primary_reason    TEXT NOT NULL,
  release_at_ms     INTEGER NOT NULL,
  source_obs_json   TEXT NOT NULL,
  last_obs_seq      INTEGER NOT NULL,
  batch_id          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alerts_fp_ver ON alerts(fingerprint, version);
CREATE INDEX IF NOT EXISTS idx_alerts_state ON alerts(state);

CREATE TABLE IF NOT EXISTS suppression_hits(
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id      TEXT NOT NULL,
  version       INTEGER NOT NULL,
  rule_id       TEXT NOT NULL,
  rule_service  TEXT NOT NULL,
  rule_zone     TEXT NOT NULL,
  hop           INTEGER NOT NULL,
  order_index   INTEGER NOT NULL,
  matched_at_ms INTEGER NOT NULL,
  release_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_hits_alert ON suppression_hits(alert_id, version);

CREATE TABLE IF NOT EXISTS root_cause_scores(
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  version             INTEGER NOT NULL,
  trigger_alert_id    TEXT NOT NULL,
  candidate_service   TEXT NOT NULL,
  hops                INTEGER NOT NULL,
  time_delta_ms       INTEGER NOT NULL,
  score_raw_milli     INTEGER NOT NULL,
  score_final_milli   INTEGER NOT NULL,
  rounding_error_nano INTEGER NOT NULL,
  rank                INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rcs_trigger ON root_cause_scores(trigger_alert_id, version);

-- Derived-structure reference registry used for cascade invalidation.
CREATE TABLE IF NOT EXISTS derived_refs(
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  version         INTEGER NOT NULL,
  service_id      TEXT NOT NULL,
  fingerprint     TEXT NOT NULL,
  ref_kind        TEXT NOT NULL,
  ref_key         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_refs_svc ON derived_refs(service_id, version);

-- Idempotent request ledger.
CREATE TABLE IF NOT EXISTS requests(
  request_id   TEXT PRIMARY KEY,
  request_kind TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  status       TEXT NOT NULL,
  result_json  TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);

-- Durable write-ahead intent records for crash recovery.
CREATE TABLE IF NOT EXISTS intents(
  intent_id    TEXT PRIMARY KEY,
  kind         TEXT NOT NULL,
  status       TEXT NOT NULL,
  version      INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);
`
