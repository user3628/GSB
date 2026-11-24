package store

const schemaSQL = `
CREATE TABLE IF NOT EXISTS state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  current_version INTEGER NOT NULL
);
INSERT OR IGNORE INTO state(id, current_version) VALUES (1, 0);

CREATE TABLE IF NOT EXISTS snapshots (
  version INTEGER PRIMARY KEY,
  status TEXT NOT NULL,
  kind TEXT NOT NULL,
  format_name TEXT NOT NULL DEFAULT '',
  format_version INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot_state (
  version INTEGER PRIMARY KEY REFERENCES snapshots(version),
  threshold_micros INTEGER NOT NULL,
  tolerance_permille INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS strategies (
  version INTEGER NOT NULL REFERENCES snapshots(version),
  service_id TEXT NOT NULL,
  spec_json TEXT NOT NULL,
  source_json TEXT NOT NULL,
  source_format TEXT NOT NULL,
  source_version INTEGER NOT NULL,
  PRIMARY KEY (version, service_id)
);

CREATE TABLE IF NOT EXISTS quota_groups (
  version INTEGER NOT NULL REFERENCES snapshots(version),
  group_id TEXT NOT NULL,
  vram_bytes INTEGER NOT NULL,
  compute_milli INTEGER NOT NULL,
  PRIMARY KEY (version, group_id)
);

CREATE TABLE IF NOT EXISTS derived_services (
  version INTEGER NOT NULL REFERENCES snapshots(version),
  service_id TEXT NOT NULL,
  desired_replicas INTEGER NOT NULL,
  concurrency_need INTEGER NOT NULL,
  resource_cap INTEGER NOT NULL,
  placements_json TEXT NOT NULL,
  PRIMARY KEY (version, service_id)
);

CREATE TABLE IF NOT EXISTS derived_index (
  version INTEGER PRIMARY KEY REFERENCES snapshots(version),
  threshold_micros INTEGER NOT NULL,
  tolerance_permille INTEGER NOT NULL,
  index_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS references_v1 (
  version INTEGER NOT NULL REFERENCES snapshots(version),
  owner_service TEXT NOT NULL,
  ref_kind TEXT NOT NULL,
  target TEXT NOT NULL,
  PRIMARY KEY (version, owner_service, ref_kind, target)
);

CREATE TABLE IF NOT EXISTS intents (
  intent_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  base_version INTEGER NOT NULL,
  target_version INTEGER NOT NULL,
  status TEXT NOT NULL,
  details_json TEXT NOT NULL,
  error_code TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS intents_status_idx ON intents(status);

CREATE TABLE IF NOT EXISTS migration_batches (
  batch_id TEXT PRIMARY KEY,
  intent_id TEXT NOT NULL,
  base_version INTEGER NOT NULL,
  target_version INTEGER NOT NULL,
  status TEXT NOT NULL,
  decision_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS import_audits (
  version INTEGER PRIMARY KEY REFERENCES snapshots(version),
  intent_id TEXT NOT NULL,
  format_name TEXT NOT NULL,
  format_version INTEGER NOT NULL,
  negotiation_json TEXT NOT NULL,
  migration_json TEXT NOT NULL,
  payload_sha256 TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rebuild_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  version INTEGER NOT NULL REFERENCES snapshots(version),
  event_type TEXT NOT NULL,
  affected_services_json TEXT NOT NULL,
  reason TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  current_version INTEGER NOT NULL,
  last_complete_version INTEGER NOT NULL,
  report_json TEXT NOT NULL
);
`
