package store

const schemaSQL = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS snapshots (
  snapshot_id TEXT PRIMARY KEY,
  load_version INTEGER NOT NULL UNIQUE,
  content_hash TEXT NOT NULL,
  raw_json TEXT NOT NULL,
  loaded_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tenants (
  snapshot_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  tenant_name TEXT NOT NULL,
  priority INTEGER NOT NULL,
  PRIMARY KEY (snapshot_id, tenant_id),
  FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS window_specs (
  snapshot_id TEXT NOT NULL,
  spec_id TEXT NOT NULL,
  window_type TEXT NOT NULL,
  width_ms INTEGER NOT NULL,
  slide_ms INTEGER NOT NULL DEFAULT 0,
  session_gap_ms INTEGER NOT NULL DEFAULT 0,
  boundary TEXT NOT NULL,
  PRIMARY KEY (snapshot_id, spec_id),
  FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS events (
  snapshot_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  group_key TEXT NOT NULL,
  timestamp_ms INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  measure_present INTEGER NOT NULL,
  measure_scaled INTEGER,
  PRIMARY KEY (snapshot_id, event_id),
  FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS events_lookup ON events(snapshot_id, tenant_id, group_key, timestamp_ms, seq);

CREATE TABLE IF NOT EXISTS windows (
  snapshot_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  spec_id TEXT NOT NULL,
  group_key TEXT NOT NULL,
  start_ms INTEGER NOT NULL,
  end_ms INTEGER NOT NULL,
  boundary TEXT NOT NULL,
  status TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 0,
  event_count INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (snapshot_id, window_id),
  FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS windows_lookup ON windows(snapshot_id, tenant_id, spec_id, group_key, start_ms);

CREATE TABLE IF NOT EXISTS rounds (
  round_id TEXT PRIMARY KEY,
  snapshot_id TEXT NOT NULL,
  status TEXT NOT NULL,
  global_concurrency INTEGER NOT NULL,
  tenant_concurrency INTEGER NOT NULL,
  time_budget_ms INTEGER NOT NULL,
  queue_capacity INTEGER NOT NULL,
  task_delay_ms INTEGER NOT NULL DEFAULT 0,
  force_recalc INTEGER NOT NULL DEFAULT 0,
  considered_count INTEGER NOT NULL DEFAULT 0,
  dispatched_count INTEGER NOT NULL DEFAULT 0,
  committed_count INTEGER NOT NULL DEFAULT 0,
  shed_count INTEGER NOT NULL DEFAULT 0,
  aborted_count INTEGER NOT NULL DEFAULT 0,
  failed_count INTEGER NOT NULL DEFAULT 0,
  consumed_budget_ms INTEGER NOT NULL DEFAULT 0,
  remaining_budget_ms INTEGER NOT NULL DEFAULT 0,
  max_global_running INTEGER NOT NULL DEFAULT 0,
  max_tenant_running INTEGER NOT NULL DEFAULT 0,
  budgets_json TEXT NOT NULL,
  error_json TEXT,
  started_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER,
  FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS recalc_tasks (
  task_id TEXT PRIMARY KEY,
  round_id TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  priority INTEGER NOT NULL,
  window_id TEXT NOT NULL,
  group_key TEXT NOT NULL,
  spec_id TEXT NOT NULL,
  window_type TEXT NOT NULL,
  start_ms INTEGER NOT NULL,
  end_ms INTEGER NOT NULL,
  cost_ms INTEGER NOT NULL,
  status TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  consumed_ms INTEGER NOT NULL DEFAULT 0,
  result_json TEXT,
  error_json TEXT,
  shed_reason_json TEXT,
  created_at_ms INTEGER NOT NULL,
  started_at_ms INTEGER,
  finished_at_ms INTEGER,
  FOREIGN KEY (round_id) REFERENCES rounds(round_id) ON DELETE CASCADE,
  FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS recalc_tasks_round_uq ON recalc_tasks(round_id, snapshot_id, window_id, group_key);
CREATE UNIQUE INDEX IF NOT EXISTS recalc_tasks_active_window_uq ON recalc_tasks(snapshot_id, window_id, group_key)
  WHERE status IN ('QUEUED', 'RUNNING');
CREATE INDEX IF NOT EXISTS recalc_tasks_round_lookup ON recalc_tasks(round_id, status);

CREATE TABLE IF NOT EXISTS window_staging (
  attempt_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  group_key TEXT NOT NULL,
  spec_id TEXT NOT NULL,
  window_type TEXT NOT NULL,
  start_ms INTEGER NOT NULL,
  end_ms INTEGER NOT NULL,
  boundary TEXT NOT NULL,
  count_value INTEGER NOT NULL,
  sum_scaled INTEGER NOT NULL,
  min_scaled INTEGER,
  max_scaled INTEGER,
  sample_size INTEGER NOT NULL,
  rounding_mode TEXT NOT NULL,
  aggregate_json TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  FOREIGN KEY (task_id) REFERENCES recalc_tasks(task_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS window_staging_attempt_uq ON window_staging(task_id, attempt_id);

CREATE TABLE IF NOT EXISTS window_aggregates (
  snapshot_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  spec_id TEXT NOT NULL,
  group_key TEXT NOT NULL,
  window_type TEXT NOT NULL,
  start_ms INTEGER NOT NULL,
  end_ms INTEGER NOT NULL,
  boundary TEXT NOT NULL,
  count_value INTEGER NOT NULL,
  sum_scaled INTEGER NOT NULL,
  min_scaled INTEGER,
  max_scaled INTEGER,
  sample_size INTEGER NOT NULL,
  rounding_mode TEXT NOT NULL,
  version INTEGER NOT NULL,
  task_id TEXT NOT NULL,
  committed_at_ms INTEGER NOT NULL,
  PRIMARY KEY (snapshot_id, window_id),
  FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS window_aggregates_tenant ON window_aggregates(snapshot_id, tenant_id, group_key);

CREATE TABLE IF NOT EXISTS tenant_intermediate (
  snapshot_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  total_count INTEGER NOT NULL,
  total_sum_scaled INTEGER NOT NULL,
  min_scaled INTEGER,
  max_scaled INTEGER,
  committed_window_count INTEGER NOT NULL,
  group_count INTEGER NOT NULL,
  group_index_json TEXT NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (snapshot_id, tenant_id),
  FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS group_index (
  snapshot_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  group_key TEXT NOT NULL,
  version INTEGER NOT NULL,
  window_count INTEGER NOT NULL,
  total_count INTEGER NOT NULL,
  total_sum_scaled INTEGER NOT NULL,
  min_scaled INTEGER,
  max_scaled INTEGER,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (snapshot_id, tenant_id, group_key),
  FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS scheduler_trace (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  round_id TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  tenant_id TEXT NOT NULL DEFAULT '',
  window_id TEXT NOT NULL DEFAULT '',
  task_id TEXT NOT NULL DEFAULT '',
  group_key TEXT NOT NULL DEFAULT '',
  priority INTEGER NOT NULL DEFAULT 0,
  cost_ms INTEGER NOT NULL DEFAULT 0,
  consumed_before_ms INTEGER NOT NULL DEFAULT 0,
  consumed_after_ms INTEGER NOT NULL DEFAULT 0,
  remaining_budget_ms INTEGER NOT NULL DEFAULT 0,
  reason TEXT NOT NULL DEFAULT '',
  detail_json TEXT,
  at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS scheduler_trace_round ON scheduler_trace(round_id, sequence);

CREATE TABLE IF NOT EXISTS shed_decisions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  round_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  group_key TEXT NOT NULL,
  priority INTEGER NOT NULL,
  window_start_ms INTEGER NOT NULL,
  decision_order INTEGER NOT NULL,
  reason_code TEXT NOT NULL,
  basis_json TEXT NOT NULL,
  consumed_budget_ms INTEGER NOT NULL,
  remaining_budget_ms INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS shed_decisions_round ON shed_decisions(round_id, decision_order);

CREATE TABLE IF NOT EXISTS intents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  intent_id TEXT NOT NULL UNIQUE,
  intent_type TEXT NOT NULL,
  status TEXT NOT NULL,
  round_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  recovery_note TEXT NOT NULL DEFAULT '',
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS intents_pending ON intents(status, id);

CREATE TABLE IF NOT EXISTS commit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  group_key TEXT NOT NULL,
  committed_version INTEGER NOT NULL,
  aggregate_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS commit_log_tenant ON commit_log(snapshot_id, tenant_id, committed_version);

CREATE TABLE IF NOT EXISTS recovery_reports (
  report_id TEXT PRIMARY KEY,
  based_on_durable_facts INTEGER NOT NULL,
  pending_intent_count INTEGER NOT NULL,
  aborted_task_ids_json TEXT NOT NULL,
  reset_window_ids_json TEXT NOT NULL,
  details_json TEXT NOT NULL,
  disclaimer TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);
`
