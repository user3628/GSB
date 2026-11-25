package bus

const schemaSQL = `
CREATE TABLE IF NOT EXISTS topics (
  topic TEXT PRIMARY KEY,
  partitions INTEGER NOT NULL CHECK (partitions > 0),
  created_at_ms INTEGER NOT NULL,
  intent_id TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS topic_groups (
  topic TEXT NOT NULL,
  group_name TEXT NOT NULL,
  algorithm TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  intent_id TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (topic, group_name),
  FOREIGN KEY (topic) REFERENCES topics(topic)
);

CREATE TABLE IF NOT EXISTS partition_counters (
  topic TEXT NOT NULL,
  partition INTEGER NOT NULL,
  last_seq INTEGER NOT NULL DEFAULT 0,
  earliest_seq INTEGER NOT NULL DEFAULT 1,
  last_intent_id TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (topic, partition),
  FOREIGN KEY (topic) REFERENCES topics(topic)
);

CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  topic TEXT NOT NULL,
  partition INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  partition_key TEXT NOT NULL,
  event_time_ms INTEGER NOT NULL,
  body_digest TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  aggregate_key TEXT NOT NULL,
  metric REAL NOT NULL,
  content_hash TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  intent_id TEXT NOT NULL DEFAULT '',
  UNIQUE (topic, partition, seq),
  FOREIGN KEY (topic) REFERENCES topics(topic)
);
CREATE INDEX IF NOT EXISTS idx_events_route ON events(topic, partition, seq);

CREATE TABLE IF NOT EXISTS consumer_offsets (
  topic TEXT NOT NULL,
  group_name TEXT NOT NULL,
  partition INTEGER NOT NULL,
  acked_offset INTEGER NOT NULL DEFAULT 0,
  replay_until INTEGER NOT NULL DEFAULT 0,
  replay_active INTEGER NOT NULL DEFAULT 0,
  version INTEGER NOT NULL DEFAULT 0,
  lease_token TEXT NOT NULL DEFAULT '',
  lease_owner TEXT NOT NULL DEFAULT '',
  lease_expires_at_ms INTEGER NOT NULL DEFAULT 0,
  updated_at_ms INTEGER NOT NULL,
  last_intent_id TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (topic, group_name, partition),
  FOREIGN KEY (topic, group_name) REFERENCES topic_groups(topic, group_name)
);

CREATE TABLE IF NOT EXISTS event_states (
  topic TEXT NOT NULL,
  group_name TEXT NOT NULL,
  partition INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  event_id TEXT NOT NULL,
  state TEXT NOT NULL,
  delivery_count INTEGER NOT NULL DEFAULT 0,
  lease_token TEXT NOT NULL DEFAULT '',
  updated_at_ms INTEGER NOT NULL,
  transition_reason TEXT NOT NULL DEFAULT '',
  last_intent_id TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (topic, group_name, partition, seq),
  FOREIGN KEY (topic, group_name) REFERENCES topic_groups(topic, group_name),
  FOREIGN KEY (event_id) REFERENCES events(event_id)
);
CREATE INDEX IF NOT EXISTS idx_event_states_lease ON event_states(topic, group_name, partition, lease_token, state);

CREATE TABLE IF NOT EXISTS acks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic TEXT NOT NULL,
  group_name TEXT NOT NULL,
  partition INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  event_id TEXT NOT NULL,
  active INTEGER NOT NULL,
  intent_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  acked_at_ms INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_acks_active ON acks(topic, group_name, partition, seq) WHERE active = 1;
CREATE INDEX IF NOT EXISTS idx_acks_intent ON acks(intent_id);

CREATE TABLE IF NOT EXISTS offset_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic TEXT NOT NULL,
  group_name TEXT NOT NULL,
  partition INTEGER NOT NULL,
  old_offset INTEGER NOT NULL,
  new_offset INTEGER NOT NULL,
  reason TEXT NOT NULL,
  intent_id TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_offset_history_intent ON offset_history(intent_id);

CREATE TABLE IF NOT EXISTS agg_states (
  topic TEXT NOT NULL,
  group_name TEXT NOT NULL,
  partition INTEGER NOT NULL,
  event_key TEXT NOT NULL,
  event_count INTEGER NOT NULL,
  sum_value REAL NOT NULL,
  compensation REAL NOT NULL,
  sum_abs REAL NOT NULL,
  buckets_json TEXT NOT NULL,
  abs_error_bound REAL NOT NULL,
  PRIMARY KEY (topic, group_name, partition, event_key)
);

CREATE TABLE IF NOT EXISTS agg_batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic TEXT NOT NULL,
  group_name TEXT NOT NULL,
  partition INTEGER NOT NULL,
  ack_batch_id TEXT NOT NULL,
  event_batch_ids_json TEXT NOT NULL,
  start_seq INTEGER NOT NULL,
  end_seq INTEGER NOT NULL,
  algorithm TEXT NOT NULL,
  event_count INTEGER NOT NULL,
  partials_json TEXT NOT NULL,
  sum_abs REAL NOT NULL,
  abs_error_bound REAL NOT NULL,
  relative_error_bound REAL NOT NULL,
  state_digest TEXT NOT NULL,
  previous_offset INTEGER NOT NULL,
  new_offset INTEGER NOT NULL,
  version INTEGER NOT NULL,
  active INTEGER NOT NULL,
  intent_id TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  reverted_at_ms INTEGER,
  reverted_by_intent_id TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_agg_batches_partition ON agg_batches(topic, group_name, partition, id);
CREATE INDEX IF NOT EXISTS idx_agg_batches_intent ON agg_batches(intent_id);

CREATE TABLE IF NOT EXISTS agg_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic TEXT NOT NULL,
  group_name TEXT NOT NULL,
  partition INTEGER NOT NULL,
  offset_value INTEGER NOT NULL,
  version INTEGER NOT NULL,
  algorithm TEXT NOT NULL,
  state_json TEXT NOT NULL,
  errors_json TEXT NOT NULL,
  state_digest TEXT NOT NULL,
  batch_id INTEGER,
  active INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_agg_snapshots_lookup ON agg_snapshots(topic, group_name, partition, active, offset_value, id);

CREATE TABLE IF NOT EXISTS intents (
  id TEXT PRIMARY KEY,
  operation TEXT NOT NULL,
  request_id TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  topic TEXT NOT NULL,
  group_name TEXT NOT NULL,
  partition INTEGER NOT NULL,
  status TEXT NOT NULL,
  prior_json TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status, id);

CREATE TABLE IF NOT EXISTS idempotency (
  request_id TEXT PRIMARY KEY,
  operation TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  response_json TEXT NOT NULL DEFAULT '',
  error_code TEXT NOT NULL DEFAULT '',
  error_message TEXT NOT NULL DEFAULT '',
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER NOT NULL,
  pending_scanned INTEGER NOT NULL,
  committed_recovered INTEGER NOT NULL,
  rolled_back INTEGER NOT NULL,
  leases_released INTEGER NOT NULL,
  deliveries_reset INTEGER NOT NULL,
  details_json TEXT NOT NULL
);
`
