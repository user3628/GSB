package store

const schemaSQL = `
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;
PRAGMA synchronous=FULL;

CREATE TABLE IF NOT EXISTS schema_meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quota_groups (
	id               TEXT PRIMARY KEY,
	memory_limit     INTEGER NOT NULL,
	compute_limit_m  INTEGER NOT NULL,
	memory_reserved  INTEGER NOT NULL DEFAULT 0,
	compute_reserved INTEGER NOT NULL DEFAULT 0,
	pick_counter     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS nodes (
	id              TEXT PRIMARY KEY,
	group_id        TEXT NOT NULL REFERENCES quota_groups(id),
	memory_total    INTEGER NOT NULL,
	compute_total_m INTEGER NOT NULL,
	memory_used     INTEGER NOT NULL DEFAULT 0,
	compute_used_m  INTEGER NOT NULL DEFAULT 0,
	health          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_nodes_group ON nodes(group_id);

CREATE TABLE IF NOT EXISTS requests (
	id               TEXT PRIMARY KEY,
	model_id         TEXT NOT NULL,
	group_id         TEXT NOT NULL REFERENCES quota_groups(id),
	memory_need      INTEGER NOT NULL,
	compute_need_m   INTEGER NOT NULL,
	samples_per_batch INTEGER NOT NULL,
	latency_limit_ms INTEGER NOT NULL,
	priority         INTEGER NOT NULL,
	state            TEXT NOT NULL,
	seq              INTEGER NOT NULL,
	assigned_node_id TEXT NOT NULL DEFAULT '',
	policy_version   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_req_state ON requests(state);
CREATE INDEX IF NOT EXISTS idx_req_group ON requests(group_id);

CREATE TABLE IF NOT EXISTS policy_snapshots (
	version     INTEGER PRIMARY KEY,
	digest      TEXT NOT NULL UNIQUE,
	prev_digest TEXT NOT NULL DEFAULT '',
	params_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_fragments (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	version     INTEGER NOT NULL,
	base_digest TEXT NOT NULL,
	digest      TEXT NOT NULL UNIQUE,
	fields_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rounds (
	id             INTEGER PRIMARY KEY AUTOINCREMENT,
	policy_version INTEGER NOT NULL,
	status         TEXT NOT NULL,
	budget_ms      INTEGER NOT NULL,
	started_at     TEXT NOT NULL,
	finished_at    TEXT NOT NULL DEFAULT '',
	report_json    TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS batches (
	id            TEXT PRIMARY KEY,
	round_id      INTEGER NOT NULL REFERENCES rounds(id),
	model_id      TEXT NOT NULL,
	node_id       TEXT NOT NULL,
	group_id      TEXT NOT NULL,
	priority      INTEGER NOT NULL,
	status        TEXT NOT NULL,
	total_samples INTEGER NOT NULL,
	total_memory  INTEGER NOT NULL,
	est_latency_ms INTEGER NOT NULL,
	seq           INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batch_round ON batches(round_id);
CREATE INDEX IF NOT EXISTS idx_batch_status ON batches(status);

CREATE TABLE IF NOT EXISTS batch_members (
	batch_id   TEXT NOT NULL REFERENCES batches(id),
	request_id TEXT NOT NULL REFERENCES requests(id),
	position   INTEGER NOT NULL,
	PRIMARY KEY (batch_id, request_id)
);
CREATE INDEX IF NOT EXISTS idx_bm_req ON batch_members(request_id);

CREATE TABLE IF NOT EXISTS intents (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	intent_type TEXT NOT NULL,
	round_id   INTEGER NOT NULL DEFAULT 0,
	request_id TEXT NOT NULL DEFAULT '',
	batch_id   TEXT NOT NULL DEFAULT '',
	payload    TEXT NOT NULL,
	status     TEXT NOT NULL,
	created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intent_status ON intents(status);
`
