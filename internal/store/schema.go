package store

const schemaSQL = `
CREATE TABLE IF NOT EXISTS state (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
INSERT OR IGNORE INTO state(key, value) VALUES('zone_epoch', 0);

CREATE TABLE IF NOT EXISTS zones (
    zone TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    generated_at_ms INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rrsets (
    zone TEXT NOT NULL,
    owner TEXT NOT NULL,
    rrtype INTEGER NOT NULL,
    ttl INTEGER NOT NULL,
    records_json TEXT NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY (zone, owner, rrtype)
);

CREATE TABLE IF NOT EXISTS zone_derived (
    zone TEXT NOT NULL,
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    owner TEXT NOT NULL,
    rrtype INTEGER NOT NULL,
    source_owner TEXT NOT NULL,
    source_type INTEGER NOT NULL,
    refs_json TEXT NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY (zone, kind, key)
);
CREATE INDEX IF NOT EXISTS idx_zone_derived_owner ON zone_derived(zone, owner);

CREATE TABLE IF NOT EXISTS cache_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cache_key TEXT NOT NULL UNIQUE,
    qname TEXT NOT NULL,
    qtype INTEGER NOT NULL,
    status TEXT NOT NULL,
    source_type INTEGER NOT NULL,
    zone TEXT NOT NULL DEFAULT '',
    zone_version INTEGER NOT NULL DEFAULT 0,
    written_ms INTEGER NOT NULL,
    ttl_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    intent_id TEXT NOT NULL DEFAULT '',
    created_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cache_zone ON cache_entries(zone);
CREATE INDEX IF NOT EXISTS idx_cache_expiry ON cache_entries(written_ms, ttl_ms);
CREATE INDEX IF NOT EXISTS idx_cache_intent ON cache_entries(intent_id);

CREATE TABLE IF NOT EXISTS cache_deps (
    cache_id INTEGER NOT NULL,
    zone TEXT NOT NULL,
    zone_version INTEGER NOT NULL,
    owner TEXT NOT NULL,
    rrtype INTEGER NOT NULL,
    PRIMARY KEY (cache_id, zone, owner, rrtype),
    FOREIGN KEY (cache_id) REFERENCES cache_entries(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_cache_deps_zone ON cache_deps(zone);

CREATE TABLE IF NOT EXISTS intention_log (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    target TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intention_status ON intention_log(status);

CREATE TABLE IF NOT EXISTS idempotency (
    kind TEXT NOT NULL,
    request_key TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    code TEXT NOT NULL DEFAULT '',
    message TEXT NOT NULL DEFAULT '',
    result_json TEXT NOT NULL DEFAULT '',
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (kind, request_key)
);

CREATE TABLE IF NOT EXISTS zone_batches (
    batch_id TEXT PRIMARY KEY,
    zone TEXT NOT NULL,
    kind TEXT NOT NULL,
    base_version INTEGER NOT NULL,
    target_version INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    status TEXT NOT NULL,
    code TEXT NOT NULL DEFAULT '',
    message TEXT NOT NULL DEFAULT '',
    result_json TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_zone_batches_status ON zone_batches(status);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_ms INTEGER NOT NULL,
    finished_ms INTEGER NOT NULL,
    report_json TEXT NOT NULL
);
`
