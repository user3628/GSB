package store

const schemaSQL = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS volumes (
    id TEXT PRIMARY KEY,
    block_count INTEGER NOT NULL CHECK (block_count > 0),
    block_size INTEGER NOT NULL CHECK (block_size > 0),
    overhead_bytes INTEGER NOT NULL CHECK (overhead_bytes >= 0),
    last_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blocks (
    volume_id TEXT NOT NULL,
    block_no INTEGER NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 0),
    written_at TEXT NOT NULL,
    digest TEXT NOT NULL,
    content BLOB,
    is_zero INTEGER NOT NULL CHECK (is_zero IN (0,1)),
    PRIMARY KEY (volume_id, block_no),
    FOREIGN KEY (volume_id) REFERENCES volumes(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS block_versions (
    volume_id TEXT NOT NULL,
    block_no INTEGER NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 0),
    written_at TEXT NOT NULL,
    digest TEXT NOT NULL,
    content BLOB,
    is_zero INTEGER NOT NULL CHECK (is_zero IN (0,1)),
    PRIMARY KEY (volume_id, block_no, version),
    FOREIGN KEY (volume_id) REFERENCES volumes(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS snapshots (
    volume_id TEXT NOT NULL,
    id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('full','incremental','merged')),
    parent_id TEXT,
    consistency_seq INTEGER NOT NULL,
    weight INTEGER NOT NULL CHECK (weight >= 0),
    ordinal INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (volume_id, id),
    UNIQUE (volume_id, ordinal),
    FOREIGN KEY (volume_id) REFERENCES volumes(id) ON DELETE CASCADE,
    FOREIGN KEY (volume_id, parent_id) REFERENCES snapshots(volume_id, id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS snapshot_refs (
    volume_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    block_no INTEGER NOT NULL,
    block_version INTEGER NOT NULL,
    PRIMARY KEY (volume_id, snapshot_id, block_no),
    FOREIGN KEY (volume_id, snapshot_id) REFERENCES snapshots(volume_id, id) ON DELETE CASCADE,
    FOREIGN KEY (volume_id, block_no, block_version) REFERENCES block_versions(volume_id, block_no, version)
);

CREATE TABLE IF NOT EXISTS snapshot_metrics (
    volume_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    computed_seq INTEGER NOT NULL,
    metrics_json TEXT NOT NULL,
    PRIMARY KEY (volume_id, snapshot_id),
    FOREIGN KEY (volume_id, snapshot_id) REFERENCES snapshots(volume_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    volume_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','completed')),
    http_status INTEGER NOT NULL DEFAULT 0,
    response_json TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    volume_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','committed','aborted','aborted_recovered')),
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_intents_state ON intents(state, created_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_volume ON snapshots(volume_id, ordinal);
CREATE INDEX IF NOT EXISTS idx_snapshot_refs_snapshot ON snapshot_refs(volume_id, snapshot_id);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    status TEXT NOT NULL,
    pending_found INTEGER NOT NULL,
    aborted_count INTEGER NOT NULL,
    committed_count INTEGER NOT NULL,
    details_json TEXT NOT NULL
);
`
