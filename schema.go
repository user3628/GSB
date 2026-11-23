package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS topics (
  name TEXT PRIMARY KEY,
  partitions INTEGER NOT NULL CHECK (partitions > 0),
  routing_rule TEXT NOT NULL CHECK (routing_rule IN ('hash','explicit')),
  created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS partitions (
  topic TEXT NOT NULL,
  partition_no INTEGER NOT NULL,
  next_seq INTEGER NOT NULL DEFAULT 0,
  visible_frontier INTEGER NOT NULL DEFAULT 0,
  consume_cursor INTEGER NOT NULL DEFAULT 0,
  earliest_retained_seq INTEGER NOT NULL DEFAULT 0,
  priority INTEGER NOT NULL DEFAULT 100,
  lease_state TEXT NOT NULL DEFAULT 'FREE' CHECK (lease_state IN ('FREE','LEASED','EXPIRED')),
  lease_owner TEXT,
  lease_version INTEGER NOT NULL DEFAULT 0,
  lease_expires_at_ms INTEGER,
  lease_cursor INTEGER NOT NULL DEFAULT 0,
  lease_last_delivered_seq INTEGER,
  replay_active INTEGER NOT NULL DEFAULT 0,
  replay_from INTEGER,
  replay_limit INTEGER,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (topic, partition_no),
  FOREIGN KEY (topic) REFERENCES topics(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS messages (
  message_id TEXT PRIMARY KEY,
  topic TEXT NOT NULL,
  partition_no INTEGER NOT NULL,
  partition_key TEXT NOT NULL DEFAULT '',
  seq INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PENDING_INVISIBLE','VISIBLE','CONSUMED','ROLLED_BACK','DEAD_LETTERED')),
  transactional INTEGER NOT NULL DEFAULT 0,
  txn_id TEXT,
  body_digest TEXT NOT NULL,
  body TEXT NOT NULL,
  produced_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  UNIQUE (topic, partition_no, seq),
  FOREIGN KEY (topic, partition_no) REFERENCES partitions(topic, partition_no)
);
CREATE INDEX IF NOT EXISTS idx_messages_partition_status ON messages(topic, partition_no, status, seq);
CREATE INDEX IF NOT EXISTS idx_messages_txn ON messages(txn_id);

CREATE TABLE IF NOT EXISTS consumer_offsets (
  topic TEXT NOT NULL,
  partition_no INTEGER NOT NULL,
  committed_seq INTEGER NOT NULL DEFAULT 0,
  lease_state TEXT NOT NULL DEFAULT 'FREE' CHECK (lease_state IN ('FREE','LEASED','EXPIRED')),
  lease_owner TEXT,
  lease_version INTEGER NOT NULL DEFAULT 0,
  lease_expires_at_ms INTEGER,
  lease_cursor INTEGER NOT NULL DEFAULT 0,
  replay_active INTEGER NOT NULL DEFAULT 0,
  replay_from INTEGER,
  replay_limit INTEGER,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (topic, partition_no),
  FOREIGN KEY (topic, partition_no) REFERENCES partitions(topic, partition_no) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS txn_visibility (
  txn_id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL UNIQUE,
  topic TEXT NOT NULL,
  partition_no INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PENDING','COMMITTED','ROLLED_BACK')),
  decision TEXT,
  query_key TEXT,
  query_count INTEGER NOT NULL DEFAULT 0,
  coordinator_result TEXT,
  queried_at_ms INTEGER,
  decided_at_ms INTEGER,
  FOREIGN KEY (message_id) REFERENCES messages(message_id)
);
CREATE INDEX IF NOT EXISTS idx_txn_pending ON txn_visibility(status, topic, partition_no, seq);

CREATE TABLE IF NOT EXISTS state_transitions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entity_kind TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  from_state TEXT NOT NULL,
  to_state TEXT NOT NULL,
  reason TEXT NOT NULL,
  at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS requests (
  request_id TEXT PRIMARY KEY,
  operation TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('RUNNING','COMMITTED','FAILED')),
  response_json TEXT,
  error_code TEXT,
  error_message TEXT,
  created_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER
);

CREATE TABLE IF NOT EXISTS intents (
  intent_id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL,
  operation TEXT NOT NULL,
  target TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PREPARED','COMMITTED','ABORTED','RECOVERED')),
  result_json TEXT,
  recovery_action TEXT,
  recovery_evidence TEXT,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
CREATE INDEX IF NOT EXISTS idx_intents_request ON intents(request_id);

CREATE TABLE IF NOT EXISTS injections (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  target TEXT NOT NULL,
  mode TEXT NOT NULL,
  delay_ms INTEGER NOT NULL DEFAULT 0,
  payload TEXT NOT NULL DEFAULT '',
  active INTEGER NOT NULL DEFAULT 1,
  created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_injections_lookup ON injections(kind, target, active, created_at_ms);

CREATE TABLE IF NOT EXISTS injection_observations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  injection_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  mode TEXT NOT NULL,
  target TEXT NOT NULL,
  phase TEXT NOT NULL,
  observed_code TEXT NOT NULL DEFAULT '',
  detail TEXT NOT NULL DEFAULT '',
  request_id TEXT NOT NULL DEFAULT '',
  observed_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_observations_recent ON injection_observations(id DESC);

CREATE TABLE IF NOT EXISTS scheduler_rounds (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at_ms INTEGER NOT NULL,
  budget_ms INTEGER NOT NULL,
  consumed_ms INTEGER NOT NULL,
  remaining_ms INTEGER NOT NULL,
  status TEXT NOT NULL,
  trace_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER NOT NULL,
  recovered_intents INTEGER NOT NULL,
  details_json TEXT NOT NULL
);

INSERT OR IGNORE INTO consumer_offsets
  (topic, partition_no, committed_seq, lease_state, lease_owner, lease_version,
   lease_expires_at_ms, lease_cursor, replay_active, replay_from, replay_limit, updated_at_ms)
SELECT topic, partition_no, consume_cursor, lease_state, lease_owner, lease_version,
       lease_expires_at_ms, lease_cursor, replay_active, replay_from, replay_limit, updated_at_ms
FROM partitions;
`

func openDB(path string) (*sql.DB, error) {
	if path == "" {
		return nil, fmt.Errorf("database path is empty")
	}
	if path != ":memory:" {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}
	dsn := path
	if !strings.HasPrefix(dsn, "file:") {
		dsn = "file:" + filepath.ToSlash(dsn)
	}
	dsn += "?_pragma=foreign_keys(1)&_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}
