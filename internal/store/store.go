package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Store struct {
	path      string
	writer    *sql.DB
	reader    *sql.DB
	writeMu   sync.Mutex
	intentSeq atomic.Uint64
}

func Open(path string) (*Store, error) {
	commonDSN := path + "?_journal_mode=WAL&_synchronous=FULL&_busy_timeout=5000&_foreign_keys=on"
	writer, err := sql.Open("sqlite3", commonDSN+"&_txlock=immediate")
	if err != nil {
		return nil, err
	}
	writer.SetMaxOpenConns(1)
	writer.SetMaxIdleConns(1)

	reader, err := sql.Open("sqlite3", commonDSN+"&_txlock=deferred")
	if err != nil {
		_ = writer.Close()
		return nil, err
	}
	reader.SetMaxOpenConns(8)
	reader.SetMaxIdleConns(4)

	store := &Store{path: path, writer: writer, reader: reader}
	if err := store.initSchema(); err != nil {
		_ = writer.Close()
		_ = reader.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	err1 := s.writer.Close()
	err2 := s.reader.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (s *Store) WriterDB() *sql.DB { return s.writer }
func (s *Store) ReaderDB() *sql.DB { return s.reader }

func (s *Store) initSchema() error {
	_, err := s.writer.Exec(schemaSQL)
	return err
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS schema_meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('schema_version', '1');

CREATE TABLE IF NOT EXISTS sessions (
	session_id TEXT PRIMARY KEY,
	status TEXT NOT NULL,
	current_version INTEGER NOT NULL,
	last_complete_version INTEGER NOT NULL,
	view_version INTEGER NOT NULL,
	propagation_max_hops INTEGER NOT NULL DEFAULT 3,
	repair_quota_total INTEGER NOT NULL DEFAULT 0,
	repair_quota_remaining INTEGER NOT NULL DEFAULT 0,
	created_at_ms INTEGER NOT NULL,
	updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
	session_id TEXT NOT NULL,
	version INTEGER NOT NULL,
	base_version INTEGER NOT NULL,
	reason TEXT NOT NULL,
	intent_id TEXT NOT NULL,
	created_at_ms INTEGER NOT NULL,
	PRIMARY KEY(session_id, version)
);

CREATE TABLE IF NOT EXISTS baselines (
	session_id TEXT NOT NULL,
	snapshot_version INTEGER NOT NULL,
	id TEXT NOT NULL,
	category TEXT NOT NULL,
	expected_json TEXT NOT NULL,
	severity_weight INTEGER NOT NULL,
	depends_on_json TEXT NOT NULL,
	PRIMARY KEY(session_id, snapshot_version, id)
);

CREATE TABLE IF NOT EXISTS observations (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id TEXT NOT NULL,
	host TEXT NOT NULL,
	baseline_id TEXT NOT NULL,
	value_json TEXT NOT NULL,
	sampled_at_ms INTEGER NOT NULL,
	source TEXT NOT NULL DEFAULT '',
	committed_version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_observations_target ON observations(session_id, host, baseline_id);

CREATE TABLE IF NOT EXISTS audit_tasks (
	session_id TEXT NOT NULL,
	host TEXT NOT NULL,
	baseline_id TEXT NOT NULL,
	task_key TEXT NOT NULL UNIQUE,
	severity_weight INTEGER NOT NULL,
	repair_cost INTEGER NOT NULL,
	status TEXT NOT NULL,
	priority_order INTEGER NOT NULL DEFAULT 0,
	error_code TEXT NOT NULL DEFAULT '',
	reason TEXT NOT NULL DEFAULT '',
	last_intent_id TEXT NOT NULL DEFAULT '',
	updated_at_ms INTEGER NOT NULL,
	PRIMARY KEY(session_id, host, baseline_id)
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON audit_tasks(session_id, status);

CREATE TABLE IF NOT EXISTS conclusions (
	session_id TEXT NOT NULL,
	host TEXT NOT NULL,
	baseline_id TEXT NOT NULL,
	category TEXT NOT NULL,
	status TEXT NOT NULL,
	drift_category TEXT NOT NULL DEFAULT '',
	own_level INTEGER NOT NULL DEFAULT 0,
	drift_level INTEGER NOT NULL DEFAULT 0,
	score_scaled INTEGER NOT NULL DEFAULT 0,
	offset_permille INTEGER NOT NULL DEFAULT 0,
	rounding_json TEXT NOT NULL DEFAULT '{}',
	missing_json TEXT NOT NULL DEFAULT '[]',
	extra_json TEXT NOT NULL DEFAULT '[]',
	recommendation TEXT NOT NULL DEFAULT '',
	propagation_json TEXT NOT NULL DEFAULT '[]',
	repair_cost INTEGER NOT NULL DEFAULT 0,
	reason TEXT NOT NULL DEFAULT '',
	error_code TEXT NOT NULL DEFAULT '',
	details_json TEXT NOT NULL DEFAULT '{}',
	view_version INTEGER NOT NULL,
	updated_at_ms INTEGER NOT NULL,
	PRIMARY KEY(session_id, host, baseline_id)
);
CREATE INDEX IF NOT EXISTS idx_conclusions_version ON conclusions(session_id, view_version);

CREATE TABLE IF NOT EXISTS host_summaries (
	session_id TEXT NOT NULL,
	host TEXT NOT NULL,
	view_version INTEGER NOT NULL,
	total INTEGER NOT NULL,
	done INTEGER NOT NULL,
	conformant INTEGER NOT NULL,
	drift INTEGER NOT NULL,
	deferred INTEGER NOT NULL,
	errors INTEGER NOT NULL,
	max_drift_level INTEGER NOT NULL,
	weighted_score_scaled INTEGER NOT NULL,
	remaining_quota INTEGER NOT NULL,
	summary_json TEXT NOT NULL DEFAULT '{}',
	updated_at_ms INTEGER NOT NULL,
	PRIMARY KEY(session_id, host)
);

CREATE TABLE IF NOT EXISTS intents (
	intent_id TEXT PRIMARY KEY,
	session_id TEXT NOT NULL,
	kind TEXT NOT NULL,
	target_version INTEGER NOT NULL DEFAULT 0,
	payload_json TEXT NOT NULL DEFAULT '{}',
	status TEXT NOT NULL,
	recovery_note TEXT NOT NULL DEFAULT '',
	created_at_ms INTEGER NOT NULL,
	updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status, session_id);

CREATE TABLE IF NOT EXISTS recovery_reports (
	recovery_id TEXT PRIMARY KEY,
	session_id TEXT NOT NULL,
	report_json TEXT NOT NULL,
	created_at_ms INTEGER NOT NULL
);
`

type Intent struct {
	IntentID      string `json:"intent_id"`
	SessionID     string `json:"session_id"`
	Kind          string `json:"kind"`
	TargetVersion int64  `json:"target_version"`
	PayloadJSON   string `json:"payload_json,omitempty"`
	Status        string `json:"status"`
	RecoveryNote  string `json:"recovery_note,omitempty"`
	CreatedAtMS   int64  `json:"created_at_ms"`
	UpdatedAtMS   int64  `json:"updated_at_ms"`
}

func (s *Store) CreateIntent(sessionID, kind string, targetVersion int64, payload any) (string, error) {
	seq := s.intentSeq.Add(1)
	id := fmt.Sprintf("%s-%d-%d", kind, time.Now().UnixNano(), seq)
	payloadJSON := "{}"
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return "", err
		}
		payloadJSON = string(data)
	}
	now := time.Now().UnixMilli()
	_, err := s.writer.Exec(`
		INSERT INTO intents(intent_id, session_id, kind, target_version, payload_json, status, created_at_ms, updated_at_ms)
		VALUES(?, ?, ?, ?, ?, 'PENDING', ?, ?)`,
		id, sessionID, kind, targetVersion, payloadJSON, now, now,
	)
	if err != nil {
		return "", err
	}
	return id, nil
}

func markIntentCommitted(tx *sql.Tx, intentID string) error {
	_, err := tx.Exec(`UPDATE intents SET status='COMMITTED', updated_at_ms=? WHERE intent_id=?`, time.Now().UnixMilli(), intentID)
	return err
}

func markIntentRejected(tx *sql.Tx, intentID string, note string) error {
	_, err := tx.Exec(`UPDATE intents SET status='REJECTED', recovery_note=?, updated_at_ms=? WHERE intent_id=?`, note, time.Now().UnixMilli(), intentID)
	return err
}

type Session struct {
	SessionID            string `json:"session_id"`
	Status               string `json:"status"`
	CurrentVersion       int64  `json:"current_version"`
	LastCompleteVersion  int64  `json:"last_complete_version"`
	ViewVersion          int64  `json:"view_version"`
	PropagationMaxHops   int    `json:"propagation_max_hops"`
	RepairQuotaTotal     int    `json:"repair_quota_total"`
	RepairQuotaRemaining int    `json:"repair_quota_remaining"`
	UpdatedAtMS          int64  `json:"updated_at_ms"`
}

func (s *Store) GetSession(sessionID string) (*Session, error) {
	row := s.reader.QueryRow(`SELECT session_id,status,current_version,last_complete_version,view_version,propagation_max_hops,repair_quota_total,repair_quota_remaining,updated_at_ms FROM sessions WHERE session_id=?`, sessionID)
	var session Session
	err := row.Scan(&session.SessionID, &session.Status, &session.CurrentVersion, &session.LastCompleteVersion, &session.ViewVersion, &session.PropagationMaxHops, &session.RepairQuotaTotal, &session.RepairQuotaRemaining, &session.UpdatedAtMS)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &session, nil
}
