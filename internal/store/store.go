package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"netpolcompiler/internal/compiler"
)

type Store struct {
	DB      *sql.DB
	Path    string
	DataDir string
}

type RequestRecord struct {
	RequestID    string `json:"requestId"`
	RequestHash  string `json:"requestHash"`
	Kind         string `json:"kind"`
	Status       string `json:"status"`
	HTTPStatus   int    `json:"httpStatus"`
	ResponseJSON string `json:"responseJson,omitempty"`
	ErrorCode    string `json:"errorCode,omitempty"`
	IntentID     string `json:"intentId,omitempty"`
	CreatedAt    string `json:"createdAt"`
	CompletedAt  string `json:"completedAt,omitempty"`
}

type Intent struct {
	IntentID      string `json:"intentId"`
	RequestID     string `json:"requestId,omitempty"`
	Kind          string `json:"kind"`
	TargetVersion int64  `json:"targetVersion"`
	Namespaces    string `json:"namespaces"`
	PayloadJSON   string `json:"payloadJson,omitempty"`
	Status        string `json:"status"`
	Reason        string `json:"reason,omitempty"`
	CreatedAt     string `json:"createdAt"`
	UpdatedAt     string `json:"updatedAt"`
}

func Open(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	dbPath := filepath.Join(dataDir, "policies.db")
	dsn := dbPath + "?_busy_timeout=5000&_journal_mode=WAL&_foreign_keys=on&_synchronous=FULL"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(0)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	store := &Store{DB: db, Path: dbPath, DataDir: dataDir}
	if err := store.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.DB.Close() }

func (s *Store) migrate(ctx context.Context) error {
	schema := `
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots (
  version INTEGER PRIMARY KEY,
  kind TEXT NOT NULL,
  base_version INTEGER NOT NULL DEFAULT 0,
  request_id TEXT NOT NULL DEFAULT '',
  generated_at TEXT NOT NULL,
  digest TEXT NOT NULL,
  canonical_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rules (
  snapshot_version INTEGER NOT NULL,
  namespace TEXT NOT NULL,
  rule_id TEXT NOT NULL,
  declaration_order INTEGER NOT NULL DEFAULT 0,
  action TEXT NOT NULL,
  protocol TEXT NOT NULL,
  priority INTEGER NOT NULL,
  rule_json TEXT NOT NULL,
  PRIMARY KEY(snapshot_version, namespace, rule_id)
);
CREATE INDEX IF NOT EXISTS idx_rules_version_namespace ON rules(snapshot_version, namespace);
CREATE TABLE IF NOT EXISTS compile_views (
  snapshot_version INTEGER NOT NULL,
  namespace TEXT NOT NULL,
  status TEXT NOT NULL,
  selector_index_json TEXT NOT NULL,
  port_view_json TEXT NOT NULL,
  decision_json TEXT NOT NULL,
  consistency_json TEXT NOT NULL,
  compiled_json TEXT NOT NULL,
  compiled_digest TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(snapshot_version, namespace)
);
CREATE TABLE IF NOT EXISTS view_references (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_version INTEGER NOT NULL,
  namespace TEXT NOT NULL,
  component TEXT NOT NULL,
  rule_id TEXT NOT NULL DEFAULT '',
  field TEXT NOT NULL,
  source_version INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_view_refs_ns_version ON view_references(namespace, snapshot_version);
CREATE TABLE IF NOT EXISTS consistency_reports (
  snapshot_version INTEGER PRIMARY KEY,
  report_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS intents (
  intent_id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL,
  target_version INTEGER NOT NULL,
  namespaces TEXT NOT NULL DEFAULT '',
  payload_json TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
CREATE TABLE IF NOT EXISTS requests (
  request_id TEXT PRIMARY KEY,
  request_hash TEXT NOT NULL,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  http_status INTEGER NOT NULL DEFAULT 0,
  response_json TEXT,
  error_code TEXT NOT NULL DEFAULT '',
  intent_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  completed_at TEXT
);
`
	_, err := s.DB.ExecContext(ctx, schema)
	return err
}

func NowRFC3339() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func (s *Store) CurrentVersion(ctx context.Context) (int64, error) {
	var value string
	err := s.DB.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='current_version'`).Scan(&value)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var version int64
	if _, scanErr := fmt.Sscanf(value, "%d", &version); scanErr != nil {
		return 0, scanErr
	}
	return version, nil
}

func (s *Store) GetRequest(ctx context.Context, requestID string) (*RequestRecord, error) {
	row := s.DB.QueryRowContext(ctx, `SELECT request_id,request_hash,kind,status,http_status,COALESCE(response_json,''),error_code,intent_id,created_at,COALESCE(completed_at,'') FROM requests WHERE request_id=?`, requestID)
	var record RequestRecord
	err := row.Scan(&record.RequestID, &record.RequestHash, &record.Kind, &record.Status, &record.HTTPStatus, &record.ResponseJSON, &record.ErrorCode, &record.IntentID, &record.CreatedAt, &record.CompletedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *Store) InsertRequest(ctx context.Context, record RequestRecord) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO requests(request_id,request_hash,kind,status,http_status,response_json,error_code,intent_id,created_at,completed_at) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		record.RequestID, record.RequestHash, record.Kind, record.Status, record.HTTPStatus, nullableString(record.ResponseJSON), record.ErrorCode, record.IntentID, record.CreatedAt, nullableString(record.CompletedAt))
	return err
}

func (s *Store) ClaimAbortedRequest(ctx context.Context, requestID, hash string) (bool, error) {
	result, err := s.DB.ExecContext(ctx, `UPDATE requests SET status='processing', request_hash=?, error_code='', completed_at=NULL WHERE request_id=? AND status IN ('aborted','processing')`, hash, requestID)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count == 1, err
}

func (s *Store) CompleteRequest(ctx context.Context, requestID, status string, httpStatus int, responseJSON, errorCode string) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE requests SET status=?,http_status=?,response_json=?,error_code=?,completed_at=? WHERE request_id=?`,
		status, httpStatus, nullableString(responseJSON), errorCode, NowRFC3339(), requestID)
	return err
}

func (s *Store) CreateIntent(ctx context.Context, intent Intent) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO intents(intent_id,request_id,kind,target_version,namespaces,payload_json,status,reason,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		intent.IntentID, intent.RequestID, intent.Kind, intent.TargetVersion, intent.Namespaces, intent.PayloadJSON, intent.Status, intent.Reason, intent.CreatedAt, intent.UpdatedAt)
	return err
}

func (s *Store) ListUnfinishedIntents(ctx context.Context) ([]Intent, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT intent_id,COALESCE(request_id,''),kind,target_version,COALESCE(namespaces,''),COALESCE(payload_json,'{}'),status,COALESCE(reason,''),created_at,updated_at FROM intents WHERE status IN ('prepared','compiling') ORDER BY created_at,intent_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var intents []Intent
	for rows.Next() {
		var intent Intent
		if err := rows.Scan(&intent.IntentID, &intent.RequestID, &intent.Kind, &intent.TargetVersion, &intent.Namespaces, &intent.PayloadJSON, &intent.Status, &intent.Reason, &intent.CreatedAt, &intent.UpdatedAt); err != nil {
			return nil, err
		}
		intents = append(intents, intent)
	}
	return intents, rows.Err()
}

func (s *Store) MarkIntent(ctx context.Context, intentID, status, reason string) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE intents SET status=?, reason=?, updated_at=? WHERE intent_id=?`, status, reason, NowRFC3339(), intentID)
	return err
}

func (s *Store) LoadCacheViews(ctx context.Context, currentVersion int64) (map[string]*compiler.CompiledNamespace, error) {
	namespaces, err := s.currentNamespaces(ctx, currentVersion)
	if err != nil {
		return nil, err
	}
	viewNamespaces, err := s.currentViewNamespaces(ctx, currentVersion)
	if err != nil {
		return nil, err
	}
	namespaceSet := map[string]struct{}{}
	for _, namespace := range namespaces {
		namespaceSet[namespace] = struct{}{}
	}
	for _, namespace := range viewNamespaces {
		namespaceSet[namespace] = struct{}{}
	}
	views := make(map[string]*compiler.CompiledNamespace, len(namespaces))
	for namespace := range namespaceSet {
		compiled, err := s.LoadViewAtOrBefore(ctx, namespace, currentVersion)
		if err != nil {
			return nil, err
		}
		if compiled != nil {
			views[namespace] = compiled
		}
	}
	return views, nil
}

func (s *Store) currentViewNamespaces(ctx context.Context, version int64) ([]string, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT DISTINCT namespace FROM compile_views WHERE snapshot_version=? AND status='ready' ORDER BY namespace`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var namespaces []string
	for rows.Next() {
		var namespace string
		if err := rows.Scan(&namespace); err != nil {
			return nil, err
		}
		namespaces = append(namespaces, namespace)
	}
	return namespaces, rows.Err()
}

func (s *Store) currentNamespaces(ctx context.Context, version int64) ([]string, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT DISTINCT namespace FROM rules WHERE snapshot_version=? ORDER BY namespace`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var namespaces []string
	for rows.Next() {
		var namespace string
		if err := rows.Scan(&namespace); err != nil {
			return nil, err
		}
		namespaces = append(namespaces, namespace)
	}
	return namespaces, rows.Err()
}

func (s *Store) LoadViewAtOrBefore(ctx context.Context, namespace string, observedVersion int64) (*compiler.CompiledNamespace, error) {
	var compiledJSON string
	err := s.DB.QueryRowContext(ctx, `SELECT compiled_json FROM compile_views WHERE namespace=? AND status='ready' AND snapshot_version<=? ORDER BY snapshot_version DESC LIMIT 1`, namespace, observedVersion).Scan(&compiledJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var compiled compiler.CompiledNamespace
	if err := json.Unmarshal([]byte(compiledJSON), &compiled); err != nil {
		return nil, err
	}
	return &compiled, nil
}

func (s *Store) LoadGlobalReportAtOrBefore(ctx context.Context, observedVersion int64) (*compiler.GlobalReport, error) {
	var reportJSON string
	err := s.DB.QueryRowContext(ctx, `SELECT report_json FROM consistency_reports WHERE snapshot_version<=? ORDER BY snapshot_version DESC LIMIT 1`, observedVersion).Scan(&reportJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var report compiler.GlobalReport
	if err := json.Unmarshal([]byte(reportJSON), &report); err != nil {
		return nil, err
	}
	return &report, nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
