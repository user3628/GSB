package qos

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type StoreOptions struct {
	DBPath         string
	DataDir        string
	SampleSeed     int64
	FailpointStage string
	FailpointFile  string
	FailpointSleep time.Duration
}

type Store struct {
	db      *sql.DB
	opts    StoreOptions
	writeMu sync.Mutex
}

type StoredIntent struct {
	RequestID        string
	Kind             string
	Status           string
	BaseVersion      int64
	TargetVersion    int64
	Phase            string
	CommittedVersion int64
	ResponseJSON     []byte
	ResponseStatus   int
	ErrorJSON        []byte
	CreatedAt        string
	CompletedAt      string
}

func OpenStore(opts StoreOptions) (*Store, error) {
	if opts.DBPath == "" {
		return nil, errors.New("database path is required")
	}
	if opts.DataDir == "" {
		opts.DataDir = filepath.Dir(opts.DBPath)
	}
	if opts.SampleSeed == 0 {
		opts.SampleSeed = DefaultSampleSeed
	}
	if opts.FailpointSleep <= 0 {
		opts.FailpointSleep = 20 * time.Second
	}
	if err := os.MkdirAll(filepath.Dir(opts.DBPath), 0o755); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(opts.DBPath)
	if err != nil {
		return nil, err
	}
	slashed := filepath.ToSlash(abs)
	if runtime.GOOS == "windows" && !strings.HasPrefix(slashed, "/") {
		slashed = "/" + slashed
	}
	u := url.URL{Scheme: "file", Path: slashed}
	q := u.Query()
	q.Add("_pragma", "busy_timeout(15000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "foreign_keys(ON)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(12)
	db.SetMaxIdleConns(12)
	db.SetConnMaxLifetime(0)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{db: db, opts: opts}
	if err := s.initSchema(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if _, be := s.RecoverPending(ctx); be != nil {
		db.Close()
		return nil, be
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) DB() *sql.DB  { return s.db }

func (s *Store) initSchema(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
INSERT OR IGNORE INTO meta(key,value) VALUES('current_version','0');

CREATE TABLE IF NOT EXISTS commits (
  version INTEGER PRIMARY KEY,
  kind TEXT NOT NULL,
  request_id TEXT NOT NULL UNIQUE,
  parent_version INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  affected_json TEXT NOT NULL,
  recomputed_json TEXT NOT NULL,
  removed_json TEXT NOT NULL,
  policy_digest TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS queues (
  queue_id TEXT PRIMARY KEY,
  cir_bps INTEGER NOT NULL,
  pir_bps INTEGER NOT NULL,
  cbs_bytes INTEGER NOT NULL,
  pbs_bytes INTEGER NOT NULL,
  priority INTEGER NOT NULL,
  src_port_min INTEGER NOT NULL,
  src_port_max INTEGER NOT NULL,
  dscp_json TEXT NOT NULL,
  parent_id TEXT,
  policy_version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS queue_views (
  queue_id TEXT PRIMARY KEY,
  effective_cir_bps INTEGER NOT NULL,
  effective_pir_bps INTEGER NOT NULL,
  effective_json TEXT NOT NULL,
  cir_source_json TEXT NOT NULL,
  pir_source_json TEXT NOT NULL,
  parent_chain_json TEXT NOT NULL,
  token_view_json TEXT NOT NULL,
  consistency_json TEXT NOT NULL,
  consistency_score INTEGER NOT NULL,
  calculation_version INTEGER NOT NULL,
  valid_through_version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS queue_view_history (
  version INTEGER NOT NULL,
  queue_id TEXT NOT NULL,
  view_json TEXT NOT NULL,
  PRIMARY KEY(version, queue_id)
);

CREATE TABLE IF NOT EXISTS intents (
  request_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  base_version INTEGER NOT NULL DEFAULT 0,
  target_version INTEGER NOT NULL DEFAULT 0,
  phase TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  response_json TEXT,
  response_status INTEGER NOT NULL DEFAULT 0,
  error_json TEXT,
  committed_version INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);

CREATE TABLE IF NOT EXISTS simulations (
  simulation_id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL UNIQUE,
  policy_version INTEGER NOT NULL,
  sample_seed INTEGER NOT NULL,
  sample_sha256 TEXT NOT NULL,
  packets INTEGER NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS simulation_results (
  simulation_id TEXT NOT NULL,
  queue_id TEXT NOT NULL,
  summary_json TEXT NOT NULL,
  PRIMARY KEY(simulation_id, queue_id)
);

CREATE TABLE IF NOT EXISTS simulation_packets (
  simulation_id TEXT NOT NULL,
  queue_id TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  packet_json TEXT NOT NULL,
  PRIMARY KEY(simulation_id, queue_id, sequence)
);

CREATE TABLE IF NOT EXISTS recovery_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  checked_at TEXT NOT NULL,
  current_version INTEGER NOT NULL,
  pending_count INTEGER NOT NULL,
  reports_json TEXT NOT NULL
);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize schema: %w", err)
	}
	return nil
}

func (s *Store) CurrentVersion(ctx context.Context) (int64, error) {
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='current_version'`).Scan(&raw); err != nil {
		return 0, err
	}
	var v int64
	if _, err := fmt.Sscan(raw, &v); err != nil {
		return 0, err
	}
	return v, nil
}

func currentVersionTx(ctx context.Context, tx *sql.Tx) (int64, error) {
	var raw string
	if err := tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='current_version'`).Scan(&raw); err != nil {
		return 0, err
	}
	var v int64
	_, err := fmt.Sscan(raw, &v)
	return v, err
}

func setCurrentVersionTx(ctx context.Context, tx *sql.Tx, version int64) error {
	_, err := tx.ExecContext(ctx, `UPDATE meta SET value=? WHERE key='current_version'`, fmt.Sprintf("%d", version))
	return err
}

func (s *Store) replayIntent(ctx context.Context, requestID string) (*StoredIntent, error) {
	row := s.db.QueryRowContext(ctx, `SELECT request_id,kind,status,base_version,target_version,phase,committed_version,
		COALESCE(response_json,''),response_status,COALESCE(error_json,''),created_at,COALESCE(completed_at,'')
		FROM intents WHERE request_id=?`, requestID)
	var it StoredIntent
	var response, errJSON string
	if err := row.Scan(&it.RequestID, &it.Kind, &it.Status, &it.BaseVersion, &it.TargetVersion, &it.Phase, &it.CommittedVersion,
		&response, &it.ResponseStatus, &errJSON, &it.CreatedAt, &it.CompletedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	it.ResponseJSON = []byte(response)
	it.ErrorJSON = []byte(errJSON)
	return &it, nil
}

func intentReplayError(it *StoredIntent) *BusinessError {
	if it == nil {
		return nil
	}
	switch it.Status {
	case "complete":
		return nil
	case "failed", "aborted":
		if len(it.ErrorJSON) > 0 {
			var be BusinessError
			if json.Unmarshal(it.ErrorJSON, &be) == nil {
				if be.Status == 0 {
					be.Status = it.ResponseStatus
				}
				if be.Status == 0 {
					be.Status = 409
				}
				if it.Status == "aborted" {
					original := be.Code
					be.Code = CodeAborted
					be.Message = "request was aborted during crash recovery"
					if be.Context == nil {
						be.Context = map[string]any{}
					}
					be.Context["original_code"] = original
				}
				return &be
			}
		}
		return NewError(CodeAborted, "request did not complete", 409, map[string]any{"request_id": it.RequestID, "phase": it.Phase})
	case "pending":
		return NewError(CodeRequestConflict, "request id is already pending", 409, map[string]any{"request_id": it.RequestID, "phase": it.Phase})
	default:
		return NewError(CodeRequestConflict, "request id has an unknown terminal status", 409, map[string]any{"request_id": it.RequestID, "status": it.Status})
	}
}

func (s *Store) insertIntent(ctx context.Context, requestID, kind string, base, target int64, payload any) error {
	b, _ := json.Marshal(payload)
	now := nowRFC3339()
	_, err := s.db.ExecContext(ctx, `INSERT INTO intents(request_id,kind,status,base_version,target_version,phase,payload_json,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?)`, requestID, kind, "pending", base, target, "validating", string(b), now, now)
	return err
}

func (s *Store) setIntentPhase(ctx context.Context, requestID, phase string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET phase=?,updated_at=? WHERE request_id=? AND status='pending'`, phase, nowRFC3339(), requestID)
	return err
}

func (s *Store) failIntent(ctx context.Context, requestID string, be *BusinessError) {
	if be == nil {
		return
	}
	b, _ := json.Marshal(be)
	status := be.Status
	if status == 0 {
		status = 500
	}
	now := nowRFC3339()
	_, _ = s.db.ExecContext(ctx, `UPDATE intents SET status='failed',phase='failed',response_status=?,error_json=?,
		updated_at=?,completed_at=? WHERE request_id=? AND status='pending'`, status, string(b), now, now, requestID)
}

func completeIntentTx(ctx context.Context, tx *sql.Tx, requestID string, version int64, response any, status int) error {
	b, err := json.Marshal(response)
	if err != nil {
		return err
	}
	now := nowRFC3339()
	_, err = tx.ExecContext(ctx, `UPDATE intents SET status='complete',phase='committed',committed_version=?,response_json=?,
		response_status=?,updated_at=?,completed_at=? WHERE request_id=? AND status='pending'`,
		version, string(b), status, now, now, requestID)
	return err
}

func validateRequestID(id string) *BusinessError {
	id = strings.TrimSpace(id)
	if id == "" || len(id) > 256 {
		return policyInvalid("request_id must be non-empty and no longer than 256 bytes", map[string]any{"request_id": id})
	}
	return nil
}

func (s *Store) maybeFailpoint(stage, requestID string) {
	if s.opts.FailpointStage == "" || stage != s.opts.FailpointStage || s.opts.FailpointFile == "" {
		return
	}
	_ = os.MkdirAll(filepath.Dir(s.opts.FailpointFile), 0o755)
	payload := map[string]any{
		"stage": stage, "request_id": requestID, "pid": os.Getpid(), "ready_at": nowRFC3339(),
	}
	b, _ := json.MarshalIndent(payload, "", "  ")
	_ = os.WriteFile(s.opts.FailpointFile, b, 0o644)
	time.Sleep(s.opts.FailpointSleep)
}
