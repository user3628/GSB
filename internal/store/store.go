package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"preprocesspipeline/internal/domain"
)

type Store struct {
	db      *sql.DB
	DataDir string
}

func Open(dataDir string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(dataDir, "objects"), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "tmp"), 0o755); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dataDir, "preprocess.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(0)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure sqlite: %w", err)
	}
	st := &Store{db: db, DataDir: dataDir}
	if err := st.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return st, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func nowISO() string { return time.Now().UTC().Format(time.RFC3339Nano) }

const schemaSQL = `
CREATE TABLE IF NOT EXISTS policies (
  version INTEGER PRIMARY KEY,
  generated_at TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  request_hash TEXT NOT NULL,
  policy_version INTEGER NOT NULL,
  status TEXT NOT NULL,
  raw_input TEXT NOT NULL DEFAULT '',
  raw_digest TEXT NOT NULL DEFAULT '',
  result_json TEXT NOT NULL DEFAULT '',
  error_code TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  completed_at TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_batches_status ON batches(status);

CREATE TABLE IF NOT EXISTS stage_summaries (
  batch_id TEXT NOT NULL,
  policy_version INTEGER NOT NULL,
  stage TEXT NOT NULL,
  seq INTEGER NOT NULL,
  input_digest TEXT NOT NULL,
  output_digest TEXT NOT NULL,
  stats_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(batch_id, policy_version, stage)
);

CREATE TABLE IF NOT EXISTS shard_artifacts (
  batch_id TEXT NOT NULL,
  policy_version INTEGER NOT NULL,
  shard_index INTEGER NOT NULL,
  state TEXT NOT NULL,
  record_count INTEGER NOT NULL DEFAULT 0,
  sampled_count INTEGER NOT NULL DEFAULT 0,
  byte_count INTEGER NOT NULL DEFAULT 0,
  sampled_bytes INTEGER NOT NULL DEFAULT 0,
  artifact_path TEXT NOT NULL DEFAULT '',
  dtype TEXT NOT NULL DEFAULT 'float32',
  sample_count INTEGER NOT NULL DEFAULT 0,
  feature_dim INTEGER NOT NULL DEFAULT 4,
  null_count INTEGER NOT NULL DEFAULT 0,
  artifact_digest TEXT NOT NULL,
  body_digest TEXT NOT NULL DEFAULT '',
  warnings_json TEXT NOT NULL DEFAULT '[]',
  updated_at TEXT NOT NULL,
  PRIMARY KEY(batch_id, policy_version, shard_index)
);
CREATE INDEX IF NOT EXISTS idx_shards_batch_state ON shard_artifacts(batch_id, state);

CREATE TABLE IF NOT EXISTS intents (
  intent_id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  policy_version INTEGER NOT NULL DEFAULT 0,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  boundary_stage TEXT NOT NULL DEFAULT '',
  plan_json TEXT NOT NULL DEFAULT '{}',
  error_code TEXT NOT NULL DEFAULT '',
  report_json TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
CREATE INDEX IF NOT EXISTS idx_intents_batch ON intents(batch_id);

CREATE TABLE IF NOT EXISTS sample_identities (
  sample_id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  policy_version INTEGER NOT NULL,
  record_digest TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_runs (
  recovery_id INTEGER PRIMARY KEY AUTOINCREMENT,
  trigger TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL,
  report_json TEXT NOT NULL
);
`

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, schemaSQL)
	return err
}

type Intent struct {
	IntentID      string `json:"intent_id"`
	BatchID       string `json:"batch_id"`
	PolicyVersion int64  `json:"policy_version"`
	Kind          string `json:"kind"`
	Status        string `json:"status"`
	BoundaryStage string `json:"boundary_stage"`
	PlanJSON      string `json:"plan_json"`
	ErrorCode     string `json:"error_code,omitempty"`
	ReportJSON    string `json:"report_json,omitempty"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
}

func (s *Store) CreateIntent(ctx context.Context, intent Intent) error {
	now := nowISO()
	_, err := s.db.ExecContext(ctx, `
INSERT INTO intents(intent_id,batch_id,policy_version,kind,status,boundary_stage,plan_json,error_code,report_json,created_at,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		intent.IntentID, intent.BatchID, intent.PolicyVersion, intent.Kind, intent.Status,
		intent.BoundaryStage, intent.PlanJSON, intent.ErrorCode, intent.ReportJSON, now, now)
	return err
}

func (s *Store) UpdateIntentBoundary(ctx context.Context, intentID, boundary string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE intents SET boundary_stage=?, updated_at=? WHERE intent_id=?`,
		boundary, nowISO(), intentID)
	return err
}

func (s *Store) FinishIntent(ctx context.Context, intentID, status, errorCode, reportJSON string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE intents SET status=?, error_code=?, report_json=?, updated_at=? WHERE intent_id=?`,
		status, errorCode, reportJSON, nowISO(), intentID)
	return err
}

func (s *Store) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func InsertStageSummaryTx(ctx context.Context, tx *sql.Tx, batchID string, policyVersion int64, seq int, stage domain.StageResult) error {
	statsJSON, err := json.Marshal(stage.Stats)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO stage_summaries(batch_id,policy_version,stage,seq,input_digest,output_digest,stats_json,created_at)
VALUES(?,?,?,?,?,?,?,?)
ON CONFLICT(batch_id,policy_version,stage) DO UPDATE SET
  seq=excluded.seq, input_digest=excluded.input_digest, output_digest=excluded.output_digest,
  stats_json=excluded.stats_json, created_at=excluded.created_at`,
		batchID, policyVersion, stage.Stage, seq, stage.InputDigest, stage.OutputDigest, string(statsJSON), nowISO())
	return err
}

func (s *Store) CurrentPolicyVersion(ctx context.Context) (int64, error) {
	var version int64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM policies`).Scan(&version)
	return version, err
}

func (s *Store) GetPolicy(ctx context.Context, version int64) (*domain.Policy, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT payload_json FROM policies WHERE version=?`, version)
	var payload string
	if err := row.Scan(&payload); err == sql.ErrNoRows {
		return nil, domain.NewError(domain.CodePolicyNotFound, fmt.Sprintf("policy version %d not found", version))
	} else if err != nil {
		return nil, err
	}
	var policy domain.Policy
	if err := json.Unmarshal([]byte(payload), &policy); err != nil {
		return nil, err
	}
	return &policy, nil
}

func (s *Store) CurrentPolicy(ctx context.Context) (*domain.Policy, error) {
	version, err := s.CurrentPolicyVersion(ctx)
	if err != nil {
		return nil, err
	}
	if version == 0 {
		return nil, domain.NewError(domain.CodePolicyNotFound, "no policy snapshot has been committed")
	}
	return s.GetPolicy(ctx, version)
}

func InsertPolicyTx(ctx context.Context, tx *sql.Tx, policy domain.Policy) error {
	payload, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO policies(version,generated_at,payload_json,digest,created_at)
VALUES(?,?,?,?,?)`, policy.Version, policy.GeneratedAt, string(payload), policy.Digest, nowISO())
	return err
}

type BatchRecord struct {
	BatchID       string `json:"batch_id"`
	RequestHash   string `json:"request_hash"`
	PolicyVersion int64  `json:"policy_version"`
	Status        string `json:"status"`
	RawInput      string `json:"raw_input,omitempty"`
	RawDigest     string `json:"raw_digest"`
	ResultJSON    string `json:"result_json,omitempty"`
	ErrorCode     string `json:"error_code,omitempty"`
	CreatedAt     string `json:"created_at"`
	CompletedAt   string `json:"completed_at,omitempty"`
}

func (s *Store) GetBatch(ctx context.Context, batchID string) (*BatchRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT batch_id,request_hash,policy_version,status,raw_input,raw_digest,result_json,error_code,created_at,completed_at
FROM batches WHERE batch_id=?`, batchID)
	var b BatchRecord
	err := row.Scan(&b.BatchID, &b.RequestHash, &b.PolicyVersion, &b.Status, &b.RawInput,
		&b.RawDigest, &b.ResultJSON, &b.ErrorCode, &b.CreatedAt, &b.CompletedAt)
	if err == sql.ErrNoRows {
		return nil, domain.NewError(domain.CodeBatchNotFound, "batch not found", map[string]any{"batch_id": batchID})
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func BatchExistsTx(ctx context.Context, tx *sql.Tx, batchID string) (bool, error) {
	var id string
	err := tx.QueryRowContext(ctx, `SELECT batch_id FROM batches WHERE batch_id=?`, batchID).Scan(&id)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func InsertRunningBatchTx(ctx context.Context, tx *sql.Tx, batch BatchRecord) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO batches(batch_id,request_hash,policy_version,status,raw_input,raw_digest,created_at)
VALUES(?,?,?,'running',?,?,?)
ON CONFLICT(batch_id) DO UPDATE SET
 request_hash=excluded.request_hash, policy_version=excluded.policy_version,
 status='running', raw_input=excluded.raw_input, raw_digest=excluded.raw_digest,
 error_code='', completed_at=''`,
		batch.BatchID, batch.RequestHash, batch.PolicyVersion, batch.RawInput, batch.RawDigest, nowISO())
	return err
}

func MarkBatchAbortedTx(ctx context.Context, tx *sql.Tx, batchID, errorCode string) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE batches SET status='aborted', error_code=?, completed_at=? WHERE batch_id=? AND status='running'`,
		errorCode, nowISO(), batchID)
	return err
}

type StageSummaryRecord struct {
	BatchID       string         `json:"batch_id"`
	PolicyVersion int64          `json:"policy_version"`
	Stage         string         `json:"stage"`
	Seq           int            `json:"seq"`
	InputDigest   string         `json:"input_digest"`
	OutputDigest  string         `json:"output_digest"`
	Stats         map[string]any `json:"stats"`
	CreatedAt     string         `json:"created_at"`
}

func (s *Store) ListStageSummaries(ctx context.Context, batchID string, policyVersion int64) ([]StageSummaryRecord, error) {
	query := `SELECT batch_id,policy_version,stage,seq,input_digest,output_digest,stats_json,created_at
FROM stage_summaries WHERE batch_id=?`
	args := []any{batchID}
	if policyVersion > 0 {
		query += ` AND policy_version=?`
		args = append(args, policyVersion)
	}
	query += ` ORDER BY policy_version DESC, seq ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]StageSummaryRecord, 0)
	for rows.Next() {
		var rec StageSummaryRecord
		var statsJSON string
		if err := rows.Scan(&rec.BatchID, &rec.PolicyVersion, &rec.Stage, &rec.Seq,
			&rec.InputDigest, &rec.OutputDigest, &statsJSON, &rec.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(statsJSON), &rec.Stats); err != nil {
			return nil, err
		}
		result = append(result, rec)
	}
	return result, rows.Err()
}

type ShardRecord struct {
	BatchID        string   `json:"batch_id"`
	PolicyVersion  int64    `json:"policy_version"`
	ShardIndex     int      `json:"shard_index"`
	State          string   `json:"state"`
	RecordCount    int      `json:"record_count"`
	SampledCount   int      `json:"sampled_count"`
	ByteCount      int64    `json:"byte_count"`
	SampledBytes   int64    `json:"sampled_bytes"`
	ArtifactPath   string   `json:"artifact_path"`
	DType          string   `json:"dtype"`
	SampleCount    int      `json:"sample_count"`
	FeatureDim     int      `json:"feature_dim"`
	NullCount      int      `json:"null_count"`
	ArtifactDigest string   `json:"artifact_digest"`
	BodyDigest     string   `json:"body_digest"`
	Warnings       []string `json:"warnings"`
	UpdatedAt      string   `json:"updated_at"`
}

func (s *Store) ListShards(ctx context.Context, batchID string, policyVersion int64, includeInvalid bool) ([]ShardRecord, error) {
	query := `SELECT batch_id,policy_version,shard_index,state,record_count,sampled_count,byte_count,sampled_bytes,
artifact_path,dtype,sample_count,feature_dim,null_count,artifact_digest,body_digest,warnings_json,updated_at
FROM shard_artifacts WHERE batch_id=?`
	args := []any{batchID}
	if policyVersion > 0 {
		query += ` AND policy_version=?`
		args = append(args, policyVersion)
	}
	if !includeInvalid {
		query += ` AND state='ready'`
	}
	query += ` ORDER BY policy_version DESC, shard_index ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ShardRecord, 0)
	for rows.Next() {
		var rec ShardRecord
		var warningsJSON string
		if err := rows.Scan(&rec.BatchID, &rec.PolicyVersion, &rec.ShardIndex, &rec.State,
			&rec.RecordCount, &rec.SampledCount, &rec.ByteCount, &rec.SampledBytes, &rec.ArtifactPath,
			&rec.DType, &rec.SampleCount, &rec.FeatureDim, &rec.NullCount, &rec.ArtifactDigest,
			&rec.BodyDigest, &warningsJSON, &rec.UpdatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(warningsJSON), &rec.Warnings)
		result = append(result, rec)
	}
	return result, rows.Err()
}

func UpsertShardTx(ctx context.Context, tx *sql.Tx, rec ShardRecord) error {
	warnings, err := json.Marshal(rec.Warnings)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO shard_artifacts(batch_id,policy_version,shard_index,state,record_count,sampled_count,byte_count,sampled_bytes,
artifact_path,dtype,sample_count,feature_dim,null_count,artifact_digest,body_digest,warnings_json,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(batch_id,policy_version,shard_index) DO UPDATE SET
 state=excluded.state, record_count=excluded.record_count, sampled_count=excluded.sampled_count,
 byte_count=excluded.byte_count, sampled_bytes=excluded.sampled_bytes, artifact_path=excluded.artifact_path,
 dtype=excluded.dtype, sample_count=excluded.sample_count, feature_dim=excluded.feature_dim,
 null_count=excluded.null_count, artifact_digest=excluded.artifact_digest, body_digest=excluded.body_digest,
 warnings_json=excluded.warnings_json, updated_at=excluded.updated_at`,
		rec.BatchID, rec.PolicyVersion, rec.ShardIndex, rec.State, rec.RecordCount, rec.SampledCount,
		rec.ByteCount, rec.SampledBytes, rec.ArtifactPath, rec.DType, rec.SampleCount, rec.FeatureDim,
		rec.NullCount, rec.ArtifactDigest, rec.BodyDigest, string(warnings), nowISO())
	return err
}

func InvalidateOlderShardsTx(ctx context.Context, tx *sql.Tx, batchID string, newPolicyVersion int64) (int64, error) {
	res, err := tx.ExecContext(ctx,
		`UPDATE shard_artifacts SET state='invalidated', updated_at=?
WHERE batch_id=? AND policy_version<? AND state='ready'`, nowISO(), batchID, newPolicyVersion)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func AddSampleIdentityTx(ctx context.Context, tx *sql.Tx, sampleID, batchID string, policyVersion int64, digest string) error {
	_, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO sample_identities(sample_id,batch_id,policy_version,record_digest,created_at)
VALUES(?,?,?,?,?)`, sampleID, batchID, policyVersion, digest, nowISO())
	return err
}

type Identity struct {
	SampleID      string `json:"sample_id"`
	BatchID       string `json:"batch_id"`
	PolicyVersion int64  `json:"policy_version"`
	RecordDigest  string `json:"record_digest"`
}

func (s *Store) GetIdentity(ctx context.Context, sampleID string) (*Identity, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT sample_id,batch_id,policy_version,record_digest FROM sample_identities WHERE sample_id=?`, sampleID)
	var id Identity
	if err := row.Scan(&id.SampleID, &id.BatchID, &id.PolicyVersion, &id.RecordDigest); err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &id, nil
}

func (s *Store) ListRunningIntents(ctx context.Context) ([]Intent, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT intent_id,batch_id,policy_version,kind,status,boundary_stage,plan_json,error_code,report_json,created_at,updated_at
FROM intents WHERE status IN ('running','prepared') ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Intent
	for rows.Next() {
		var intent Intent
		if err := rows.Scan(&intent.IntentID, &intent.BatchID, &intent.PolicyVersion, &intent.Kind,
			&intent.Status, &intent.BoundaryStage, &intent.PlanJSON, &intent.ErrorCode,
			&intent.ReportJSON, &intent.CreatedAt, &intent.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, intent)
	}
	return result, rows.Err()
}

func (s *Store) SaveRecoveryRun(ctx context.Context, trigger, reportJSON string) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO recovery_runs(trigger,started_at,finished_at,report_json) VALUES(?,?,?,?)`,
		trigger, nowISO(), nowISO(), reportJSON)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) LatestRecoveryReport(ctx context.Context) (int64, string, error) {
	var id int64
	var report string
	err := s.db.QueryRowContext(ctx,
		`SELECT recovery_id,report_json FROM recovery_runs ORDER BY recovery_id DESC LIMIT 1`).Scan(&id, &report)
	if err == sql.ErrNoRows {
		return 0, "", domain.NewError(domain.CodeBatchNotFound, "no recovery report")
	}
	return id, report, err
}

func (s *Store) SaveBatchResult(ctx context.Context, batchID, status, resultJSON, errorCode string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE batches SET status=?, result_json=?, error_code=?, completed_at=? WHERE batch_id=?`,
		status, resultJSON, errorCode, nowISO(), batchID)
	return err
}

func (s *Store) UpdateIntentPlan(ctx context.Context, intentID, planJSON string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE intents SET plan_json=?, updated_at=? WHERE intent_id=?`,
		planJSON, nowISO(), intentID)
	return err
}

func (s *Store) ListCommittedBatches(ctx context.Context) ([]BatchRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT batch_id,request_hash,policy_version,status,raw_input,raw_digest,result_json,error_code,created_at,completed_at
FROM batches WHERE status='committed' ORDER BY created_at ASC, batch_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []BatchRecord
	for rows.Next() {
		var b BatchRecord
		if err := rows.Scan(&b.BatchID, &b.RequestHash, &b.PolicyVersion, &b.Status, &b.RawInput,
			&b.RawDigest, &b.ResultJSON, &b.ErrorCode, &b.CreatedAt, &b.CompletedAt); err != nil {
			return nil, err
		}
		result = append(result, b)
	}
	return result, rows.Err()
}
