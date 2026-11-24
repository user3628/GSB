package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"driftconverge/internal/apperr"
	"driftconverge/internal/batch"
	"driftconverge/internal/detect"
	"driftconverge/internal/model"
	"driftconverge/internal/plan"

	_ "modernc.org/sqlite"
)

type Store struct {
	db      *sql.DB
	path    string
	writeMu sync.Mutex
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, apperr.New(apperr.InvalidArgument, "database path is required")
	}
	if path != ":memory:" && !strings.HasPrefix(path, "file:") {
		dir := filepath.Dir(path)
		if dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, apperr.New(apperr.PersistenceFailed, "create database directory: %v", err)
			}
		}
	}
	dsn := databaseDSN(path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, apperr.New(apperr.PersistenceFailed, "open sqlite: %v", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(0)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, apperr.New(apperr.PersistenceFailed, "ping sqlite: %v", err)
	}
	for _, pragma := range []string{
		"PRAGMA busy_timeout=5000",
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			db.Close()
			return nil, apperr.New(apperr.PersistenceFailed, "sqlite pragma %q: %v", pragma, err)
		}
	}
	s := &Store{db: db, path: path}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func databaseDSN(path string) string {
	if path == ":memory:" {
		return "file:driftconverge-" + randomID() + "?mode=memory&cache=shared&_pragma=busy_timeout(5000)"
	}
	if strings.HasPrefix(path, "file:") {
		return path
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	return "file:" + filepath.ToSlash(abs) + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS policy_snapshots (
			version INTEGER PRIMARY KEY,
			generated_at TEXT NOT NULL,
			checksum TEXT NOT NULL,
			payload TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS cluster_objects (
			snapshot_version INTEGER NOT NULL,
			object_id TEXT NOT NULL,
			kind TEXT NOT NULL,
			namespace TEXT NOT NULL,
			actual_json TEXT NOT NULL,
			sources_json TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (snapshot_version, object_id),
			FOREIGN KEY (snapshot_version) REFERENCES policy_snapshots(version)
		)`,
		`CREATE TABLE IF NOT EXISTS drift_results (
			snapshot_version INTEGER NOT NULL,
			object_id TEXT NOT NULL,
			has_drift INTEGER NOT NULL,
			categories_json TEXT NOT NULL,
			primary_category TEXT NOT NULL,
			payload TEXT NOT NULL,
			created_at TEXT NOT NULL,
			PRIMARY KEY (snapshot_version, object_id),
			FOREIGN KEY (snapshot_version) REFERENCES policy_snapshots(version)
		)`,
		`CREATE TABLE IF NOT EXISTS batch_versions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			snapshot_version INTEGER NOT NULL,
			round_id TEXT NOT NULL DEFAULT '',
			intent_id TEXT NOT NULL DEFAULT '',
			state TEXT NOT NULL CHECK (state IN ('staging','complete','aborted')),
			signature TEXT NOT NULL,
			payload TEXT NOT NULL,
			error_code TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			completed_at TEXT,
			FOREIGN KEY (snapshot_version) REFERENCES policy_snapshots(version)
		)`,
		`CREATE TABLE IF NOT EXISTS batch_entries (
			batch_version_id INTEGER NOT NULL,
			batch_index INTEGER NOT NULL,
			batch_id TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (batch_version_id, batch_index),
			FOREIGN KEY (batch_version_id) REFERENCES batch_versions(id)
		)`,
		`CREATE TABLE IF NOT EXISTS rounds (
			round_id TEXT PRIMARY KEY,
			snapshot_version INTEGER NOT NULL,
			batch_version_id INTEGER NOT NULL,
			state TEXT NOT NULL CHECK (state IN ('running','completed','failed','aborted')),
			result_json TEXT NOT NULL DEFAULT '',
			error_code TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS write_intents (
			intent_id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			round_id TEXT NOT NULL DEFAULT '',
			batch_id TEXT NOT NULL DEFAULT '',
			object_id TEXT NOT NULL DEFAULT '',
			state TEXT NOT NULL CHECK (state IN ('pending','committed','rolled_back')),
			payload TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS write_records (
			record_id INTEGER PRIMARY KEY AUTOINCREMENT,
			intent_id TEXT NOT NULL,
			round_id TEXT NOT NULL,
			batch_id TEXT NOT NULL,
			object_id TEXT NOT NULL,
			attempt INTEGER NOT NULL,
			state TEXT NOT NULL,
			error_code TEXT NOT NULL DEFAULT '',
			payload TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			UNIQUE (intent_id),
			FOREIGN KEY (intent_id) REFERENCES write_intents(intent_id)
		)`,
		`CREATE TABLE IF NOT EXISTS intent_log (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			intent_id TEXT NOT NULL,
			event TEXT NOT NULL,
			detail TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_drift_primary ON drift_results(snapshot_version, primary_category)`,
		`CREATE INDEX IF NOT EXISTS idx_batch_state ON batch_versions(snapshot_version, state)`,
		`CREATE INDEX IF NOT EXISTS idx_intent_state ON write_intents(state)`,
		`CREATE INDEX IF NOT EXISTS idx_round_state ON rounds(state)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return apperr.New(apperr.PersistenceFailed, "sqlite migration failed: %v", err)
		}
	}
	return nil
}

func (s *Store) CurrentPolicyVersion(ctx context.Context) (int64, error) {
	var version sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(version) FROM policy_snapshots`).Scan(&version); err != nil {
		return 0, apperr.New(apperr.PersistenceFailed, "read current snapshot version: %v", err)
	}
	if !version.Valid {
		return 0, nil
	}
	return version.Int64, nil
}

func (s *Store) CommitAnalysis(ctx context.Context, cluster *model.ClusterSpec, policy *model.Policy, drift *detect.Report) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	if err := cluster.NormalizeAndValidate(); err != nil {
		return err
	}
	if err := policy.ValidateAgainstCluster(cluster); err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	current, err := s.currentVersionLocked(ctx)
	if err != nil {
		return err
	}
	if policy.Version <= current {
		return apperr.WithDetails(apperr.StaleSnapshot, map[string]int64{"currentVersion": current, "incomingVersion": policy.Version}, "snapshot version %d is not greater than current version %d", policy.Version, current)
	}
	now := nowText()
	intentID := "snap-" + randomID()
	intentPayload, _ := json.Marshal(map[string]any{
		"kind": "SNAPSHOT_COMMIT", "snapshotVersion": policy.Version, "checksum": policy.Checksum,
	})
	if _, err := s.db.ExecContext(ctx, `INSERT INTO write_intents(intent_id,kind,state,payload,created_at,updated_at) VALUES(?,?,?,?,?,?)`,
		intentID, "SNAPSHOT_COMMIT", "pending", string(intentPayload), now, now); err != nil {
		return apperr.New(apperr.PersistenceFailed, "write snapshot intent: %v", err)
	}
	_ = s.logIntent(ctx, intentID, "prepared", string(intentPayload))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.rollbackIntent(ctx, intentID, "snapshot begin transaction failed")
		return apperr.New(apperr.PersistenceFailed, "begin snapshot transaction: %v", err)
	}
	rollback := func(code apperr.Code, format string, args ...any) error {
		_ = tx.Rollback()
		s.rollbackIntent(ctx, intentID, fmt.Sprintf(format, args...))
		return apperr.New(code, format, args...)
	}
	policyJSON, _ := json.Marshal(policy)
	if _, err := tx.ExecContext(ctx, `INSERT INTO policy_snapshots(version,generated_at,checksum,payload,created_at) VALUES(?,?,?,?,?)`,
		policy.Version, policy.GeneratedAt, policy.Checksum, string(policyJSON), now); err != nil {
		if isUniqueViolation(err) {
			_ = tx.Rollback()
			s.rollbackIntent(ctx, intentID, "snapshot became stale during commit")
			return apperr.New(apperr.StaleSnapshot, "snapshot version %d was committed concurrently", policy.Version)
		}
		return rollback(apperr.PersistenceFailed, "insert policy snapshot: %v", err)
	}
	for _, obj := range cluster.Objects {
		actualJSON, _ := json.Marshal(obj.Actual)
		sourcesJSON, _ := json.Marshal(obj.Sources)
		objJSON, _ := json.Marshal(obj)
		if _, err := tx.ExecContext(ctx, `INSERT INTO cluster_objects(snapshot_version,object_id,kind,namespace,actual_json,sources_json,payload) VALUES(?,?,?,?,?,?,?)`,
			policy.Version, obj.ID, obj.Kind, obj.Namespace, string(actualJSON), string(sourcesJSON), string(objJSON)); err != nil {
			return rollback(apperr.PersistenceFailed, "insert cluster object %s: %v", obj.ID, err)
		}
	}
	for i := range drift.Results {
		result := drift.Results[i]
		result.SnapshotVersion = policy.Version
		categories, _ := json.Marshal(result.Categories)
		payload, _ := json.Marshal(result)
		if _, err := tx.ExecContext(ctx, `INSERT INTO drift_results(snapshot_version,object_id,has_drift,categories_json,primary_category,payload,created_at) VALUES(?,?,?,?,?,?,?)`,
			policy.Version, result.ObjectID, boolInt(result.HasDrift), string(categories), string(result.PrimaryCategory), string(payload), now); err != nil {
			return rollback(apperr.PersistenceFailed, "insert drift result %s: %v", result.ObjectID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		s.rollbackIntent(ctx, intentID, "snapshot data commit failed")
		return apperr.New(apperr.PersistenceFailed, "commit snapshot data: %v", err)
	}
	if err := s.markIntent(ctx, intentID, "committed"); err != nil {
		return err
	}
	_ = s.logIntent(ctx, intentID, "committed", "snapshot and drift facts committed")
	return nil
}

func (s *Store) CommitBatchPlan(ctx context.Context, snapshotVersion int64, roundID string, batchReport *batch.Report, costPlan *plan.Report) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := nowText()
	intentID := "batch-" + randomID()
	intentPayload, _ := json.Marshal(map[string]any{
		"kind": "BATCH_COMMIT", "snapshotVersion": snapshotVersion, "roundId": roundID, "signature": batchReport.StableSignature,
	})
	if _, err := s.db.ExecContext(ctx, `INSERT INTO write_intents(intent_id,kind,round_id,state,payload,created_at,updated_at) VALUES(?,?,?,?,?,?,?)`,
		intentID, "BATCH_COMMIT", roundID, "pending", string(intentPayload), now, now); err != nil {
		return 0, apperr.New(apperr.PersistenceFailed, "write batch intent: %v", err)
	}
	planByIndex := make(map[int]plan.BatchPlan, len(costPlan.Batches))
	for _, bp := range costPlan.Batches {
		planByIndex[bp.Index] = bp
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.rollbackIntent(ctx, intentID, "begin batch transaction failed")
		return 0, apperr.New(apperr.PersistenceFailed, "begin batch transaction: %v", err)
	}
	rollback := func(format string, args ...any) (int64, error) {
		_ = tx.Rollback()
		s.rollbackIntent(ctx, intentID, fmt.Sprintf(format, args...))
		return 0, apperr.New(apperr.PersistenceFailed, format, args...)
	}
	payload, _ := json.Marshal(costPlan)
	result, err := tx.ExecContext(ctx, `INSERT INTO batch_versions(snapshot_version,round_id,intent_id,state,signature,payload,error_code,created_at) VALUES(?,?,?,?,?,?,?,?)`,
		snapshotVersion, roundID, intentID, "staging", batchReport.StableSignature, string(payload), batchReport.ErrorCode, now)
	if err != nil {
		return rollback("insert batch version: %v", err)
	}
	versionID, err := result.LastInsertId()
	if err != nil {
		return rollback("read batch version id: %v", err)
	}
	for _, b := range batchReport.Batches {
		bp := planByIndex[b.Index]
		entry, _ := json.Marshal(bp)
		if _, err := tx.ExecContext(ctx, `INSERT INTO batch_entries(batch_version_id,batch_index,batch_id,payload) VALUES(?,?,?,?)`,
			versionID, b.Index, b.ID, string(entry)); err != nil {
			return rollback("insert batch entry %d: %v", b.Index, err)
		}
	}
	if err := tx.Commit(); err != nil {
		s.rollbackIntent(ctx, intentID, "batch data commit failed")
		return 0, apperr.New(apperr.PersistenceFailed, "commit batch data: %v", err)
	}
	tx2, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return versionID, apperr.New(apperr.PersistenceFailed, "begin batch finalize: %v", err)
	}
	if _, err := tx2.ExecContext(ctx, `UPDATE batch_versions SET state='complete',completed_at=? WHERE id=?`, nowText(), versionID); err != nil {
		_ = tx2.Rollback()
		return versionID, apperr.New(apperr.PersistenceFailed, "complete batch version: %v", err)
	}
	if _, err := tx2.ExecContext(ctx, `UPDATE write_intents SET state='committed',updated_at=? WHERE intent_id=?`, nowText(), intentID); err != nil {
		_ = tx2.Rollback()
		return versionID, apperr.New(apperr.PersistenceFailed, "commit batch intent: %v", err)
	}
	if _, err := tx2.ExecContext(ctx, `INSERT INTO intent_log(intent_id,event,detail,created_at) VALUES(?,?,?,?)`,
		intentID, "committed", "batch version complete", nowText()); err != nil {
		_ = tx2.Rollback()
		return versionID, apperr.New(apperr.PersistenceFailed, "write batch intent log: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		return versionID, apperr.New(apperr.PersistenceFailed, "finalize batch version: %v", err)
	}
	return versionID, nil
}

type WriteOutcome struct {
	State     string
	ErrorCode string
	Attempt   int
	Detail    any
	Actual    map[string]model.FieldValue
	Sources   map[string]model.FieldSource
	Request   any
	Readback  any
}

func (s *Store) BeginWriteAttempt(ctx context.Context, roundID, batchID, objectID string, attempt int, request any) (string, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := nowText()
	intentID := "write-" + randomID()
	requestJSON, _ := json.Marshal(request)
	payload, _ := json.Marshal(map[string]any{
		"attempt": attempt, "request": json.RawMessage(requestJSON), "state": "prepared",
	})
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", apperr.New(apperr.PersistenceFailed, "begin write intent: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO write_intents(intent_id,kind,round_id,batch_id,object_id,state,payload,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)`,
		intentID, "WRITE_COMMIT", roundID, batchID, objectID, "pending", string(payload), now, now); err != nil {
		_ = tx.Rollback()
		return "", apperr.New(apperr.PersistenceFailed, "insert write intent: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO write_records(intent_id,round_id,batch_id,object_id,attempt,state,payload,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)`,
		intentID, roundID, batchID, objectID, attempt, "prepared", string(payload), now, now); err != nil {
		_ = tx.Rollback()
		return "", apperr.New(apperr.PersistenceFailed, "insert write record: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO intent_log(intent_id,event,detail,created_at) VALUES(?,?,?,?)`,
		intentID, "prepared", string(payload), now); err != nil {
		_ = tx.Rollback()
		return "", apperr.New(apperr.PersistenceFailed, "write intent log: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return "", apperr.New(apperr.PersistenceFailed, "commit write intent: %v", err)
	}
	return intentID, nil
}

func (s *Store) CompleteWriteAttempt(ctx context.Context, intentID string, outcome WriteOutcome) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := nowText()
	detail := map[string]any{
		"state": outcome.State, "errorCode": outcome.ErrorCode, "attempt": outcome.Attempt,
		"detail": outcome.Detail, "readback": outcome.Readback,
	}
	detailJSON, _ := json.Marshal(detail)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return apperr.New(apperr.PersistenceFailed, "begin write completion: %v", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE write_records SET state=?,error_code=?,payload=?,updated_at=? WHERE intent_id=?`,
		outcome.State, outcome.ErrorCode, string(detailJSON), now, intentID)
	if err != nil {
		_ = tx.Rollback()
		return apperr.New(apperr.PersistenceFailed, "update write record: %v", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		_ = tx.Rollback()
		return apperr.New(apperr.PersistenceFailed, "write record for intent %s not found", intentID)
	}
	if outcome.Actual != nil {
		if err := s.updateObjectStateTx(ctx, tx, intentID, outcome.Actual, outcome.Sources); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE write_intents SET state='committed',updated_at=? WHERE intent_id=?`, now, intentID); err != nil {
		_ = tx.Rollback()
		return apperr.New(apperr.PersistenceFailed, "commit write intent: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO intent_log(intent_id,event,detail,created_at) VALUES(?,?,?,?)`,
		intentID, "committed", string(detailJSON), now); err != nil {
		_ = tx.Rollback()
		return apperr.New(apperr.PersistenceFailed, "write completion intent log: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return apperr.New(apperr.PersistenceFailed, "commit write completion: %v", err)
	}
	return nil
}

func (s *Store) updateObjectStateTx(ctx context.Context, tx *sql.Tx, intentID string, actual map[string]model.FieldValue, sources map[string]model.FieldSource) error {
	var roundID, objectID string
	if err := tx.QueryRowContext(ctx, `SELECT round_id,object_id FROM write_intents WHERE intent_id=?`, intentID).Scan(&roundID, &objectID); err != nil {
		return apperr.New(apperr.PersistenceFailed, "resolve write intent object: %v", err)
	}
	var payload string
	if err := tx.QueryRowContext(ctx, `SELECT payload FROM cluster_objects WHERE snapshot_version=(SELECT snapshot_version FROM rounds WHERE round_id=?) AND object_id=?`, roundID, objectID).Scan(&payload); err != nil {
		return apperr.New(apperr.PersistenceFailed, "load object payload for %s: %v", objectID, err)
	}
	var obj model.Object
	if err := json.Unmarshal([]byte(payload), &obj); err != nil {
		return apperr.New(apperr.PersistenceFailed, "decode object payload for %s: %v", objectID, err)
	}
	obj.Actual = actual
	if sources != nil {
		obj.Sources = sources
	}
	actualJSON, _ := json.Marshal(obj.Actual)
	sourcesJSON, _ := json.Marshal(obj.Sources)
	objJSON, _ := json.Marshal(obj)
	result, err := tx.ExecContext(ctx, `UPDATE cluster_objects SET actual_json=?,sources_json=?,payload=? WHERE snapshot_version=(SELECT snapshot_version FROM rounds WHERE round_id=?) AND object_id=?`,
		string(actualJSON), string(sourcesJSON), string(objJSON), roundID, objectID)
	if err != nil {
		return apperr.New(apperr.PersistenceFailed, "persist object state %s: %v", objectID, err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return apperr.New(apperr.PersistenceFailed, "object state %s not found", objectID)
	}
	return nil
}

func (s *Store) currentVersionLocked(ctx context.Context) (int64, error) {
	var version sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(version) FROM policy_snapshots`).Scan(&version); err != nil {
		return 0, apperr.New(apperr.PersistenceFailed, "read current snapshot version: %v", err)
	}
	if !version.Valid {
		return 0, nil
	}
	return version.Int64, nil
}

func (s *Store) markIntent(ctx context.Context, intentID, state string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE write_intents SET state=?,updated_at=? WHERE intent_id=?`, state, nowText(), intentID)
	if err != nil {
		return apperr.New(apperr.PersistenceFailed, "mark intent %s %s: %v", intentID, state, err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return apperr.New(apperr.PersistenceFailed, "intent %s not found", intentID)
	}
	return nil
}

func (s *Store) rollbackIntent(ctx context.Context, intentID, detail string) {
	_, _ = s.db.ExecContext(ctx, `UPDATE write_intents SET state='rolled_back',updated_at=? WHERE intent_id=?`, nowText(), intentID)
	_ = s.logIntent(ctx, intentID, "rolled_back", detail)
}

func (s *Store) logIntent(ctx context.Context, intentID, event, detail string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO intent_log(intent_id,event,detail,created_at) VALUES(?,?,?,?)`, intentID, event, detail, nowText())
	return err
}

type RoundRecord struct {
	RoundID         string
	SnapshotVersion int64
	BatchVersionID  int64
	State           string
	ResultJSON      string
	ErrorCode       string
	CreatedAt       string
	UpdatedAt       string
}

func (s *Store) GetRound(ctx context.Context, roundID string) (*RoundRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT round_id,snapshot_version,batch_version_id,state,result_json,error_code,created_at,updated_at FROM rounds WHERE round_id=?`, roundID)
	var r RoundRecord
	if err := row.Scan(&r.RoundID, &r.SnapshotVersion, &r.BatchVersionID, &r.State, &r.ResultJSON, &r.ErrorCode, &r.CreatedAt, &r.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, apperr.New(apperr.PersistenceFailed, "read round: %v", err)
	}
	return &r, nil
}

func (s *Store) CreateRound(ctx context.Context, roundID string, snapshotVersion, batchVersionID int64) (*RoundRecord, bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := nowText()
	result, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO rounds(round_id,snapshot_version,batch_version_id,state,created_at,updated_at) VALUES(?,?,?,?,?,?)`,
		roundID, snapshotVersion, batchVersionID, "running", now, now)
	if err != nil {
		return nil, false, apperr.New(apperr.PersistenceFailed, "create round: %v", err)
	}
	affected, _ := result.RowsAffected()
	round, err := s.GetRound(ctx, roundID)
	if err != nil {
		return nil, false, err
	}
	return round, affected == 1, nil
}

func (s *Store) SaveRoundResult(ctx context.Context, roundID, state, errorCode string, result any) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	payload, _ := json.Marshal(result)
	resultExec, err := s.db.ExecContext(ctx, `UPDATE rounds SET state=?,error_code=?,result_json=?,updated_at=? WHERE round_id=?`,
		state, errorCode, string(payload), nowText(), roundID)
	if err != nil {
		return apperr.New(apperr.PersistenceFailed, "save round result: %v", err)
	}
	if affected, _ := resultExec.RowsAffected(); affected != 1 {
		return apperr.New(apperr.PersistenceFailed, "round %s not found", roundID)
	}
	return nil
}

func nowText() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func randomID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "unique constraint") || strings.Contains(text, "constraint failed")
}
