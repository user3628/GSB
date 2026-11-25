package convergence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db   *sql.DB
	path string
}

type IntentRecord struct {
	ID          string `json:"intent_id"`
	Kind        string `json:"kind"`
	ScopeID     string `json:"scope_id"`
	PayloadJSON string `json:"payload_json,omitempty"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

type RolledBackBatch struct {
	Version              int64  `json:"version"`
	BatchID              string `json:"batch_id"`
	IntentID             string `json:"intent_id"`
	PreviousStatus       string `json:"previous_status"`
	NewStatus            string `json:"new_status"`
	DiscardedObjectFacts int64  `json:"discarded_object_facts"`
}

type RecoveryReport struct {
	DatabasePath            string            `json:"database_path"`
	CurrentCommittedVersion int64             `json:"current_committed_version"`
	PreparedIntentCount     int               `json:"prepared_intent_count"`
	AbortedSnapshotSwitches []IntentRecord    `json:"aborted_snapshot_switches"`
	AbortedPlanBuilds       []IntentRecord    `json:"aborted_plan_builds"`
	RolledBackWriteBatches  []RolledBackBatch `json:"rolled_back_write_batches"`
	AbortedOtherIntents     []IntentRecord    `json:"aborted_other_intents"`
	ScannedIntents          []IntentRecord    `json:"scanned_intents"`
	RecoveryBoundary        string            `json:"recovery_boundary"`
	ExitReportPromise       bool              `json:"exit_report_promise"`
}

func OpenStore(ctx context.Context, path string) (*Store, error) {
	if path == "" {
		path = ":memory:"
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, WrapError(CodePersistence, "cannot create database directory", err, map[string]any{"path": path})
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, WrapError(CodePersistence, "cannot open sqlite", err, map[string]any{"path": path})
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, pragma := range []string{
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
		"PRAGMA journal_mode=WAL",
	} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			_ = db.Close()
			return nil, WrapError(CodePersistence, "cannot configure sqlite", err, map[string]any{"pragma": pragma})
		}
	}
	store := &Store{db: db, path: path}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			version INTEGER PRIMARY KEY,
			base_version INTEGER NOT NULL DEFAULT 0,
			kind TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			delta_json TEXT NOT NULL DEFAULT '',
			checksum TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'committed',
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS derived_versions (
			version INTEGER PRIMARY KEY,
			derived_json TEXT NOT NULL,
			content_hash TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			intent_id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			scope_id TEXT NOT NULL,
			payload_json TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS batch_runs (
			batch_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			content_hash TEXT NOT NULL,
			request_hash TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			result_json TEXT NOT NULL DEFAULT '',
			error_code TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL,
			PRIMARY KEY(version, batch_id)
		)`,
		`CREATE TABLE IF NOT EXISTS object_write_intents (
			batch_id TEXT NOT NULL,
			object_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			status TEXT NOT NULL,
			expected_json TEXT NOT NULL,
			result_json TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL,
			PRIMARY KEY(version, batch_id, object_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_batch_runs_version ON batch_runs(version)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return WrapError(CodePersistence, "cannot migrate sqlite schema", err, map[string]any{"statement": statement})
		}
	}
	return nil
}

func nowStamp() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func (s *Store) CreateIntent(ctx context.Context, kind, scopeID string, payload any) (*IntentRecord, error) {
	payloadJSON := ""
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, WrapError(CodePersistence, "cannot encode intent payload", err, nil)
		}
		payloadJSON = string(data)
	}
	intent := &IntentRecord{
		ID:          newIntentID(kind),
		Kind:        kind,
		ScopeID:     scopeID,
		PayloadJSON: payloadJSON,
		Status:      "prepared",
		CreatedAt:   nowStamp(),
		UpdatedAt:   nowStamp(),
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO intents(intent_id, kind, scope_id, payload_json, status, created_at, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?)`,
		intent.ID, intent.Kind, intent.ScopeID, intent.PayloadJSON, intent.Status, intent.CreatedAt, intent.UpdatedAt)
	if err != nil {
		return nil, WrapError(CodePersistence, "cannot persist intent", err, map[string]any{"kind": kind, "scope_id": scopeID})
	}
	return intent, nil
}

func (s *Store) UpdateIntentStatus(ctx context.Context, intentID, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET status=?, updated_at=? WHERE intent_id=?`, status, nowStamp(), intentID)
	return err
}

func (s *Store) ListPreparedIntents(ctx context.Context) ([]IntentRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT intent_id, kind, scope_id, payload_json, status, created_at, updated_at
		FROM intents WHERE status='prepared' ORDER BY created_at ASC, intent_id ASC`)
	if err != nil {
		return nil, WrapError(CodePersistence, "cannot scan intents", err, nil)
	}
	defer rows.Close()
	result := []IntentRecord{}
	for rows.Next() {
		var intent IntentRecord
		if err := rows.Scan(&intent.ID, &intent.Kind, &intent.ScopeID, &intent.PayloadJSON, &intent.Status, &intent.CreatedAt, &intent.UpdatedAt); err != nil {
			return nil, WrapError(CodePersistence, "cannot scan intent row", err, nil)
		}
		result = append(result, intent)
	}
	return result, nil
}

func (s *Store) CurrentVersion(ctx context.Context) (int64, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='current_version'`).Scan(&value)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, WrapError(CodePersistence, "cannot read current version", err, nil)
	}
	var version int64
	if _, err := fmt.Sscan(value, &version); err != nil {
		return 0, WrapError(CodePersistence, "invalid current version meta", err, map[string]any{"value": value})
	}
	return version, nil
}

type saveVersionParams struct {
	IntentID  string
	IntentIDs []string
	Kind      string
	Base      int64
	Snapshot  *Snapshot
	Delta     *Delta
	Derived   *derivedRecord
}

type derivedRecord struct {
	DriftList []DriftReport  `json:"drift_reports"`
	Plan      *Plan          `json:"plan"`
	Summary   Summary        `json:"summary"`
	Cascade   *CascadeReport `json:"cascade,omitempty"`
}

func (s *Store) SaveCommittedVersion(ctx context.Context, params saveVersionParams) error {
	snapshotJSON, err := json.Marshal(params.Snapshot)
	if err != nil {
		return err
	}
	derivedJSON, err := json.Marshal(params.Derived)
	if err != nil {
		return err
	}
	deltaJSON := ""
	if params.Delta != nil {
		data, err := json.Marshal(params.Delta)
		if err != nil {
			return err
		}
		deltaJSON = string(data)
	}
	derivedHash, _ := ContentHash(params.Derived)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WrapError(CodePersistence, "cannot begin version transaction", err, nil)
	}
	defer func() { _ = tx.Rollback() }()

	now := nowStamp()
	if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(version, base_version, kind, payload_json, delta_json, checksum, status, created_at)
		VALUES(?, ?, ?, ?, ?, ?, 'committed', ?)`,
		params.Snapshot.SnapshotVersion, params.Base, params.Kind, string(snapshotJSON), deltaJSON, params.Snapshot.Checksum, now); err != nil {
		return WrapError(CodePersistence, "cannot insert snapshot", err, map[string]any{"version": params.Snapshot.SnapshotVersion})
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO derived_versions(version, derived_json, content_hash, created_at)
		VALUES(?, ?, ?, ?) ON CONFLICT(version) DO UPDATE SET derived_json=excluded.derived_json, content_hash=excluded.content_hash, created_at=excluded.created_at`,
		params.Snapshot.SnapshotVersion, string(derivedJSON), derivedHash, now); err != nil {
		return WrapError(CodePersistence, "cannot insert derived version", err, nil)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES('current_version',?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, fmt.Sprint(params.Snapshot.SnapshotVersion)); err != nil {
		return WrapError(CodePersistence, "cannot update current version", err, nil)
	}
	intentIDs := append([]string{}, params.IntentIDs...)
	if params.IntentID != "" {
		intentIDs = append(intentIDs, params.IntentID)
	}
	for _, intentID := range intentIDs {
		if intentID == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed', updated_at=? WHERE intent_id=?`, now, intentID); err != nil {
			return WrapError(CodePersistence, "cannot commit intent", err, map[string]any{"intent_id": intentID})
		}
	}
	if err := tx.Commit(); err != nil {
		return WrapError(CodePersistence, "cannot commit version transaction", err, nil)
	}
	return nil
}

func (s *Store) LoadCommittedSnapshot(ctx context.Context, version int64) (*Snapshot, *derivedRecord, error) {
	var payload string
	var derivedJSON string
	err := s.db.QueryRowContext(ctx, `SELECT payload_json FROM snapshots WHERE version=? AND status='committed'`, version).Scan(&payload)
	if err != nil {
		return nil, nil, WrapError(CodePersistence, "cannot load snapshot", err, map[string]any{"version": version})
	}
	var snapshot Snapshot
	if err := json.Unmarshal([]byte(payload), &snapshot); err != nil {
		return nil, nil, WrapError(CodePersistence, "cannot decode persisted snapshot", err, nil)
	}
	err = s.db.QueryRowContext(ctx, `SELECT derived_json FROM derived_versions WHERE version=?`, version).Scan(&derivedJSON)
	if err != nil && err != sql.ErrNoRows {
		return nil, nil, WrapError(CodePersistence, "cannot load derived version", err, nil)
	}
	derived := &derivedRecord{}
	if derivedJSON != "" {
		if err := json.Unmarshal([]byte(derivedJSON), derived); err != nil {
			return nil, nil, WrapError(CodePersistence, "cannot decode persisted derived version", err, nil)
		}
	}
	return &snapshot, derived, nil
}

func (s *Store) PrepareBatchRun(ctx context.Context, version int64, state *BatchState, objectIDs []string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	now := nowStamp()
	if _, err := tx.ExecContext(ctx, `INSERT INTO batch_runs(batch_id, version, content_hash, request_hash, status, attempts, updated_at)
		VALUES(?, ?, ?, ?, 'applying', 0, ?)
		ON CONFLICT(version, batch_id) DO UPDATE SET content_hash=excluded.content_hash,
		  request_hash=excluded.request_hash, status='applying', updated_at=excluded.updated_at`,
		state.Batch.BatchID, version, state.ContentHash, state.RequestHash, now); err != nil {
		return WrapError(CodePersistence, "cannot prepare batch run", err, map[string]any{"batch_id": state.Batch.BatchID})
	}
	for _, objectID := range objectIDs {
		expectedJSON, _ := json.Marshal(map[string]any{"object_id": objectID})
		if _, err := tx.ExecContext(ctx, `INSERT INTO object_write_intents(batch_id, object_id, version, status, expected_json, updated_at)
			VALUES(?, ?, ?, 'prepared', ?, ?)
			ON CONFLICT(version, batch_id, object_id) DO UPDATE SET status='prepared', result_json='', updated_at=excluded.updated_at`,
			state.Batch.BatchID, objectID, version, string(expectedJSON), now); err != nil {
			return WrapError(CodePersistence, "cannot prepare object write intent", err, map[string]any{"batch_id": state.Batch.BatchID, "object_id": objectID})
		}
	}
	return tx.Commit()
}

func (s *Store) CompleteBatchRun(ctx context.Context, intentID string, result *BatchApplyResult) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return err
	}
	errorCode := ""
	if result.Error != nil {
		errorCode = result.Error.Code
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	now := nowStamp()
	status := result.Status
	if status == "" {
		status = "failed"
	}
	if _, err := tx.ExecContext(ctx, `UPDATE batch_runs SET status=?, attempts=?, result_json=?, error_code=?, updated_at=? WHERE version=? AND batch_id=?`,
		status, result.Attempts, string(resultJSON), errorCode, now, result.Version, result.BatchID); err != nil {
		return WrapError(CodePersistence, "cannot complete batch run", err, map[string]any{"batch_id": result.BatchID})
	}
	for _, objectResult := range result.ObjectResults {
		payload, _ := json.Marshal(objectResult)
		objectStatus := objectResult.Status
		if objectStatus == "" {
			objectStatus = "attempted"
		}
		if _, err := tx.ExecContext(ctx, `UPDATE object_write_intents SET status=?, result_json=?, updated_at=? WHERE version=? AND batch_id=? AND object_id=?`,
			objectStatus, string(payload), now, result.Version, result.BatchID, objectResult.ObjectID); err != nil {
			return WrapError(CodePersistence, "cannot complete object write fact", err, map[string]any{"object_id": objectResult.ObjectID})
		}
	}
	if intentID != "" {
		intentStatus := "completed"
		if status == "succeeded" {
			intentStatus = "committed"
		}
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status=?, updated_at=? WHERE intent_id=?`, intentStatus, now, intentID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

type persistedBatchRun struct {
	BatchID     string
	Version     int64
	ContentHash string
	RequestHash string
	Status      string
	Attempts    int
	ResultJSON  string
	ErrorCode   string
	UpdatedAt   string
}

func (s *Store) LoadBatchRuns(ctx context.Context, version int64) ([]persistedBatchRun, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT batch_id, version, content_hash, request_hash, status, attempts, result_json, error_code, updated_at
		FROM batch_runs WHERE version=? ORDER BY batch_id`, version)
	if err != nil {
		return nil, WrapError(CodePersistence, "cannot load batch runs", err, nil)
	}
	defer rows.Close()
	result := []persistedBatchRun{}
	for rows.Next() {
		var row persistedBatchRun
		if err := rows.Scan(&row.BatchID, &row.Version, &row.ContentHash, &row.RequestHash, &row.Status, &row.Attempts, &row.ResultJSON, &row.ErrorCode, &row.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, nil
}

func (s *Store) FindBatchRun(ctx context.Context, version int64, batchID string) (*persistedBatchRun, error) {
	var row persistedBatchRun
	err := s.db.QueryRowContext(ctx, `SELECT batch_id, version, content_hash, request_hash, status, attempts, result_json, error_code, updated_at
		FROM batch_runs WHERE version=? AND batch_id=?`, version, batchID).Scan(
		&row.BatchID, &row.Version, &row.ContentHash, &row.RequestHash, &row.Status, &row.Attempts, &row.ResultJSON, &row.ErrorCode, &row.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, WrapError(CodePersistence, "cannot find batch run", err, map[string]any{"version": version, "batch_id": batchID})
	}
	return &row, nil
}

func (s *Store) Recover(ctx context.Context) (*RecoveryReport, error) {
	currentVersion, err := s.CurrentVersion(ctx)
	if err != nil {
		return nil, err
	}
	intents, err := s.ListPreparedIntents(ctx)
	if err != nil {
		return nil, err
	}
	report := &RecoveryReport{
		DatabasePath:            s.path,
		CurrentCommittedVersion: currentVersion,
		PreparedIntentCount:     len(intents),
		ScannedIntents:          intents,
		AbortedSnapshotSwitches: []IntentRecord{},
		AbortedPlanBuilds:       []IntentRecord{},
		RolledBackWriteBatches:  []RolledBackBatch{},
		AbortedOtherIntents:     []IntentRecord{},
		RecoveryBoundary: "Only local SQLite intent/run status is rewound to a persisted pending boundary. " +
			"An HTTP write may have reached the loopback target before termination; the next desired-state apply is idempotent and performs GET readback. " +
			"No forced-kill process can generate or promise an OS exit report; this report is reconstructed after restart from committed SQLite facts.",
		ExitReportPromise: false,
	}

	for _, intent := range intents {
		switch intent.Kind {
		case "snapshot_switch":
			if err := s.UpdateIntentStatus(ctx, intent.ID, "aborted"); err != nil {
				return report, err
			}
			intent.Status = "aborted"
			report.AbortedSnapshotSwitches = append(report.AbortedSnapshotSwitches, intent)
		case "plan_build":
			if err := s.UpdateIntentStatus(ctx, intent.ID, "aborted"); err != nil {
				return report, err
			}
			intent.Status = "aborted"
			report.AbortedPlanBuilds = append(report.AbortedPlanBuilds, intent)
		case "batch_apply":
			rolled, err := s.rollbackWriteIntent(ctx, intent)
			if err != nil {
				return report, err
			}
			if rolled != nil {
				report.RolledBackWriteBatches = append(report.RolledBackWriteBatches, *rolled)
			}
		default:
			if err := s.UpdateIntentStatus(ctx, intent.ID, "aborted"); err != nil {
				return report, err
			}
			intent.Status = "aborted"
			report.AbortedOtherIntents = append(report.AbortedOtherIntents, intent)
		}
	}

	data, _ := json.Marshal(report)
	_, _ = s.db.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES('last_recovery_report',?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, string(data))
	return report, nil
}

func (s *Store) rollbackWriteIntent(ctx context.Context, intent IntentRecord) (*RolledBackBatch, error) {
	version, batchID := parseBatchScope(intent.ScopeID)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var previousStatus string
	if version > 0 {
		err = tx.QueryRowContext(ctx, `SELECT status FROM batch_runs WHERE version=? AND batch_id=?`, version, batchID).Scan(&previousStatus)
	} else {
		err = tx.QueryRowContext(ctx, `SELECT status FROM batch_runs WHERE batch_id=? ORDER BY version DESC LIMIT 1`, batchID).Scan(&previousStatus)
	}
	if err == sql.ErrNoRows {
		previousStatus = "missing"
	} else if err != nil {
		return nil, err
	}

	var objectFacts int64
	objectQuery := `SELECT count(*) FROM object_write_intents WHERE batch_id=? AND status IN ('prepared','writing')`
	args := []any{batchID}
	if version > 0 {
		objectQuery += ` AND version=?`
		args = append(args, version)
	}
	if err := tx.QueryRowContext(ctx, objectQuery, args...).Scan(&objectFacts); err != nil {
		return nil, err
	}
	if version > 0 {
		_, err = tx.ExecContext(ctx, `UPDATE batch_runs SET status='pending', error_code='E_RECOVERY_ROLLBACK', updated_at=? WHERE version=? AND batch_id=? AND status IN ('applying','prepared')`, nowStamp(), version, batchID)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE batch_runs SET status='pending', error_code='E_RECOVERY_ROLLBACK', updated_at=? WHERE batch_id=? AND status IN ('applying','prepared')`, nowStamp(), batchID)
	}
	if err != nil {
		return nil, err
	}
	if version > 0 {
		_, err = tx.ExecContext(ctx, `UPDATE object_write_intents SET status='rolled_back', updated_at=? WHERE version=? AND batch_id=? AND status IN ('prepared','writing')`, nowStamp(), version, batchID)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE object_write_intents SET status='rolled_back', updated_at=? WHERE batch_id=? AND status IN ('prepared','writing')`, nowStamp(), batchID)
	}
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='rolled_back', updated_at=? WHERE intent_id=?`, nowStamp(), intent.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &RolledBackBatch{
		Version:              version,
		BatchID:              batchID,
		IntentID:             intent.ID,
		PreviousStatus:       previousStatus,
		NewStatus:            "pending",
		DiscardedObjectFacts: objectFacts,
	}, nil
}

func batchScope(version int64, batchID string) string {
	return fmt.Sprintf("%d/%s", version, batchID)
}

func parseBatchScope(scope string) (int64, string) {
	parts := strings.SplitN(scope, "/", 2)
	if len(parts) == 2 {
		var version int64
		if _, err := fmt.Sscan(parts[0], &version); err == nil {
			return version, parts[1]
		}
	}
	return 0, scope
}

func newIntentID(kind string) string {
	return kind + "-" + randomHex(8)
}

func randomHex(n int) string {
	bytes := make([]byte, n)
	_, _ = readRandom(bytes)
	return fmt.Sprintf("%x", bytes)
}
