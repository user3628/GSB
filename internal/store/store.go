package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"motionlab/internal/model"
)

type Store struct {
	db      *sql.DB
	dataDir string
	lockDir string
	writeMu sync.Mutex
}

type RunRow struct {
	RunID           string
	SequenceID      string
	PolicyVersion   int
	PolicyHash      string
	PolicyJSON      string
	PolicyEpoch     int
	ConfigJSON      string
	ConfigHash      string
	Status          string
	CheckpointSeq   int64
	InitialStateSHA string
	InitialFrameSHA string
	CreatedAt       string
	UpdatedAt       string
	RecoveryCount   int
	ReportJSON      string
	ReportSHA       string
}

type BranchRow struct {
	RunID          string
	BranchID       int
	Status         model.BranchStatus
	Reason         string
	Wave           int
	RequestedSteps int
	TargetSteps    int
	NextStep       int
	QuotaUsed      int
	ReclaimedQuota int
	FinalMAE       float64
	StateJSON      string
	StateSHA       string
	LastFrameSHA   string
	ErrorCurveJSON string
	CommittedSeq   int64
}

type BranchSeed struct {
	BranchID       int
	Status         model.BranchStatus
	Reason         string
	Wave           int
	RequestedSteps int
	TargetSteps    int
}

type RunSeed struct {
	RunID           string
	SequenceID      string
	Policy          model.PolicySnapshot
	PolicyJSON      string
	ConfigJSON      string
	ConfigHash      string
	Status          string
	InitialStateSHA string
	InitialFrameSHA string
	Branches        []BranchSeed
}

type StepCommit struct {
	RunID       string
	PolicyHash  string
	ExpectedSeq int64
	BranchID    int
	StepNo      int
	StateJSON   string
	StateSHA    string
	FramePNG    []byte
	FrameSHA    string
	TruthSHA    string
	MAE         float64
	RangeOK     bool
	Decision    string
	Branch      BranchMutation
	Event       *Event
}

type BranchMutation struct {
	Status         model.BranchStatus
	Reason         string
	NextStep       int
	QuotaUsed      int
	ReclaimedQuota int
	FinalMAE       float64
	StateJSON      string
	StateSHA       string
	LastFrameSHA   string
	ErrorCurveJSON string
}

type Event struct {
	Kind       string
	StepNo     int
	DetailJSON string
}

type EventRow struct {
	BranchID   int
	StepNo     int
	Kind       string
	DetailJSON string
	CreatedAt  string
}

type StepRow struct {
	StepNo       int
	StateJSON    string
	StateSHA     string
	FrameSHA     string
	TruthSHA     string
	MAE          float64
	RangeOK      bool
	Decision     string
	CommittedSeq int64
	CommittedAt  string
}

type PolicyEvent struct {
	FromVersion    int    `json:"from_version"`
	ToVersion      int    `json:"to_version"`
	FromHash       string `json:"from_hash"`
	ToHash         string `json:"to_hash"`
	Classification string `json:"classification"`
	Reason         string `json:"reason"`
	CreatedAt      string `json:"created_at"`
}

func Open(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	lockDir := filepath.Join(dataDir, ".locks")
	if err := os.MkdirAll(lockDir, 0o755); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dataDir, "motionlab.db")
	dsn := "file:" + filepath.ToSlash(dbPath) + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db, dataDir: dataDir, lockDir: lockDir}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS runs (
			run_id TEXT PRIMARY KEY,
			sequence_id TEXT NOT NULL,
			policy_version INTEGER NOT NULL,
			policy_hash TEXT NOT NULL,
			policy_json TEXT NOT NULL,
			policy_epoch INTEGER NOT NULL DEFAULT 1,
			config_json TEXT NOT NULL,
			config_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			checkpoint_seq INTEGER NOT NULL DEFAULT 0,
			initial_state_sha TEXT NOT NULL,
			initial_frame_sha TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			recovery_count INTEGER NOT NULL DEFAULT 0,
			report_json TEXT NOT NULL DEFAULT '',
			report_sha TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS branches (
			run_id TEXT NOT NULL,
			branch_id INTEGER NOT NULL,
			status TEXT NOT NULL,
			reason TEXT NOT NULL DEFAULT '',
			wave INTEGER NOT NULL DEFAULT 0,
			requested_steps INTEGER NOT NULL DEFAULT 0,
			target_steps INTEGER NOT NULL DEFAULT 0,
			next_step INTEGER NOT NULL DEFAULT 1,
			quota_used INTEGER NOT NULL DEFAULT 0,
			reclaimed_quota INTEGER NOT NULL DEFAULT 0,
			final_mae REAL NOT NULL DEFAULT 0,
			state_json TEXT NOT NULL DEFAULT '',
			state_sha TEXT NOT NULL DEFAULT '',
			last_frame_sha TEXT NOT NULL DEFAULT '',
			error_curve_json TEXT NOT NULL DEFAULT '[]',
			committed_seq INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (run_id, branch_id),
			FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS steps (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id TEXT NOT NULL,
			branch_id INTEGER NOT NULL,
			step_no INTEGER NOT NULL,
			valid INTEGER NOT NULL DEFAULT 1,
			state_json TEXT NOT NULL,
			state_sha TEXT NOT NULL,
			frame_sha TEXT NOT NULL,
			frame_png BLOB,
			truth_sha TEXT NOT NULL,
			mae REAL NOT NULL,
			range_ok INTEGER NOT NULL,
			decision TEXT NOT NULL,
			committed_seq INTEGER NOT NULL,
			committed_at TEXT NOT NULL,
			FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ux_steps_active ON steps(run_id, branch_id, step_no) WHERE valid = 1`,
		`CREATE INDEX IF NOT EXISTS ix_steps_branch ON steps(run_id, branch_id, step_no, valid)`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			scope TEXT NOT NULL,
			seq INTEGER NOT NULL,
			run_id TEXT NOT NULL,
			policy_hash TEXT NOT NULL,
			payload_sha TEXT NOT NULL,
			created_at TEXT NOT NULL,
			PRIMARY KEY (scope, seq)
		)`,
		`CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id TEXT NOT NULL,
			branch_id INTEGER NOT NULL,
			step_no INTEGER NOT NULL,
			kind TEXT NOT NULL,
			detail_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS ix_events_run ON events(run_id, branch_id, id)`,
		`CREATE TABLE IF NOT EXISTS policy_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id TEXT NOT NULL,
			from_version INTEGER NOT NULL,
			to_version INTEGER NOT NULL,
			from_hash TEXT NOT NULL,
			to_hash TEXT NOT NULL,
			classification TEXT NOT NULL,
			reason TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO schema_meta(key,value) VALUES('schema_version','1')`)
	return err
}

func nowString() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func (s *Store) CreateRun(ctx context.Context, seed RunSeed) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := nowString()
	_, err = tx.ExecContext(ctx, `INSERT INTO runs(
		run_id,sequence_id,policy_version,policy_hash,policy_json,policy_epoch,config_json,config_hash,
		status,checkpoint_seq,initial_state_sha,initial_frame_sha,created_at,updated_at,recovery_count
	) VALUES(?,?,?,?,?,1,?,?,?,0,?,?,?,?,0)`,
		seed.RunID, seed.SequenceID, seed.Policy.Version, seed.Policy.Hash(), seed.PolicyJSON,
		seed.ConfigJSON, seed.ConfigHash, seed.Status, seed.InitialStateSHA, seed.InitialFrameSHA, now, now)
	if err != nil {
		return conflictOr(err)
	}
	for _, b := range seed.Branches {
		_, err = tx.ExecContext(ctx, `INSERT INTO branches(
			run_id,branch_id,status,reason,wave,requested_steps,target_steps,next_step,error_curve_json
		) VALUES(?,?,?,?,?,?,?,?, '[]')`, seed.RunID, b.BranchID, b.Status, b.Reason, b.Wave, b.RequestedSteps, b.TargetSteps, 1)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func conflictOr(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "unique constraint") {
		return model.NewError(model.CodePolicyConflict, "record already exists for this deterministic key")
	}
	return err
}

func (s *Store) GetRun(ctx context.Context, runID string) (*RunRow, error) {
	row := s.db.QueryRowContext(ctx, `SELECT run_id,sequence_id,policy_version,policy_hash,policy_json,policy_epoch,
		config_json,config_hash,status,checkpoint_seq,initial_state_sha,initial_frame_sha,created_at,updated_at,
		recovery_count,report_json,report_sha FROM runs WHERE run_id=?`, runID)
	var r RunRow
	err := row.Scan(&r.RunID, &r.SequenceID, &r.PolicyVersion, &r.PolicyHash, &r.PolicyJSON, &r.PolicyEpoch,
		&r.ConfigJSON, &r.ConfigHash, &r.Status, &r.CheckpointSeq, &r.InitialStateSHA, &r.InitialFrameSHA,
		&r.CreatedAt, &r.UpdatedAt, &r.RecoveryCount, &r.ReportJSON, &r.ReportSHA)
	if errors.Is(err, sql.ErrNoRows) {
		e := model.NewError(model.CodeNotFound, "run not found")
		e.Details = map[string]any{"run_id": runID}
		return nil, e
	}
	return &r, err
}

func (s *Store) ListRuns(ctx context.Context, limit int) ([]RunRow, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT run_id,sequence_id,policy_version,policy_hash,policy_json,policy_epoch,
		config_json,config_hash,status,checkpoint_seq,initial_state_sha,initial_frame_sha,created_at,updated_at,
		recovery_count,report_json,report_sha FROM runs ORDER BY updated_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RunRow
	for rows.Next() {
		var r RunRow
		if err := rows.Scan(&r.RunID, &r.SequenceID, &r.PolicyVersion, &r.PolicyHash, &r.PolicyJSON, &r.PolicyEpoch,
			&r.ConfigJSON, &r.ConfigHash, &r.Status, &r.CheckpointSeq, &r.InitialStateSHA, &r.InitialFrameSHA,
			&r.CreatedAt, &r.UpdatedAt, &r.RecoveryCount, &r.ReportJSON, &r.ReportSHA); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) LoadBranches(ctx context.Context, runID string) ([]BranchRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT run_id,branch_id,status,reason,wave,requested_steps,target_steps,next_step,
		quota_used,reclaimed_quota,final_mae,state_json,state_sha,last_frame_sha,error_curve_json,committed_seq
		FROM branches WHERE run_id=? ORDER BY branch_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BranchRow
	for rows.Next() {
		var b BranchRow
		if err := rows.Scan(&b.RunID, &b.BranchID, &b.Status, &b.Reason, &b.Wave, &b.RequestedSteps, &b.TargetSteps,
			&b.NextStep, &b.QuotaUsed, &b.ReclaimedQuota, &b.FinalMAE, &b.StateJSON, &b.StateSHA, &b.LastFrameSHA,
			&b.ErrorCurveJSON, &b.CommittedSeq); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) LoadSteps(ctx context.Context, runID string, branchID int) ([]StepRow, error) {
	query := `SELECT step_no,state_json,state_sha,frame_sha,truth_sha,mae,range_ok,decision,committed_seq,committed_at
		FROM steps WHERE run_id=? AND branch_id=? AND valid=1 ORDER BY step_no`
	rows, err := s.db.QueryContext(ctx, query, runID, branchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StepRow
	for rows.Next() {
		var r StepRow
		if err := rows.Scan(&r.StepNo, &r.StateJSON, &r.StateSHA, &r.FrameSHA, &r.TruthSHA, &r.MAE,
			&r.RangeOK, &r.Decision, &r.CommittedSeq, &r.CommittedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) LoadStepPNG(ctx context.Context, runID string, branchID, step int) ([]byte, string, error) {
	var data []byte
	var sha string
	err := s.db.QueryRowContext(ctx, `SELECT frame_png,frame_sha FROM steps WHERE run_id=? AND branch_id=? AND step_no=? AND valid=1`,
		runID, branchID, step).Scan(&data, &sha)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", model.NewError(model.CodeNotFound, "predicted frame not found")
	}
	return data, sha, err
}

func (s *Store) LoadEvents(ctx context.Context, runID string) ([]EventRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,branch_id,step_no,kind,detail_json,created_at FROM events
		WHERE run_id=? ORDER BY id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventRow
	for rows.Next() {
		var e EventRow
		var id int64
		if err := rows.Scan(&id, &e.BranchID, &e.StepNo, &e.Kind, &e.DetailJSON, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func checkpointScope(runID string, epoch int) string {
	return fmt.Sprintf("run:%s:e%d", runID, epoch)
}

// CommitStep atomically publishes one branch step, branch progress, event and
// a strictly consecutive run checkpoint.
func (s *Store) CommitStep(ctx context.Context, c StepCommit) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	scope := checkpointScope(c.RunID, 0)
	var epoch int
	if err := s.db.QueryRowContext(ctx, `SELECT policy_epoch FROM runs WHERE run_id=?`, c.RunID).Scan(&epoch); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, model.NewError(model.CodeNotFound, "run not found")
		}
		return 0, err
	}
	scope = checkpointScope(c.RunID, epoch)
	var seq int64
	err := s.withFileLock(ctx, scope, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var current int64
		var policyHash string
		if err := tx.QueryRowContext(ctx, `SELECT checkpoint_seq,policy_hash FROM runs WHERE run_id=?`, c.RunID).Scan(&current, &policyHash); err != nil {
			return err
		}
		if policyHash != c.PolicyHash {
			e := model.NewError(model.CodePolicyConflict, "policy snapshot changed while a branch was running")
			e.Details = map[string]any{"expected_policy_hash": c.PolicyHash, "actual_policy_hash": policyHash}
			return e
		}
		if c.ExpectedSeq == 0 {
			c.ExpectedSeq = current + 1
		}
		if c.ExpectedSeq <= current {
			return staleCheckpoint(scope, c.ExpectedSeq, current)
		}
		if c.ExpectedSeq != current+1 {
			return staleCheckpoint(scope, c.ExpectedSeq, current)
		}
		seq = c.ExpectedSeq
		now := nowString()
		res, err := tx.ExecContext(ctx, `UPDATE runs SET checkpoint_seq=?,updated_at=? WHERE run_id=? AND checkpoint_seq=? AND policy_hash=?`,
			seq, now, c.RunID, seq-1, c.PolicyHash)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return staleCheckpoint(scope, seq, current)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO steps(
			run_id,branch_id,step_no,valid,state_json,state_sha,frame_sha,frame_png,truth_sha,mae,range_ok,decision,committed_seq,committed_at
		) VALUES(?,?,?,1,?,?,?,?,?,?,?,?,?,?)`, c.RunID, c.BranchID, c.StepNo, c.StateJSON, c.StateSHA,
			c.FrameSHA, c.FramePNG, c.TruthSHA, c.MAE, boolInt(c.RangeOK), c.Decision, seq, now); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique constraint") {
				return model.NewError(model.CodeStaleCheckpoint, "a valid record already exists for this branch step")
			}
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE branches SET status=?,reason=?,next_step=?,quota_used=?,reclaimed_quota=?,
			final_mae=?,state_json=?,state_sha=?,last_frame_sha=?,error_curve_json=?,committed_seq=? WHERE run_id=? AND branch_id=?`,
			c.Branch.Status, c.Branch.Reason, c.Branch.NextStep, c.Branch.QuotaUsed, c.Branch.ReclaimedQuota,
			c.Branch.FinalMAE, c.Branch.StateJSON, c.Branch.StateSHA, c.Branch.LastFrameSHA, c.Branch.ErrorCurveJSON,
			seq, c.RunID, c.BranchID)
		if err != nil {
			return err
		}
		if c.Event != nil {
			if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,branch_id,step_no,kind,detail_json,created_at)
				VALUES(?,?,?,?,?,?)`, c.RunID, c.BranchID, c.Event.StepNo, c.Event.Kind, c.Event.DetailJSON, now); err != nil {
				return err
			}
		}
		payload := model.Digest(map[string]any{
			"run_id": c.RunID, "branch_id": c.BranchID, "step": c.StepNo, "state": c.StateSHA,
			"frame": c.FrameSHA, "mae": c.MAE, "decision": c.Decision,
		})
		if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoints(scope,seq,run_id,policy_hash,payload_sha,created_at)
			VALUES(?,?,?,?,?,?)`, scope, seq, c.RunID, c.PolicyHash, payload, now); err != nil {
			return err
		}
		return tx.Commit()
	})
	return seq, err
}

func staleCheckpoint(scope string, expected, actual int64) *model.AppError {
	e := model.NewError(model.CodeStaleCheckpoint,
		fmt.Sprintf("checkpoint sequence must advance consecutively: expected %d, stored %d", expected, actual))
	e.Details = map[string]any{"scope": scope, "expected_seq": expected, "stored_seq": actual}
	return e
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (s *Store) UpdateBranchStatus(ctx context.Context, runID string, branchID int, status model.BranchStatus, reason string, nextStep int, curveJSON string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE branches SET status=?,reason=?,next_step=?,error_curve_json=? WHERE run_id=? AND branch_id=?`,
		status, reason, nextStep, curveJSON, runID, branchID)
	return err
}

func (s *Store) SetRunStatus(ctx context.Context, runID, status string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE runs SET status=?,updated_at=? WHERE run_id=?`, status, nowString(), runID)
	return err
}

func (s *Store) IncrementRecovery(ctx context.Context, runID string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE runs SET recovery_count=recovery_count+1,updated_at=? WHERE run_id=?`, nowString(), runID)
	return err
}

func (s *Store) FinalizeRun(ctx context.Context, runID, status, reportJSON, reportSHA string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE runs SET status=?,report_json=?,report_sha=?,updated_at=? WHERE run_id=?`,
		status, reportJSON, reportSHA, nowString(), runID)
	return err
}

func (s *Store) GetReportJSON(ctx context.Context, runID string) (string, string, string, error) {
	var status, report, sha string
	err := s.db.QueryRowContext(ctx, `SELECT status,report_json,report_sha FROM runs WHERE run_id=?`, runID).Scan(&status, &report, &sha)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", "", model.NewError(model.CodeNotFound, "run not found")
	}
	return status, report, sha, err
}

func (s *Store) LatestCheckpoint(ctx context.Context, runID string) (int64, error) {
	var epoch int
	if err := s.db.QueryRowContext(ctx, `SELECT policy_epoch FROM runs WHERE run_id=?`, runID).Scan(&epoch); err != nil {
		return 0, err
	}
	var seq int64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM checkpoints WHERE scope=?`, checkpointScope(runID, epoch)).Scan(&seq)
	return seq, err
}

// PublishCheckpoint is exposed for multi-worker publication tests and tooling.
func (s *Store) PublishCheckpoint(ctx context.Context, runID string, seq int64, payloadSHA string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	scope := checkpointScope(runID, run.PolicyEpoch)
	return s.withFileLock(ctx, scope, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var current int64
		var policyHash string
		if err := tx.QueryRowContext(ctx, `SELECT checkpoint_seq,policy_hash FROM runs WHERE run_id=?`, runID).Scan(&current, &policyHash); err != nil {
			return err
		}
		if seq != current+1 {
			e := staleCheckpoint(scope, seq, current)
			return e
		}
		res, err := tx.ExecContext(ctx, `UPDATE runs SET checkpoint_seq=?,updated_at=? WHERE run_id=? AND checkpoint_seq=? AND policy_hash=?`,
			seq, nowString(), runID, current, policyHash)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return staleCheckpoint(scope, seq, current)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoints(scope,seq,run_id,policy_hash,payload_sha,created_at)
			VALUES(?,?,?,?,?,?)`, scope, seq, runID, run.PolicyHash, payloadSHA, nowString()); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func (s *Store) ApplyPolicy(ctx context.Context, runID string, next model.PolicySnapshot, policyJSON, configJSON, configHash string, plans []BranchSeed, classification, reason string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var fromVersion, epoch int
	var fromHash string
	if err := tx.QueryRowContext(ctx, `SELECT policy_version,policy_hash,policy_epoch FROM runs WHERE run_id=?`, runID).Scan(&fromVersion, &fromHash, &epoch); err != nil {
		return err
	}
	now := nowString()
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET policy_version=?,policy_hash=?,policy_json=?,policy_epoch=?,config_json=?,config_hash=?,
		status='pending',checkpoint_seq=0,report_json='',report_sha='',updated_at=? WHERE run_id=?`,
		next.Version, next.Hash(), policyJSON, epoch+1, configJSON, configHash, now, runID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE steps SET valid=0 WHERE run_id=? AND valid=1`, runID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM branches WHERE run_id=?`, runID); err != nil {
		return err
	}
	for _, b := range plans {
		if _, err := tx.ExecContext(ctx, `INSERT INTO branches(
			run_id,branch_id,status,reason,wave,requested_steps,target_steps,next_step,error_curve_json
		) VALUES(?,?,?,?,?,?,?,1,'[]')`, runID, b.BranchID, b.Status, b.Reason, b.Wave, b.RequestedSteps, b.TargetSteps); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO policy_events(run_id,from_version,to_version,from_hash,to_hash,classification,reason,created_at)
		VALUES(?,?,?,?,?,?,?,?)`, runID, fromVersion, next.Version, fromHash, next.Hash(), classification, reason, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) AdvancePolicy(ctx context.Context, runID string, next model.PolicySnapshot, policyJSON, classification, reason string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var fromVersion int
	var fromHash string
	if err := tx.QueryRowContext(ctx, `SELECT policy_version,policy_hash FROM runs WHERE run_id=?`, runID).Scan(&fromVersion, &fromHash); err != nil {
		return err
	}
	now := nowString()
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET policy_version=?,policy_hash=?,policy_json=?,updated_at=? WHERE run_id=?`,
		next.Version, next.Hash(), policyJSON, now, runID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO policy_events(run_id,from_version,to_version,from_hash,to_hash,classification,reason,created_at)
		VALUES(?,?,?,?,?,?,?,?)`, runID, fromVersion, next.Version, fromHash, next.Hash(), classification, reason, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) DeleteRun(ctx context.Context, runID string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, stmt := range []string{
		`DELETE FROM checkpoints WHERE run_id=?`,
		`DELETE FROM events WHERE run_id=?`,
		`DELETE FROM policy_events WHERE run_id=?`,
		`DELETE FROM steps WHERE run_id=?`,
		`DELETE FROM branches WHERE run_id=?`,
		`DELETE FROM runs WHERE run_id=?`,
	} {
		if _, err := tx.ExecContext(ctx, stmt, runID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) PolicyEvents(ctx context.Context, runID string) ([]PolicyEvent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT from_version,to_version,from_hash,to_hash,classification,reason,created_at
		FROM policy_events WHERE run_id=? ORDER BY id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PolicyEvent
	for rows.Next() {
		var e PolicyEvent
		if err := rows.Scan(&e.FromVersion, &e.ToVersion, &e.FromHash, &e.ToHash, &e.Classification, &e.Reason, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) LoadEventsStructured(ctx context.Context, runID string) ([]map[string]any, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT branch_id,step_no,kind,detail_json,created_at FROM events WHERE run_id=? ORDER BY id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var branch, step int
		var kind, detail, created string
		if err := rows.Scan(&branch, &step, &kind, &detail, &created); err != nil {
			return nil, err
		}
		var parsed any
		_ = json.Unmarshal([]byte(detail), &parsed)
		out = append(out, map[string]any{"branch_id": branch, "step": step, "kind": kind, "detail": parsed, "created_at": created})
	}
	return out, rows.Err()
}
