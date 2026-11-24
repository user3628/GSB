package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	_ "modernc.org/sqlite"

	"hostacl-workbench/internal/model"
)

type Store struct {
	db *sql.DB
}

type Head struct {
	Version     int64  `json:"version"`
	Digest      string `json:"digest"`
	ViewDigest  string `json:"view_digest"`
	TreePath    string `json:"tree_path"`
	GeneratedAt string `json:"generated_at,omitempty"`
}

type Intent struct {
	ID            string `json:"id"`
	Kind          string `json:"kind"`
	State         string `json:"state"`
	BaseVersion   int64  `json:"base_version"`
	TargetVersion int64  `json:"target_version"`
	PayloadJSON   string `json:"payload_json"`
	CreatedAt     string `json:"created_at"`
	FinishedAt    string `json:"finished_at,omitempty"`
	OutcomeJSON   string `json:"outcome_json,omitempty"`
	Error         string `json:"error,omitempty"`
}

type StoredVerificationResult struct {
	SubjectID    string `json:"subject_id"`
	Path         string `json:"path"`
	OpenMode     string `json:"open_mode"`
	Requested    string `json:"requested"`
	Actual       string `json:"actual"`
	KernelDenied bool   `json:"kernel_denied"`
	Expected     string `json:"expected"`
	Match        bool   `json:"match"`
	Identity     string `json:"identity"`
	Engine       string `json:"engine"`
	Error        string `json:"error,omitempty"`
}

type ApplyRun struct {
	ID         string          `json:"id"`
	IntentID   string          `json:"intent_id"`
	Version    int64           `json:"version"`
	TreePath   string          `json:"tree_path"`
	ReportJSON json.RawMessage `json:"report"`
	CreatedAt  string          `json:"created_at"`
}

const schema = `
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS policy_versions (
  version INTEGER PRIMARY KEY,
  base_version INTEGER NOT NULL,
  generated_at TEXT NOT NULL,
  digest TEXT NOT NULL,
  spec_json TEXT NOT NULL,
  view_digest TEXT NOT NULL,
  tree_path TEXT NOT NULL,
  cascade_json TEXT NOT NULL,
  committed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS policy_head (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  version INTEGER NOT NULL,
  digest TEXT NOT NULL,
  view_digest TEXT NOT NULL,
  tree_path TEXT NOT NULL
);
INSERT OR IGNORE INTO policy_head(singleton,version,digest,view_digest,tree_path)
VALUES(1,0,'','','');
CREATE TABLE IF NOT EXISTS effective_views (
  version INTEGER NOT NULL,
  subject_id TEXT NOT NULL,
  path TEXT NOT NULL,
  can_read INTEGER NOT NULL,
  can_write INTEGER NOT NULL,
  digest TEXT NOT NULL,
  detail_json TEXT NOT NULL,
  PRIMARY KEY(version, subject_id, path)
);
CREATE TABLE IF NOT EXISTS intents (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  state TEXT NOT NULL,
  base_version INTEGER NOT NULL,
  target_version INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  finished_at TEXT,
  outcome_json TEXT NOT NULL DEFAULT '',
  error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS intents_state_idx ON intents(state, created_at);
CREATE TABLE IF NOT EXISTS verification_runs (
  id TEXT PRIMARY KEY,
  intent_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  status TEXT NOT NULL,
  budget_ms INTEGER NOT NULL,
  max_concurrency INTEGER NOT NULL,
  queue_size INTEGER NOT NULL,
  report_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS verification_results (
  run_id TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  path TEXT NOT NULL,
  open_mode TEXT NOT NULL,
  requested TEXT NOT NULL,
  actual TEXT NOT NULL,
  kernel_denied INTEGER NOT NULL,
  expected TEXT NOT NULL,
  match INTEGER NOT NULL,
  identity TEXT NOT NULL,
  engine TEXT NOT NULL,
  error TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(run_id, subject_id, path, open_mode)
);
CREATE TABLE IF NOT EXISTS apply_runs (
  id TEXT PRIMARY KEY,
  intent_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  tree_path TEXT NOT NULL,
  report_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recovery_reports (
  id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  report_json TEXT NOT NULL
);
`

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepathDir(path), 0o750); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func filepathDir(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == '\\' {
			return path[:i]
		}
	}
	return "."
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Head(ctx context.Context) (Head, error) {
	var h Head
	err := s.db.QueryRowContext(ctx, `SELECT version,digest,view_digest,tree_path FROM policy_head WHERE singleton=1`).Scan(&h.Version, &h.Digest, &h.ViewDigest, &h.TreePath)
	if err != nil {
		return Head{}, err
	}
	if h.Version > 0 {
		_ = s.db.QueryRowContext(ctx, `SELECT generated_at FROM policy_versions WHERE version=?`, h.Version).Scan(&h.GeneratedAt)
	}
	return h, nil
}

func (s *Store) LoadPolicy(ctx context.Context, version int64) (*model.Policy, error) {
	if version == 0 {
		return nil, nil
	}
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT spec_json FROM policy_versions WHERE version=?`, version).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var p model.Policy
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Store) CurrentPolicy(ctx context.Context) (*model.Policy, error) {
	h, err := s.Head(ctx)
	if err != nil {
		return nil, err
	}
	return s.LoadPolicy(ctx, h.Version)
}

func (s *Store) LoadViews(ctx context.Context, version int64) ([]model.EffectiveView, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT detail_json FROM effective_views WHERE version=? ORDER BY subject_id,path`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var views []model.EffectiveView
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var view model.EffectiveView
		if err := json.Unmarshal([]byte(raw), &view); err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	return views, rows.Err()
}

func (s *Store) GetView(ctx context.Context, version int64, subjectID, path string) (*model.EffectiveView, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT detail_json FROM effective_views WHERE version=? AND subject_id=? AND path=?`, version, subjectID, path).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var view model.EffectiveView
	if err := json.Unmarshal([]byte(raw), &view); err != nil {
		return nil, err
	}
	return &view, nil
}

func (s *Store) StartIntent(ctx context.Context, intent Intent) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO intents(id,kind,state,base_version,target_version,payload_json,created_at) VALUES(?,?,?,?,?,?,?)`,
		intent.ID, intent.Kind, "pending", intent.BaseVersion, intent.TargetVersion, intent.PayloadJSON, intent.CreatedAt)
	return err
}

func (s *Store) AbortIntent(ctx context.Context, id string, cause error, outcome any) error {
	raw := ""
	if outcome != nil {
		b, _ := json.Marshal(outcome)
		raw = string(b)
	}
	errText := ""
	if cause != nil {
		errText = cause.Error()
	}
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET state='aborted',finished_at=?,error=?,outcome_json=? WHERE id=? AND state='pending'`,
		now(), errText, raw, id)
	return err
}

func (s *Store) CommitPolicy(ctx context.Context, intentID string, expectedBase int64, policy *model.Policy, views []model.EffectiveView, cascade model.CascadeReport, treePath string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current int64
	if err := tx.QueryRowContext(ctx, `SELECT version FROM policy_head WHERE singleton=1`).Scan(&current); err != nil {
		return err
	}
	if current != expectedBase {
		return fmt.Errorf("policy head moved from %d to %d", expectedBase, current)
	}
	spec, _ := json.Marshal(policy)
	cascadeJSON, _ := json.Marshal(cascade)
	committedAt := now()
	if _, err := tx.ExecContext(ctx, `INSERT INTO policy_versions(version,base_version,generated_at,digest,spec_json,view_digest,tree_path,cascade_json,committed_at) VALUES(?,?,?,?,?,?,?,?,?)`,
		policy.Version, expectedBase, policy.GeneratedAt, policy.Digest, string(spec), cascade.ViewDigest, treePath, string(cascadeJSON), committedAt); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE verification_runs SET status='superseded' WHERE version<>? AND status<>'superseded'`, policy.Version); err != nil {
		return err
	}
	for _, view := range views {
		detail, _ := json.Marshal(view)
		if _, err := tx.ExecContext(ctx, `INSERT INTO effective_views(version,subject_id,path,can_read,can_write,digest,detail_json) VALUES(?,?,?,?,?,?,?)`,
			view.Version, view.SubjectID, view.Path, boolInt(view.Rights.Read), boolInt(view.Rights.Write), view.Digest, string(detail)); err != nil {
			return err
		}
	}
	res, err := tx.ExecContext(ctx, `UPDATE policy_head SET version=?,digest=?,view_digest=?,tree_path=? WHERE singleton=1 AND version=?`,
		policy.Version, policy.Digest, cascade.ViewDigest, treePath, expectedBase)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("policy head compare-and-swap failed")
	}
	outcome, _ := json.Marshal(map[string]any{"version": policy.Version, "view_digest": cascade.ViewDigest, "tree_path": treePath})
	res, err = tx.ExecContext(ctx, `UPDATE intents SET state='completed',finished_at=?,outcome_json=? WHERE id=? AND state='pending'`, committedAt, string(outcome), intentID)
	if err != nil {
		return err
	}
	n, _ = res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("intent %s is not pending", intentID)
	}
	return tx.Commit()
}

func (s *Store) CommitApply(ctx context.Context, intentID string, version int64, treePath string, report any) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	h, err := headTx(ctx, tx)
	if err != nil {
		return err
	}
	if h.Version != version {
		return fmt.Errorf("apply version %d no longer current (head %d)", version, h.Version)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE policy_head SET tree_path=? WHERE singleton=1 AND version=?`, treePath, version); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE policy_versions SET tree_path=? WHERE version=?`, treePath, version); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE verification_runs SET status='superseded' WHERE version=? AND status<>'superseded'`, version); err != nil {
		return err
	}
	raw, _ := json.Marshal(report)
	runID := "apply-" + intentID
	if _, err := tx.ExecContext(ctx, `INSERT INTO apply_runs(id,intent_id,version,tree_path,report_json,created_at) VALUES(?,?,?,?,?,?)`,
		runID, intentID, version, treePath, string(raw), now()); err != nil {
		return err
	}
	outcome, _ := json.Marshal(map[string]any{"version": version, "tree_path": treePath})
	res, err := tx.ExecContext(ctx, `UPDATE intents SET state='completed',finished_at=?,outcome_json=? WHERE id=? AND state='pending'`, now(), string(outcome), intentID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("intent %s is not pending", intentID)
	}
	return tx.Commit()
}

func (s *Store) CommitCascade(ctx context.Context, intentID string, policy *model.Policy, views []model.EffectiveView, cascade model.CascadeReport) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	h, err := headTx(ctx, tx)
	if err != nil {
		return err
	}
	if h.Version != policy.Version {
		return fmt.Errorf("cascade version %d no longer current (head %d)", policy.Version, h.Version)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM effective_views WHERE version=?`, policy.Version); err != nil {
		return err
	}
	for _, view := range views {
		detail, _ := json.Marshal(view)
		if _, err := tx.ExecContext(ctx, `INSERT INTO effective_views(version,subject_id,path,can_read,can_write,digest,detail_json) VALUES(?,?,?,?,?,?,?)`,
			view.Version, view.SubjectID, view.Path, boolInt(view.Rights.Read), boolInt(view.Rights.Write), view.Digest, string(detail)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE policy_head SET view_digest=? WHERE singleton=1 AND version=?`, cascade.ViewDigest, policy.Version); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE policy_versions SET view_digest=?,cascade_json=? WHERE version=?`, cascade.ViewDigest, mustJSON(cascade), policy.Version); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE verification_runs SET status='superseded' WHERE version=? AND status<>'superseded'`, policy.Version); err != nil {
		return err
	}
	outcome, _ := json.Marshal(map[string]any{"version": policy.Version, "view_digest": cascade.ViewDigest, "views": len(views)})
	res, err := tx.ExecContext(ctx, `UPDATE intents SET state='completed',finished_at=?,outcome_json=? WHERE id=? AND state='pending'`, now(), string(outcome), intentID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("intent %s is not pending", intentID)
	}
	return tx.Commit()
}

func headTx(ctx context.Context, tx *sql.Tx) (Head, error) {
	var h Head
	err := tx.QueryRowContext(ctx, `SELECT version,digest,view_digest,tree_path FROM policy_head WHERE singleton=1`).Scan(&h.Version, &h.Digest, &h.ViewDigest, &h.TreePath)
	return h, err
}

func (s *Store) CommitVerification(ctx context.Context, intentID string, report model.VerificationReport) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	raw, _ := json.Marshal(report)
	if _, err := tx.ExecContext(ctx, `INSERT INTO verification_runs(id,intent_id,version,status,budget_ms,max_concurrency,queue_size,report_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)`,
		report.RunID, intentID, report.Version, report.Status, report.BudgetMS, report.MaxConcurrency, report.QueueSize, string(raw), now()); err != nil {
		return err
	}
	for _, result := range report.Results {
		if _, err := tx.ExecContext(ctx, `INSERT INTO verification_results(run_id,subject_id,path,open_mode,requested,actual,kernel_denied,expected,match,identity,engine,error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
			report.RunID, result.SubjectID, result.Path, result.OpenMode, result.Requested, result.Actual, boolInt(result.KernelDenied), result.Expected, boolInt(result.Match), result.Identity, result.Engine, result.Error); err != nil {
			return err
		}
	}
	outcome, _ := json.Marshal(map[string]any{"run_id": report.RunID, "status": report.Status, "mismatches": report.Mismatches})
	res, err := tx.ExecContext(ctx, `UPDATE intents SET state='completed',finished_at=?,outcome_json=? WHERE id=? AND state='pending'`, now(), string(outcome), intentID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("intent %s is not pending", intentID)
	}
	return tx.Commit()
}

func (s *Store) PendingIntents(ctx context.Context) ([]Intent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,kind,state,base_version,target_version,payload_json,created_at,COALESCE(finished_at,''),outcome_json,error FROM intents WHERE state='pending' ORDER BY created_at,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Intent
	for rows.Next() {
		var intent Intent
		if err := rows.Scan(&intent.ID, &intent.Kind, &intent.State, &intent.BaseVersion, &intent.TargetVersion, &intent.PayloadJSON, &intent.CreatedAt, &intent.FinishedAt, &intent.OutcomeJSON, &intent.Error); err != nil {
			return nil, err
		}
		out = append(out, intent)
	}
	return out, rows.Err()
}

func (s *Store) MarkRecoveryAborted(ctx context.Context, intentID, reason string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET state='aborted',finished_at=?,error=? WHERE id=? AND state='pending'`, now(), reason, intentID)
	return err
}

func (s *Store) MarkRecoveryCommittedEvidence(ctx context.Context, intentID, evidence string) error {
	outcome, _ := json.Marshal(map[string]any{"recovered": true, "evidence": evidence})
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET state='completed',finished_at=?,outcome_json=?,error='' WHERE id=? AND state='pending'`, now(), string(outcome), intentID)
	return err
}

func (s *Store) UpdateTreePath(ctx context.Context, version int64, treePath string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE policy_head SET tree_path=? WHERE singleton=1 AND version=?`, treePath, version); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE policy_versions SET tree_path=? WHERE version=?`, treePath, version); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) SaveRecoveryReport(ctx context.Context, id string, report any) error {
	raw, _ := json.Marshal(report)
	_, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO recovery_reports(id,created_at,report_json) VALUES(?,?,?)`, id, now(), string(raw))
	return err
}

func (s *Store) LatestRecoveryReport(ctx context.Context) (json.RawMessage, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM recovery_reports ORDER BY created_at DESC LIMIT 1`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return json.RawMessage(raw), err
}

func (s *Store) ListVersions(ctx context.Context) ([]Head, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT version,digest,view_digest,tree_path,generated_at FROM policy_versions ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Head
	for rows.Next() {
		var h Head
		if err := rows.Scan(&h.Version, &h.Digest, &h.ViewDigest, &h.TreePath, &h.GeneratedAt); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func (s *Store) VerificationRun(ctx context.Context, runID string) (json.RawMessage, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM verification_runs WHERE id=?`, runID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return json.RawMessage(raw), err
}

type VerificationRunStatus struct {
	RunID     string          `json:"run_id"`
	Version   int64           `json:"version"`
	Status    string          `json:"status"`
	CreatedAt string          `json:"created_at"`
	Report    json.RawMessage `json:"report,omitempty"`
}

func (s *Store) VerificationRunStatus(ctx context.Context, runID string) (*VerificationRunStatus, error) {
	var out VerificationRunStatus
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT id,version,status,created_at,report_json FROM verification_runs WHERE id=?`, runID).
		Scan(&out.RunID, &out.Version, &out.Status, &out.CreatedAt, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out.Report = json.RawMessage(raw)
	return &out, nil
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
