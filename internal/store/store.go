package store

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
	"time"

	_ "modernc.org/sqlite"

	"local/rollout-guard/internal/domain"
)

type Store struct {
	db   *sql.DB
	path string
}

type Querier interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type ObservationRecord struct {
	Observation   domain.Observation
	BucketStartMS int64
	PolicyVersion int64
	CommitSeq     int64
}

type Cooldown struct {
	Service       string `json:"service"`
	BatchID       string `json:"batch_id"`
	TriggeredAtMS int64  `json:"triggered_at_ms"`
	ExpiresAtMS   int64  `json:"expires_at_ms"`
	CommitSeq     int64  `json:"commit_seq"`
}

type Intent struct {
	ID            int64          `json:"id"`
	Kind          string         `json:"kind"`
	BatchID       string         `json:"batch_id,omitempty"`
	Status        string         `json:"status"`
	Detail        map[string]any `json:"detail,omitempty"`
	CreatedAtMS   int64          `json:"created_at_ms"`
	UpdatedAtMS   int64          `json:"updated_at_ms"`
	CompletedAtMS int64          `json:"completed_at_ms,omitempty"`
	Note          string         `json:"note,omitempty"`
}

type RecoveryRun struct {
	ID          int64          `json:"id"`
	CreatedAtMS int64          `json:"created_at_ms"`
	Report      map[string]any `json:"report"`
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("database path is required")
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}
	dsn := path
	if path != ":memory:" && !strings.HasPrefix(path, "file:") {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		cleanPath := filepath.ToSlash(absolute)
		if runtime.GOOS == "windows" && filepath.VolumeName(absolute) != "" && !strings.HasPrefix(cleanPath, "/") {
			cleanPath = "/" + cleanPath
		}
		fileURL := url.URL{Scheme: "file", Path: cleanPath}
		dsn = fileURL.String()
	}
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	dsn += separator + "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	store := &Store{db: db, path: path}
	if err := store.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Path() string { return s.path }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Begin(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, &sql.TxOptions{})
}

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS commits (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			kind TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS policies (
			version INTEGER PRIMARY KEY,
			generated_at_ms INTEGER NOT NULL,
			observation_window_ms INTEGER NOT NULL,
			bucket_ms INTEGER NOT NULL,
			min_samples INTEGER NOT NULL,
			fast_sample_limit INTEGER NOT NULL,
			document_json TEXT NOT NULL,
			checksum TEXT NOT NULL,
			commit_seq INTEGER NOT NULL,
			created_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS policies_current (
			singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
			version INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS batches (
			id TEXT PRIMARY KEY,
			service TEXT NOT NULL,
			change_type TEXT NOT NULL,
			batch_size INTEGER NOT NULL,
			baseline_json TEXT NOT NULL,
			baseline_timestamp_ms INTEGER NOT NULL,
			status TEXT NOT NULL,
			revision INTEGER NOT NULL DEFAULT 0,
			last_observation_ts_ms INTEGER NOT NULL DEFAULT 0,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL,
			commit_seq INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_batches_service ON batches(service)`,
		`CREATE TABLE IF NOT EXISTS observation_buckets (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			batch_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			timestamp_ms INTEGER NOT NULL,
			bucket_start_ms INTEGER NOT NULL,
			metric_type TEXT NOT NULL,
			sample_count INTEGER NOT NULL,
			success_count INTEGER NOT NULL,
			latency_json TEXT NOT NULL,
			payload_hash TEXT NOT NULL,
			policy_version INTEGER NOT NULL,
			commit_seq INTEGER NOT NULL,
			created_at_ms INTEGER NOT NULL,
			UNIQUE(batch_id, seq)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_observation_window ON observation_buckets(batch_id, timestamp_ms, commit_seq)`,
		`CREATE TABLE IF NOT EXISTS risk_evaluations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			batch_id TEXT NOT NULL,
			policy_version INTEGER NOT NULL,
			policy_checksum TEXT NOT NULL,
			mode TEXT NOT NULL,
			score_bp INTEGER NOT NULL,
			level TEXT NOT NULL,
			action TEXT NOT NULL,
			status_before TEXT NOT NULL,
			status_after TEXT NOT NULL,
			as_of_commit_seq INTEGER NOT NULL,
			observation_revision INTEGER NOT NULL,
			latest_observation_ms INTEGER NOT NULL,
			window_start_ms INTEGER NOT NULL,
			window_end_ms INTEGER NOT NULL,
			sampled INTEGER NOT NULL,
			result_json TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL,
			commit_seq INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_risk_batch ON risk_evaluations(batch_id, id DESC)`,
		`CREATE TABLE IF NOT EXISTS cooldowns (
			service TEXT PRIMARY KEY,
			batch_id TEXT NOT NULL,
			triggered_at_ms INTEGER NOT NULL,
			expires_at_ms INTEGER NOT NULL,
			commit_seq INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			kind TEXT NOT NULL,
			batch_id TEXT,
			status TEXT NOT NULL,
			detail_json TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL,
			completed_at_ms INTEGER NOT NULL DEFAULT 0,
			note TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status, id)`,
		`CREATE TABLE IF NOT EXISTS recovery_runs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			report_json TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

func NowMS() int64 { return time.Now().UnixMilli() }

func NextCommit(ctx context.Context, tx *sql.Tx, kind string, nowMS int64) (int64, error) {
	result, err := tx.ExecContext(ctx, `INSERT INTO commits(kind, created_at_ms) VALUES(?, ?)`, kind, nowMS)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func CurrentCommit(ctx context.Context, q Querier) (int64, error) {
	var id int64
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(MAX(id), 0) FROM commits`).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func CurrentPolicy(ctx context.Context, q Querier) (domain.Policy, int64, error) {
	var raw string
	var commit int64
	err := q.QueryRowContext(ctx, `
		SELECT p.document_json, p.commit_seq
		FROM policies_current c JOIN policies p ON p.version = c.version
		WHERE c.singleton = 1`).Scan(&raw, &commit)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Policy{}, 0, domain.NewBizError("E_POLICY_MISSING", "尚无已提交策略快照", nil)
	}
	if err != nil {
		return domain.Policy{}, 0, err
	}
	var policy domain.Policy
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return domain.Policy{}, 0, err
	}
	return policy, commit, nil
}

func CurrentPolicyVersion(ctx context.Context, q Querier) (int64, error) {
	var version int64
	err := q.QueryRowContext(ctx, `SELECT version FROM policies_current WHERE singleton = 1`).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

func InsertPolicyTx(ctx context.Context, tx *sql.Tx, policy domain.Policy, nowMS int64) (int64, error) {
	commit, err := NextCommit(ctx, tx, "policy_submit", nowMS)
	if err != nil {
		return 0, err
	}
	raw, err := json.Marshal(policy)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO policies(version, generated_at_ms, observation_window_ms, bucket_ms, min_samples,
			fast_sample_limit, document_json, checksum, commit_seq, created_at_ms)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		policy.Version, policy.GeneratedAtMS, policy.ObservationWindowMS, policy.BucketMS, policy.MinSamples,
		policy.FastSampleLimit, string(raw), policy.Checksum, commit, nowMS); err != nil {
		return 0, err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO policies_current(singleton, version) VALUES(1, ?)
		ON CONFLICT(singleton) DO UPDATE SET version = excluded.version`, policy.Version)
	return commit, err
}

func InsertBatchTx(ctx context.Context, tx *sql.Tx, batch domain.Batch, nowMS int64) (int64, error) {
	commit, err := NextCommit(ctx, tx, "batch_submit", nowMS)
	if err != nil {
		return 0, err
	}
	baseline, err := json.Marshal(batch.Baseline)
	if err != nil {
		return 0, err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO batches(id, service, change_type, batch_size, baseline_json, baseline_timestamp_ms,
			status, revision, last_observation_ts_ms, created_at_ms, updated_at_ms, commit_seq)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		batch.ID, batch.Service, batch.ChangeType, batch.BatchSize, string(baseline), batch.Baseline.TimestampMS,
		domain.StatusObserving, 0, 0, nowMS, nowMS, commit)
	return commit, err
}

func GetBatch(ctx context.Context, q Querier, id string) (domain.Batch, error) {
	var batch domain.Batch
	var baselineJSON string
	err := q.QueryRowContext(ctx, `
		SELECT id, service, change_type, batch_size, baseline_json, status, revision,
			last_observation_ts_ms, created_at_ms, updated_at_ms
		FROM batches WHERE id = ?`, id).Scan(
		&batch.ID, &batch.Service, &batch.ChangeType, &batch.BatchSize, &baselineJSON, &batch.Status,
		&batch.Revision, &batch.LastObservationTSMS, &batch.CreatedAtMS, &batch.UpdatedAtMS)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Batch{}, domain.NewBizError("E_NOT_FOUND", "变更批次不存在", map[string]any{"batch_id": id})
	}
	if err != nil {
		return domain.Batch{}, err
	}
	if err := json.Unmarshal([]byte(baselineJSON), &batch.Baseline); err != nil {
		return domain.Batch{}, err
	}
	return batch, nil
}

func ListBatches(ctx context.Context, q Querier) ([]domain.Batch, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT id, service, change_type, batch_size, baseline_json, status, revision,
			last_observation_ts_ms, created_at_ms, updated_at_ms
		FROM batches ORDER BY created_at_ms DESC, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	batches := make([]domain.Batch, 0)
	for rows.Next() {
		var batch domain.Batch
		var raw string
		if err := rows.Scan(&batch.ID, &batch.Service, &batch.ChangeType, &batch.BatchSize, &raw, &batch.Status,
			&batch.Revision, &batch.LastObservationTSMS, &batch.CreatedAtMS, &batch.UpdatedAtMS); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(raw), &batch.Baseline); err != nil {
			return nil, err
		}
		batches = append(batches, batch)
	}
	return batches, rows.Err()
}

func LoadObservations(ctx context.Context, q Querier, batchID string) ([]ObservationRecord, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT seq, timestamp_ms, bucket_start_ms, metric_type, sample_count, success_count,
			latency_json, policy_version, commit_seq
		FROM observation_buckets WHERE batch_id = ? ORDER BY timestamp_ms, seq`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ObservationRecord, 0)
	for rows.Next() {
		var record ObservationRecord
		var latencyJSON string
		if err := rows.Scan(&record.Observation.Seq, &record.Observation.TimestampMS, &record.BucketStartMS,
			&record.Observation.MetricType, &record.Observation.SampleCount, &record.Observation.SuccessCount,
			&latencyJSON, &record.PolicyVersion, &record.CommitSeq); err != nil {
			return nil, err
		}
		if latencyJSON != "" && latencyJSON != "[]" {
			if err := json.Unmarshal([]byte(latencyJSON), &record.Observation.LatencySamplesMS); err != nil {
				return nil, err
			}
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func ObservationBySeq(ctx context.Context, q Querier, batchID string, seq int64) (string, error) {
	var hash string
	err := q.QueryRowContext(ctx, `SELECT payload_hash FROM observation_buckets WHERE batch_id=? AND seq=?`, batchID, seq).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return hash, err
}

func InsertObservationTx(ctx context.Context, tx *sql.Tx, batchID string, observation domain.Observation,
	bucketStartMS, policyVersion, commit, nowMS int64) error {
	latency := observation.LatencySamplesMS
	if latency == nil {
		latency = []int64{}
	}
	latencyJSON, err := json.Marshal(latency)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO observation_buckets(batch_id, seq, timestamp_ms, bucket_start_ms, metric_type,
			sample_count, success_count, latency_json, payload_hash, policy_version, commit_seq, created_at_ms)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		batchID, observation.Seq, observation.TimestampMS, bucketStartMS, observation.MetricType,
		observation.SampleCount, observation.SuccessCount, string(latencyJSON), observation.PayloadHash(),
		policyVersion, commit, nowMS)
	return err
}

func TouchBatchAfterIngestTx(ctx context.Context, tx *sql.Tx, batchID string, lastTS, nowMS int64) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE batches SET revision = revision + 1, last_observation_ts_ms = ?, updated_at_ms = ? WHERE id = ?`,
		lastTS, nowMS, batchID)
	return err
}

func UpdateBatchStatusTx(ctx context.Context, tx *sql.Tx, batchID, status string, nowMS int64) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE batches SET status = ?, revision = revision + 1, updated_at_ms = ? WHERE id = ?`, status, nowMS, batchID)
	return err
}

func UpsertCooldownTx(ctx context.Context, tx *sql.Tx, cooldown Cooldown) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO cooldowns(service, batch_id, triggered_at_ms, expires_at_ms, commit_seq)
		VALUES(?, ?, ?, ?, ?)
		ON CONFLICT(service) DO UPDATE SET batch_id=excluded.batch_id,
			triggered_at_ms=excluded.triggered_at_ms, expires_at_ms=excluded.expires_at_ms, commit_seq=excluded.commit_seq`,
		cooldown.Service, cooldown.BatchID, cooldown.TriggeredAtMS, cooldown.ExpiresAtMS, cooldown.CommitSeq)
	return err
}

func GetCooldown(ctx context.Context, q Querier, service string) (*Cooldown, error) {
	var cooldown Cooldown
	err := q.QueryRowContext(ctx, `
		SELECT service, batch_id, triggered_at_ms, expires_at_ms, commit_seq FROM cooldowns WHERE service=?`, service).Scan(
		&cooldown.Service, &cooldown.BatchID, &cooldown.TriggeredAtMS, &cooldown.ExpiresAtMS, &cooldown.CommitSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &cooldown, nil
}

func ListCooldowns(ctx context.Context, q Querier) ([]Cooldown, error) {
	rows, err := q.QueryContext(ctx, `SELECT service, batch_id, triggered_at_ms, expires_at_ms, commit_seq FROM cooldowns ORDER BY expires_at_ms DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Cooldown, 0)
	for rows.Next() {
		var item Cooldown
		if err := rows.Scan(&item.Service, &item.BatchID, &item.TriggeredAtMS, &item.ExpiresAtMS, &item.CommitSeq); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func InsertRiskEvaluationTx(ctx context.Context, tx *sql.Tx, batchID, statusBefore, statusAfter string,
	evaluationJSON []byte, mode string, scoreBP int64, level, action string, policyVersion int64, checksum string,
	asOf, revision, latest, windowStart, windowEnd int64, sampled bool, commit, nowMS int64) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO risk_evaluations(batch_id, policy_version, policy_checksum, mode, score_bp, level, action,
			status_before, status_after, as_of_commit_seq, observation_revision, latest_observation_ms,
			window_start_ms, window_end_ms, sampled, result_json, created_at_ms, commit_seq)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		batchID, policyVersion, checksum, mode, scoreBP, level, action, statusBefore, statusAfter, asOf, revision,
		latest, windowStart, windowEnd, boolInt(sampled), string(evaluationJSON), nowMS, commit)
	return err
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func CreateIntent(ctx context.Context, s *Store, kind, batchID string, detail map[string]any) (int64, error) {
	raw, _ := json.Marshal(detail)
	now := NowMS()
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO intents(kind, batch_id, status, detail_json, created_at_ms, updated_at_ms)
		VALUES(?, ?, 'PENDING', ?, ?, ?)`, kind, batchID, string(raw), now, now)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func SetIntentTx(ctx context.Context, tx *sql.Tx, id int64, status, note string, extra map[string]any) error {
	now := NowMS()
	completed := int64(0)
	if status == "COMPLETED" || status == "FAILED" || status == "ABORTED" {
		completed = now
	}
	if extra != nil {
		var detail string
		if err := tx.QueryRowContext(ctx, `SELECT detail_json FROM intents WHERE id=?`, id).Scan(&detail); err == nil {
			var existing map[string]any
			_ = json.Unmarshal([]byte(detail), &existing)
			if existing == nil {
				existing = map[string]any{}
			}
			for key, value := range extra {
				existing[key] = value
			}
			raw, _ := json.Marshal(existing)
			_, err := tx.ExecContext(ctx, `UPDATE intents SET status=?, note=?, detail_json=?, updated_at_ms=?, completed_at_ms=? WHERE id=?`,
				status, note, string(raw), now, completed, id)
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `UPDATE intents SET status=?, note=?, updated_at_ms=?, completed_at_ms=? WHERE id=?`,
		status, note, now, completed, id)
	return err
}

func SetIntentDirect(ctx context.Context, s *Store, id int64, status, note string) error {
	now := NowMS()
	completed := int64(0)
	if status == "COMPLETED" || status == "FAILED" || status == "ABORTED" {
		completed = now
	}
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET status=?, note=?, updated_at_ms=?, completed_at_ms=? WHERE id=?`,
		status, note, now, completed, id)
	return err
}

func GetIntent(ctx context.Context, q Querier, id int64) (Intent, error) {
	var item Intent
	var detail string
	err := q.QueryRowContext(ctx, `SELECT id, kind, COALESCE(batch_id,''), status, detail_json, created_at_ms,
		updated_at_ms, completed_at_ms, note FROM intents WHERE id=?`, id).Scan(
		&item.ID, &item.Kind, &item.BatchID, &item.Status, &detail, &item.CreatedAtMS, &item.UpdatedAtMS,
		&item.CompletedAtMS, &item.Note)
	if err != nil {
		return Intent{}, err
	}
	_ = json.Unmarshal([]byte(detail), &item.Detail)
	return item, nil
}

func ListIntents(ctx context.Context, q Querier, where string, args ...any) ([]Intent, error) {
	query := `SELECT id, kind, COALESCE(batch_id,''), status, detail_json, created_at_ms, updated_at_ms,
		completed_at_ms, note FROM intents`
	if where != "" {
		query += " WHERE " + where
	}
	query += " ORDER BY id"
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Intent, 0)
	for rows.Next() {
		var item Intent
		var detail string
		if err := rows.Scan(&item.ID, &item.Kind, &item.BatchID, &item.Status, &detail, &item.CreatedAtMS,
			&item.UpdatedAtMS, &item.CompletedAtMS, &item.Note); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(detail), &item.Detail)
		result = append(result, item)
	}
	return result, rows.Err()
}

func SaveRecoveryRun(ctx context.Context, s *Store, report map[string]any) (int64, error) {
	raw, err := json.Marshal(report)
	if err != nil {
		return 0, err
	}
	now := NowMS()
	result, err := s.db.ExecContext(ctx, `INSERT INTO recovery_runs(report_json, created_at_ms) VALUES(?, ?)`, string(raw), now)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func LatestRecoveryRun(ctx context.Context, q Querier) (RecoveryRun, error) {
	var run RecoveryRun
	var raw string
	err := q.QueryRowContext(ctx, `SELECT id, created_at_ms, report_json FROM recovery_runs ORDER BY id DESC LIMIT 1`).Scan(
		&run.ID, &run.CreatedAtMS, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return RecoveryRun{}, domain.NewBizError("E_NOT_FOUND", "尚无恢复报告", nil)
	}
	if err != nil {
		return RecoveryRun{}, err
	}
	if err := json.Unmarshal([]byte(raw), &run.Report); err != nil {
		return RecoveryRun{}, err
	}
	return run, nil
}

func ResetAll(ctx context.Context, s *Store) error {
	statements := []string{
		`DELETE FROM observation_buckets`, `DELETE FROM risk_evaluations`, `DELETE FROM cooldowns`,
		`DELETE FROM batches`, `DELETE FROM policies_current`, `DELETE FROM policies`, `DELETE FROM intents`,
		`DELETE FROM recovery_runs`, `DELETE FROM commits`,
	}
	tx, err := s.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return tx.Commit()
}
