package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"example.com/joinshuffle/internal/model"
	"example.com/joinshuffle/internal/sum"

	_ "modernc.org/sqlite"
)

type Store struct {
	db   *sql.DB
	path string
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("database path is empty")
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}
	s := &Store{db: db, path: path}
	if err := s.initSchema(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Path() string { return s.path }

func (s *Store) initSchema() error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`INSERT OR IGNORE INTO meta(key, value) VALUES('commit_seq', '0')`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			side TEXT PRIMARY KEY,
			version INTEGER NOT NULL,
			generated_at_unix_nano INTEGER NOT NULL,
			row_count INTEGER NOT NULL,
			path TEXT NOT NULL,
			checksum_path TEXT NOT NULL,
			sha256 TEXT NOT NULL,
			loaded_at_unix_nano INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS requests (
			request_id TEXT PRIMARY KEY,
			fingerprint TEXT NOT NULL,
			status TEXT NOT NULL,
			request_json TEXT NOT NULL,
			response_json TEXT NOT NULL DEFAULT '',
			error_json TEXT NOT NULL DEFAULT '',
			commit_version INTEGER,
			attempts INTEGER NOT NULL DEFAULT 0,
			created_at_unix_nano INTEGER NOT NULL,
			updated_at_unix_nano INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_requests_status ON requests(status)`,
		`CREATE TABLE IF NOT EXISTS intents (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			request_id TEXT NOT NULL,
			fingerprint TEXT NOT NULL,
			operation TEXT NOT NULL,
			state TEXT NOT NULL,
			detail TEXT NOT NULL,
			created_at_unix_nano INTEGER NOT NULL,
			updated_at_unix_nano INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_request ON intents(request_id)`,
		`CREATE TABLE IF NOT EXISTS job_plans (
			request_id TEXT PRIMARY KEY,
			partition_count INTEGER NOT NULL,
			join_type TEXT NOT NULL,
			algorithm TEXT NOT NULL,
			path TEXT NOT NULL,
			merge_rule TEXT NOT NULL,
			tolerance REAL NOT NULL,
			skew_threshold REAL NOT NULL,
			fixed_scale_bits INTEGER NOT NULL,
			route_json TEXT NOT NULL,
			skew_json TEXT NOT NULL,
			error_declaration_json TEXT NOT NULL,
			FOREIGN KEY(request_id) REFERENCES requests(request_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS partitions (
			request_id TEXT NOT NULL,
			partition INTEGER NOT NULL,
			left_records INTEGER NOT NULL,
			right_records INTEGER NOT NULL,
			record_count INTEGER NOT NULL,
			joined_records INTEGER NOT NULL,
			bytes_estimate INTEGER NOT NULL,
			record_share REAL NOT NULL,
			skewed INTEGER NOT NULL,
			affected_keys_json TEXT NOT NULL,
			PRIMARY KEY(request_id, partition),
			FOREIGN KEY(request_id) REFERENCES requests(request_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS partial_sums (
			request_id TEXT NOT NULL,
			partition INTEGER NOT NULL,
			group_key INTEGER NOT NULL,
			partial_sum REAL NOT NULL,
			compensation REAL NOT NULL,
			fixed_point TEXT NOT NULL,
			group_count INTEGER NOT NULL,
			value_count INTEGER NOT NULL,
			abs_sum REAL NOT NULL,
			compensation_digest TEXT NOT NULL,
			PRIMARY KEY(request_id, partition, group_key),
			FOREIGN KEY(request_id) REFERENCES requests(request_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS group_results (
			request_id TEXT NOT NULL,
			group_key INTEGER NOT NULL,
			sum_value REAL,
			avg_value REAL,
			group_count INTEGER NOT NULL,
			value_count INTEGER NOT NULL,
			abs_sum REAL NOT NULL,
			abs_error_bound REAL NOT NULL,
			rel_error_bound REAL NOT NULL,
			avg_abs_error_bound REAL NOT NULL DEFAULT 0,
			avg_rel_error_bound REAL NOT NULL DEFAULT 0,
			compensation_digest TEXT NOT NULL,
			max_relative_deviation REAL NOT NULL,
			PRIMARY KEY(request_id, group_key),
			FOREIGN KEY(request_id) REFERENCES requests(request_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS recovery_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			request_id TEXT NOT NULL,
			action TEXT NOT NULL,
			detail TEXT NOT NULL,
			created_at_unix_nano INTEGER NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := s.db.Exec(statement); err != nil {
			return fmt.Errorf("initialize schema: %w", err)
		}
	}
	for _, column := range []struct {
		name       string
		definition string
	}{
		{name: "avg_abs_error_bound", definition: "REAL NOT NULL DEFAULT 0"},
		{name: "avg_rel_error_bound", definition: "REAL NOT NULL DEFAULT 0"},
	} {
		if err := s.ensureColumn("group_results", column.name, column.definition); err != nil {
			return fmt.Errorf("migrate group_results: %w", err)
		}
	}
	return nil
}

func (s *Store) SaveSnapshotPair(ctx context.Context, pair model.SnapshotPairMeta) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC().UnixNano()
	stmt := `INSERT INTO snapshots(side, version, generated_at_unix_nano, row_count, path, checksum_path, sha256, loaded_at_unix_nano)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(side) DO UPDATE SET
			version=excluded.version,
			generated_at_unix_nano=excluded.generated_at_unix_nano,
			row_count=excluded.row_count,
			path=excluded.path,
			checksum_path=excluded.checksum_path,
			sha256=excluded.sha256,
			loaded_at_unix_nano=excluded.loaded_at_unix_nano`
	for _, meta := range []model.SnapshotMeta{pair.Left, pair.Right} {
		if _, err := tx.ExecContext(ctx, stmt, meta.Side, int64(meta.Version), meta.GeneratedAt, meta.Rows, meta.Path, meta.ChecksumPath, meta.SHA256, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) LoadSnapshotPair(ctx context.Context) (model.SnapshotPairMeta, bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT side, version, generated_at_unix_nano, row_count, path, checksum_path, sha256, loaded_at_unix_nano FROM snapshots ORDER BY side`)
	if err != nil {
		return model.SnapshotPairMeta{}, false, err
	}
	defer rows.Close()
	var pair model.SnapshotPairMeta
	found := 0
	for rows.Next() {
		var meta model.SnapshotMeta
		var version int64
		if err := rows.Scan(&meta.Side, &version, &meta.GeneratedAt, &meta.Rows, &meta.Path, &meta.ChecksumPath, &meta.SHA256, &meta.LoadedAt); err != nil {
			return model.SnapshotPairMeta{}, false, err
		}
		meta.Version = uint64(version)
		switch meta.Side {
		case "left":
			pair.Left = meta
			found |= 1
		case "right":
			pair.Right = meta
			found |= 2
		}
	}
	if err := rows.Err(); err != nil {
		return model.SnapshotPairMeta{}, false, err
	}
	return pair, found == 3, nil
}

func (s *Store) GetRequest(ctx context.Context, requestID string) (*model.RequestRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT request_id, fingerprint, status, request_json, response_json, error_json, COALESCE(commit_version, 0), attempts
		FROM requests WHERE request_id=?`, requestID)
	var rec model.RequestRecord
	if err := row.Scan(&rec.RequestID, &rec.Fingerprint, &rec.Status, &rec.RequestJSON, &rec.ResponseJSON, &rec.ErrorJSON, &rec.CommitVersion, &rec.Attempts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &rec, nil
}

func (s *Store) BeginAttempt(ctx context.Context, requestID, fingerprint, requestJSON string) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	now := time.Now().UTC().UnixNano()
	var existingStatus, existingFingerprint string
	var attempts int
	newRequest := false
	err = tx.QueryRowContext(ctx, `SELECT status, fingerprint, attempts FROM requests WHERE request_id=?`, requestID).
		Scan(&existingStatus, &existingFingerprint, &attempts)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		attempts = 1
		newRequest = true
	case err != nil:
		return 0, err
	default:
		if existingFingerprint != fingerprint {
			return 0, model.NewError(model.CodeConflictingRequest,
				"request id was already used with different content",
				"request_id", requestID,
				"existing_fingerprint", existingFingerprint,
				"new_fingerprint", fingerprint)
		}
		if existingStatus == "running" {
			return 0, model.NewError(model.CodeExecutionInProgress, "request is already running", "request_id", requestID)
		}
		if existingStatus == "completed" || existingStatus == "failed" {
			return 0, model.NewError(model.CodeConflictingRequest,
				"request is already final and must be reused",
				"request_id", requestID,
				"status", existingStatus)
		}
		attempts++
	}
	detail := model.JSON(map[string]any{"request_id": requestID, "fingerprint": fingerprint, "attempt": attempts})
	_, err = tx.ExecContext(ctx, `INSERT INTO intents(request_id, fingerprint, operation, state, detail, created_at_unix_nano, updated_at_unix_nano)
		VALUES(?, ?, 'aggregate', 'pending', ?, ?, ?)`, requestID, fingerprint, detail, now, now)
	if err != nil {
		return 0, err
	}
	if newRequest {
		_, err = tx.ExecContext(ctx, `INSERT INTO requests(request_id, fingerprint, status, request_json, attempts, created_at_unix_nano, updated_at_unix_nano)
			VALUES(?, ?, 'running', ?, ?, ?, ?)`, requestID, fingerprint, requestJSON, attempts, now, now)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE requests SET status='running', attempts=?, response_json='', error_json='', commit_version=NULL, updated_at_unix_nano=? WHERE request_id=?`, attempts, now, requestID)
	}
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return attempts, nil
}

func (s *Store) CommitAttempt(ctx context.Context, requestID string, response *model.AggregateResponse, partials []sum.Partial, fixedScaleBits int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC().UnixNano()
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM requests WHERE request_id=?`, requestID).Scan(&status); err != nil {
		return err
	}
	if status != "running" {
		return model.NewError(model.CodeExecutionAborted, "request is no longer in a committable running state", "request_id", requestID, "status", status)
	}
	commitVersion, err := nextCommitVersion(ctx, tx)
	if err != nil {
		return err
	}
	response.CommitVersion = commitVersion
	response.Status = "completed"
	for _, table := range []string{"partitions", "partial_sums", "group_results", "job_plans"} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE request_id=?`, requestID); err != nil {
			return err
		}
	}
	plan := response.Route
	if _, err := tx.ExecContext(ctx, `INSERT INTO job_plans(request_id, partition_count, join_type, algorithm, path, merge_rule, tolerance, skew_threshold, fixed_scale_bits, route_json, skew_json, error_declaration_json)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		requestID, response.PartitionCount, response.JoinType, plan.ResolvedAlgorithm, plan.Path, response.MergeRule,
		response.PartitionInvariance.Tolerance, response.Skew.Threshold, fixedScaleBits, model.JSON(response.Route), model.JSON(response.Skew), model.JSON(response.ErrorDeclaration)); err != nil {
		return err
	}
	for _, p := range response.Partitions {
		affected := model.JSON(p.AffectedKeys)
		if _, err := tx.ExecContext(ctx, `INSERT INTO partitions(request_id, partition, left_records, right_records, record_count, joined_records, bytes_estimate, record_share, skewed, affected_keys_json)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, requestID, p.Partition, p.LeftRecords, p.RightRecords, p.RecordCount, p.JoinedRecords, p.BytesEstimate, p.RecordShare, boolInt(p.Skewed), affected); err != nil {
			return err
		}
	}
	for _, p := range partials {
		if _, err := tx.ExecContext(ctx, `INSERT INTO partial_sums(request_id, partition, group_key, partial_sum, compensation, fixed_point, group_count, value_count, abs_sum, compensation_digest)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, requestID, p.Partition, p.Key, p.Sum, p.Compensation, p.FixedPoint, p.Count, p.ValueCount, p.AbsSum, p.Digest); err != nil {
			return err
		}
	}
	for _, group := range response.Groups {
		var sumValue, avgValue any
		if group.Sum != nil {
			sumValue = *group.Sum
		}
		if group.Average != nil {
			avgValue = *group.Average
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO group_results(request_id, group_key, sum_value, avg_value, group_count, value_count, abs_sum, abs_error_bound, rel_error_bound, avg_abs_error_bound, avg_rel_error_bound, compensation_digest, max_relative_deviation)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			requestID, group.Key, sumValue, avgValue, group.GroupCount, group.ValueCount, group.AbsSum,
			group.AbsoluteErrorBound, group.RelativeErrorBound, group.AverageAbsErrorBound, group.AverageRelErrorBound,
			group.CompensationDigest, group.MaxRelativeDeviation); err != nil {
			return err
		}
	}
	responseJSON, err := json.Marshal(response)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE requests SET status='completed', response_json=?, error_json='', commit_version=?, updated_at_unix_nano=? WHERE request_id=?`, string(responseJSON), commitVersion, now, requestID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='completed', updated_at_unix_nano=? WHERE id=(SELECT id FROM intents WHERE request_id=? ORDER BY id DESC LIMIT 1)`, now, requestID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) FailAttempt(ctx context.Context, requestID string, businessErr *model.BusinessError) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC().UnixNano()
	errorJSON, _ := json.Marshal(businessErr)
	if _, err := tx.ExecContext(ctx, `UPDATE requests SET status='failed', response_json='', error_json=?, commit_version=NULL, updated_at_unix_nano=? WHERE request_id=?`, string(errorJSON), now, requestID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='failed', updated_at_unix_nano=? WHERE id=(SELECT id FROM intents WHERE request_id=? ORDER BY id DESC LIMIT 1)`, now, requestID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) RecoverRunning(ctx context.Context) (model.RecoveryReport, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	defer tx.Rollback()
	now := time.Now().UTC().UnixNano()
	lastComplete, err := currentCommitVersion(ctx, tx)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT request_id, fingerprint, attempts FROM requests WHERE status='running' ORDER BY request_id`)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	var aborted []string
	type runningRequest struct {
		id, fingerprint string
		attempts        int
	}
	var running []runningRequest
	for rows.Next() {
		var rec runningRequest
		if err := rows.Scan(&rec.id, &rec.fingerprint, &rec.attempts); err != nil {
			rows.Close()
			return model.RecoveryReport{}, err
		}
		running = append(running, rec)
	}
	if err := rows.Close(); err != nil {
		return model.RecoveryReport{}, err
	}
	orphanCount := 0
	rolledBackArtifacts := false
	for _, rec := range running {
		artifactCounts := make(map[string]int64, 4)
		var artifactTotal int64
		for _, table := range []string{"job_plans", "partitions", "partial_sums", "group_results"} {
			count, err := countRequestRows(ctx, tx, table, rec.id)
			if err != nil {
				return model.RecoveryReport{}, err
			}
			artifactCounts[table] = count
			artifactTotal += count
		}
		if artifactTotal > 0 {
			for _, table := range []string{"partial_sums", "group_results", "partitions", "job_plans"} {
				if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE request_id=?`, rec.id); err != nil {
					return model.RecoveryReport{}, err
				}
			}
			rolledBackArtifacts = true
		}
		abortedErr := model.NewError(model.CodeExecutionAborted,
			"process terminated before the atomic result transaction committed; retry with the same request_id is allowed",
			"request_id", rec.id,
			"attempt", rec.attempts,
			"last_complete_commit_version", lastComplete)
		errorJSON, _ := json.Marshal(abortedErr)
		if _, err := tx.ExecContext(ctx, `UPDATE requests SET status='aborted', error_json=?, updated_at_unix_nano=? WHERE request_id=?`, string(errorJSON), now, rec.id); err != nil {
			return model.RecoveryReport{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='aborted', updated_at_unix_nano=? WHERE request_id=? AND state='pending'`, now, rec.id); err != nil {
			return model.RecoveryReport{}, err
		}
		detail := model.JSON(map[string]any{
			"attempt":                      rec.attempts,
			"fingerprint":                  rec.fingerprint,
			"last_complete_commit_version": lastComplete,
			"artifact_rows_found":          artifactCounts,
			"artifact_rows_total":          artifactTotal,
			"rollback_applied":             artifactTotal > 0,
			"reason":                       "unfinished intent and no atomic final transaction",
		})
		if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_events(request_id, action, detail, created_at_unix_nano)
			VALUES(?, 'aborted_uncommitted_attempt', ?, ?)`, rec.id, detail, now); err != nil {
			return model.RecoveryReport{}, err
		}
		aborted = append(aborted, rec.id)
	}
	orphanRows, err := tx.QueryContext(ctx, `SELECT id, request_id, fingerprint FROM intents WHERE state='pending' ORDER BY id`)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	type orphanIntent struct {
		id          int64
		requestID   string
		fingerprint string
	}
	var orphans []orphanIntent
	for orphanRows.Next() {
		var intent orphanIntent
		if err := orphanRows.Scan(&intent.id, &intent.requestID, &intent.fingerprint); err != nil {
			orphanRows.Close()
			return model.RecoveryReport{}, err
		}
		orphans = append(orphans, intent)
	}
	if err := orphanRows.Close(); err != nil {
		return model.RecoveryReport{}, err
	}
	for _, intent := range orphans {
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='aborted', updated_at_unix_nano=? WHERE id=?`, now, intent.id); err != nil {
			return model.RecoveryReport{}, err
		}
		detail := model.JSON(map[string]any{
			"intent_id":                    intent.id,
			"request_id":                   intent.requestID,
			"fingerprint":                  intent.fingerprint,
			"last_complete_commit_version": lastComplete,
			"reason":                       "pending intent had no running request or atomic final commit",
		})
		if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_events(request_id, action, detail, created_at_unix_nano)
			VALUES(?, 'aborted_orphan_intent', ?, ?)`, intent.requestID, detail, now); err != nil {
			return model.RecoveryReport{}, err
		}
		orphanCount++
	}
	if err := tx.Commit(); err != nil {
		return model.RecoveryReport{}, err
	}
	report, err := s.RecoveryReport(ctx)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	if len(aborted) == 0 && orphanCount == 0 {
		report.Actions = []string{"no_unfinished_attempts_found"}
	} else {
		report.Actions = []string{"marked_unfinished_requests_aborted", "left_last_atomic_commit_visible"}
		if orphanCount > 0 {
			report.Actions = append(report.Actions, "marked_orphan_pending_intents_aborted")
		}
	}
	if rolledBackArtifacts {
		report.Actions = append(report.Actions, "rolled_back_uncommitted_result_artifacts")
	}
	return report, nil
}

func (s *Store) RecoveryReport(ctx context.Context) (model.RecoveryReport, error) {
	report := model.RecoveryReport{
		GeneratedAtNano:  time.Now().UTC().UnixNano(),
		AbortedRequests:  []string{},
		RequestsByStatus: make(map[string]int),
		IntentsByState:   make(map[string]int),
		Events:           []model.RecoveryEvent{},
		FactSource:       "sqlite: requests, intents, job_plans, partitions, partial_sums, group_results, recovery_events",
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(commit_version), 0) FROM requests WHERE status='completed'`).Scan(&report.LastCompleteCommitVersion); err != nil {
		return model.RecoveryReport{}, err
	}
	if err := countBy(ctx, s.db, `SELECT status, COUNT(*) FROM requests GROUP BY status`, report.RequestsByStatus); err != nil {
		return model.RecoveryReport{}, err
	}
	if err := countBy(ctx, s.db, `SELECT state, COUNT(*) FROM intents GROUP BY state`, report.IntentsByState); err != nil {
		return model.RecoveryReport{}, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM intents WHERE state='pending'`).Scan(&report.PendingIntentsRemaining); err != nil {
		return model.RecoveryReport{}, err
	}
	abortedRows, err := s.db.QueryContext(ctx, `SELECT request_id FROM requests WHERE status='aborted' ORDER BY request_id`)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	for abortedRows.Next() {
		var requestID string
		if err := abortedRows.Scan(&requestID); err != nil {
			abortedRows.Close()
			return model.RecoveryReport{}, err
		}
		report.AbortedRequests = append(report.AbortedRequests, requestID)
	}
	if err := abortedRows.Close(); err != nil {
		return model.RecoveryReport{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, request_id, action, detail, created_at_unix_nano FROM recovery_events ORDER BY id`)
	if err != nil {
		return model.RecoveryReport{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var event model.RecoveryEvent
		if err := rows.Scan(&event.ID, &event.RequestID, &event.Action, &event.Detail, &event.CreatedAtNano); err != nil {
			return model.RecoveryReport{}, err
		}
		report.Events = append(report.Events, event)
	}
	if err := rows.Err(); err != nil {
		return model.RecoveryReport{}, err
	}
	if report.PendingIntentsRemaining == 0 {
		report.Actions = append(report.Actions, "no_pending_intents")
	}
	return report, nil
}

func (s *Store) Partitions(ctx context.Context, requestID string) ([]model.PartitionStat, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT partition, left_records, right_records, record_count, joined_records, bytes_estimate, record_share, skewed, affected_keys_json
		FROM partitions WHERE request_id=? ORDER BY partition`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.PartitionStat
	for rows.Next() {
		var p model.PartitionStat
		var skewed int
		var affected string
		if err := rows.Scan(&p.Partition, &p.LeftRecords, &p.RightRecords, &p.RecordCount, &p.JoinedRecords, &p.BytesEstimate, &p.RecordShare, &skewed, &affected); err != nil {
			return nil, err
		}
		p.Skewed = skewed != 0
		if err := json.Unmarshal([]byte(affected), &p.AffectedKeys); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) Groups(ctx context.Context, requestID string) ([]model.GroupResult, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT group_key, sum_value, avg_value, group_count, value_count, abs_sum, abs_error_bound, rel_error_bound, avg_abs_error_bound, avg_rel_error_bound, compensation_digest, max_relative_deviation
		FROM group_results WHERE request_id=? ORDER BY group_key`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.GroupResult
	for rows.Next() {
		var group model.GroupResult
		var sumValue, avgValue sql.NullFloat64
		if err := rows.Scan(&group.Key, &sumValue, &avgValue, &group.GroupCount, &group.ValueCount, &group.AbsSum, &group.AbsoluteErrorBound, &group.RelativeErrorBound, &group.AverageAbsErrorBound, &group.AverageRelErrorBound, &group.CompensationDigest, &group.MaxRelativeDeviation); err != nil {
			return nil, err
		}
		if sumValue.Valid {
			value := sumValue.Float64
			group.Sum = &value
		}
		if avgValue.Valid {
			value := avgValue.Float64
			group.Average = &value
		}
		out = append(out, group)
	}
	return out, rows.Err()
}

func (s *Store) PartialSums(ctx context.Context, requestID string) ([]sum.Partial, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT group_key, partition, group_count, value_count, abs_sum, partial_sum, compensation, fixed_point, compensation_digest
		FROM partial_sums WHERE request_id=? ORDER BY partition, group_key`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []sum.Partial
	for rows.Next() {
		var p sum.Partial
		if err := rows.Scan(&p.Key, &p.Partition, &p.Count, &p.ValueCount, &p.AbsSum, &p.Sum, &p.Compensation, &p.FixedPoint, &p.Digest); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func nextCommitVersion(ctx context.Context, tx *sql.Tx) (int64, error) {
	current, err := currentCommitVersion(ctx, tx)
	if err != nil {
		return 0, err
	}
	next := current + 1
	if _, err := tx.ExecContext(ctx, `UPDATE meta SET value=? WHERE key='commit_seq'`, strconv.FormatInt(next, 10)); err != nil {
		return 0, err
	}
	return next, nil
}

func currentCommitVersion(ctx context.Context, tx *sql.Tx) (int64, error) {
	var value string
	if err := tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='commit_seq'`).Scan(&value); err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func countBy(ctx context.Context, db *sql.DB, query string, target map[string]int) error {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var count int
		if err := rows.Scan(&key, &count); err != nil {
			return err
		}
		target[key] = count
	}
	return rows.Err()
}

func countRequestRows(ctx context.Context, tx *sql.Tx, table, requestID string) (int64, error) {
	switch table {
	case "job_plans", "partitions", "partial_sums", "group_results":
	default:
		return 0, fmt.Errorf("unsupported artifact table %q", table)
	}
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table+` WHERE request_id=?`, requestID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Store) ensureColumn(table, column, definition string) error {
	rows, err := s.db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			return err
		}
		if name == column {
			found = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if found {
		return nil
	}
	_, err = s.db.Exec(`ALTER TABLE ` + table + ` ADD COLUMN ` + column + ` ` + definition)
	return err
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
