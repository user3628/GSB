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
	"strings"

	"columnar-executor/internal/model"

	_ "modernc.org/sqlite"
)

type Store struct {
	db   *sql.DB
	path string
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("database path is required")
	}
	if path != ":memory:" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("resolve database path: %w", err)
		}
		path = absolute
		if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	} else {
		return nil, fmt.Errorf("use a file-backed SQLite database so recovery is verifiable")
	}

	dsn := sqliteFileDSN(path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}
	return &Store{db: db, path: path}, nil
}

func sqliteFileDSN(path string) string {
	slashed := filepath.ToSlash(path)
	// On Windows url.URL needs /C:/... to emit file:///C:/...; on Unix the
	// absolute path already starts with a slash.
	if filepath.VolumeName(path) != "" && !strings.HasPrefix(slashed, "/") {
		slashed = "/" + slashed
	}
	u := &url.URL{Scheme: "file", Path: slashed}
	query := u.Query()
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "journal_mode(WAL)")
	query.Add("_pragma", "synchronous(FULL)")
	query.Add("_pragma", "foreign_keys(1)")
	u.RawQuery = query.Encode()
	return u.String()
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) DBPath() string {
	return s.path
}

func (s *Store) SaveSnapshot(ctx context.Context, snapshot model.Snapshot) error {
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO snapshots(version, generated_at, row_count, payload, sha256, loaded_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		snapshot.Version, snapshot.GeneratedAt, snapshot.RowCount, string(raw), snapshot.SHA256Digest, model.NowUTC())
	if err != nil {
		return fmt.Errorf("insert snapshot: %w", err)
	}
	return nil
}

func (s *Store) GetSnapshot(ctx context.Context, version int64) (model.Snapshot, error) {
	var payload string
	err := s.db.QueryRowContext(ctx, `SELECT payload FROM snapshots WHERE version = ?`, version).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Snapshot{}, model.NewBizError(model.CodeNotFound, "snapshot version not found").With("version", version)
	}
	if err != nil {
		return model.Snapshot{}, fmt.Errorf("read snapshot: %w", err)
	}
	var snapshot model.Snapshot
	if err := json.Unmarshal([]byte(payload), &snapshot); err != nil {
		return model.Snapshot{}, fmt.Errorf("decode snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *Store) LatestSnapshotVersion(ctx context.Context) (int64, error) {
	var version int64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM snapshots`).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("read latest snapshot version: %w", err)
	}
	return version, nil
}

func (s *Store) ListSnapshots(ctx context.Context) ([]model.Snapshot, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT payload FROM snapshots ORDER BY version ASC`)
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}
	defer rows.Close()
	var out []model.Snapshot
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var snapshot model.Snapshot
		if err := json.Unmarshal([]byte(payload), &snapshot); err != nil {
			return nil, fmt.Errorf("decode snapshot: %w", err)
		}
		out = append(out, snapshot)
	}
	return out, rows.Err()
}

func (s *Store) SubmitChain(ctx context.Context, chain model.Chain) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin chain submission: %w", err)
	}
	defer tx.Rollback()
	var latest int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM chains WHERE chain_id = ?`, chain.ID).Scan(&latest); err != nil {
		return fmt.Errorf("read latest chain version: %w", err)
	}
	if chain.Version <= latest {
		return model.NewBizError(model.CodeStaleVersion, "chain version is not newer than the committed version").
			With("chain_id", chain.ID).With("submitted_version", chain.Version).With("latest_version", latest)
	}
	createdAt := chain.CreatedAt
	if createdAt == "" {
		createdAt = model.NowUTC()
	}
	raw, err := json.Marshal(chain)
	if err != nil {
		return fmt.Errorf("marshal chain: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO chains(chain_id, version, base_snapshot_version, payload, state, created_at)
		VALUES (?, ?, ?, ?, 'submitted', ?)`,
		chain.ID, chain.Version, chain.BaseSnapshotVersion, string(raw), createdAt); err != nil {
		return fmt.Errorf("insert chain: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit chain: %w", err)
	}
	return nil
}

func (s *Store) GetChain(ctx context.Context, chainID string, version int64) (model.Chain, error) {
	var payload string
	err := s.db.QueryRowContext(ctx, `
		SELECT payload FROM chains WHERE chain_id = ? AND version = ?`, chainID, version).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Chain{}, model.NewBizError(model.CodeNotFound, "chain version not found").
			With("chain_id", chainID).With("version", version)
	}
	if err != nil {
		return model.Chain{}, fmt.Errorf("read chain: %w", err)
	}
	var chain model.Chain
	if err := json.Unmarshal([]byte(payload), &chain); err != nil {
		return model.Chain{}, fmt.Errorf("decode chain: %w", err)
	}
	return chain, nil
}

func (s *Store) LatestChainVersion(ctx context.Context, chainID string) (int64, error) {
	var version int64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM chains WHERE chain_id = ?`, chainID).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("read latest chain version: %w", err)
	}
	return version, nil
}

func (s *Store) CreateExecution(ctx context.Context, view model.ExecutionView, batchNumbers []int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin execution create: %w", err)
	}
	defer tx.Rollback()

	payload := map[string]any{
		"path":            view.Path.Path,
		"reason":          view.Path.Reason,
		"batch_size":      view.BatchSize,
		"large_threshold": view.LargeThreshold,
		"selected_rows":   view.Path.SelectedRows,
		"batch_count":     view.Path.BatchCount,
	}
	if err := insertIntent(ctx, tx, view.ExecutionID, view.ChainID, view.ChainVersion, "path_selected", payload); err != nil {
		return err
	}

	bitmapJSON, err := json.Marshal(view.Bitmap)
	if err != nil {
		return fmt.Errorf("marshal selection bitmap: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO executions(
			execution_id, chain_id, chain_version, snapshot_version, bitmap_form, bitmap_json,
			batch_size, large_threshold, selected_rows, path, decision, batch_count,
			status, error_code, error_context, started_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '{}', ?, '')`,
		view.ExecutionID, view.ChainID, view.ChainVersion, view.SnapshotVersion, view.BitmapForm, string(bitmapJSON),
		view.BatchSize, view.LargeThreshold, view.Path.SelectedRows, view.Path.Path, view.Path.Reason, view.Path.BatchCount,
		view.Status, view.StartedAt); err != nil {
		return fmt.Errorf("insert execution: %w", err)
	}
	for _, batchNo := range batchNumbers {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO batches(execution_id, batch_no, status)
			VALUES (?, ?, ?)`, view.ExecutionID, batchNo, model.BatchPending); err != nil {
			return fmt.Errorf("insert pending batch: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit execution create: %w", err)
	}
	return nil
}

func (s *Store) AppendIntent(ctx context.Context, executionID, kind string, payload map[string]any) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin intent: %w", err)
	}
	defer tx.Rollback()
	var chainID string
	var chainVersion int64
	if err := tx.QueryRowContext(ctx, `SELECT chain_id, chain_version FROM executions WHERE execution_id = ?`, executionID).Scan(&chainID, &chainVersion); err != nil {
		return fmt.Errorf("read execution for intent: %w", err)
	}
	if err := insertIntent(ctx, tx, executionID, chainID, chainVersion, kind, payload); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit intent: %w", err)
	}
	return nil
}

func insertIntent(ctx context.Context, tx *sql.Tx, executionID, chainID string, chainVersion int64, kind string, payload map[string]any) error {
	if payload == nil {
		payload = map[string]any{}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal intent: %w", err)
	}
	now := model.NowUTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO intents(execution_id, chain_id, chain_version, kind, payload_json, status, created_at, applied_at)
		VALUES (?, ?, ?, ?, ?, 'applied', ?, ?)`,
		executionID, chainID, chainVersion, kind, string(raw), now, now); err != nil {
		return fmt.Errorf("insert intent: %w", err)
	}
	return nil
}

func (s *Store) SaveProvisionalBatch(ctx context.Context, executionID string, trace model.BatchTrace, aggregates []model.AggregateValue) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin provisional batch: %w", err)
	}
	defer tx.Rollback()

	var chainID string
	var chainVersion int64
	if err := tx.QueryRowContext(ctx, `SELECT chain_id, chain_version FROM executions WHERE execution_id = ?`, executionID).Scan(&chainID, &chainVersion); err != nil {
		return fmt.Errorf("read execution for batch: %w", err)
	}
	if err := insertIntent(ctx, tx, executionID, chainID, chainVersion, "batch_result", map[string]any{
		"batch":           trace.BatchNo,
		"input_rows":      trace.InputRows,
		"filtered_rows":   trace.FilteredRows,
		"aggregate_count": len(aggregates),
	}); err != nil {
		return err
	}
	traceJSON, err := json.Marshal(trace)
	if err != nil {
		return fmt.Errorf("marshal batch trace: %w", err)
	}
	aggJSON, err := json.Marshal(aggregates)
	if err != nil {
		return fmt.Errorf("marshal batch aggregate: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE batches
		SET row_start = ?, row_end = ?, selected_count = ?, status = ?, trace_json = ?, aggregate_json = ?
		WHERE execution_id = ? AND batch_no = ?`,
		trace.RowStart, trace.RowEnd, trace.OutputRows, model.BatchProvisional,
		string(traceJSON), string(aggJSON), executionID, trace.BatchNo)
	if err != nil {
		return fmt.Errorf("update provisional batch: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("batch %d does not exist for execution %s", trace.BatchNo, executionID)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit provisional batch: %w", err)
	}
	return nil
}

func (s *Store) FinalizeExecution(ctx context.Context, executionID string, aggregates []model.AggregateValue) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin finalize: %w", err)
	}
	defer tx.Rollback()

	var chainID, status string
	var chainVersion int64
	if err := tx.QueryRowContext(ctx, `
		SELECT chain_id, chain_version, status FROM executions WHERE execution_id = ?`, executionID).
		Scan(&chainID, &chainVersion, &status); err != nil {
		return fmt.Errorf("read execution for finalize: %w", err)
	}
	if status != model.StatusRunning {
		return fmt.Errorf("execution %s is not running", executionID)
	}
	var pending int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM batches WHERE execution_id = ? AND status != ?`,
		executionID, model.BatchProvisional).Scan(&pending); err != nil {
		return fmt.Errorf("check provisional batches: %w", err)
	}
	if pending != 0 {
		return fmt.Errorf("execution %s has %d batches without provisional results", executionID, pending)
	}
	if err := insertIntent(ctx, tx, executionID, chainID, chainVersion, "finalize", map[string]any{
		"aggregate_count": len(aggregates),
	}); err != nil {
		return err
	}
	aggJSON, err := json.Marshal(aggregates)
	if err != nil {
		return fmt.Errorf("marshal final aggregates: %w", err)
	}
	now := model.NowUTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO aggregate_results(execution_id, aggregate_json, committed_at)
		VALUES (?, ?, ?)`, executionID, string(aggJSON), now); err != nil {
		return fmt.Errorf("insert final aggregates: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE batches SET status = ? WHERE execution_id = ? AND status = ?`,
		model.BatchCommitted, executionID, model.BatchProvisional); err != nil {
		return fmt.Errorf("publish batches: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE executions SET status = ?, finished_at = ? WHERE execution_id = ?`,
		model.StatusCommitted, now, executionID); err != nil {
		return fmt.Errorf("publish execution: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit finalize: %w", err)
	}
	return nil
}

func (s *Store) FailExecution(ctx context.Context, executionID, finalStatus, code string, errorContext map[string]any) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin failure: %w", err)
	}
	defer tx.Rollback()
	var chainID, currentStatus string
	var chainVersion int64
	if err := tx.QueryRowContext(ctx, `SELECT chain_id, chain_version, status FROM executions WHERE execution_id = ?`, executionID).Scan(&chainID, &chainVersion, &currentStatus); err != nil {
		return fmt.Errorf("read execution for failure: %w", err)
	}
	if currentStatus != model.StatusRunning {
		// Never clobber an already committed/terminal version if a commit
		// acknowledgement error reaches the caller.
		return tx.Commit()
	}
	contextJSON, err := json.Marshal(errorContext)
	if err != nil {
		return fmt.Errorf("marshal error context: %w", err)
	}
	if err := insertIntent(ctx, tx, executionID, chainID, chainVersion, "failure_cleanup", map[string]any{
		"status":  finalStatus,
		"code":    code,
		"context": errorContext,
	}); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM aggregate_results WHERE execution_id = ?`, executionID); err != nil {
		return fmt.Errorf("discard aggregate result: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE batches SET status = ?, trace_json = '', aggregate_json = ''
		WHERE execution_id = ?`, model.BatchPending, executionID); err != nil {
		return fmt.Errorf("rollback provisional batches: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE executions SET status = ?, error_code = ?, error_context = ?, finished_at = ?
		WHERE execution_id = ?`,
		finalStatus, code, string(contextJSON), model.NowUTC(), executionID); err != nil {
		return fmt.Errorf("mark execution failure: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failure: %w", err)
	}
	return nil
}

func (s *Store) GetExecution(ctx context.Context, executionID string) (model.ExecutionView, error) {
	var view model.ExecutionView
	var bitmapJSON, decision, errorContext string
	var selectedRows int
	var chainID string
	var chainVersion, snapshotVersion int64
	var batchSize, threshold, batchCount int
	var path, status, errorCode, startedAt, finishedAt string
	err := s.db.QueryRowContext(ctx, `
		SELECT chain_id, chain_version, snapshot_version, bitmap_form, bitmap_json,
		       batch_size, large_threshold, selected_rows, path, decision, batch_count,
		       status, error_code, error_context, started_at, finished_at
		FROM executions WHERE execution_id = ?`, executionID).
		Scan(&chainID, &chainVersion, &snapshotVersion, &view.BitmapForm, &bitmapJSON,
			&batchSize, &threshold, &selectedRows, &path, &decision, &batchCount,
			&status, &errorCode, &errorContext, &startedAt, &finishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return model.ExecutionView{}, model.NewBizError(model.CodeNotFound, "execution not found").With("execution_id", executionID)
	}
	if err != nil {
		return model.ExecutionView{}, fmt.Errorf("read execution: %w", err)
	}
	if err := json.Unmarshal([]byte(bitmapJSON), &view.Bitmap); err != nil {
		return model.ExecutionView{}, fmt.Errorf("decode execution bitmap: %w", err)
	}
	view.ExecutionID = executionID
	view.ChainID = chainID
	view.ChainVersion = chainVersion
	view.SnapshotVersion = snapshotVersion
	view.BatchSize = batchSize
	view.LargeThreshold = threshold
	view.Path = model.PathDecision{
		Path:           path,
		Reason:         decision,
		SelectedRows:   selectedRows,
		BatchSize:      batchSize,
		LargeThreshold: threshold,
		BatchCount:     batchCount,
	}
	view.Status = status
	view.Published = status == model.StatusCommitted
	view.ErrorCode = errorCode
	if errorContext != "" {
		_ = json.Unmarshal([]byte(errorContext), &view.ErrorContext)
	}
	view.StartedAt = startedAt
	view.FinishedAt = finishedAt

	// Readers only receive a fully published version. Provisional batch rows and
	// aggregate rows are deliberately invisible while status is running.
	if status != model.StatusCommitted {
		return view, nil
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT trace_json FROM batches WHERE execution_id = ? AND status = ? ORDER BY batch_no ASC`,
		executionID, model.BatchCommitted)
	if err != nil {
		return model.ExecutionView{}, fmt.Errorf("read committed batches: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var traceJSON string
		if err := rows.Scan(&traceJSON); err != nil {
			return model.ExecutionView{}, err
		}
		var trace model.BatchTrace
		if err := json.Unmarshal([]byte(traceJSON), &trace); err != nil {
			return model.ExecutionView{}, fmt.Errorf("decode batch trace: %w", err)
		}
		view.Batches = append(view.Batches, trace)
	}
	if err := rows.Err(); err != nil {
		return model.ExecutionView{}, err
	}
	var aggJSON string
	if err := s.db.QueryRowContext(ctx, `
		SELECT aggregate_json FROM aggregate_results WHERE execution_id = ?`, executionID).Scan(&aggJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.ExecutionView{}, fmt.Errorf("committed execution %s has no aggregate result", executionID)
		}
		return model.ExecutionView{}, fmt.Errorf("read final aggregate: %w", err)
	}
	if err := json.Unmarshal([]byte(aggJSON), &view.Aggregates); err != nil {
		return model.ExecutionView{}, fmt.Errorf("decode final aggregate: %w", err)
	}
	return view, nil
}

func (s *Store) ListExecutions(ctx context.Context, chainID string, chainVersion int64) ([]model.ExecutionSummary, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT execution_id, bitmap_form, path, decision, selected_rows, batch_size, large_threshold,
		       batch_count, status, snapshot_version, error_code, started_at, finished_at
		FROM executions WHERE chain_id = ? AND chain_version = ?
		ORDER BY started_at DESC, execution_id DESC`, chainID, chainVersion)
	if err != nil {
		return nil, fmt.Errorf("list executions: %w", err)
	}
	defer rows.Close()
	var out []model.ExecutionSummary
	for rows.Next() {
		var summary model.ExecutionSummary
		var path, decision, status, errorCode, startedAt, finishedAt string
		var selectedRows, batchSize, threshold, batchCount int
		if err := rows.Scan(&summary.ExecutionID, &summary.BitmapForm, &path, &decision,
			&selectedRows, &batchSize, &threshold, &batchCount, &status,
			&summary.SnapshotVersion, &errorCode, &startedAt, &finishedAt); err != nil {
			return nil, err
		}
		summary.ChainID = chainID
		summary.ChainVersion = chainVersion
		summary.Path = model.PathDecision{
			Path:           path,
			Reason:         decision,
			SelectedRows:   selectedRows,
			BatchSize:      batchSize,
			LargeThreshold: threshold,
			BatchCount:     batchCount,
		}
		summary.Status = status
		summary.Published = status == model.StatusCommitted
		summary.ErrorCode = errorCode
		summary.StartedAt = startedAt
		summary.FinishedAt = finishedAt
		out = append(out, summary)
	}
	return out, rows.Err()
}

func (s *Store) RecoverIncomplete(ctx context.Context) ([]model.RecoveryReport, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT execution_id FROM executions WHERE status = ? ORDER BY started_at ASC`, model.StatusRunning)
	if err != nil {
		return nil, fmt.Errorf("scan incomplete executions: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	reports := make([]model.RecoveryReport, 0, len(ids))
	for _, id := range ids {
		report, err := s.recoverExecution(ctx, id)
		if err != nil {
			return nil, err
		}
		reports = append(reports, report)
	}
	return reports, nil
}

func (s *Store) recoverExecution(ctx context.Context, executionID string) (model.RecoveryReport, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.RecoveryReport{}, fmt.Errorf("begin recovery: %w", err)
	}
	defer tx.Rollback()

	var chainID, path, previousStatus, startedAt string
	var chainVersion int64
	if err := tx.QueryRowContext(ctx, `
		SELECT chain_id, chain_version, path, status, started_at
		FROM executions WHERE execution_id = ?`, executionID).
		Scan(&chainID, &chainVersion, &path, &previousStatus, &startedAt); err != nil {
		return model.RecoveryReport{}, fmt.Errorf("read execution for recovery: %w", err)
	}

	type batchFact struct {
		batchNo int
		status  string
		hasAgg  bool
	}
	var facts []batchFact
	batchRows, err := tx.QueryContext(ctx, `
		SELECT batch_no, status, CASE WHEN aggregate_json = '' THEN 0 ELSE 1 END
		FROM batches WHERE execution_id = ? ORDER BY batch_no`, executionID)
	if err != nil {
		return model.RecoveryReport{}, fmt.Errorf("read batches for recovery: %w", err)
	}
	for batchRows.Next() {
		var fact batchFact
		if err := batchRows.Scan(&fact.batchNo, &fact.status, &fact.hasAgg); err != nil {
			batchRows.Close()
			return model.RecoveryReport{}, err
		}
		facts = append(facts, fact)
	}
	batchRows.Close()
	if err := batchRows.Err(); err != nil {
		return model.RecoveryReport{}, err
	}

	report := model.RecoveryReport{
		ExecutionID:     executionID,
		ChainID:         chainID,
		ChainVersion:    chainVersion,
		PreviousStatus:  previousStatus,
		RecoveredStatus: model.StatusRecovered,
		Path:            path,
		CreatedAt:       model.NowUTC(),
	}
	for _, fact := range facts {
		switch fact.status {
		case model.BatchCommitted:
			report.CommittedBatches = append(report.CommittedBatches, fact.batchNo)
		case model.BatchProvisional:
			report.RolledBackBatches = append(report.RolledBackBatches, fact.batchNo)
			if fact.hasAgg {
				report.DiscardedAggregates = append(report.DiscardedAggregates, fact.batchNo)
			}
			report.Differences = append(report.Differences, model.RecoveryDiff{
				Kind:     "batch",
				BatchNo:  fact.batchNo,
				Before:   model.BatchProvisional,
				After:    model.BatchPending,
				Evidence: "provisional row persisted in batches; no committed execution version",
			})
		default:
			report.Differences = append(report.Differences, model.RecoveryDiff{
				Kind:     "batch",
				BatchNo:  fact.batchNo,
				Before:   fact.status,
				After:    fact.status,
				Evidence: "batch intent exists but no batch result was persisted",
			})
		}
	}
	var finalAggCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM aggregate_results WHERE execution_id = ?`, executionID).Scan(&finalAggCount); err != nil {
		return model.RecoveryReport{}, fmt.Errorf("count aggregate results: %w", err)
	}
	if finalAggCount > 0 {
		report.Differences = append(report.Differences, model.RecoveryDiff{
			Kind:     "final_aggregate",
			Before:   "persisted",
			After:    "discarded",
			Evidence: "execution never reached committed status",
		})
	}
	report.Differences = append(report.Differences, model.RecoveryDiff{
		Kind:     "execution_status",
		Before:   previousStatus,
		After:    model.StatusRecovered,
		Evidence: "startup recovery found an execution without a committed version",
	})

	intentRows, err := tx.QueryContext(ctx, `
		SELECT kind, status, payload_json, created_at FROM intents
		WHERE execution_id = ? ORDER BY id ASC`, executionID)
	if err != nil {
		return model.RecoveryReport{}, fmt.Errorf("read intents for recovery: %w", err)
	}
	for intentRows.Next() {
		var fact model.IntentFact
		var payloadJSON string
		if err := intentRows.Scan(&fact.Kind, &fact.Status, &payloadJSON, &fact.CreatedAt); err != nil {
			intentRows.Close()
			return model.RecoveryReport{}, err
		}
		_ = json.Unmarshal([]byte(payloadJSON), &fact.Payload)
		report.Intents = append(report.Intents, fact)
	}
	intentRows.Close()
	if err := intentRows.Err(); err != nil {
		return model.RecoveryReport{}, err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM aggregate_results WHERE execution_id = ?`, executionID); err != nil {
		return model.RecoveryReport{}, fmt.Errorf("discard uncommitted aggregate results: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE batches SET status = ?, trace_json = '', aggregate_json = ''
		WHERE execution_id = ? AND status = ?`,
		model.BatchPending, executionID, model.BatchProvisional); err != nil {
		return model.RecoveryReport{}, fmt.Errorf("rollback provisional batches: %w", err)
	}
	errorContext, _ := json.Marshal(map[string]any{
		"execution_id": executionID,
		"recovered":    true,
		"reason":       "process terminated before committed version",
	})
	if _, err := tx.ExecContext(ctx, `
		UPDATE executions SET status = ?, error_code = ?, error_context = ?, finished_at = ?
		WHERE execution_id = ?`,
		model.StatusRecovered, model.CodeExecAborted, string(errorContext), model.NowUTC(), executionID); err != nil {
		return model.RecoveryReport{}, fmt.Errorf("mark recovered execution: %w", err)
	}

	reportJSON, err := json.Marshal(report)
	if err != nil {
		return model.RecoveryReport{}, fmt.Errorf("marshal recovery report: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
		INSERT INTO recovery_reports(execution_id, payload_json, created_at)
		VALUES (?, ?, ?)`, executionID, string(reportJSON), report.CreatedAt)
	if err != nil {
		return model.RecoveryReport{}, fmt.Errorf("insert recovery report: %w", err)
	}
	id, _ := result.LastInsertId()
	report.ID = id
	reportJSON, err = json.Marshal(report)
	if err != nil {
		return model.RecoveryReport{}, fmt.Errorf("marshal recovery report id: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE recovery_reports SET payload_json = ? WHERE id = ?`, string(reportJSON), id); err != nil {
		return model.RecoveryReport{}, fmt.Errorf("store recovery report id: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return model.RecoveryReport{}, fmt.Errorf("commit recovery: %w", err)
	}
	return report, nil
}

func (s *Store) ListRecoveryReports(ctx context.Context) ([]model.RecoveryReport, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT payload_json FROM recovery_reports ORDER BY id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list recovery reports: %w", err)
	}
	defer rows.Close()
	var out []model.RecoveryReport
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var report model.RecoveryReport
		if err := json.Unmarshal([]byte(payload), &report); err != nil {
			return nil, fmt.Errorf("decode recovery report: %w", err)
		}
		out = append(out, report)
	}
	return out, rows.Err()
}
