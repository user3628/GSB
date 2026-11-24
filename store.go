package abeval

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

type Store struct {
	db  *sql.DB
	dsn string
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type batchRow struct {
	BatchID           string
	ExperimentID      string
	Version           int64
	ConfigHash        string
	SampleHash        string
	ContentHash       string
	ConfigJSON        string
	TotalStages       int
	CurrentStage      int
	Status            string
	StopRequested     int
	FinalDecision     string
	FinalDecisionCode string
	FinalVerdictJSON  string
	ReportJSON        string
}

func OpenStore(path string) (*Store, error) {
	dsn, err := sqliteDSN(path)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db, dsn: dsn}
	if err := store.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func sqliteDSN(path string) (string, error) {
	if path == ":memory:" {
		return ":memory:?_pragma=busy_timeout(10000)&_pragma=foreign_keys(ON)&_pragma=synchronous(FULL)", nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	u := url.URL{Scheme: "file", Path: absolute}
	query := url.Values{}
	query.Add("_pragma", "busy_timeout(10000)")
	query.Add("_pragma", "journal_mode(WAL)")
	query.Add("_pragma", "foreign_keys(ON)")
	query.Add("_pragma", "synchronous(FULL)")
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS batches (
			batch_id TEXT PRIMARY KEY,
			experiment_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			config_hash TEXT NOT NULL,
			sample_hash TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			config_json TEXT NOT NULL,
			total_stages INTEGER NOT NULL,
			current_stage INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL,
			stop_requested INTEGER NOT NULL DEFAULT 0,
			final_decision TEXT NOT NULL DEFAULT '',
			final_decision_code TEXT NOT NULL DEFAULT '',
			final_verdict_json TEXT NOT NULL DEFAULT '',
			report_json TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS revisions (
			batch_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			config_hash TEXT NOT NULL,
			sample_hash TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			PRIMARY KEY (batch_id, version),
			UNIQUE (batch_id, content_hash)
		)`,
		`CREATE TABLE IF NOT EXISTS events (
			batch_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			seq INTEGER NOT NULL,
			event_id TEXT NOT NULL,
			timestamp_ms INTEGER NOT NULL,
			user_id TEXT NOT NULL,
			group_name TEXT NOT NULL,
			item_id TEXT NOT NULL,
			clicked INTEGER NOT NULL,
			latency_us INTEGER NOT NULL,
			stage_index INTEGER NOT NULL,
			PRIMARY KEY (batch_id, version, seq),
			UNIQUE (batch_id, version, event_id)
		)`,
		`CREATE TABLE IF NOT EXISTS stage_checkpoints (
			batch_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			content_hash TEXT NOT NULL,
			stage_index INTEGER NOT NULL,
			status TEXT NOT NULL,
			stats_json TEXT NOT NULL,
			verdict_json TEXT NOT NULL,
			stats_hash TEXT NOT NULL,
			writer_id TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (batch_id, version, stage_index)
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_stage_checkpoint_active
		 ON stage_checkpoints(batch_id, stage_index)
		 WHERE status = 'valid'`,
		`CREATE INDEX IF NOT EXISTS idx_events_batch_order
		 ON events(batch_id, version, seq)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate sqlite schema: %w", err)
		}
	}
	return nil
}

func (s *Store) beginImmediate(ctx context.Context) (*sql.Conn, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		_ = conn.Close()
		return nil, mapSQLiteError(err)
	}
	return conn, nil
}

func rollbackConn(conn *sql.Conn) {
	_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
	_ = conn.Close()
}

func commitConn(conn *sql.Conn) error {
	if _, err := conn.ExecContext(context.Background(), `COMMIT`); err != nil {
		_ = conn.Close()
		return mapSQLiteError(err)
	}
	return conn.Close()
}

func mapSQLiteError(err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	if strings.Contains(message, "UNIQUE constraint failed") {
		return NewError(CodeStaleCheckpoint, "concurrent write lost conditional commit").
			WithStage("sqlite_transaction").WithDetail("sqlite_error", message)
	}
	if strings.Contains(message, "database is locked") || strings.Contains(message, "SQLITE_BUSY") {
		businessError := NewError(CodeBusy, "sqlite database is busy").
			WithStage("sqlite_transaction").
			WithDetail("sqlite_error", message)
		businessError.Retryable = true
		return businessError
	}
	return err
}

func requireBatchID(batchID string, dataset *Dataset) string {
	if batchID != "" {
		return batchID
	}
	if dataset != nil && dataset.BatchID != "" {
		return dataset.BatchID
	}
	if dataset != nil {
		return dataset.Config.ExperimentID
	}
	return ""
}

func (s *Store) ImportDataset(ctx context.Context, batchID string, input Dataset) (*ImportResult, error) {
	dataset := input
	if dataset.Version <= 0 {
		dataset.Version = 1
	}
	if err := ValidateDataset(&dataset); err != nil {
		return nil, err
	}
	batchID = requireBatchID(batchID, &dataset)
	if batchID == "" {
		return nil, NewError(CodeInvalidConfig, "batch_id is required").
			WithStage("import_dataset").WithField("batch_id")
	}
	revision, err := HashDataset(dataset)
	if err != nil {
		return nil, err
	}
	configJSON, err := CanonicalJSON(dataset.Config)
	if err != nil {
		return nil, err
	}

	conn, err := s.beginImmediate(ctx)
	if err != nil {
		return nil, err
	}
	defer rollbackConn(conn)

	var current batchRow
	err = scanBatch(conn.QueryRowContext(ctx, `
		SELECT batch_id, experiment_id, version, config_hash, sample_hash, content_hash,
		       config_json, total_stages, current_stage, status, stop_requested,
		       final_decision, final_decision_code, final_verdict_json, report_json
		FROM batches WHERE batch_id = ?`, batchID), &current)

	if err == sql.ErrNoRows {
		if err := insertBatch(ctx, conn, batchID, dataset, revision, string(configJSON)); err != nil {
			return nil, err
		}
		if err := insertRevision(ctx, conn, batchID, revision, "active"); err != nil {
			return nil, err
		}
		if err := insertEvents(ctx, conn, batchID, dataset); err != nil {
			return nil, err
		}
		if err := commitConn(conn); err != nil {
			return nil, err
		}
		return &ImportResult{
			BatchID: batchID, Created: true, CurrentVersion: dataset.Version,
			ContentHash: revision.ContentHash, EventCount: len(dataset.Events),
		}, nil
	}
	if err != nil {
		return nil, err
	}

	if current.ContentHash == revision.ContentHash {
		if err := commitConn(conn); err != nil {
			return nil, err
		}
		return &ImportResult{
			BatchID: batchID, Idempotent: true, CurrentVersion: current.Version,
			ContentHash: current.ContentHash, EventCount: len(dataset.Events),
		}, nil
	}

	var priorVersion int64
	priorErr := conn.QueryRowContext(ctx, `
		SELECT version FROM revisions WHERE batch_id = ? AND content_hash = ?`,
		batchID, revision.ContentHash).Scan(&priorVersion)
	if priorErr != nil && priorErr != sql.ErrNoRows {
		return nil, priorErr
	}
	if priorErr == nil {
		return nil, NewError(CodeVersionRollback, "content digest matches a superseded revision; rollback is rejected").
			WithStage("import_dataset").
			WithField("version").
			WithDetail("batch_id", batchID).
			WithDetail("prior_version", priorVersion).
			WithDetail("current_version", current.Version).
			WithDetail("incoming_version", dataset.Version).
			WithDetail("content_hash", revision.ContentHash)
	}
	if dataset.Version <= current.Version {
		return nil, NewError(CodeVersionRollback, "incoming version is not newer than active version").
			WithStage("import_dataset").
			WithField("version").
			WithDetail("batch_id", batchID).
			WithDetail("current_version", current.Version).
			WithDetail("incoming_version", dataset.Version).
			WithDetail("content_hash", revision.ContentHash)
	}

	if _, err := conn.ExecContext(ctx, `
		UPDATE stage_checkpoints SET status = 'superseded'
		WHERE batch_id = ? AND status = 'valid'`, batchID); err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `
		UPDATE revisions SET status = 'superseded'
		WHERE batch_id = ? AND status = 'active'`, batchID); err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `DELETE FROM events WHERE batch_id = ?`, batchID); err != nil {
		return nil, err
	}
	if err := insertRevision(ctx, conn, batchID, revision, "active"); err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `
		UPDATE batches
		SET experiment_id=?, version=?, config_hash=?, sample_hash=?, content_hash=?,
		    config_json=?, total_stages=?, current_stage=0, status=?, stop_requested=0,
		    final_decision='', final_decision_code='', final_verdict_json='', report_json=''
		WHERE batch_id=?`,
		dataset.Config.ExperimentID, revision.Version, revision.ConfigHash, revision.SampleHash,
		revision.ContentHash, string(configJSON), len(dataset.Config.Stages), StatusImported, batchID,
	); err != nil {
		return nil, err
	}
	if err := insertEvents(ctx, conn, batchID, dataset); err != nil {
		return nil, err
	}
	if err := commitConn(conn); err != nil {
		return nil, err
	}
	return &ImportResult{
		BatchID: batchID, Replaced: true, PreviousVersion: current.Version,
		CurrentVersion: dataset.Version, ContentHash: revision.ContentHash,
		EventCount: len(dataset.Events),
	}, nil
}

func insertBatch(ctx context.Context, conn *sql.Conn, batchID string, dataset Dataset, revision RevisionInfo, configJSON string) error {
	_, err := conn.ExecContext(ctx, `
		INSERT INTO batches (
			batch_id, experiment_id, version, config_hash, sample_hash, content_hash,
			config_json, total_stages, current_stage, status, stop_requested
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0)`,
		batchID, dataset.Config.ExperimentID, revision.Version, revision.ConfigHash,
		revision.SampleHash, revision.ContentHash, configJSON, len(dataset.Config.Stages), StatusImported)
	return err
}

func insertRevision(ctx context.Context, conn *sql.Conn, batchID string, revision RevisionInfo, status string) error {
	_, err := conn.ExecContext(ctx, `
		INSERT INTO revisions (batch_id, version, config_hash, sample_hash, content_hash, status)
		VALUES (?, ?, ?, ?, ?, ?)`,
		batchID, revision.Version, revision.ConfigHash, revision.SampleHash, revision.ContentHash, status)
	return err
}

func insertEvents(ctx context.Context, conn *sql.Conn, batchID string, dataset Dataset) error {
	for index, event := range dataset.Events {
		stageIndex, err := StageIndexAt(event.TimestampMS, dataset.Config)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `
			INSERT INTO events (
				batch_id, version, seq, event_id, timestamp_ms, user_id, group_name,
				item_id, clicked, latency_us, stage_index
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			batchID, dataset.Version, index, event.EventID, event.TimestampMS, event.UserID,
			event.Group, event.ItemID, event.Clicked, event.LatencyUS, stageIndex)
		if err != nil {
			return mapSQLiteError(err)
		}
	}
	return nil
}

func scanBatch(scanner interface{ Scan(...any) error }, row *batchRow) error {
	return scanner.Scan(
		&row.BatchID, &row.ExperimentID, &row.Version, &row.ConfigHash, &row.SampleHash,
		&row.ContentHash, &row.ConfigJSON, &row.TotalStages, &row.CurrentStage, &row.Status,
		&row.StopRequested, &row.FinalDecision, &row.FinalDecisionCode,
		&row.FinalVerdictJSON, &row.ReportJSON,
	)
}

const batchSelectColumns = `
	batch_id, experiment_id, version, config_hash, sample_hash, content_hash,
	config_json, total_stages, current_stage, status, stop_requested,
	final_decision, final_decision_code, final_verdict_json, report_json`

func (s *Store) getBatchRow(ctx context.Context, q querier, batchID string) (batchRow, error) {
	var row batchRow
	err := scanBatch(q.QueryRowContext(ctx,
		`SELECT `+batchSelectColumns+` FROM batches WHERE batch_id = ?`, batchID), &row)
	if err == sql.ErrNoRows {
		return row, NewError(CodeNotFound, "batch not found").
			WithStage("load_batch").WithDetail("batch_id", batchID)
	}
	return row, err
}

func (s *Store) GetStatus(ctx context.Context, batchID string) (*BatchStatus, error) {
	row, err := s.getBatchRow(ctx, s.db, batchID)
	if err != nil {
		return nil, err
	}
	return batchStatusFromRow(row), nil
}

func batchStatusFromRow(row batchRow) *BatchStatus {
	return &BatchStatus{
		BatchID: row.BatchID, ExperimentID: row.ExperimentID, Version: row.Version,
		ConfigHash: row.ConfigHash, SampleHash: row.SampleHash, ContentHash: row.ContentHash,
		TotalStages: row.TotalStages, CurrentStage: row.CurrentStage, Status: row.Status,
		StopRequested: row.StopRequested == 1, Decision: row.FinalDecision,
		DecisionCode: row.FinalDecisionCode,
	}
}

func (s *Store) GetRevision(ctx context.Context, batchID string) (*RevisionInfo, error) {
	status, err := s.GetStatus(ctx, batchID)
	if err != nil {
		return nil, err
	}
	return &RevisionInfo{
		Version: status.Version, ConfigHash: status.ConfigHash,
		SampleHash: status.SampleHash, ContentHash: status.ContentHash,
	}, nil
}

func (s *Store) LoadDataset(ctx context.Context, batchID string) (*Dataset, error) {
	row, err := s.getBatchRow(ctx, s.db, batchID)
	if err != nil {
		return nil, err
	}
	var config Config
	if err := json.Unmarshal([]byte(row.ConfigJSON), &config); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT event_id, timestamp_ms, user_id, group_name, item_id, clicked, latency_us
		FROM events WHERE batch_id = ? AND version = ? ORDER BY seq ASC`,
		batchID, row.Version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]Event, 0)
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.EventID, &event.TimestampMS, &event.UserID, &event.Group,
			&event.ItemID, &event.Clicked, &event.LatencyUS); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &Dataset{
		Version: row.Version, BatchID: batchID, Config: config, Events: events,
	}, nil
}
