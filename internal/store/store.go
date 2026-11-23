package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"h2diag/internal/model"

	_ "modernc.org/sqlite"
)

type Store struct {
	db                *sql.DB
	AfterIntentCommit func(model.PlanCommit)
}

func Open(ctx context.Context, path string) (*Store, error) {
	dsn := path
	if !strings.HasPrefix(dsn, "file:") && dsn != ":memory:" {
		dsn = "file:" + dsn
	}
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	dsn += separator + "_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	store := &Store{db: db}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.Recover(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS samples (
    sample_id TEXT PRIMARY KEY,
    canonical_hash TEXT NOT NULL,
    canonical_json TEXT NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL,
    state_digest TEXT NOT NULL,
    cumulative_rounding_error_ppm INTEGER NOT NULL DEFAULT 0,
    result_json BLOB,
    error_json BLOB,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sample_drafts (
    sample_id TEXT PRIMARY KEY,
    metadata_json TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sample_batches (
    sample_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    frames_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (sample_id, batch_id)
);
CREATE TABLE IF NOT EXISTS connections (
    sample_id TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    initial_window INTEGER NOT NULL,
    max_window INTEGER NOT NULL,
    window_remaining INTEGER NOT NULL,
    priority_levels INTEGER NOT NULL,
    window_updated_bytes INTEGER NOT NULL,
    PRIMARY KEY (sample_id, connection_id)
);
CREATE TABLE IF NOT EXISTS streams (
    sample_id TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    stream_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    initial_window INTEGER NOT NULL,
    max_window INTEGER NOT NULL,
    window_remaining INTEGER NOT NULL,
    sent_unacked INTEGER NOT NULL,
    total_sent INTEGER NOT NULL,
    weight INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    pending_bytes INTEGER NOT NULL,
    pending_json TEXT NOT NULL,
    PRIMARY KEY (sample_id, connection_id, stream_id)
);
CREATE TABLE IF NOT EXISTS commits (
    sample_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    kind TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    round_no INTEGER NOT NULL,
    connection_id TEXT NOT NULL,
    trace_json TEXT NOT NULL,
    state_json TEXT NOT NULL,
    digest TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (sample_id, version)
);
CREATE TABLE IF NOT EXISTS intents (
    intent_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id INTEGER NOT NULL,
    sample_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    intent_status TEXT NOT NULL,
    outcome TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

func (s *Store) GetSampleMeta(ctx context.Context, sampleID string) (*model.SampleMeta, bool, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT sample_id, canonical_hash, status, version, state_digest, result_json, error_json
FROM samples WHERE sample_id = ?`, sampleID)
	var meta model.SampleMeta
	var result, errorJSON []byte
	err := row.Scan(&meta.SampleID, &meta.CanonicalHash, &meta.Status, &meta.Version, &meta.StateDigest, &result, &errorJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	meta.ResultJSON = result
	meta.ErrorJSON = errorJSON
	return &meta, true, nil
}

func (s *Store) CreateSample(ctx context.Context, plan *model.Plan) error {
	intentID, err := s.beginIntent(ctx, plan.Sample.SampleID, 0, "INIT", plan.Hash)
	if err != nil {
		return err
	}
	if s.AfterIntentCommit != nil {
		s.AfterIntentCommit(model.PlanCommit{Version: 0, Kind: "INIT", Snapshot: plan.Initial, Digest: model.SnapshotDigest(plan.Initial)})
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	stateJSON, _ := json.Marshal(plan.Initial)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO samples(sample_id, canonical_hash, canonical_json, status, version, state_digest,
                    cumulative_rounding_error_ppm, created_at, updated_at)
VALUES(?,?,?,?,?,?,?,?,?)`,
		plan.Sample.SampleID, plan.Hash, string(plan.Canonical), "RUNNING", 0,
		model.SnapshotDigest(plan.Initial), 0, now, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO commits(sample_id, version, kind, timestamp, round_no, connection_id, trace_json, state_json, digest, created_at)
VALUES(?,?,?,?,?,?,?,?,?,?)`,
		plan.Sample.SampleID, 0, "INIT", 0, 0, "", "null", string(stateJSON), model.SnapshotDigest(plan.Initial), now); err != nil {
		return err
	}
	if err := replaceSnapshot(ctx, tx, plan.Sample.SampleID, plan.Initial); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return s.finishIntent(ctx, intentID)
}

func (s *Store) ApplyCommit(ctx context.Context, sampleID string, commit model.PlanCommit) error {
	intentID, err := s.beginIntent(ctx, sampleID, commit.Version, commit.Kind, commit.Digest)
	if err != nil {
		return err
	}
	if s.AfterIntentCommit != nil {
		s.AfterIntentCommit(commit)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := replaceSnapshot(ctx, tx, sampleID, commit.Snapshot); err != nil {
		return err
	}
	traceJSON := "null"
	if commit.Round != nil {
		data, marshalErr := json.Marshal(commit.Round)
		if marshalErr != nil {
			return marshalErr
		}
		traceJSON = string(data)
	}
	stateJSON, err := json.Marshal(commit.Snapshot)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO commits(sample_id, version, kind, timestamp, round_no, connection_id, trace_json, state_json, digest, created_at)
VALUES(?,?,?,?,?,?,?,?,?,?)`,
		sampleID, commit.Version, commit.Kind, commit.Timestamp, commit.RoundNo, commit.Connection,
		traceJSON, string(stateJSON), commit.Digest, now); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
UPDATE samples SET version=?, state_digest=?, cumulative_rounding_error_ppm=?, updated_at=?
WHERE sample_id=? AND status='RUNNING'`,
		commit.Version, commit.Digest, commit.Snapshot.CumulativeRoundingErrorPPM, now, sampleID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("sample %s is not RUNNING", sampleID)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return s.finishIntent(ctx, intentID)
}

func (s *Store) FinalizeSample(ctx context.Context, plan *model.Plan, response model.SubmitResponse) error {
	resultJSON, err := json.Marshal(response)
	if err != nil {
		return err
	}
	intentID, err := s.beginIntent(ctx, plan.Sample.SampleID, plan.Final.Version, "FINALIZE", plan.Digest)
	if err != nil {
		return err
	}
	if s.AfterIntentCommit != nil {
		s.AfterIntentCommit(model.PlanCommit{Version: plan.Final.Version, Kind: "FINALIZE", Snapshot: plan.Final, Digest: plan.Digest})
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := s.db.ExecContext(ctx, `
UPDATE samples SET status='DONE', result_json=?, updated_at=?, state_digest=?, version=?
WHERE sample_id=? AND status='RUNNING'`,
		resultJSON, now, plan.Digest, plan.Final.Version, plan.Sample.SampleID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("sample %s is not RUNNING at finalization", plan.Sample.SampleID)
	}
	return s.finishIntent(ctx, intentID)
}

func (s *Store) MarkSampleAborted(ctx context.Context, sampleID, code, detail string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	errorJSON, _ := json.Marshal(model.Biz(code, detail, map[string]any{"sample_id": sampleID}))
	_, err := s.db.ExecContext(ctx, `
UPDATE samples SET status='ABORTED', error_json=?, updated_at=? WHERE sample_id=? AND status='RUNNING'`,
		errorJSON, now, sampleID)
	return err
}

func replaceSnapshot(ctx context.Context, tx *sql.Tx, sampleID string, snapshot model.Snapshot) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM connections WHERE sample_id=?`, sampleID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM streams WHERE sample_id=?`, sampleID); err != nil {
		return err
	}
	for _, connection := range snapshot.Connections {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO connections(sample_id, connection_id, initial_window, max_window, window_remaining,
                        priority_levels, window_updated_bytes)
VALUES(?,?,?,?,?,?,?)`,
			sampleID, connection.ID, connection.InitialWindow, connection.MaxWindow,
			connection.WindowRemaining, connection.PriorityLevels, connection.WindowUpdatedBytes); err != nil {
			return err
		}
	}
	for _, stream := range snapshot.Streams {
		pendingJSON, err := json.Marshal(stream.Pending)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO streams(sample_id, connection_id, stream_id, state, initial_window, max_window,
                    window_remaining, sent_unacked, total_sent, weight, priority, pending_bytes, pending_json)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			sampleID, stream.ConnectionID, stream.StreamID, stream.State, stream.InitialWindow,
			stream.MaxWindow, stream.WindowRemaining, stream.SentUnacked, stream.TotalSent,
			stream.Weight, stream.Priority, stream.PendingBytes, string(pendingJSON)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) beginIntent(ctx context.Context, sampleID string, version int64, kind, payloadHash string) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := s.db.ExecContext(ctx, `
INSERT INTO intents(sample_id, version, kind, payload_hash, status, created_at, updated_at)
VALUES(?,?,?,?,?,?,?)`, sampleID, version, kind, payloadHash, "RUNNING", now, now)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (s *Store) finishIntent(ctx context.Context, intentID int64) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET status='COMMITTED', updated_at=? WHERE intent_id=?`, now, intentID)
	return err
}

func (s *Store) LoadSnapshot(ctx context.Context, sampleID string, verify bool) (*model.Snapshot, *model.SampleMeta, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = tx.Rollback() }()
	meta := &model.SampleMeta{}
	var resultJSON, errorJSON []byte
	var cumulative int64
	err = tx.QueryRowContext(ctx, `
SELECT sample_id, canonical_hash, status, version, state_digest, result_json, error_json,
       cumulative_rounding_error_ppm
FROM samples WHERE sample_id=?`, sampleID).Scan(
		&meta.SampleID, &meta.CanonicalHash, &meta.Status, &meta.Version, &meta.StateDigest,
		&resultJSON, &errorJSON, &cumulative)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, model.Biz(model.CodeNotFound, "sample not found", map[string]any{"sample_id": sampleID})
	}
	if err != nil {
		return nil, nil, err
	}
	meta.ResultJSON = resultJSON
	meta.ErrorJSON = errorJSON
	snapshot := model.Snapshot{
		SampleID:                   sampleID,
		Version:                    meta.Version,
		CumulativeRoundingErrorPPM: cumulative,
	}
	connRows, err := tx.QueryContext(ctx, `
SELECT connection_id, initial_window, max_window, window_remaining, priority_levels, window_updated_bytes
FROM connections WHERE sample_id=? ORDER BY connection_id`, sampleID)
	if err != nil {
		return nil, nil, err
	}
	for connRows.Next() {
		var connection model.ConnectionSnapshot
		if err := connRows.Scan(&connection.ID, &connection.InitialWindow, &connection.MaxWindow,
			&connection.WindowRemaining, &connection.PriorityLevels, &connection.WindowUpdatedBytes); err != nil {
			_ = connRows.Close()
			return nil, nil, err
		}
		snapshot.Connections = append(snapshot.Connections, connection)
	}
	if err := connRows.Close(); err != nil {
		return nil, nil, err
	}
	streamRows, err := tx.QueryContext(ctx, `
SELECT connection_id, stream_id, state, initial_window, max_window, window_remaining,
       sent_unacked, total_sent, weight, priority, pending_bytes, pending_json
FROM streams WHERE sample_id=? ORDER BY connection_id, stream_id`, sampleID)
	if err != nil {
		return nil, nil, err
	}
	for streamRows.Next() {
		var stream model.StreamSnapshot
		var pendingJSON string
		if err := streamRows.Scan(&stream.ConnectionID, &stream.StreamID, &stream.State,
			&stream.InitialWindow, &stream.MaxWindow, &stream.WindowRemaining,
			&stream.SentUnacked, &stream.TotalSent, &stream.Weight, &stream.Priority,
			&stream.PendingBytes, &pendingJSON); err != nil {
			_ = streamRows.Close()
			return nil, nil, err
		}
		if err := json.Unmarshal([]byte(pendingJSON), &stream.Pending); err != nil {
			_ = streamRows.Close()
			return nil, nil, err
		}
		snapshot.Streams = append(snapshot.Streams, stream)
	}
	if err := streamRows.Close(); err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	if verify {
		actual := model.SnapshotDigest(snapshot)
		if actual != meta.StateDigest {
			return &snapshot, meta, model.Biz(model.CodeCheckpointCorrupt, "persisted snapshot digest does not match checkpoint", map[string]any{
				"sample_id":       sampleID,
				"version":         meta.Version,
				"expected_digest": meta.StateDigest,
				"actual_digest":   actual,
			})
		}
	}
	return &snapshot, meta, nil
}

func (s *Store) LoadSnapshotAtVersion(ctx context.Context, sampleID string, version int64) (*model.Snapshot, error) {
	row := s.db.QueryRowContext(ctx, `SELECT state_json, digest FROM commits WHERE sample_id=? AND version=?`, sampleID, version)
	var stateJSON, digest string
	if err := row.Scan(&stateJSON, &digest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, model.Biz(model.CodeNotFound, "sample version not found", map[string]any{
				"sample_id": sampleID, "version": version,
			})
		}
		return nil, err
	}
	var snapshot model.Snapshot
	if err := json.Unmarshal([]byte(stateJSON), &snapshot); err != nil {
		return nil, model.Biz(model.CodeCheckpointCorrupt, "checkpoint state JSON is corrupt", map[string]any{
			"sample_id": sampleID, "version": version, "detail": err.Error(),
		})
	}
	actual := model.SnapshotDigest(snapshot)
	if actual != digest {
		return nil, model.Biz(model.CodeCheckpointCorrupt, "checkpoint state digest does not match commit", map[string]any{
			"sample_id": sampleID, "version": version, "expected_digest": digest, "actual_digest": actual,
		})
	}
	return &snapshot, nil
}

func (s *Store) ListRounds(ctx context.Context, sampleID string, limit, offset int) ([]model.StoredRound, error) {
	if limit <= 0 || limit > 5000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT version, round_no, timestamp, connection_id, trace_json
FROM commits WHERE sample_id=? AND kind='ROUND'
ORDER BY version LIMIT ? OFFSET ?`, sampleID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rounds []model.StoredRound
	for rows.Next() {
		var round model.StoredRound
		var traceJSON string
		if err := rows.Scan(&round.Version, &round.RoundNo, &round.Timestamp, &round.Connection, &traceJSON); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(traceJSON), &round.Trace); err != nil {
			return nil, err
		}
		rounds = append(rounds, round)
	}
	return rounds, rows.Err()
}

func (s *Store) CommitCount(ctx context.Context, sampleID string) (int64, error) {
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM commits WHERE sample_id=?`, sampleID)
	var count int64
	err := row.Scan(&count)
	return count, err
}

func (s *Store) SaveBatch(ctx context.Context, sampleID, batchID string, metadata *model.SampleMetadata, frames []model.Frame) (bool, error) {
	canonicalFrames := append([]model.Frame(nil), frames...)
	for index := range canonicalFrames {
		flags := append([]string{}, canonicalFrames[index].Flags...)
		sort.Strings(flags)
		canonicalFrames[index].Flags = flags
	}
	sort.SliceStable(canonicalFrames, func(i, j int) bool {
		if canonicalFrames[i].Sequence == canonicalFrames[j].Sequence {
			return canonicalFrames[i].Timestamp < canonicalFrames[j].Timestamp
		}
		return canonicalFrames[i].Sequence < canonicalFrames[j].Sequence
	})
	framesJSON, err := json.Marshal(canonicalFrames)
	if err != nil {
		return false, err
	}
	contentHash := sha256Hex(framesJSON)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var existingHash string
	err = tx.QueryRowContext(ctx, `SELECT content_hash FROM sample_batches WHERE sample_id=? AND batch_id=?`, sampleID, batchID).Scan(&existingHash)
	if err == nil {
		if existingHash != contentHash {
			return false, model.Biz(model.CodeConflictingSample, "batch id was reused with different frame content", map[string]any{
				"sample_id": sampleID, "batch_id": batchID,
			})
		}
		return true, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	var draftStatus string
	err = tx.QueryRowContext(ctx, `SELECT status FROM sample_drafts WHERE sample_id=?`, sampleID).Scan(&draftStatus)
	if errors.Is(err, sql.ErrNoRows) {
		if metadata == nil {
			return false, model.Biz(model.CodeFrameInvalid, "first batch must include metadata", map[string]any{"sample_id": sampleID})
		}
		metadataJSON, marshalErr := json.Marshal(metadata)
		if marshalErr != nil {
			return false, marshalErr
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO sample_drafts(sample_id, metadata_json, status, created_at, updated_at)
VALUES(?,?,?,?,?)`, sampleID, string(metadataJSON), "COLLECTING", now, now); err != nil {
			return false, err
		}
	} else if err != nil {
		return false, err
	} else if draftStatus != "COLLECTING" {
		return false, model.Biz(model.CodeConflictingSample, "sample draft is no longer collecting", map[string]any{"sample_id": sampleID})
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO sample_batches(sample_id, batch_id, content_hash, frames_json, created_at)
VALUES(?,?,?,?,?)`, sampleID, batchID, contentHash, string(framesJSON), now); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sample_drafts SET updated_at=? WHERE sample_id=?`, now, sampleID); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return false, nil
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (s *Store) LoadBatches(ctx context.Context, sampleID string) (*model.SampleMetadata, []model.Frame, bool, error) {
	var metadataJSON string
	var status string
	err := s.db.QueryRowContext(ctx, `SELECT metadata_json, status FROM sample_drafts WHERE sample_id=?`, sampleID).Scan(&metadataJSON, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	var metadata model.SampleMetadata
	if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
		return nil, nil, true, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT frames_json FROM sample_batches WHERE sample_id=? ORDER BY batch_id`, sampleID)
	if err != nil {
		return nil, nil, true, err
	}
	defer rows.Close()
	var frames []model.Frame
	for rows.Next() {
		var framesJSON string
		if err := rows.Scan(&framesJSON); err != nil {
			return nil, nil, true, err
		}
		var batch []model.Frame
		if err := json.Unmarshal([]byte(framesJSON), &batch); err != nil {
			return nil, nil, true, err
		}
		frames = append(frames, batch...)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, true, err
	}
	return &metadata, frames, true, nil
}

func (s *Store) BatchFrames(ctx context.Context, sampleID, batchID string) ([]model.Frame, bool, error) {
	var framesJSON string
	err := s.db.QueryRowContext(ctx, `
SELECT frames_json FROM sample_batches WHERE sample_id=? AND batch_id=?`, sampleID, batchID).Scan(&framesJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var frames []model.Frame
	if err := json.Unmarshal([]byte(framesJSON), &frames); err != nil {
		return nil, true, err
	}
	return frames, true, nil
}

func (s *Store) MarkDraftConsumed(ctx context.Context, sampleID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `UPDATE sample_drafts SET status='CONSUMED', updated_at=? WHERE sample_id=?`, now, sampleID)
	return err
}

func (s *Store) DraftSummary(ctx context.Context, sampleID string) (map[string]any, error) {
	var status string
	var createdAt string
	err := s.db.QueryRowContext(ctx, `SELECT status, created_at FROM sample_drafts WHERE sample_id=?`, sampleID).Scan(&status, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var batchCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sample_batches WHERE sample_id=?`, sampleID).Scan(&batchCount); err != nil {
		return nil, err
	}
	return map[string]any{"status": status, "created_at": createdAt, "batch_count": batchCount}, nil
}

func (s *Store) SetAfterIntentHook(hook func(model.PlanCommit)) {
	s.AfterIntentCommit = hook
}
