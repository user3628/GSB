package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "modernc.org/sqlite"

	"localsearch/multirecall/internal/model"
)

type Store struct {
	db       *sql.DB
	path     string
	lockPath string
}

type RunRecord struct {
	RunID             string `json:"run_id"`
	ConfigVersion     string `json:"config_version"`
	ConfigFingerprint string `json:"config_fingerprint"`
	Seed              int64  `json:"seed"`
	DataFingerprint   string `json:"data_fingerprint"`
	Status            string `json:"status"`
	SummaryDigest     string `json:"summary_digest"`
	SummaryJSON       string `json:"summary_json"`
	Stopped           bool   `json:"stopped"`
}

type Checkpoint struct {
	QueryID            string `json:"query_id"`
	Stage              string `json:"stage"`
	Sequence           int    `json:"sequence"`
	Revision           int    `json:"revision"`
	InputFingerprint   string `json:"input_fingerprint"`
	ConfigVersion      string `json:"config_version"`
	ArtifactDigest     string `json:"artifact_digest"`
	Payload            []byte `json:"payload"`
	Valid              bool   `json:"valid"`
	InvalidationReason string `json:"invalidation_reason,omitempty"`
}

type PublishRequest struct {
	RunID              string
	QueryID            string
	Stage              string
	Sequence           int
	Revision           int
	InputFingerprint   string
	ConfigVersion      string
	ArtifactDigest     string
	PrerequisiteDigest string
	Payload            []byte
}

type FailureRecord struct {
	RunID   string
	Shard   int
	QueryID string
	Code    string
	Message string
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, model.NewError(model.CodeInvalidConfig, "database path is required", "")
	}
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
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
		"PRAGMA busy_timeout=10000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			return nil, model.NewError(model.CodeInternal, "configure sqlite: "+err.Error(), "")
		}
	}
	s := &Store{db: db, path: path, lockPath: path + ".publish.lock"}
	if err := s.initSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) initSchema() error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS runs (
			run_id TEXT PRIMARY KEY,
			config_version TEXT NOT NULL,
			config_fingerprint TEXT NOT NULL,
			seed INTEGER NOT NULL,
			data_fingerprint TEXT NOT NULL,
			status TEXT NOT NULL,
			summary_digest TEXT NOT NULL DEFAULT '',
			summary_json TEXT NOT NULL DEFAULT '',
			stopped INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id TEXT NOT NULL,
			query_id TEXT NOT NULL,
			stage TEXT NOT NULL CHECK(stage IN ('recall','fusion','evaluation')),
			sequence INTEGER NOT NULL,
			revision INTEGER NOT NULL,
			input_fingerprint TEXT NOT NULL,
			config_version TEXT NOT NULL,
			artifact_digest TEXT NOT NULL,
			payload BLOB NOT NULL,
			valid INTEGER NOT NULL CHECK(valid IN (0,1)),
			invalidation_reason TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoint_heads (
			query_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			sequence INTEGER NOT NULL,
			revision INTEGER NOT NULL,
			input_fingerprint TEXT NOT NULL,
			artifact_digest TEXT NOT NULL,
			valid INTEGER NOT NULL CHECK(valid IN (0,1)),
			PRIMARY KEY(query_id, stage)
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_checkpoint_valid ON checkpoints(query_id, stage) WHERE valid = 1`,
		`CREATE INDEX IF NOT EXISTS idx_checkpoint_lookup ON checkpoints(query_id, stage, revision DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_checkpoint_run ON checkpoints(run_id, query_id, stage)`,
		`CREATE TABLE IF NOT EXISTS failures (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id TEXT NOT NULL,
			shard INTEGER NOT NULL,
			query_id TEXT NOT NULL,
			code TEXT NOT NULL,
			message TEXT NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := s.db.Exec(statement); err != nil {
			return model.NewError(model.CodeInternal, "initialize sqlite schema: "+err.Error(), "")
		}
	}
	return nil
}

func (s *Store) UpsertRun(record RunRecord) error {
	_, err := s.db.Exec(`INSERT INTO runs(run_id, config_version, config_fingerprint, seed, data_fingerprint, status, summary_digest, summary_json, stopped)
		VALUES(?,?,?,?,?,?,?,?,?)
		ON CONFLICT(run_id) DO UPDATE SET
			config_version=excluded.config_version,
			config_fingerprint=excluded.config_fingerprint,
			seed=excluded.seed,
			data_fingerprint=excluded.data_fingerprint,
			status=excluded.status,
			summary_digest=excluded.summary_digest,
			summary_json=excluded.summary_json,
			stopped=excluded.stopped`,
		record.RunID, record.ConfigVersion, record.ConfigFingerprint, record.Seed, record.DataFingerprint,
		record.Status, record.SummaryDigest, record.SummaryJSON, boolInt(record.Stopped))
	if err != nil {
		return model.NewError(model.CodeInternal, "upsert run: "+err.Error(), "")
	}
	return nil
}

func (s *Store) UpdateRunStatus(runID, status string, stopped bool) error {
	_, err := s.db.Exec(`UPDATE runs SET status=?, stopped=? WHERE run_id=?`, status, boolInt(stopped), runID)
	if err != nil {
		return model.NewError(model.CodeInternal, "update run status: "+err.Error(), "")
	}
	return nil
}

func (s *Store) SaveSummary(runID, digest, summaryJSON, status string, stopped bool) error {
	_, err := s.db.Exec(`UPDATE runs SET summary_digest=?, summary_json=?, status=?, stopped=? WHERE run_id=?`,
		digest, summaryJSON, status, boolInt(stopped), runID)
	if err != nil {
		return model.NewError(model.CodeInternal, "save run summary: "+err.Error(), "")
	}
	return nil
}

func (s *Store) GetRun(runID string) (RunRecord, error) {
	var record RunRecord
	var stopped int
	err := s.db.QueryRow(`SELECT run_id, config_version, config_fingerprint, seed, data_fingerprint, status, summary_digest, summary_json, stopped FROM runs WHERE run_id=?`, runID).
		Scan(&record.RunID, &record.ConfigVersion, &record.ConfigFingerprint, &record.Seed, &record.DataFingerprint, &record.Status, &record.SummaryDigest, &record.SummaryJSON, &stopped)
	if errors.Is(err, sql.ErrNoRows) {
		return RunRecord{}, model.NewError(model.CodeDataLoad, "run not found", "")
	}
	if err != nil {
		return RunRecord{}, model.NewError(model.CodeInternal, "get run: "+err.Error(), "")
	}
	record.Stopped = stopped != 0
	return record, nil
}

func (s *Store) LatestRun() (RunRecord, error) {
	var runID string
	err := s.db.QueryRow(`SELECT run_id FROM runs ORDER BY rowid DESC LIMIT 1`).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return RunRecord{}, model.NewError(model.CodeDataLoad, "no runs found", "")
	}
	if err != nil {
		return RunRecord{}, model.NewError(model.CodeInternal, "find latest run: "+err.Error(), "")
	}
	return s.GetRun(runID)
}

func (s *Store) ListRuns() ([]RunRecord, error) {
	rows, err := s.db.Query(`SELECT run_id, config_version, config_fingerprint, seed, data_fingerprint, status, summary_digest, summary_json, stopped FROM runs ORDER BY rowid`)
	if err != nil {
		return nil, model.NewError(model.CodeInternal, "list runs: "+err.Error(), "")
	}
	defer rows.Close()
	out := make([]RunRecord, 0)
	for rows.Next() {
		var record RunRecord
		var stopped int
		if err := rows.Scan(&record.RunID, &record.ConfigVersion, &record.ConfigFingerprint, &record.Seed, &record.DataFingerprint, &record.Status, &record.SummaryDigest, &record.SummaryJSON, &stopped); err != nil {
			return nil, model.NewError(model.CodeInternal, "scan run: "+err.Error(), "")
		}
		record.Stopped = stopped != 0
		out = append(out, record)
	}
	return out, nil
}

func (s *Store) LatestRevision(ctx context.Context, queryID, stage string) (int, error) {
	var revision int
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(revision),0) FROM checkpoints WHERE query_id=? AND stage=?`, queryID, stage).Scan(&revision)
	if err != nil {
		return 0, model.NewError(model.CodeInternal, "read checkpoint revision: "+err.Error(), queryID)
	}
	return revision, nil
}

func (s *Store) LoadReusable(ctx context.Context, queryID, stage, inputFingerprint string) (Checkpoint, bool, error) {
	var cp Checkpoint
	var payload []byte
	var valid int
	err := s.db.QueryRowContext(ctx, `SELECT query_id, stage, sequence, revision, input_fingerprint, config_version, artifact_digest, payload, valid, invalidation_reason
		FROM checkpoints WHERE query_id=? AND stage=? AND valid=1 AND input_fingerprint=? ORDER BY revision DESC LIMIT 1`,
		queryID, stage, inputFingerprint).
		Scan(&cp.QueryID, &cp.Stage, &cp.Sequence, &cp.Revision, &cp.InputFingerprint, &cp.ConfigVersion, &cp.ArtifactDigest, &payload, &valid, &cp.InvalidationReason)
	if errors.Is(err, sql.ErrNoRows) {
		return Checkpoint{}, false, nil
	}
	if err != nil {
		return Checkpoint{}, false, model.NewError(model.CodeInternal, "load checkpoint: "+err.Error(), queryID)
	}
	if err := s.verifyHead(ctx, cp); err != nil {
		return Checkpoint{}, false, err
	}
	computed := model.SHA256(payload)
	if computed != cp.ArtifactDigest {
		_ = s.invalidateTampered(context.Background(), queryID, stage, cp.Revision)
		return Checkpoint{}, false, model.NewErrorDetails(model.CodeStaleCheckpoint,
			"checkpoint payload does not match artifact digest", queryID,
			map[string]any{"classification": "tampered", "stage": stage, "revision": cp.Revision})
	}
	cp.Payload = payload
	cp.Valid = valid != 0
	return cp, true, nil
}

func (s *Store) ValidCheckpoint(ctx context.Context, queryID, stage string) (Checkpoint, bool, error) {
	var cp Checkpoint
	var payload []byte
	var valid int
	err := s.db.QueryRowContext(ctx, `SELECT query_id, stage, sequence, revision, input_fingerprint, config_version, artifact_digest, payload, valid, invalidation_reason
		FROM checkpoints WHERE query_id=? AND stage=? AND valid=1 ORDER BY revision DESC LIMIT 1`, queryID, stage).
		Scan(&cp.QueryID, &cp.Stage, &cp.Sequence, &cp.Revision, &cp.InputFingerprint, &cp.ConfigVersion, &cp.ArtifactDigest, &payload, &valid, &cp.InvalidationReason)
	if errors.Is(err, sql.ErrNoRows) {
		return Checkpoint{}, false, nil
	}
	if err != nil {
		return Checkpoint{}, false, model.NewError(model.CodeInternal, "load valid checkpoint: "+err.Error(), queryID)
	}
	if err := s.verifyHead(ctx, cp); err != nil {
		return Checkpoint{}, false, err
	}
	if model.SHA256(payload) != cp.ArtifactDigest {
		return Checkpoint{}, false, model.NewErrorDetails(model.CodeStaleCheckpoint,
			"checkpoint payload does not match artifact digest", queryID,
			map[string]any{"classification": "tampered", "stage": stage, "revision": cp.Revision})
	}
	cp.Payload = payload
	cp.Valid = valid != 0
	return cp, true, nil
}

func (s *Store) Publish(ctx context.Context, request PublishRequest) (Checkpoint, error) {
	if err := validatePublishRequest(request); err != nil {
		return Checkpoint{}, err
	}
	computedDigest := model.SHA256(request.Payload)
	if computedDigest != request.ArtifactDigest {
		return Checkpoint{}, staleError(request.QueryID, "tampered", request.Stage, 0,
			"artifact digest does not match payload", nil)
	}
	lock, err := AcquireFileLock(ctx, s.lockPath)
	if err != nil {
		return Checkpoint{}, model.NewError(model.CodeInternal, "acquire publish lock: "+err.Error(), request.QueryID)
	}
	defer func() { _ = lock.Release() }()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return Checkpoint{}, model.NewError(model.CodeInternal, "open sqlite connection: "+err.Error(), request.QueryID)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return Checkpoint{}, model.NewError(model.CodeInternal, "begin checkpoint transaction: "+err.Error(), request.QueryID)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	if err := validateStageChain(ctx, conn, request); err != nil {
		return Checkpoint{}, err
	}
	latest, found, err := latestRow(ctx, conn, request.QueryID, request.Stage)
	if err != nil {
		return Checkpoint{}, err
	}
	head, headFound, err := latestHead(ctx, conn, request.QueryID, request.Stage)
	if err != nil {
		return Checkpoint{}, err
	}
	if headFound {
		if !found {
			return Checkpoint{}, staleError(request.QueryID, "tampered", request.Stage, head.Revision,
				"checkpoint head exists without a backing row", nil)
		}
		if latest.Revision != head.Revision || latest.InputFingerprint != head.InputFingerprint || latest.ArtifactDigest != head.ArtifactDigest {
			return Checkpoint{}, staleError(request.QueryID, "tampered", request.Stage, head.Revision,
				"checkpoint row was replaced as a whole", map[string]any{"head_artifact_digest": head.ArtifactDigest})
		}
	} else if found {
		return Checkpoint{}, staleError(request.QueryID, "tampered", request.Stage, latest.Revision,
			"checkpoint row has no publish head", nil)
	}
	revision := request.Revision
	if found {
		switch {
		case revision < latest.Revision:
			classification := "rollback"
			if request.InputFingerprint == latest.InputFingerprint && request.ArtifactDigest == latest.ArtifactDigest {
				classification = "duplicate"
			}
			return Checkpoint{}, staleError(request.QueryID, classification, request.Stage, latest.Revision,
				"publish is behind the latest checkpoint revision", map[string]any{"requested_revision": revision})
		case revision == latest.Revision:
			if request.InputFingerprint == latest.InputFingerprint && request.ArtifactDigest == latest.ArtifactDigest {
				return Checkpoint{}, staleError(request.QueryID, "duplicate", request.Stage, latest.Revision,
					"checkpoint revision and digest were already published", nil)
			}
			return Checkpoint{}, staleError(request.QueryID, "tampered", request.Stage, latest.Revision,
				"checkpoint was replaced at the same revision", map[string]any{
					"latest_input_fingerprint": latest.InputFingerprint,
				})
		case revision > latest.Revision+1:
			return Checkpoint{}, staleError(request.QueryID, "rollback", request.Stage, latest.Revision,
				"checkpoint revision would skip the continuous sequence", map[string]any{"requested_revision": revision})
		}
	} else if revision != 1 {
		return Checkpoint{}, staleError(request.QueryID, "rollback", request.Stage, 0,
			"first checkpoint revision must be 1", map[string]any{"requested_revision": revision})
	}

	if _, err := conn.ExecContext(ctx, `UPDATE checkpoints SET valid=0, invalidation_reason=? WHERE query_id=? AND valid=1 AND stage IN (?,?,?)`,
		"superseded_by_"+request.Stage+"_revision_"+fmt.Sprintf("%d", revision), request.QueryID,
		request.Stage, model.NextStage(request.Stage), downstreamStage(request.Stage)); err != nil {
		return Checkpoint{}, model.NewError(model.CodeInternal, "invalidate checkpoints: "+err.Error(), request.QueryID)
	}
	if _, err := conn.ExecContext(ctx, `UPDATE checkpoint_heads SET valid=0 WHERE query_id=? AND stage IN (?,?)`,
		request.QueryID, model.NextStage(request.Stage), downstreamStage(request.Stage)); err != nil {
		return Checkpoint{}, model.NewError(model.CodeInternal, "invalidate checkpoint heads: "+err.Error(), request.QueryID)
	}
	sequence := model.StageSequence(request.Stage)
	_, err = conn.ExecContext(ctx, `INSERT INTO checkpoints(run_id, query_id, stage, sequence, revision, input_fingerprint, config_version, artifact_digest, payload, valid, invalidation_reason)
		VALUES(?,?,?,?,?,?,?,?,?,1,'')`,
		request.RunID, request.QueryID, request.Stage, sequence, revision, request.InputFingerprint,
		request.ConfigVersion, request.ArtifactDigest, request.Payload)
	if err != nil {
		return Checkpoint{}, model.NewError(model.CodeInternal, "insert checkpoint: "+err.Error(), request.QueryID)
	}
	headResult, err := conn.ExecContext(ctx, `INSERT INTO checkpoint_heads(query_id, stage, sequence, revision, input_fingerprint, artifact_digest, valid)
		VALUES(?,?,?,?,?,?,1)
		ON CONFLICT(query_id, stage) DO UPDATE SET sequence=excluded.sequence, revision=excluded.revision,
			input_fingerprint=excluded.input_fingerprint, artifact_digest=excluded.artifact_digest, valid=1
		WHERE checkpoint_heads.revision = ?`,
		request.QueryID, request.Stage, sequence, revision, request.InputFingerprint, request.ArtifactDigest, revision-1)
	if err != nil {
		return Checkpoint{}, model.NewError(model.CodeInternal, "update checkpoint head: "+err.Error(), request.QueryID)
	}
	affected, err := headResult.RowsAffected()
	if err != nil {
		return Checkpoint{}, model.NewError(model.CodeInternal, "read checkpoint head update result: "+err.Error(), request.QueryID)
	}
	if affected != 1 {
		return Checkpoint{}, staleError(request.QueryID, "rollback", request.Stage, revision-1,
			"conditional checkpoint head update lost a concurrent revision race", map[string]any{"requested_revision": revision})
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return Checkpoint{}, model.NewError(model.CodeInternal, "commit checkpoint transaction: "+err.Error(), request.QueryID)
	}
	committed = true
	return Checkpoint{
		QueryID: request.QueryID, Stage: request.Stage, Sequence: sequence, Revision: revision,
		InputFingerprint: request.InputFingerprint, ConfigVersion: request.ConfigVersion,
		ArtifactDigest: request.ArtifactDigest, Payload: request.Payload, Valid: true,
	}, nil
}

func (s *Store) invalidateTampered(ctx context.Context, queryID, stage string, revision int) error {
	lock, err := AcquireFileLock(ctx, s.lockPath)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Release() }()
	_, err = s.db.ExecContext(ctx, `UPDATE checkpoints SET valid=0, invalidation_reason='payload_digest_mismatch' WHERE query_id=? AND stage=? AND revision=? AND valid=1`, queryID, stage, revision)
	return err
}

type latestCheckpoint struct {
	Sequence         int
	Revision         int
	InputFingerprint string
	ArtifactDigest   string
	Valid            bool
}

func latestHead(ctx context.Context, conn *sql.Conn, queryID, stage string) (latestCheckpoint, bool, error) {
	var row latestCheckpoint
	var valid int
	err := conn.QueryRowContext(ctx, `SELECT sequence, revision, input_fingerprint, artifact_digest, valid FROM checkpoint_heads WHERE query_id=? AND stage=?`, queryID, stage).
		Scan(&row.Sequence, &row.Revision, &row.InputFingerprint, &row.ArtifactDigest, &valid)
	if errors.Is(err, sql.ErrNoRows) {
		return latestCheckpoint{}, false, nil
	}
	if err != nil {
		return latestCheckpoint{}, false, model.NewError(model.CodeInternal, "read checkpoint head: "+err.Error(), queryID)
	}
	row.Valid = valid != 0
	return row, true, nil
}

func (s *Store) verifyHead(ctx context.Context, cp Checkpoint) error {
	var sequence, revision, valid int
	var inputFingerprint, artifactDigest string
	err := s.db.QueryRowContext(ctx, `SELECT sequence, revision, input_fingerprint, artifact_digest, valid FROM checkpoint_heads WHERE query_id=? AND stage=?`, cp.QueryID, cp.Stage).
		Scan(&sequence, &revision, &inputFingerprint, &artifactDigest, &valid)
	if errors.Is(err, sql.ErrNoRows) {
		return model.NewErrorDetails(model.CodeStaleCheckpoint, "checkpoint head is missing", cp.QueryID,
			map[string]any{"classification": "tampered", "stage": cp.Stage, "revision": cp.Revision})
	}
	if err != nil {
		return model.NewError(model.CodeInternal, "read checkpoint head: "+err.Error(), cp.QueryID)
	}
	if valid == 0 || sequence != cp.Sequence || revision != cp.Revision || inputFingerprint != cp.InputFingerprint || artifactDigest != cp.ArtifactDigest {
		return model.NewErrorDetails(model.CodeStaleCheckpoint, "checkpoint row and publish head disagree", cp.QueryID,
			map[string]any{"classification": "tampered", "stage": cp.Stage, "revision": cp.Revision, "head_revision": revision})
	}
	return nil
}

func latestRow(ctx context.Context, conn *sql.Conn, queryID, stage string) (latestCheckpoint, bool, error) {
	var row latestCheckpoint
	var valid int
	err := conn.QueryRowContext(ctx, `SELECT sequence, revision, input_fingerprint, artifact_digest, valid FROM checkpoints WHERE query_id=? AND stage=? ORDER BY revision DESC LIMIT 1`, queryID, stage).
		Scan(&row.Sequence, &row.Revision, &row.InputFingerprint, &row.ArtifactDigest, &valid)
	if errors.Is(err, sql.ErrNoRows) {
		return latestCheckpoint{}, false, nil
	}
	if err != nil {
		return latestCheckpoint{}, false, model.NewError(model.CodeInternal, "read latest checkpoint: "+err.Error(), queryID)
	}
	row.Valid = valid != 0
	return row, true, nil
}

func validateStageChain(ctx context.Context, conn *sql.Conn, request PublishRequest) error {
	expectedSequence := model.StageSequence(request.Stage)
	if expectedSequence == 0 {
		return model.NewError(model.CodeIllegalStageTransition, "unknown stage: "+request.Stage, request.QueryID)
	}
	if request.Sequence != 0 && request.Sequence != expectedSequence {
		return model.NewErrorDetails(model.CodeIllegalStageTransition, "checkpoint stage sequence does not match its stage", request.QueryID,
			map[string]any{"stage": request.Stage, "requested_sequence": request.Sequence, "expected_sequence": expectedSequence})
	}
	if request.Stage != model.StageRecall {
		upstreamStage := upstreamStage(request.Stage)
		var upstreamDigest string
		var upstreamSequence int
		err := conn.QueryRowContext(ctx, `SELECT artifact_digest, sequence FROM checkpoints WHERE query_id=? AND stage=? AND valid=1 ORDER BY revision DESC LIMIT 1`, request.QueryID, upstreamStage).
			Scan(&upstreamDigest, &upstreamSequence)
		if errors.Is(err, sql.ErrNoRows) {
			return model.NewErrorDetails(model.CodeMissingPrerequisite, "missing valid upstream stage: "+upstreamStage, request.QueryID,
				map[string]any{"stage": request.Stage, "prerequisite_stage": upstreamStage})
		}
		if err != nil {
			return model.NewError(model.CodeInternal, "read prerequisite checkpoint: "+err.Error(), request.QueryID)
		}
		if upstreamSequence != expectedSequence-1 {
			return model.NewErrorDetails(model.CodeIllegalStageTransition, "upstream stage sequence is not continuous", request.QueryID,
				map[string]any{"stage": request.Stage, "upstream_sequence": upstreamSequence})
		}
		if request.PrerequisiteDigest != upstreamDigest {
			return staleError(request.QueryID, "cross_query_misalignment", request.Stage, 0,
				"publish prerequisite digest does not match the valid upstream artifact", map[string]any{"prerequisite_stage": upstreamStage})
		}
	}
	return nil
}

func validatePublishRequest(request PublishRequest) error {
	if request.QueryID == "" || request.RunID == "" {
		return model.NewError(model.CodeInvalidConfig, "run_id and query_id are required", request.QueryID)
	}
	if model.StageSequence(request.Stage) == 0 {
		return model.NewError(model.CodeIllegalStageTransition, "unknown checkpoint stage", request.QueryID)
	}
	if request.Revision <= 0 || request.InputFingerprint == "" || request.ConfigVersion == "" || request.ArtifactDigest == "" || len(request.Payload) == 0 {
		return model.NewError(model.CodeInvalidConfig, "checkpoint publish fields must be complete", request.QueryID)
	}
	return nil
}

func staleError(queryID, classification, stage string, latestRevision int, message string, extra map[string]any) *model.AppError {
	details := map[string]any{"classification": classification, "stage": stage, "latest_revision": latestRevision}
	for key, value := range extra {
		details[key] = value
	}
	return model.NewErrorDetails(model.CodeStaleCheckpoint, message, queryID, details)
}

func upstreamStage(stage string) string {
	switch stage {
	case model.StageFusion:
		return model.StageRecall
	case model.StageEvaluation:
		return model.StageFusion
	default:
		return ""
	}
}

func downstreamStage(stage string) string {
	switch stage {
	case model.StageRecall:
		return model.StageEvaluation
	case model.StageFusion:
		return model.StageEvaluation
	default:
		return ""
	}
}

func (s *Store) SaveFailure(ctx context.Context, failure FailureRecord) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO failures(run_id, shard, query_id, code, message) VALUES(?,?,?,?,?)`,
		failure.RunID, failure.Shard, failure.QueryID, failure.Code, failure.Message)
	if err != nil {
		return model.NewError(model.CodeInternal, "save failure: "+err.Error(), failure.QueryID)
	}
	return nil
}

func (s *Store) ListCheckpoints(ctx context.Context, queryID string) ([]Checkpoint, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT query_id, stage, sequence, revision, input_fingerprint, config_version, artifact_digest, payload, valid, invalidation_reason FROM checkpoints WHERE query_id=? ORDER BY revision, stage`, queryID)
	if err != nil {
		return nil, model.NewError(model.CodeInternal, "list checkpoints: "+err.Error(), queryID)
	}
	defer rows.Close()
	out := make([]Checkpoint, 0)
	for rows.Next() {
		var cp Checkpoint
		var payload []byte
		var valid int
		if err := rows.Scan(&cp.QueryID, &cp.Stage, &cp.Sequence, &cp.Revision, &cp.InputFingerprint, &cp.ConfigVersion, &cp.ArtifactDigest, &payload, &valid, &cp.InvalidationReason); err != nil {
			return nil, model.NewError(model.CodeInternal, "scan checkpoint: "+err.Error(), queryID)
		}
		cp.Payload = payload
		cp.Valid = valid != 0
		out = append(out, cp)
	}
	if err := rows.Err(); err != nil {
		return nil, model.NewError(model.CodeInternal, "iterate checkpoints: "+err.Error(), queryID)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].QueryID != out[j].QueryID {
			return out[i].QueryID < out[j].QueryID
		}
		if out[i].Sequence != out[j].Sequence {
			return out[i].Sequence < out[j].Sequence
		}
		return out[i].Revision < out[j].Revision
	})
	return out, nil
}

func (s *Store) CountValidCheckpoints(ctx context.Context) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoints WHERE valid=1`).Scan(&count); err != nil {
		return 0, model.NewError(model.CodeInternal, "count checkpoints: "+err.Error(), "")
	}
	return count, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
