// Package store persists four-stage shard checkpoints in SQLite. Every
// publish, reconciliation, and invalidation is serialized by both an OS file
// lock and an immediate SQLite transaction.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"local/quantops/internal/filelock"
	"local/quantops/pkg/canon"
	"local/quantops/pkg/qerr"

	_ "modernc.org/sqlite"
)

const (
	SchemaVersion      = "quantops.sqlite.v1"
	guardSchemaVersion = "quantops.guard.v1"
)

type Stage string

const (
	Calibration    Stage = "calibration"
	Forward        Stage = "forward"
	Dequantization Stage = "dequantization"
	Verification   Stage = "verification"
)

var stageOrder = []Stage{Calibration, Forward, Dequantization, Verification}

func Stages() []Stage { return append([]Stage(nil), stageOrder...) }

func (s Stage) Seq() int {
	for i, stage := range stageOrder {
		if s == stage {
			return i + 1
		}
	}
	return 0
}

func StageAt(seq int) (Stage, bool) {
	if seq < 1 || seq > len(stageOrder) {
		return "", false
	}
	return stageOrder[seq-1], true
}

func NextStage(s Stage) (Stage, bool) {
	seq := s.Seq()
	if seq == 0 || seq == len(stageOrder) {
		return "", false
	}
	return stageOrder[seq], true
}

type Progress struct {
	ShardID            string `json:"shard_id"`
	ShardOrdinal       int    `json:"shard_ordinal"`
	NextStageSeq       int    `json:"next_stage_seq"`
	VersionFingerprint string `json:"version_fingerprint"`
	GlobalSequence     int64  `json:"global_sequence"`
}

type State struct {
	ShardID          string `json:"shard_id"`
	ShardOrdinal     int    `json:"shard_ordinal"`
	Stage            Stage  `json:"stage"`
	StageSeq         int    `json:"stage_seq"`
	GlobalSequence   int64  `json:"global_sequence"`
	ConfigVersion    string `json:"config_version"`
	InputFingerprint string `json:"input_fingerprint"`
	StageDigest      string `json:"stage_digest"`
	Payload          []byte `json:"-"`
	PayloadDigest    string `json:"payload_digest"`
}

type PublishRequest struct {
	ShardID          string
	ShardOrdinal     int
	Stage            Stage
	StageSeq         int
	ExpectedVersion  string
	ConfigVersion    string
	InputFingerprint string
	Payload          []byte
}

type Published struct {
	State    State    `json:"state"`
	Progress Progress `json:"progress"`
}

type Classification string

const (
	ClassNormal      Classification = "normal"
	ClassReplacement Classification = "replacement"
	ClassRollback    Classification = "rollback"
)

type ExpectedStage struct {
	Stage         Stage  `json:"stage"`
	StageSeq      int    `json:"stage_seq"`
	ConfigVersion string `json:"config_version"`
}

type ReconcileRequest struct {
	ShardID              string          `json:"shard_id"`
	ShardOrdinal         int             `json:"shard_ordinal"`
	BaseInputFingerprint string          `json:"base_input_fingerprint"`
	Expected             []ExpectedStage `json:"expected"`
}

type ReconcileResult struct {
	Classification  Classification `json:"classification"`
	InvalidatedFrom int            `json:"invalidated_from,omitempty"`
	Reason          string         `json:"reason,omitempty"`
	Progress        *Progress      `json:"progress,omitempty"`
}

type BudgetRejection struct {
	ShardID           string    `json:"shard_id"`
	ShardOrdinal      int       `json:"shard_ordinal"`
	EstimatedElements int64     `json:"estimated_elements"`
	WorkingSetBytes   int64     `json:"working_set_bytes"`
	BudgetRemaining   int64     `json:"budget_remaining"`
	MemoryRemaining   int64     `json:"memory_remaining"`
	Reason            string    `json:"reason"`
	Code              qerr.Code `json:"code"`
}

type Store struct {
	db        *sql.DB
	path      string
	lockPath  string
	guardPath string
	mu        sync.Mutex
}

// guardFile is a small sidecar high-water mark. It detects replacement of the
// whole SQLite database or rolling a shard's progress back to an older
// snapshot, cases that are invisible to tables that were replaced together.
type guardFile struct {
	SchemaVersion     string           `json:"schema_version"`
	DatabaseID        string           `json:"database_id"`
	MaxGlobalSequence int64            `json:"max_global_sequence"`
	ShardSequences    map[string]int64 `json:"shard_sequences"`
}

func Open(path string) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, err
	}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(10000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Set("_txlock", "immediate")
	// A Windows absolute path such as C:/x must become /C:/x in a file URI;
	// otherwise net/url treats "C:" as a host.
	dsnPath := filepath.ToSlash(abs)
	if !strings.HasPrefix(dsnPath, "/") {
		dsnPath = "/" + dsnPath
	}
	dsn := (&url.URL{Scheme: "file", Path: dsnPath, RawQuery: q.Encode()}).String()
	lockPath := abs + ".lock"
	// Schema creation changes SQLite's journal mode and creates tables. Hold
	// the same OS lock used for stage publication so two fresh worker
	// processes cannot race initialization.
	initLock, err := filelock.Acquire(lockPath)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		_ = initLock.Close()
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	s := &Store{db: db, path: abs, lockPath: lockPath, guardPath: abs + ".guard"}
	initErr := s.init(context.Background())
	if initErr == nil {
		initErr = s.ensureGuardLocked(context.Background(), 0, "")
	}
	closeErr := initLock.Close()
	if initErr != nil {
		_ = db.Close()
		return nil, initErr
	}
	if closeErr != nil {
		_ = db.Close()
		return nil, closeErr
	}
	return s, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Path() string { return s.path }

func (s *Store) init(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS quantops_meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS sequence_counter (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			value INTEGER NOT NULL
		)`,
		`INSERT OR IGNORE INTO sequence_counter(id, value) VALUES (1, 0)`,
		`CREATE TABLE IF NOT EXISTS shard_progress (
			shard_id TEXT PRIMARY KEY,
			shard_ordinal INTEGER NOT NULL,
			next_stage_seq INTEGER NOT NULL,
			version_fingerprint TEXT NOT NULL,
			global_sequence INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS stage_states (
			shard_id TEXT NOT NULL,
			shard_ordinal INTEGER NOT NULL,
			stage TEXT NOT NULL,
			stage_seq INTEGER NOT NULL,
			global_sequence INTEGER NOT NULL UNIQUE,
			config_version TEXT NOT NULL,
			input_fingerprint TEXT NOT NULL,
			stage_digest TEXT NOT NULL,
			payload BLOB NOT NULL,
			PRIMARY KEY (shard_id, stage),
			UNIQUE (shard_id, stage_seq)
		)`,
		`CREATE TABLE IF NOT EXISTS invalidations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			shard_id TEXT NOT NULL,
			from_stage_seq INTEGER NOT NULL,
			reason TEXT NOT NULL,
			prior_version TEXT NOT NULL,
			new_version TEXT NOT NULL,
			global_sequence INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS budget_rejections (
			shard_id TEXT PRIMARY KEY,
			shard_ordinal INTEGER NOT NULL,
			estimated_elements INTEGER NOT NULL,
			working_set_bytes INTEGER NOT NULL,
			budget_remaining INTEGER NOT NULL,
			memory_remaining INTEGER NOT NULL,
			reason TEXT NOT NULL,
			code TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("initialize schema: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO quantops_meta(key,value) VALUES('schema_version',?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, SchemaVersion); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO quantops_meta(key,value) VALUES('manifest_fingerprint','')`); err != nil {
		return err
	}
	var dbID string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM quantops_meta WHERE key='database_id'`).Scan(&dbID)
	if err == sql.ErrNoRows {
		var raw [16]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return err
		}
		dbID = hex.EncodeToString(raw[:])
		if _, err := s.db.ExecContext(ctx, `INSERT INTO quantops_meta(key,value) VALUES('database_id',?)`, dbID); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}

func (s *Store) databaseID(ctx context.Context) (string, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM quantops_meta WHERE key='database_id'`).Scan(&id)
	if err == sql.ErrNoRows || id == "" {
		return "", qerr.New(qerr.CodeStaleCheckpoint, "", "", "", "database identity metadata is missing", nil)
	}
	return id, err
}

func (s *Store) readGuard() (*guardFile, error) {
	b, err := os.ReadFile(s.guardPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var g guardFile
	if err := json.Unmarshal(b, &g); err != nil {
		return nil, qerr.New(qerr.CodeStaleCheckpoint, "", "", "", "checkpoint guard is corrupt", err)
	}
	if g.SchemaVersion != guardSchemaVersion || g.DatabaseID == "" {
		return nil, qerr.New(qerr.CodeStaleCheckpoint, "", "", "", "checkpoint guard schema mismatch", nil)
	}
	if g.ShardSequences == nil {
		g.ShardSequences = map[string]int64{}
	}
	return &g, nil
}

func (s *Store) writeGuard(g *guardFile) error {
	if g.ShardSequences == nil {
		g.ShardSequences = map[string]int64{}
	}
	b, err := json.MarshalIndent(g, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return canon.WriteFileAtomic(s.guardPath, b, 0o600)
}

// ensureGuardLocked validates or creates the sidecar while the caller holds
// the OS checkpoint lock. explicitlyAdvancedShard/globalSeq are used after a
// committed change when the guard must move forward atomically.
func (s *Store) ensureGuardLocked(ctx context.Context, globalSeq int64, shardID string) error {
	dbID, err := s.databaseID(ctx)
	if err != nil {
		return err
	}
	g, err := s.readGuard()
	if err != nil {
		return err
	}
	var counter int64
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM sequence_counter WHERE id=1`).Scan(&counter); err != nil {
		return err
	}
	if g == nil {
		g = &guardFile{SchemaVersion: guardSchemaVersion, DatabaseID: dbID, ShardSequences: map[string]int64{}}
		progress, err := s.ListProgress(ctx)
		if err != nil {
			return err
		}
		for _, p := range progress {
			g.ShardSequences[p.ShardID] = p.GlobalSequence
			if p.GlobalSequence > g.MaxGlobalSequence {
				g.MaxGlobalSequence = p.GlobalSequence
			}
		}
		if counter > g.MaxGlobalSequence {
			g.MaxGlobalSequence = counter
		}
		return s.writeGuard(g)
	}
	if g.DatabaseID != dbID {
		return qerr.New(qerr.CodeStaleCheckpoint, shardID, "", "",
			"database identity differs from checkpoint guard (whole database replacement)", nil).
			WithDetail(map[string]any{"classification": ClassRollback})
	}
	if counter < g.MaxGlobalSequence {
		return qerr.New(qerr.CodeStaleCheckpoint, shardID, "", "",
			"global sequence moved backwards relative to checkpoint guard", nil).
			WithDetail(map[string]any{"classification": ClassRollback, "guard_sequence": g.MaxGlobalSequence, "database_sequence": counter})
	}
	changed := false
	if counter > g.MaxGlobalSequence {
		// A process can be killed after the SQLite commit but before the
		// sidecar write. The DB is authoritative and ahead of the guard.
		g.MaxGlobalSequence = counter
		changed = true
	}
	if globalSeq > g.MaxGlobalSequence {
		g.MaxGlobalSequence = globalSeq
		changed = true
	}
	if shardID != "" {
		if globalSeq > g.ShardSequences[shardID] {
			g.ShardSequences[shardID] = globalSeq
			changed = true
		}
	}
	if changed {
		return s.writeGuard(g)
	}
	return nil
}

func (s *Store) SetManifestFingerprint(ctx context.Context, fp string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO quantops_meta(key,value) VALUES('manifest_fingerprint',?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, fp)
	return err
}

func (s *Store) ManifestFingerprint(ctx context.Context) (string, error) {
	var fp string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM quantops_meta WHERE key='manifest_fingerprint'`).Scan(&fp)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return fp, err
}

func (s *Store) Progress(ctx context.Context, shardID string) (*Progress, error) {
	row := s.db.QueryRowContext(ctx, `SELECT shard_id, shard_ordinal, next_stage_seq, version_fingerprint, global_sequence
		FROM shard_progress WHERE shard_id=?`, shardID)
	return scanProgress(row)
}

func (s *Store) ListProgress(ctx context.Context) ([]Progress, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT shard_id, shard_ordinal, next_stage_seq, version_fingerprint, global_sequence
		FROM shard_progress ORDER BY shard_ordinal`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Progress
	for rows.Next() {
		p, err := scanProgress(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanProgress(row rowScanner) (*Progress, error) {
	var p Progress
	err := row.Scan(&p.ShardID, &p.ShardOrdinal, &p.NextStageSeq, &p.VersionFingerprint, &p.GlobalSequence)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Store) States(ctx context.Context, shardID string) ([]State, error) {
	return statesQuery(ctx, s.db, shardID)
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func statesQuery(ctx context.Context, q queryer, shardID string) ([]State, error) {
	rows, err := q.QueryContext(ctx, `SELECT shard_id, shard_ordinal, stage, stage_seq, global_sequence,
		config_version, input_fingerprint, stage_digest, payload
		FROM stage_states WHERE shard_id=? ORDER BY stage_seq`, shardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []State
	for rows.Next() {
		var st State
		if err := rows.Scan(&st.ShardID, &st.ShardOrdinal, &st.Stage, &st.StageSeq, &st.GlobalSequence,
			&st.ConfigVersion, &st.InputFingerprint, &st.StageDigest, &st.Payload); err != nil {
			return nil, err
		}
		st.PayloadDigest = canon.HashBytes(st.Payload)
		out = append(out, st)
	}
	return out, rows.Err()
}

func (s *Store) State(ctx context.Context, shardID string, stage Stage) (*State, error) {
	var st State
	err := s.db.QueryRowContext(ctx, `SELECT shard_id, shard_ordinal, stage, stage_seq, global_sequence,
		config_version, input_fingerprint, stage_digest, payload
		FROM stage_states WHERE shard_id=? AND stage=?`, shardID, stage).
		Scan(&st.ShardID, &st.ShardOrdinal, &st.Stage, &st.StageSeq, &st.GlobalSequence,
			&st.ConfigVersion, &st.InputFingerprint, &st.StageDigest, &st.Payload)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	st.PayloadDigest = canon.HashBytes(st.Payload)
	return &st, nil
}

func (s *Store) Publish(ctx context.Context, req PublishRequest) (*Published, error) {
	if err := validatePublishRequest(req); err != nil {
		return nil, err
	}
	digest := canon.HashBytes(req.Payload)
	s.mu.Lock()
	defer s.mu.Unlock()

	lock, err := filelock.Acquire(s.lockPath)
	if err != nil {
		return nil, qerr.New(qerr.CodeInternal, req.ShardID, string(req.Stage), "", "acquire checkpoint file lock", err)
	}
	defer lock.Close()
	if err := s.ensureGuardLocked(ctx, 0, ""); err != nil {
		return nil, err
	}
	guard, err := s.readGuard()
	if err != nil {
		return nil, err
	}
	if guard != nil {
		if gseq, ok := guard.ShardSequences[req.ShardID]; ok {
			if p, err := s.Progress(ctx, req.ShardID); err == nil && (p == nil || p.GlobalSequence < gseq) {
				return nil, qerr.New(qerr.CodeStaleCheckpoint, req.ShardID, "", "",
					"shard progress moved backwards relative to checkpoint guard", nil).
					WithDetail(map[string]any{"classification": ClassRollback, "guard_shard_sequence": gseq})
			}
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	progress, err := progressTx(ctx, tx, req.ShardID)
	if err != nil {
		return nil, err
	}
	existingStates, err := statesQuery(ctx, tx, req.ShardID)
	if err != nil {
		return nil, err
	}
	currentStage := currentStageText(progress, existingStates)
	if progress == nil {
		if req.StageSeq > 1 {
			return nil, business(qerr.CodeMissingArtifact, req.ShardID, currentStage, req.Stage,
				"publish requires committed predecessor artifacts", map[string]any{
					"requested_stage_seq": req.StageSeq,
					"classification":      ClassRollback,
				})
		}
		if req.Stage != Calibration {
			return nil, business(qerr.CodeInvalidTransition, req.ShardID, currentStage, req.Stage,
				"first stage must be calibration", map[string]any{"requested_stage": req.Stage})
		}
	} else {
		if progress.ShardOrdinal != req.ShardOrdinal {
			return nil, business(qerr.CodeStaleCheckpoint, req.ShardID, currentStage, req.Stage,
				"shard ordinal does not match checkpoint (cross-shard publish)", map[string]any{
					"checkpoint_shard_ordinal": progress.ShardOrdinal,
					"request_shard_ordinal":    req.ShardOrdinal,
					"classification":           ClassRollback,
				})
		}
		if req.StageSeq < progress.NextStageSeq {
			return nil, business(qerr.CodeStaleCheckpoint, req.ShardID, currentStage, req.Stage,
				"stage sequence is behind the committed checkpoint", map[string]any{
					"checkpoint_next_stage_seq": progress.NextStageSeq,
					"request_stage_seq":         req.StageSeq,
					"classification":            ClassRollback,
				})
		}
		if req.StageSeq > progress.NextStageSeq {
			return nil, business(qerr.CodeStaleCheckpoint, req.ShardID, currentStage, req.Stage,
				"stage sequence is ahead of the committed checkpoint", map[string]any{
					"checkpoint_next_stage_seq": progress.NextStageSeq,
					"request_stage_seq":         req.StageSeq,
					"classification":            ClassReplacement,
				})
		}
		expected, _ := StageAt(req.StageSeq)
		if req.Stage != expected {
			code := qerr.CodeInvalidTransition
			if req.Stage.Seq() < req.StageSeq {
				code = qerr.CodeStaleCheckpoint
			}
			return nil, business(code, req.ShardID, currentStage, req.Stage,
				"stage does not match the next stage in the shard sequence", map[string]any{
					"expected_stage":  expected,
					"requested_stage": req.Stage,
				})
		}
		if req.StageSeq > 1 {
			prior, err := stateTx(ctx, tx, req.ShardID, StageAtMust(req.StageSeq-1))
			if err != nil {
				return nil, err
			}
			if prior == nil {
				return nil, business(qerr.CodeMissingArtifact, req.ShardID, currentStage, req.Stage,
					"predecessor stage artifact is missing", map[string]any{"predecessor_stage_seq": req.StageSeq - 1})
			}
			if req.InputFingerprint != prior.StageDigest {
				return nil, business(qerr.CodeStaleCheckpoint, req.ShardID, currentStage, req.Stage,
					"input fingerprint does not match predecessor stage digest", map[string]any{
						"expected_input_fingerprint": prior.StageDigest,
						"request_input_fingerprint":  req.InputFingerprint,
						"classification":             ClassReplacement,
					})
			}
		}
		if req.ExpectedVersion != "" && req.ExpectedVersion != progress.VersionFingerprint {
			return nil, business(qerr.CodeStaleCheckpoint, req.ShardID, currentStage, req.Stage,
				"expected version fingerprint is stale", map[string]any{
					"checkpoint_version": progress.VersionFingerprint,
					"expected_version":   req.ExpectedVersion,
					"classification":     ClassRollback,
				})
		}
		if req.ExpectedVersion == "" && progress.VersionFingerprint != "" {
			return nil, business(qerr.CodeStaleCheckpoint, req.ShardID, currentStage, req.Stage,
				"expected version fingerprint is missing for an existing checkpoint", map[string]any{
					"checkpoint_version": progress.VersionFingerprint,
					"classification":     ClassRollback,
				})
		}
	}

	seq, err := nextSequenceTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	ordinal := req.ShardOrdinal
	_, err = tx.ExecContext(ctx, `INSERT INTO stage_states
		(shard_id, shard_ordinal, stage, stage_seq, global_sequence, config_version, input_fingerprint, stage_digest, payload)
		VALUES(?,?,?,?,?,?,?,?,?)`,
		req.ShardID, ordinal, req.Stage, req.StageSeq, seq, req.ConfigVersion, req.InputFingerprint, digest, req.Payload)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return nil, business(qerr.CodeStaleCheckpoint, req.ShardID, currentStage, req.Stage,
				"duplicate stage publication rejected", map[string]any{"classification": ClassRollback})
		}
		return nil, err
	}
	newState := State{
		ShardID: req.ShardID, ShardOrdinal: ordinal, Stage: req.Stage, StageSeq: req.StageSeq,
		GlobalSequence: seq, ConfigVersion: req.ConfigVersion, InputFingerprint: req.InputFingerprint,
		StageDigest: digest, Payload: req.Payload, PayloadDigest: digest,
	}
	allStates := append(existingStates, newState)
	version := chainVersion(allStates)
	_, err = tx.ExecContext(ctx, `INSERT INTO shard_progress
		(shard_id, shard_ordinal, next_stage_seq, version_fingerprint, global_sequence)
		VALUES(?,?,?,?,?)
		ON CONFLICT(shard_id) DO UPDATE SET
			shard_ordinal=excluded.shard_ordinal,
			next_stage_seq=excluded.next_stage_seq,
			version_fingerprint=excluded.version_fingerprint,
			global_sequence=excluded.global_sequence`,
		req.ShardID, ordinal, req.StageSeq+1, version, seq)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if err := s.ensureGuardLocked(ctx, seq, req.ShardID); err != nil {
		return nil, err
	}
	p := Progress{ShardID: req.ShardID, ShardOrdinal: ordinal, NextStageSeq: req.StageSeq + 1,
		VersionFingerprint: version, GlobalSequence: seq}
	return &Published{State: newState, Progress: p}, nil
}

func progressTx(ctx context.Context, tx *sql.Tx, shardID string) (*Progress, error) {
	row := tx.QueryRowContext(ctx, `SELECT shard_id, shard_ordinal, next_stage_seq, version_fingerprint, global_sequence
		FROM shard_progress WHERE shard_id=?`, shardID)
	return scanProgress(row)
}

func stateTx(ctx context.Context, tx *sql.Tx, shardID string, stage Stage) (*State, error) {
	var st State
	err := tx.QueryRowContext(ctx, `SELECT shard_id, shard_ordinal, stage, stage_seq, global_sequence,
		config_version, input_fingerprint, stage_digest, payload
		FROM stage_states WHERE shard_id=? AND stage=?`, shardID, stage).
		Scan(&st.ShardID, &st.ShardOrdinal, &st.Stage, &st.StageSeq, &st.GlobalSequence,
			&st.ConfigVersion, &st.InputFingerprint, &st.StageDigest, &st.Payload)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	st.PayloadDigest = canon.HashBytes(st.Payload)
	return &st, nil
}

func nextSequenceTx(ctx context.Context, tx *sql.Tx) (int64, error) {
	var seq int64
	err := tx.QueryRowContext(ctx, `UPDATE sequence_counter SET value=value+1 WHERE id=1 RETURNING value`).Scan(&seq)
	return seq, err
}

func (s *Store) Reconcile(ctx context.Context, req ReconcileRequest) (*ReconcileResult, error) {
	if req.ShardID == "" || req.ShardOrdinal <= 0 || len(req.Expected) != len(stageOrder) {
		return nil, qerr.New(qerr.CodeInvalidInput, req.ShardID, "", "", "invalid reconcile request", nil)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	lock, err := filelock.Acquire(s.lockPath)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	if err := s.ensureGuardLocked(ctx, 0, ""); err != nil {
		return nil, err
	}
	guard, err := s.readGuard()
	if err != nil {
		return nil, err
	}
	if guard != nil {
		if gseq, ok := guard.ShardSequences[req.ShardID]; ok {
			if p, err := s.Progress(ctx, req.ShardID); err == nil && (p == nil || p.GlobalSequence < gseq) {
				return nil, qerr.New(qerr.CodeStaleCheckpoint, req.ShardID, "", "",
					"shard progress moved backwards relative to checkpoint guard", nil).
					WithDetail(map[string]any{"classification": ClassRollback, "guard_shard_sequence": gseq})
			}
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	progress, err := progressTx(ctx, tx, req.ShardID)
	if err != nil {
		return nil, err
	}
	states, err := statesQuery(ctx, tx, req.ShardID)
	if err != nil {
		return nil, err
	}
	if progress == nil {
		if len(states) > 0 {
			return nil, staleRollback(req.ShardID, "", "stage states exist without progress (checkpoint rollback)")
		}
		return &ReconcileResult{Classification: ClassNormal}, nil
	}
	if progress.ShardOrdinal != req.ShardOrdinal {
		return nil, staleRollback(req.ShardID, "", "progress row belongs to a different shard ordinal")
	}
	var counter int64
	if err := tx.QueryRowContext(ctx, `SELECT value FROM sequence_counter WHERE id=1`).Scan(&counter); err != nil {
		return nil, err
	}
	if counter < progress.GlobalSequence {
		return nil, staleRollback(req.ShardID, "", "global sequence counter moved backwards")
	}
	for i, st := range states {
		if st.GlobalSequence > progress.GlobalSequence || st.GlobalSequence > counter {
			return nil, staleRollback(req.ShardID, string(st.Stage), "state global sequence is ahead of progress")
		}
		if st.StageSeq != i+1 {
			return nil, staleRollback(req.ShardID, string(st.Stage), "stage sequence has a gap or duplicate")
		}
		if i > 0 && st.GlobalSequence <= states[i-1].GlobalSequence {
			return nil, staleRollback(req.ShardID, string(st.Stage), "state global sequence is not strictly monotonic")
		}
	}
	if progress.NextStageSeq < len(states)+1 {
		return nil, staleRollback(req.ShardID, currentStageText(progress, states), "progress next sequence moved backwards")
	}

	replaceFrom := 0
	reason := ""
	markReplace := func(seq int, why string) {
		if seq <= 0 {
			return
		}
		if replaceFrom == 0 || seq < replaceFrom {
			replaceFrom = seq
		}
		if reason == "" {
			reason = why
		}
	}
	for i, st := range states {
		seq := i + 1
		exp := req.Expected[i]
		if st.Stage != exp.Stage || st.StageSeq != exp.StageSeq {
			markReplace(seq, "stage identity does not match manifest sequence")
			continue
		}
		if st.ShardOrdinal != req.ShardOrdinal {
			markReplace(seq, "state row shard ordinal mismatch")
			continue
		}
		if st.PayloadDigest != st.StageDigest {
			markReplace(seq, "stage payload digest mismatch")
			continue
		}
		expectedInput := req.BaseInputFingerprint
		if seq > 1 {
			expectedInput = states[i-1].StageDigest
		}
		if st.InputFingerprint != expectedInput {
			markReplace(seq, "stage input fingerprint mismatch")
			continue
		}
		if st.ConfigVersion != exp.ConfigVersion {
			markReplace(seq, "stage configuration version mismatch")
		}
	}
	if replaceFrom == 0 && progress.NextStageSeq > len(states)+1 {
		replaceFrom = len(states) + 1
		reason = "progress is ahead of committed stage records"
	}
	if replaceFrom == 0 {
		want := chainVersion(states)
		if progress.VersionFingerprint != want {
			// The stage rows are self-consistent but the progress snapshot is
			// not. This is a replacement of the state/progress table pair;
			// rebuilding from the earliest stage is conservative and safe.
			replaceFrom = 1
			reason = "progress version fingerprint mismatch"
		}
	}
	if replaceFrom > 0 {
		result, err := invalidateTx(ctx, tx, progress.ShardID, replaceFrom, reason, req.Expected)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		if result != nil && result.Progress != nil {
			if err := s.ensureGuardLocked(ctx, result.Progress.GlobalSequence, req.ShardID); err != nil {
				return nil, err
			}
		}
		result.Classification = ClassReplacement
		return result, nil
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if err := s.ensureGuardLocked(ctx, progress.GlobalSequence, req.ShardID); err != nil {
		return nil, err
	}
	return &ReconcileResult{Classification: ClassNormal, Progress: progress}, nil
}

// InvalidateFrom explicitly deletes a stage and all downstream stages. It is
// used by version propagation and by tests that simulate snapshot swaps.
func (s *Store) InvalidateFrom(ctx context.Context, shardID string, fromStageSeq int, reason string) error {
	if fromStageSeq < 1 || fromStageSeq > len(stageOrder)+1 {
		return qerr.New(qerr.CodeInvalidInput, shardID, "", "", "invalid invalidation stage", nil)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	lock, err := filelock.Acquire(s.lockPath)
	if err != nil {
		return err
	}
	defer lock.Close()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	p, err := progressTx(ctx, tx, shardID)
	if err != nil {
		return err
	}
	if p == nil {
		return nil
	}
	expected := make([]ExpectedStage, len(stageOrder))
	for i, st := range stageOrder {
		expected[i] = ExpectedStage{Stage: st, StageSeq: i + 1}
	}
	if fromStageSeq > len(stageOrder) {
		fromStageSeq = len(stageOrder)
	}
	result, err := invalidateTx(ctx, tx, shardID, fromStageSeq, reason, expected)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if result != nil && result.Progress != nil {
		return s.ensureGuardLocked(ctx, result.Progress.GlobalSequence, shardID)
	}
	return nil
}

func invalidateTx(ctx context.Context, tx *sql.Tx, shardID string, fromSeq int, reason string, expected []ExpectedStage) (*ReconcileResult, error) {
	p, err := progressTx(ctx, tx, shardID)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, nil
	}
	states, err := statesQuery(ctx, tx, shardID)
	if err != nil {
		return nil, err
	}
	keep := states
	if fromSeq <= len(states) {
		keep = states[:fromSeq-1]
	}
	seq, err := nextSequenceTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM stage_states WHERE shard_id=? AND stage_seq>=?`, shardID, fromSeq); err != nil {
		return nil, err
	}
	version := chainVersion(keep)
	next := fromSeq
	if next < 1 {
		next = 1
	}
	// If invalidation starts beyond the current tail, keep the actual tail.
	if next > len(keep)+1 {
		next = len(keep) + 1
	}
	_, err = tx.ExecContext(ctx, `UPDATE shard_progress SET next_stage_seq=?, version_fingerprint=?, global_sequence=?
		WHERE shard_id=?`, next, version, seq, shardID)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO invalidations
		(shard_id, from_stage_seq, reason, prior_version, new_version, global_sequence)
		VALUES(?,?,?,?,?,?)`, shardID, fromSeq, reason, p.VersionFingerprint, version, seq)
	if err != nil {
		return nil, err
	}
	return &ReconcileResult{
		Classification: ClassReplacement, InvalidatedFrom: fromSeq, Reason: reason,
		Progress: &Progress{ShardID: shardID, ShardOrdinal: p.ShardOrdinal, NextStageSeq: next,
			VersionFingerprint: version, GlobalSequence: seq},
	}, nil
}

func (s *Store) SaveBudgetRejections(ctx context.Context, rejections []BudgetRejection) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	lock, err := filelock.Acquire(s.lockPath)
	if err != nil {
		return err
	}
	defer lock.Close()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM budget_rejections`); err != nil {
		return err
	}
	for _, r := range rejections {
		_, err := tx.ExecContext(ctx, `INSERT INTO budget_rejections
			(shard_id, shard_ordinal, estimated_elements, working_set_bytes, budget_remaining, memory_remaining, reason, code)
			VALUES(?,?,?,?,?,?,?,?)`,
			r.ShardID, r.ShardOrdinal, r.EstimatedElements, r.WorkingSetBytes, r.BudgetRemaining, r.MemoryRemaining, r.Reason, r.Code)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) BudgetRejections(ctx context.Context) ([]BudgetRejection, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT shard_id, shard_ordinal, estimated_elements, working_set_bytes,
		budget_remaining, memory_remaining, reason, code FROM budget_rejections ORDER BY shard_ordinal`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BudgetRejection
	for rows.Next() {
		var r BudgetRejection
		if err := rows.Scan(&r.ShardID, &r.ShardOrdinal, &r.EstimatedElements, &r.WorkingSetBytes,
			&r.BudgetRemaining, &r.MemoryRemaining, &r.Reason, &r.Code); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Invalidations(ctx context.Context, shardID string) ([]map[string]any, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT from_stage_seq, reason, prior_version, new_version, global_sequence
		FROM invalidations WHERE shard_id=? ORDER BY id`, shardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var from int
		var reason, prior, next string
		var seq int64
		if err := rows.Scan(&from, &reason, &prior, &next, &seq); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"from_stage_seq": from, "reason": reason, "prior_version": prior,
			"new_version": next, "global_sequence": seq,
		})
	}
	return out, rows.Err()
}

func (s *Store) VerifyStateDigest(st State) bool {
	return canon.HashBytes(st.Payload) == st.StageDigest
}

func verifyStateDigest(st State) bool { return canon.HashBytes(st.Payload) == st.StageDigest }

func chainVersion(states []State) string {
	sorted := append([]State(nil), states...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].StageSeq < sorted[j].StageSeq })
	parts := make([]map[string]any, 0, len(sorted))
	for _, st := range sorted {
		parts = append(parts, map[string]any{"stage": st.Stage, "seq": st.StageSeq, "digest": st.StageDigest})
	}
	h, err := canon.Hash(parts)
	if err != nil {
		return ""
	}
	return h
}

func validatePublishRequest(req PublishRequest) error {
	if req.ShardID == "" || req.ShardOrdinal <= 0 {
		return qerr.New(qerr.CodeInvalidInput, req.ShardID, string(req.Stage), "", "missing shard identity", nil)
	}
	if req.Stage.Seq() == 0 {
		return qerr.New(qerr.CodeInvalidInput, req.ShardID, string(req.Stage), "", "unknown stage", nil)
	}
	if req.StageSeq != req.Stage.Seq() {
		return qerr.New(qerr.CodeInvalidTransition, req.ShardID, string(req.Stage), "",
			"stage sequence and stage name disagree", nil)
	}
	if len(req.Payload) == 0 {
		return qerr.New(qerr.CodeInvalidInput, req.ShardID, string(req.Stage), "", "empty stage payload", nil)
	}
	if req.ConfigVersion == "" || req.InputFingerprint == "" {
		return qerr.New(qerr.CodeInvalidInput, req.ShardID, string(req.Stage), "",
			"config version and input fingerprint are required", nil)
	}
	return nil
}

func StageAtMust(seq int) Stage {
	st, ok := StageAt(seq)
	if !ok {
		panic("stage sequence out of range")
	}
	return st
}

func currentStageText(p *Progress, states []State) string {
	if p != nil {
		if st, ok := StageAt(p.NextStageSeq); ok {
			return string(st)
		}
		return "complete"
	}
	if len(states) > 0 {
		return string(states[len(states)-1].Stage)
	}
	return string(Calibration)
}

func business(code qerr.Code, shard, currentStage string, requested Stage, msg string, details map[string]any) error {
	stage := currentStage
	if stage == "" {
		stage = string(requested)
	}
	return qerr.New(code, shard, stage, "", msg, nil).WithDetail(details)
}

func staleRollback(shard, stage, msg string) error {
	if stage == "" {
		stage = string(Calibration)
	}
	return qerr.New(qerr.CodeStaleCheckpoint, shard, stage, "", msg, nil).WithDetail(map[string]any{
		"classification": ClassRollback,
	})
}
