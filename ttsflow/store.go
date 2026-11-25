package ttsflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
	_ "modernc.org/sqlite"
)

const dbSchema = `
CREATE TABLE IF NOT EXISTS dataset_versions (
	kind TEXT NOT NULL,
	version INTEGER NOT NULL,
	digest TEXT NOT NULL,
	status TEXT NOT NULL,
	revision INTEGER NOT NULL DEFAULT 0,
	previous_digest TEXT NOT NULL DEFAULT '',
	payload BLOB NOT NULL,
	created_at TEXT NOT NULL,
	PRIMARY KEY (kind, version)
);
CREATE INDEX IF NOT EXISTS dataset_versions_digest ON dataset_versions(kind, digest);

CREATE TABLE IF NOT EXISTS stream_state (
	utt_id TEXT PRIMARY KEY,
	generation INTEGER NOT NULL,
	completed_segment INTEGER NOT NULL,
	text_digest TEXT NOT NULL,
	config_digest TEXT NOT NULL,
	active_digest TEXT NOT NULL,
	last_event_seq INTEGER NOT NULL,
	status TEXT NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS checkpoints (
	row_id INTEGER PRIMARY KEY AUTOINCREMENT,
	utt_id TEXT NOT NULL,
	segment_seq INTEGER NOT NULL,
	generation INTEGER NOT NULL,
	stage_mask INTEGER NOT NULL DEFAULT 0,
	unit_digest TEXT NOT NULL DEFAULT '',
	unit_summary TEXT NOT NULL DEFAULT '',
	text_digest TEXT NOT NULL,
	config_digest TEXT NOT NULL,
	frontend_fingerprint TEXT NOT NULL DEFAULT '',
	acoustic_fingerprint TEXT NOT NULL DEFAULT '',
	wave_fingerprint TEXT NOT NULL DEFAULT '',
	consistency_fingerprint TEXT NOT NULL DEFAULT '',
	frontend_digest TEXT NOT NULL DEFAULT '',
	acoustic_digest TEXT NOT NULL DEFAULT '',
	pcm_digest TEXT NOT NULL DEFAULT '',
	consistency_digest TEXT NOT NULL DEFAULT '',
	frame_start INTEGER NOT NULL DEFAULT 0,
	frame_count INTEGER NOT NULL DEFAULT 0,
	sample_start INTEGER NOT NULL DEFAULT 0,
	sample_end INTEGER NOT NULL DEFAULT 0,
	clamp_count INTEGER NOT NULL DEFAULT 0,
	frontend_artifact BLOB,
	acoustic_artifact BLOB,
	wave_artifact BLOB,
	stats_artifact BLOB,
	checkpoint_seq INTEGER NOT NULL DEFAULT 0,
	active INTEGER NOT NULL DEFAULT 1,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS checkpoints_one_active
	ON checkpoints(utt_id, segment_seq) WHERE active = 1;
CREATE INDEX IF NOT EXISTS checkpoints_lookup ON checkpoints(utt_id, segment_seq, active);

CREATE TABLE IF NOT EXISTS checkpoint_events (
	seq INTEGER PRIMARY KEY AUTOINCREMENT,
	checkpoint_row_id INTEGER NOT NULL,
	utt_id TEXT NOT NULL,
	segment_seq INTEGER NOT NULL,
	stage TEXT NOT NULL,
	stage_mask_after INTEGER NOT NULL,
	generation INTEGER NOT NULL,
	text_digest TEXT NOT NULL,
	config_digest TEXT NOT NULL,
	stage_fingerprint TEXT NOT NULL,
	unit_digest TEXT NOT NULL,
	artifact_digest TEXT NOT NULL,
	active_digest_after TEXT NOT NULL,
	created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stop_requests (
	utt_id TEXT PRIMARY KEY,
	requested_at TEXT NOT NULL
);
`

type Store struct {
	db        *sql.DB
	path      string
	lockPath  string
	lock      *flock.Flock
	publishMu sync.Mutex
}

func OpenStore(path string) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, err
	}
	dsnPath := filepath.ToSlash(abs)
	if !strings.HasPrefix(dsnPath, "/") {
		dsnPath = "/" + dsnPath
	}
	dsn := "file:" + dsnPath + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(dbSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize sqlite schema: %w", err)
	}
	if err := migrateSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{
		db:       db,
		path:     abs,
		lockPath: abs + ".publish.lock",
		lock:     flock.New(abs + ".publish.lock"),
	}, nil
}

func migrateSchema(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(checkpoint_events)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	hasUnitSummary := false
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull, pk int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		if name == "unit_summary" {
			hasUnitSummary = true
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if !hasUnitSummary {
		if _, err := db.Exec(`ALTER TABLE checkpoint_events ADD COLUMN unit_summary TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Path() string { return s.path }

func (s *Store) withPublishLock(ctx context.Context, fn func() error) error {
	// flock is process-scoped on Unix in the sense that the same open file
	// description may re-enter. The mutex serializes goroutines sharing this
	// Store; flock then serializes independent processes.
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	locked, err := s.lock.TryLockContext(ctx, 5*time.Millisecond)
	if err != nil {
		return biz(CodeLock, "could not acquire publish file lock").withCause(err)
	}
	if !locked {
		return biz(CodeLock, "publish file lock was not acquired")
	}
	defer func() { _ = s.lock.Unlock() }()
	return fn()
}

func nowString() string { return time.Now().UTC().Format(time.RFC3339Nano) }

type VersionRecord struct {
	Kind           string `json:"kind"`
	Version        int64  `json:"version"`
	Digest         string `json:"digest"`
	Status         string `json:"status"`
	Revision       int64  `json:"revision"`
	PreviousDigest string `json:"previous_digest"`
	CreatedAt      string `json:"created_at"`
}

type SnapshotMode string

const (
	SnapshotAuto    SnapshotMode = "auto"
	SnapshotAppend  SnapshotMode = "append"
	SnapshotUpdate  SnapshotMode = "update"
	SnapshotReplace SnapshotMode = "replace"
)

func normalizeMode(m SnapshotMode) SnapshotMode {
	if m == "" {
		return SnapshotAuto
	}
	return m
}

func (s *Store) LatestVersion(ctx context.Context, kind string) (VersionRecord, bool, error) {
	var v VersionRecord
	err := s.db.QueryRowContext(ctx, `SELECT kind, version, digest, status, revision, previous_digest, created_at
		FROM dataset_versions WHERE kind=? ORDER BY version DESC LIMIT 1`, kind).
		Scan(&v.Kind, &v.Version, &v.Digest, &v.Status, &v.Revision, &v.PreviousDigest, &v.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return VersionRecord{}, false, nil
	}
	return v, err == nil, err
}

// ObserveTextSet classifies a corpus transition as append, update, replace,
// unchanged, or rollback. Auto mode treats edits to an existing corpus as an
// incremental update and lets the engine locate the earliest affected segment.
// Explicit replace invalidates all derived state before the first recovery.
func (s *Store) ObserveTextSet(ctx context.Context, samples []TextSample, mode SnapshotMode) (VersionRecord, error) {
	mode = normalizeMode(mode)
	cp := append([]TextSample(nil), samples...)
	sort.Slice(cp, func(i, j int) bool { return cp[i].UTTID < cp[j].UTTID })
	seen := map[string]bool{}
	for _, sample := range cp {
		if err := sample.Validate(); err != nil {
			return VersionRecord{}, biz(CodeDatasetInvalid, err.Error())
		}
		if seen[sample.UTTID] {
			return VersionRecord{}, biz(CodeDatasetInvalid, "duplicate utt_id in text set")
		}
		seen[sample.UTTID] = true
	}
	digest := TextSetDigest(cp)
	payload, err := json.Marshal(cp)
	if err != nil {
		return VersionRecord{}, err
	}
	var result VersionRecord
	err = s.withPublishLock(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		latest, exists, err := latestVersionTx(ctx, tx, "texts")
		if err != nil {
			return err
		}
		if exists && latest.Digest == digest {
			result = latest
			result.Status = "unchanged"
			return tx.Commit()
		}
		history, err := versionHistoryTx(ctx, tx, "texts")
		if err != nil {
			return err
		}
		if oldVersion, ok := history[digest]; ok {
			return &BizError{Code: CodeRollback, Message: "text-set digest matches an older version",
				CurrentSeq: int(latest.Version), Context: fmt.Sprintf("old_version=%d requested_mode=%s", oldVersion, mode)}
		}
		status := string(SnapshotUpdate)
		var previous []TextSample
		if exists {
			var previousPayload []byte
			if err := tx.QueryRowContext(ctx, `SELECT payload FROM dataset_versions WHERE kind='texts' AND version=?`, latest.Version).Scan(&previousPayload); err == nil {
				_ = json.Unmarshal(previousPayload, &previous)
			}
			status = classifyTextSet(previous, cp, mode)
		} else {
			status = "initialize"
		}
		if mode == SnapshotAppend && status != "append" && status != "initialize" {
			return biz(CodeDatasetInvalid, "append mode requires all existing utterances to remain unchanged")
		}
		version := int64(1)
		prevDigest := ""
		if exists {
			version = latest.Version + 1
			prevDigest = latest.Digest
		}
		now := nowString()
		if _, err := tx.ExecContext(ctx, `INSERT INTO dataset_versions(kind,version,digest,status,revision,previous_digest,payload,created_at)
			VALUES('texts',?,?,?,?,?,?,?)`, version, digest, status, 0, prevDigest, payload, now); err != nil {
			return err
		}
		if status == string(SnapshotReplace) || status == "replace" || status == "initialize" {
			if err := invalidateAllTx(ctx, tx); err != nil {
				return err
			}
		}
		result = VersionRecord{Kind: "texts", Version: version, Digest: digest, Status: status, PreviousDigest: prevDigest, CreatedAt: now}
		return tx.Commit()
	})
	return result, err
}

func classifyTextSet(old, new []TextSample, mode SnapshotMode) string {
	if mode == SnapshotReplace {
		return "replace"
	}
	oldMap := make(map[string]TextSample, len(old))
	for _, sample := range old {
		oldMap[sample.UTTID] = sample
	}
	newMap := make(map[string]TextSample, len(new))
	for _, sample := range new {
		newMap[sample.UTTID] = sample
	}
	allOldPresent := true
	allOldUnchanged := true
	for id, sample := range oldMap {
		next, ok := newMap[id]
		if !ok {
			allOldPresent = false
			break
		}
		if next.Digest() != sample.Digest() {
			allOldUnchanged = false
		}
	}
	if mode == SnapshotAppend && !allOldUnchanged {
		return "invalid-append"
	}
	if allOldPresent && allOldUnchanged && len(new) >= len(old) {
		return "append"
	}
	if allOldPresent {
		return string(SnapshotUpdate)
	}
	return "replace"
}

// ObserveConfig records a configuration revision. A lower/equal revision with
// different content is rollback; an explicit replace or a revision jump causes
// whole-state invalidation. A normal next revision is left to stage-level
// incremental invalidation.
func (s *Store) ObserveConfig(ctx context.Context, cfg Config, raw []byte, mode SnapshotMode) (VersionRecord, error) {
	mode = normalizeMode(mode)
	digest := cfg.Digest()
	payload, err := json.Marshal(struct {
		Config Config `json:"config"`
		Raw    string `json:"raw"`
	}{Config: cfg, Raw: string(raw)})
	if err != nil {
		return VersionRecord{}, err
	}
	var result VersionRecord
	err = s.withPublishLock(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		latest, exists, err := latestVersionTx(ctx, tx, "config")
		if err != nil {
			return err
		}
		if exists && latest.Digest == digest {
			result = latest
			result.Status = "unchanged"
			return tx.Commit()
		}
		history, err := versionHistoryTx(ctx, tx, "config")
		if err != nil {
			return err
		}
		if oldVersion, ok := history[digest]; ok {
			return &BizError{Code: CodeRollback, Message: "config digest matches an older version",
				CurrentSeq: int(latest.Version), Context: fmt.Sprintf("old_version=%d", oldVersion)}
		}
		status := string(SnapshotUpdate)
		if !exists {
			status = "initialize"
		} else if mode == SnapshotReplace || cfg.Revision > latest.Revision+1 {
			status = "replace"
		} else if cfg.Revision <= latest.Revision {
			return &BizError{Code: CodeRollback, Message: "config revision must increase for changed content",
				CurrentSeq: int(latest.Version), Context: fmt.Sprintf("stored_revision=%d incoming_revision=%d", latest.Revision, cfg.Revision)}
		}
		version := int64(1)
		prevDigest := ""
		if exists {
			version = latest.Version + 1
			prevDigest = latest.Digest
		}
		now := nowString()
		if _, err := tx.ExecContext(ctx, `INSERT INTO dataset_versions(kind,version,digest,status,revision,previous_digest,payload,created_at)
			VALUES('config',?,?,?,?,?,?,?)`, version, digest, status, cfg.Revision, prevDigest, payload, now); err != nil {
			return err
		}
		if status == "replace" || status == "initialize" {
			if err := invalidateAllTx(ctx, tx); err != nil {
				return err
			}
		}
		result = VersionRecord{Kind: "config", Version: version, Digest: digest, Status: status, Revision: cfg.Revision, PreviousDigest: prevDigest, CreatedAt: now}
		return tx.Commit()
	})
	return result, err
}

func latestVersionTx(ctx context.Context, tx *sql.Tx, kind string) (VersionRecord, bool, error) {
	var v VersionRecord
	err := tx.QueryRowContext(ctx, `SELECT kind, version, digest, status, revision, previous_digest, created_at
		FROM dataset_versions WHERE kind=? ORDER BY version DESC LIMIT 1`, kind).
		Scan(&v.Kind, &v.Version, &v.Digest, &v.Status, &v.Revision, &v.PreviousDigest, &v.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return VersionRecord{}, false, nil
	}
	return v, err == nil, err
}

func versionHistoryTx(ctx context.Context, tx *sql.Tx, kind string) (map[string]int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT digest, version FROM dataset_versions WHERE kind=?`, kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var digest string
		var version int64
		if err := rows.Scan(&digest, &version); err != nil {
			return nil, err
		}
		out[digest] = version
	}
	return out, rows.Err()
}

func invalidateAllTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `UPDATE checkpoints SET active=0, updated_at=? WHERE active=1`, nowString()); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM stream_state`)
	return err
}

type StreamState struct {
	UTTID            string `json:"utt_id"`
	Generation       int64  `json:"generation"`
	CompletedSegment int    `json:"completed_segment"`
	TextDigest       string `json:"text_digest"`
	ConfigDigest     string `json:"config_digest"`
	ActiveDigest     string `json:"active_digest"`
	LastEventSeq     int64  `json:"last_event_seq"`
	Status           string `json:"status"`
	UpdatedAt        string `json:"updated_at"`
}

func scanStreamState(scanner interface{ Scan(...any) error }) (StreamState, error) {
	var s StreamState
	err := scanner.Scan(&s.UTTID, &s.Generation, &s.CompletedSegment, &s.TextDigest, &s.ConfigDigest,
		&s.ActiveDigest, &s.LastEventSeq, &s.Status, &s.UpdatedAt)
	return s, err
}

func getStreamStateTx(ctx context.Context, tx *sql.Tx, uttID string) (StreamState, bool, error) {
	state, err := scanStreamState(tx.QueryRowContext(ctx, `SELECT utt_id,generation,completed_segment,text_digest,config_digest,active_digest,last_event_seq,status,updated_at FROM stream_state WHERE utt_id=?`, uttID))
	if errors.Is(err, sql.ErrNoRows) {
		return StreamState{}, false, nil
	}
	return state, err == nil, err
}

func (s *Store) StreamState(ctx context.Context, uttID string) (StreamState, bool, error) {
	state, err := scanStreamState(s.db.QueryRowContext(ctx, `SELECT utt_id,generation,completed_segment,text_digest,config_digest,active_digest,last_event_seq,status,updated_at FROM stream_state WHERE utt_id=?`, uttID))
	if errors.Is(err, sql.ErrNoRows) {
		return StreamState{}, false, nil
	}
	return state, err == nil, err
}

type CheckpointRow struct {
	RowID                  int64  `json:"row_id"`
	UTTID                  string `json:"utt_id"`
	SegmentSeq             int    `json:"segment_seq"`
	Generation             int64  `json:"generation"`
	StageMask              int    `json:"stage_mask"`
	UnitDigest             string `json:"unit_digest"`
	UnitSummary            string `json:"unit_summary"`
	TextDigest             string `json:"text_digest"`
	ConfigDigest           string `json:"config_digest"`
	FrontendFingerprint    string `json:"frontend_fingerprint"`
	AcousticFingerprint    string `json:"acoustic_fingerprint"`
	WaveFingerprint        string `json:"wave_fingerprint"`
	ConsistencyFingerprint string `json:"consistency_fingerprint"`
	FrontendDigest         string `json:"frontend_digest"`
	AcousticDigest         string `json:"acoustic_digest"`
	PCMDigest              string `json:"pcm_digest"`
	ConsistencyDigest      string `json:"consistency_digest"`
	FrameStart             int    `json:"frame_start"`
	FrameCount             int    `json:"frame_count"`
	SampleStart            int    `json:"sample_start"`
	SampleEnd              int    `json:"sample_end"`
	ClampCount             int    `json:"clamp_count"`
	FrontendArtifact       []byte `json:"-"`
	AcousticArtifact       []byte `json:"-"`
	WaveArtifact           []byte `json:"-"`
	StatsArtifact          []byte `json:"-"`
	CheckpointSeq          int64  `json:"checkpoint_seq"`
	Active                 bool   `json:"active"`
	CreatedAt              string `json:"created_at"`
	UpdatedAt              string `json:"updated_at"`
}

func (r CheckpointRow) has(stage Stage) bool { return r.StageMask&stage.bit() != 0 }

func scanCheckpoint(scanner interface{ Scan(...any) error }) (CheckpointRow, error) {
	var r CheckpointRow
	err := scanner.Scan(&r.RowID, &r.UTTID, &r.SegmentSeq, &r.Generation, &r.StageMask, &r.UnitDigest, &r.UnitSummary,
		&r.TextDigest, &r.ConfigDigest, &r.FrontendFingerprint, &r.AcousticFingerprint, &r.WaveFingerprint,
		&r.ConsistencyFingerprint, &r.FrontendDigest, &r.AcousticDigest, &r.PCMDigest, &r.ConsistencyDigest,
		&r.FrameStart, &r.FrameCount, &r.SampleStart, &r.SampleEnd, &r.ClampCount,
		&r.FrontendArtifact, &r.AcousticArtifact, &r.WaveArtifact, &r.StatsArtifact,
		&r.CheckpointSeq, &r.Active, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

const checkpointColumns = `row_id,utt_id,segment_seq,generation,stage_mask,unit_digest,unit_summary,text_digest,config_digest,
frontend_fingerprint,acoustic_fingerprint,wave_fingerprint,consistency_fingerprint,frontend_digest,acoustic_digest,pcm_digest,consistency_digest,
frame_start,frame_count,sample_start,sample_end,clamp_count,frontend_artifact,acoustic_artifact,wave_artifact,stats_artifact,checkpoint_seq,active,created_at,updated_at`

func (s *Store) ActiveCheckpoints(ctx context.Context, uttID string) ([]CheckpointRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+checkpointColumns+` FROM checkpoints WHERE utt_id=? AND active=1 ORDER BY segment_seq`, uttID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CheckpointRow
	for rows.Next() {
		row, err := scanCheckpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) QueryCheckpoint(ctx context.Context, uttID string, segmentSeq int) (CheckpointRow, bool, error) {
	query := `SELECT ` + checkpointColumns + ` FROM checkpoints WHERE utt_id=? AND active=1`
	args := []any{uttID}
	if segmentSeq >= 0 {
		query += ` AND segment_seq=?`
		args = append(args, segmentSeq)
	} else {
		query += ` ORDER BY segment_seq DESC LIMIT 1`
	}
	row, err := scanCheckpoint(s.db.QueryRowContext(ctx, query, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return CheckpointRow{}, false, nil
	}
	return row, err == nil, err
}

type StageArtifact struct {
	UTTID          string
	SegmentSeq     int
	Generation     int64
	Stage          Stage
	TextDigest     string
	ConfigDigest   string
	UnitDigest     string
	UnitSummary    string
	Fingerprint    string
	ArtifactDigest string
	Artifact       []byte
	FrameStart     int
	FrameCount     int
	SampleStart    int
	SampleEnd      int
	ClampCount     int
}

type PublishOutcome struct {
	Kind       string `json:"kind"`
	EventSeq   int64  `json:"event_seq"`
	CurrentSeq int    `json:"current_seq"`
	StageMask  int    `json:"stage_mask"`
	Generation int64  `json:"generation"`
}

func staleErr(message string, current, expected int, kind string) *BizError {
	return &BizError{Code: CodeStaleCheckpoint, Message: message, CurrentSeq: current, ExpectedSeq: expected, SnapshotKind: kind}
}

func (s *Store) PublishStage(ctx context.Context, artifact StageArtifact) (PublishOutcome, error) {
	if artifact.ArtifactDigest == "" {
		artifact.ArtifactDigest = digestBytes(artifact.Artifact)
	}
	if actual := digestBytes(artifact.Artifact); actual != artifact.ArtifactDigest {
		return PublishOutcome{}, staleErr("artifact digest does not match artifact bytes", 0, artifact.SegmentSeq, "replacement")
	}
	var outcome PublishOutcome
	err := s.withPublishLock(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		state, exists, err := getStreamStateTx(ctx, tx, artifact.UTTID)
		if err != nil {
			return err
		}
		activeDigest, maxCompleted, err := activeDigestTx(ctx, tx, artifact.UTTID)
		if err != nil {
			return err
		}
		if exists {
			if err := validateSnapshotTx(ctx, tx, state, activeDigest, maxCompleted); err != nil {
				return err
			}
			if state.TextDigest != artifact.TextDigest || state.ConfigDigest != artifact.ConfigDigest {
				return staleErr("publish targets a different text/config snapshot", state.CompletedSegment, artifact.SegmentSeq, "replacement")
			}
		} else {
			artifact.Generation = 1
		}
		if artifact.Generation > 0 && exists && artifact.Generation != state.Generation {
			return staleErr("checkpoint generation is stale", state.CompletedSegment, artifact.SegmentSeq, "rollback")
		}
		completed := -1
		generation := int64(1)
		status := "running"
		if exists {
			completed = state.CompletedSegment
			generation = state.Generation
		}
		artifact.Generation = generation
		if artifact.SegmentSeq < completed {
			return staleErr("segment is behind the durable checkpoint", completed, artifact.SegmentSeq, "rollback")
		}
		if artifact.SegmentSeq > completed+1 {
			return staleErr("segment would create a checkpoint gap", completed, artifact.SegmentSeq, "gap")
		}

		row, hasRow, err := getCheckpointRowTx(ctx, tx, artifact.UTTID, artifact.SegmentSeq)
		if err != nil {
			return err
		}
		mask := 0
		if hasRow {
			mask = row.StageMask
		}
		expectedMask := (1 << uint(artifact.Stage)) - 1
		if mask&artifact.Stage.bit() != 0 {
			return staleErr("stage checkpoint is a duplicate", completed, artifact.SegmentSeq, "duplicate")
		}
		if mask != expectedMask {
			return staleErr("stage checkpoints must be published in frontend/acoustic/wave/consistency order", completed, artifact.SegmentSeq, "replacement")
		}
		if artifact.SegmentSeq == completed && mask == completeStageMask {
			return staleErr("segment is already fully committed", completed, artifact.SegmentSeq, "duplicate")
		}

		now := nowString()
		var rowID int64
		if !hasRow {
			res, err := tx.ExecContext(ctx, `INSERT INTO checkpoints(utt_id,segment_seq,generation,stage_mask,unit_digest,unit_summary,text_digest,config_digest,
				frontend_fingerprint,acoustic_fingerprint,wave_fingerprint,consistency_fingerprint,frontend_digest,acoustic_digest,pcm_digest,consistency_digest,
				frame_start,frame_count,sample_start,sample_end,clamp_count,frontend_artifact,acoustic_artifact,wave_artifact,stats_artifact,checkpoint_seq,active,created_at,updated_at)
				VALUES(?,?,?,0,'','',?,?,'','','','','','','','',0,0,0,0,0,NULL,NULL,NULL,NULL,0,1,?,?)`,
				artifact.UTTID, artifact.SegmentSeq, generation, artifact.TextDigest, artifact.ConfigDigest, now, now)
			if err != nil {
				return err
			}
			rowID, err = res.LastInsertId()
			if err != nil {
				return err
			}
		} else {
			rowID = row.RowID
		}

		fpColumn := map[Stage]string{
			StageFrontend: "frontend_fingerprint", StageAcoustic: "acoustic_fingerprint",
			StageWave: "wave_fingerprint", StageConsistency: "consistency_fingerprint",
		}[artifact.Stage]
		digestColumn := map[Stage]string{
			StageFrontend: "frontend_digest", StageAcoustic: "acoustic_digest",
			StageWave: "pcm_digest", StageConsistency: "consistency_digest",
		}[artifact.Stage]
		blobColumn := map[Stage]string{
			StageFrontend: "frontend_artifact", StageAcoustic: "acoustic_artifact",
			StageWave: "wave_artifact", StageConsistency: "stats_artifact",
		}[artifact.Stage]
		newMask := mask | artifact.Stage.bit()
		sets := []string{
			"stage_mask=?", "unit_digest=?", "unit_summary=?", "text_digest=?", "config_digest=?",
			fpColumn + "=?", digestColumn + "=?", blobColumn + "=?", "frame_start=?", "frame_count=?",
			"sample_start=?", "sample_end=?", "clamp_count=?", "updated_at=?",
		}
		args := []any{newMask, artifact.UnitDigest, artifact.UnitSummary, artifact.TextDigest, artifact.ConfigDigest,
			artifact.Fingerprint, artifact.ArtifactDigest, artifact.Artifact, artifact.FrameStart, artifact.FrameCount,
			artifact.SampleStart, artifact.SampleEnd, artifact.ClampCount, now, rowID, mask}
		res, err := tx.ExecContext(ctx, `UPDATE checkpoints SET `+strings.Join(sets, ",")+` WHERE row_id=? AND stage_mask=? AND active=1`, args...)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 1 {
			return staleErr("conditional checkpoint update lost a race", completed, artifact.SegmentSeq, "replacement")
		}

		res, err = tx.ExecContext(ctx, `INSERT INTO checkpoint_events(checkpoint_row_id,utt_id,segment_seq,stage,stage_mask_after,generation,text_digest,config_digest,
			stage_fingerprint,unit_digest,unit_summary,artifact_digest,active_digest_after,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'',?)`,
			rowID, artifact.UTTID, artifact.SegmentSeq, artifact.Stage.String(), newMask, generation, artifact.TextDigest, artifact.ConfigDigest,
			artifact.Fingerprint, artifact.UnitDigest, artifact.UnitSummary, artifact.ArtifactDigest, now)
		if err != nil {
			return err
		}
		eventSeq, err := res.LastInsertId()
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE checkpoints SET checkpoint_seq=? WHERE row_id=?`, eventSeq, rowID); err != nil {
			return err
		}
		if newMask == completeStageMask {
			completed = artifact.SegmentSeq
			status = "running"
		}
		activeDigest, maxCompleted, err = activeDigestTx(ctx, tx, artifact.UTTID)
		if err != nil {
			return err
		}
		if maxCompleted > completed {
			return staleErr("active snapshot advanced beyond stream state", completed, maxCompleted, "replacement")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE checkpoint_events SET active_digest_after=? WHERE seq=?`, activeDigest, eventSeq); err != nil {
			return err
		}
		if exists {
			res, err := tx.ExecContext(ctx, `UPDATE stream_state SET generation=?,completed_segment=?,text_digest=?,config_digest=?,active_digest=?,last_event_seq=?,status=?,updated_at=?
				WHERE utt_id=? AND generation=? AND completed_segment=? AND active_digest=?`,
				generation, completed, artifact.TextDigest, artifact.ConfigDigest, activeDigest, eventSeq, status, now,
				artifact.UTTID, state.Generation, state.CompletedSegment, state.ActiveDigest)
			if err != nil {
				return err
			}
			affected, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if affected != 1 {
				return staleErr("conditional stream-state update lost a race", state.CompletedSegment, artifact.SegmentSeq, "replacement")
			}
		} else {
			if _, err := tx.ExecContext(ctx, `INSERT INTO stream_state(utt_id,generation,completed_segment,text_digest,config_digest,active_digest,last_event_seq,status,updated_at)
				VALUES(?,?,?,?,?,?,?,?,?)`, artifact.UTTID, generation, completed, artifact.TextDigest, artifact.ConfigDigest, activeDigest, eventSeq, status, now); err != nil {
				return err
			}
		}
		kind := "advance"
		if newMask != completeStageMask {
			kind = "partial"
		}
		outcome = PublishOutcome{Kind: kind, EventSeq: eventSeq, CurrentSeq: completed, StageMask: newMask, Generation: generation}
		return tx.Commit()
	})
	return outcome, err
}

func getCheckpointRowTx(ctx context.Context, tx *sql.Tx, uttID string, segmentSeq int) (CheckpointRow, bool, error) {
	row, err := scanCheckpoint(tx.QueryRowContext(ctx, `SELECT `+checkpointColumns+` FROM checkpoints WHERE utt_id=? AND segment_seq=? AND active=1`, uttID, segmentSeq))
	if errors.Is(err, sql.ErrNoRows) {
		return CheckpointRow{}, false, nil
	}
	return row, err == nil, err
}

func activeDigestTx(ctx context.Context, tx *sql.Tx, uttID string) (string, int, error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+checkpointColumns+` FROM checkpoints WHERE utt_id=? AND active=1 ORDER BY segment_seq`, uttID)
	if err != nil {
		return "", -1, err
	}
	defer rows.Close()
	parts := make([]any, 0, 64)
	maxCompleted := -1
	for rows.Next() {
		row, err := scanCheckpoint(rows)
		if err != nil {
			return "", -1, err
		}
		parts = append(parts, row.RowID, row.SegmentSeq, row.Generation, row.StageMask, row.UnitDigest, row.TextDigest,
			row.ConfigDigest, row.FrontendFingerprint, row.AcousticFingerprint, row.WaveFingerprint, row.ConsistencyFingerprint,
			row.FrontendDigest, row.AcousticDigest, row.PCMDigest, row.ConsistencyDigest,
			row.FrameStart, row.FrameCount, row.SampleStart, row.SampleEnd, row.ClampCount, row.CheckpointSeq,
			digestBytes(row.FrontendArtifact), digestBytes(row.AcousticArtifact), digestBytes(row.WaveArtifact), digestBytes(row.StatsArtifact))
		if row.StageMask == completeStageMask && row.SegmentSeq > maxCompleted {
			maxCompleted = row.SegmentSeq
		}
	}
	if err := rows.Err(); err != nil {
		return "", -1, err
	}
	return digestParts(parts...), maxCompleted, nil
}

func validateSnapshotTx(ctx context.Context, tx *sql.Tx, state StreamState, actualDigest string, maxCompleted int) error {
	if actualDigest != state.ActiveDigest {
		kind := "replacement"
		if state.CompletedSegment > maxCompleted {
			kind = "rollback"
		}
		return staleErr("active checkpoint snapshot digest mismatch", state.CompletedSegment, state.CompletedSegment, kind)
	}
	if state.LastEventSeq > 0 {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoint_events WHERE seq=? AND utt_id=?`, state.LastEventSeq, state.UTTID).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return staleErr("checkpoint event history is missing", state.CompletedSegment, state.CompletedSegment, "rollback")
		}
	}
	for seq := 0; seq <= state.CompletedSegment; seq++ {
		var mask int
		err := tx.QueryRowContext(ctx, `SELECT stage_mask FROM checkpoints WHERE utt_id=? AND segment_seq=? AND active=1`, state.UTTID, seq).Scan(&mask)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && mask != completeStageMask) {
			return staleErr("completed checkpoint chain has a gap", state.CompletedSegment, seq, "rollback")
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	if maxCompleted > state.CompletedSegment {
		return staleErr("active snapshot advanced outside the stream state", state.CompletedSegment, maxCompleted, "replacement")
	}
	return nil
}

// InvalidateFrom removes all artifacts downstream of the earliest mismatch. If
// the mismatch is at an acoustic or later stage, the same segment's earlier
// stage bit is retained so that the frontend artifact can be reused.
func (s *Store) InvalidateFrom(ctx context.Context, uttID string, segmentSeq int, stage Stage) error {
	return s.withPublishLock(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		state, exists, err := getStreamStateTx(ctx, tx, uttID)
		if err != nil || !exists {
			return err
		}
		now := nowString()
		if _, err := tx.ExecContext(ctx, `UPDATE checkpoints SET active=0, updated_at=? WHERE utt_id=? AND active=1 AND segment_seq>?`, now, uttID, segmentSeq); err != nil {
			return err
		}
		if stage == StageFrontend {
			if _, err := tx.ExecContext(ctx, `UPDATE checkpoints SET active=0, updated_at=? WHERE utt_id=? AND active=1 AND segment_seq=?`, now, uttID, segmentSeq); err != nil {
				return err
			}
		} else {
			keepMask := (1 << uint(stage)) - 1
			if _, err := tx.ExecContext(ctx, `UPDATE checkpoints SET stage_mask=(stage_mask & ?), updated_at=? WHERE utt_id=? AND active=1 AND segment_seq=?`,
				keepMask, now, uttID, segmentSeq); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE checkpoints SET active=0 WHERE utt_id=? AND segment_seq=? AND active=1 AND stage_mask=0`, uttID, segmentSeq); err != nil {
				return err
			}
		}
		completed := state.CompletedSegment
		if segmentSeq <= completed {
			completed = segmentSeq - 1
		}
		activeDigest, maxCompleted, err := activeDigestTx(ctx, tx, uttID)
		if err != nil {
			return err
		}
		if maxCompleted > completed {
			completed = maxCompleted
		}
		status := "invalidated"
		res, err := tx.ExecContext(ctx, `UPDATE stream_state SET completed_segment=?,active_digest=?,status=?,updated_at=? WHERE utt_id=? AND generation=? AND active_digest=?`,
			completed, activeDigest, status, now, uttID, state.Generation, state.ActiveDigest)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 1 {
			return staleErr("stream state changed during invalidation", state.CompletedSegment, segmentSeq, "replacement")
		}
		return tx.Commit()
	})
}

// PrepareStream updates the target text/config digests after invalidation. The
// active artifact snapshot itself is not changed.
func (s *Store) PrepareStream(ctx context.Context, uttID, textDigest, configDigest string) error {
	return s.withPublishLock(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		state, exists, err := getStreamStateTx(ctx, tx, uttID)
		if err != nil || !exists {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE stream_state SET text_digest=?,config_digest=?,updated_at=? WHERE utt_id=? AND generation=? AND active_digest=?`,
			textDigest, configDigest, nowString(), uttID, state.Generation, state.ActiveDigest)
		if err != nil {
			return err
		}
		return tx.Commit()
	})
}

// SetStreamStatus records the lifecycle state of an utterance without changing
// its checkpoint sequence or active artifact snapshot.
func (s *Store) SetStreamStatus(ctx context.Context, uttID, status string) error {
	if status == "" {
		return biz(CodeDatasetInvalid, "stream status must not be empty")
	}
	return s.withPublishLock(ctx, func() error {
		res, err := s.db.ExecContext(ctx, `UPDATE stream_state SET status=?,updated_at=? WHERE utt_id=?`, status, nowString(), uttID)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 1 {
			return staleErr("stream state does not exist for status update", -1, 0, "replacement")
		}
		return nil
	})
}

func (s *Store) DeleteStream(ctx context.Context, uttID string) error {
	return s.withPublishLock(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, `UPDATE checkpoints SET active=0, updated_at=? WHERE utt_id=? AND active=1`, nowString(), uttID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM stream_state WHERE utt_id=?`, uttID); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func (s *Store) RequestStop(ctx context.Context, uttID string) error {
	if err := s.withPublishLock(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `INSERT INTO stop_requests(utt_id,requested_at) VALUES(?,?)
			ON CONFLICT(utt_id) DO UPDATE SET requested_at=excluded.requested_at`, uttID, nowString())
		return err
	}); err != nil {
		return err
	}
	return nil
}

func (s *Store) ConsumeStop(ctx context.Context, uttID string) (bool, error) {
	var found bool
	err := s.withPublishLock(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM stop_requests WHERE utt_id=?`, uttID).Scan(&count); err != nil {
			return err
		}
		found = count > 0
		if found {
			if _, err := tx.ExecContext(ctx, `DELETE FROM stop_requests WHERE utt_id=?`, uttID); err != nil {
				return err
			}
		}
		return tx.Commit()
	})
	return found, err
}

func (s *Store) ClearStops(ctx context.Context, uttID string) error {
	return s.withPublishLock(ctx, func() error {
		if uttID == "" {
			_, err := s.db.ExecContext(ctx, `DELETE FROM stop_requests`)
			return err
		}
		_, err := s.db.ExecContext(ctx, `DELETE FROM stop_requests WHERE utt_id=?`, uttID)
		return err
	})
}

// InspectSnapshot classifies the durable state without changing it.
func (s *Store) InspectSnapshot(ctx context.Context, uttID string) (string, error) {
	state, exists, err := s.StreamState(ctx, uttID)
	if err != nil {
		return "", err
	}
	if !exists {
		return "empty", nil
	}
	var activeDigest string
	var maxCompleted int
	err = s.withPublishLock(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		activeDigest, maxCompleted, err = activeDigestTx(ctx, tx, uttID)
		return tx.Commit()
	})
	if err != nil {
		return "", err
	}
	if activeDigest != state.ActiveDigest {
		if state.CompletedSegment > maxCompleted {
			return "rollback", nil
		}
		return "replacement", nil
	}
	if maxCompleted > state.CompletedSegment {
		return "external_advance", nil
	}
	return "normal", nil
}

func (s *Store) CompletedSegments(ctx context.Context, uttID string) (int, error) {
	state, exists, err := s.StreamState(ctx, uttID)
	if err != nil || !exists {
		return 0, err
	}
	return state.CompletedSegment + 1, nil
}

func (s *Store) EventCount(ctx context.Context, uttID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoint_events WHERE utt_id=?`, uttID).Scan(&count)
	return count, err
}
