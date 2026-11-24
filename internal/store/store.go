package store

import (
	"bytes"
	"compress/zlib"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"maskrevision/internal/model"
)

type Store struct {
	db   *sql.DB
	path string
}

type Checkpoint struct {
	Seq            int64              `json:"seq"`
	ShardID        string             `json:"shard_id"`
	Revision       int                `json:"revision"`
	ParentRevision int                `json:"parent_revision"`
	InputFP        string             `json:"input_fp"`
	LogPrefixFP    string             `json:"log_prefix_fp"`
	RevisionFP     string             `json:"revision_fp"`
	SegmentDigest  string             `json:"segment_table_digest"`
	PixelDigest    string             `json:"pixel_digest"`
	SnapshotDigest string             `json:"snapshot_digest"`
	CreatedAt      string             `json:"created_at"`
	State          *model.ReplayState `json:"-"`
	Snapshot       *model.Snapshot    `json:"-"`
}

type PublicationState struct {
	ShardID        string `json:"shard_id"`
	Revision       int    `json:"revision"`
	SnapshotDigest string `json:"snapshot_digest"`
	InputFP        string `json:"input_fp"`
	LogPrefixFP    string `json:"log_prefix_fp"`
	LastSeq        int64  `json:"last_seq"`
	RowVersion     int64  `json:"row_version"`
	UpdatedAt      string `json:"updated_at"`
}

type PublishRequest struct {
	ShardID          string `json:"shard_id"`
	Revision         int    `json:"revision"`
	ExpectedRevision int    `json:"expected_revision"`
	SnapshotDigest   string `json:"snapshot_digest,omitempty"`
	Publisher        string `json:"publisher"`
}

type PublishResult struct {
	Accepted  bool             `json:"accepted"`
	Seq       int64            `json:"seq"`
	State     PublicationState `json:"state"`
	Code      string           `json:"code"`
	Publisher string           `json:"publisher"`
}

type CheckpointMeta struct {
	Seq            int64  `json:"seq"`
	ShardID        string `json:"shard_id"`
	Revision       int    `json:"revision"`
	ParentRevision int    `json:"parent_revision"`
	InputFP        string `json:"input_fp"`
	LogPrefixFP    string `json:"log_prefix_fp"`
	RevisionFP     string `json:"revision_fp"`
	SegmentDigest  string `json:"segment_table_digest"`
	PixelDigest    string `json:"pixel_digest"`
	SnapshotDigest string `json:"snapshot_digest"`
	CreatedAt      string `json:"created_at"`
}

type StoredLog struct {
	ShardID           string `json:"shard_id"`
	InputFP           string `json:"input_fp"`
	SourceSHA256      string `json:"source_sha256"`
	OriginalVersion   int    `json:"original_version"`
	LogJSON           string `json:"log_json"`
	UpgradeReportJSON string `json:"upgrade_report_json,omitempty"`
	UpdatedAt         string `json:"updated_at"`
}

func Open(path string) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, err
	}
	dsn := abs + "?_pragma=busy_timeout(15000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(0)
	s := &Store{db: db, path: abs}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Path() string { return s.path }

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS checkpoints (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			shard_id TEXT NOT NULL,
			revision INTEGER NOT NULL,
			parent_revision INTEGER NOT NULL,
			input_fp TEXT NOT NULL,
			log_prefix_fp TEXT NOT NULL,
			revision_fp TEXT NOT NULL,
			segment_digest TEXT NOT NULL,
			pixel_digest TEXT NOT NULL,
			snapshot_digest TEXT NOT NULL,
			state_blob BLOB NOT NULL,
			snapshot_blob BLOB NOT NULL,
			created_at TEXT NOT NULL,
			UNIQUE(shard_id, revision)
		)`,
		`CREATE INDEX IF NOT EXISTS checkpoints_head ON checkpoints(shard_id, revision DESC)`,
		`CREATE TABLE IF NOT EXISTS publish_state (
			shard_id TEXT PRIMARY KEY,
			revision INTEGER NOT NULL,
			snapshot_digest TEXT NOT NULL,
			input_fp TEXT NOT NULL,
			log_prefix_fp TEXT NOT NULL,
			last_seq INTEGER NOT NULL,
			row_version INTEGER NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS publications (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			shard_id TEXT NOT NULL,
			revision INTEGER NOT NULL,
			snapshot_digest TEXT NOT NULL,
			revision_fp TEXT NOT NULL,
			input_fp TEXT NOT NULL,
			log_prefix_fp TEXT NOT NULL,
			publisher TEXT NOT NULL,
			created_at TEXT NOT NULL,
			UNIQUE(shard_id, revision)
		)`,
		`CREATE TABLE IF NOT EXISTS audit_events (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			shard_id TEXT NOT NULL,
			revision INTEGER NOT NULL,
			kind TEXT NOT NULL,
			code TEXT NOT NULL,
			detail TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS upgrade_reports (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			shard_id TEXT NOT NULL,
			report_json TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS shard_logs (
			shard_id TEXT PRIMARY KEY,
			input_fp TEXT NOT NULL,
			source_sha256 TEXT NOT NULL,
			original_version INTEGER NOT NULL,
			log_json TEXT NOT NULL,
			upgrade_report_json TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("migrate sqlite: %w", err)
		}
	}
	return nil
}

func nowString() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func (s *Store) SaveCheckpoint(rec model.RevisionRecord, state *model.ReplayState, snap model.Snapshot) (int64, *model.BusinessError) {
	stateBlob, err := marshalZlib(state)
	if err != nil {
		return 0, model.NewBusinessError("E_CHECKPOINT_ENCODE", err.Error(), nil)
	}
	snapBlob, err := marshalZlib(snap)
	if err != nil {
		return 0, model.NewBusinessError("E_CHECKPOINT_ENCODE", err.Error(), nil)
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, model.NewBusinessError("E_CHECKPOINT_TX", err.Error(), nil)
	}
	defer tx.Rollback()
	var maxRevision int
	err = tx.QueryRow(`SELECT COALESCE(MAX(revision), 0) FROM checkpoints WHERE shard_id = ?`, state.ShardID).Scan(&maxRevision)
	if err != nil {
		return 0, model.NewBusinessError("E_CHECKPOINT_TX", err.Error(), nil)
	}
	if rec.Revision <= maxRevision {
		var existingSeq int64
		var existingDigest string
		err := tx.QueryRow(`SELECT seq, snapshot_digest FROM checkpoints WHERE shard_id = ? AND revision = ?`, state.ShardID, rec.Revision).Scan(&existingSeq, &existingDigest)
		if err == nil && existingDigest == rec.SnapshotDigest {
			return existingSeq, nil
		}
		return 0, model.WithContext(model.NewBusinessError("E_CHECKPOINT_CONFLICT", "checkpoint already exists with a different snapshot", map[string]any{
			"existing_revision": maxRevision, "existing_snapshot_digest": existingDigest,
		}), state.ShardID, rec.Revision)
	}
	res, err := tx.Exec(`INSERT INTO checkpoints
		(shard_id, revision, parent_revision, input_fp, log_prefix_fp, revision_fp, segment_digest, pixel_digest, snapshot_digest, state_blob, snapshot_blob, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		state.ShardID, rec.Revision, rec.ParentRevision, state.InputFingerprint, rec.LogPrefixFP, rec.RevisionFP,
		rec.SegmentDigest, rec.PixelDigest, rec.SnapshotDigest, stateBlob, snapBlob, nowString())
	if err != nil {
		return 0, model.WithContext(model.NewBusinessError("E_CHECKPOINT_TX", err.Error(), nil), state.ShardID, rec.Revision)
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return 0, model.NewBusinessError("E_CHECKPOINT_TX", err.Error(), nil)
	}
	if err := tx.Commit(); err != nil {
		return 0, model.WithContext(model.NewBusinessError("E_CHECKPOINT_TX", err.Error(), nil), state.ShardID, rec.Revision)
	}
	return seq, nil
}

func (s *Store) Latest(shardID string) (*Checkpoint, bool, error) {
	row := s.db.QueryRow(`SELECT seq, shard_id, revision, parent_revision, input_fp, log_prefix_fp, revision_fp,
		segment_digest, pixel_digest, snapshot_digest, state_blob, snapshot_blob, created_at
		FROM checkpoints WHERE shard_id = ? ORDER BY revision DESC LIMIT 1`, shardID)
	cp, err := scanCheckpoint(row)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return cp, true, nil
}

func (s *Store) At(shardID string, revision int) (*Checkpoint, bool, error) {
	row := s.db.QueryRow(`SELECT seq, shard_id, revision, parent_revision, input_fp, log_prefix_fp, revision_fp,
		segment_digest, pixel_digest, snapshot_digest, state_blob, snapshot_blob, created_at
		FROM checkpoints WHERE shard_id = ? AND revision = ?`, shardID, revision)
	cp, err := scanCheckpoint(row)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return cp, true, nil
}

func (s *Store) List(shardID string) ([]CheckpointMeta, error) {
	rows, err := s.db.Query(`SELECT seq, shard_id, revision, parent_revision, input_fp, log_prefix_fp, revision_fp,
		segment_digest, pixel_digest, snapshot_digest, created_at FROM checkpoints WHERE shard_id = ? ORDER BY revision`, shardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CheckpointMeta
	for rows.Next() {
		var c CheckpointMeta
		if err := rows.Scan(&c.Seq, &c.ShardID, &c.Revision, &c.ParentRevision, &c.InputFP, &c.LogPrefixFP,
			&c.RevisionFP, &c.SegmentDigest, &c.PixelDigest, &c.SnapshotDigest, &c.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) ListShards() ([]string, error) {
	rows, err := s.db.Query(`SELECT DISTINCT shard_id FROM checkpoints ORDER BY shard_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) DeleteFrom(shardID string, revision int) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM checkpoints WHERE shard_id = ? AND revision >= ?`, shardID, revision)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) Clear(shardID string) error {
	_, err := s.db.Exec(`DELETE FROM checkpoints WHERE shard_id = ?`, shardID)
	return err
}

func (s *Store) RecordAudit(shardID string, revision int, kind, code, detail string) error {
	_, err := s.db.Exec(`INSERT INTO audit_events(shard_id, revision, kind, code, detail, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		shardID, revision, kind, code, detail, nowString())
	return err
}

func (s *Store) RecordUpgrade(shardID string, report any) error {
	data, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO upgrade_reports(shard_id, report_json, created_at) VALUES (?, ?, ?)`, shardID, string(data), nowString())
	return err
}

func (s *Store) LoadLog(shardID string) (StoredLog, bool, error) {
	var out StoredLog
	err := s.db.QueryRow(`SELECT shard_id, input_fp, source_sha256, original_version, log_json, upgrade_report_json, updated_at
		FROM shard_logs WHERE shard_id = ?`, shardID).Scan(&out.ShardID, &out.InputFP, &out.SourceSHA256,
		&out.OriginalVersion, &out.LogJSON, &out.UpgradeReportJSON, &out.UpdatedAt)
	if err == sql.ErrNoRows {
		return StoredLog{}, false, nil
	}
	return out, err == nil, err
}

func (s *Store) SaveLog(log StoredLog) error {
	log.UpdatedAt = nowString()
	_, err := s.db.Exec(`INSERT INTO shard_logs(shard_id, input_fp, source_sha256, original_version, log_json, upgrade_report_json, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(shard_id) DO UPDATE SET input_fp=excluded.input_fp, source_sha256=excluded.source_sha256,
		original_version=excluded.original_version, log_json=excluded.log_json,
		upgrade_report_json=excluded.upgrade_report_json, updated_at=excluded.updated_at`,
		log.ShardID, log.InputFP, log.SourceSHA256, log.OriginalVersion, log.LogJSON, log.UpgradeReportJSON, log.UpdatedAt)
	return err
}

func scanCheckpoint(scanner interface{ Scan(...any) error }) (*Checkpoint, error) {
	var cp Checkpoint
	var stateBlob, snapBlob []byte
	if err := scanner.Scan(&cp.Seq, &cp.ShardID, &cp.Revision, &cp.ParentRevision, &cp.InputFP, &cp.LogPrefixFP,
		&cp.RevisionFP, &cp.SegmentDigest, &cp.PixelDigest, &cp.SnapshotDigest, &stateBlob, &snapBlob, &cp.CreatedAt); err != nil {
		return nil, err
	}
	var state model.ReplayState
	if err := unmarshalZlib(stateBlob, &state); err != nil {
		return nil, fmt.Errorf("decode checkpoint state: %w", err)
	}
	var snap model.Snapshot
	if err := unmarshalZlib(snapBlob, &snap); err != nil {
		return nil, fmt.Errorf("decode checkpoint snapshot: %w", err)
	}
	cp.State = &state
	cp.Snapshot = &snap
	return &cp, nil
}

func marshalZlib(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var b bytes.Buffer
	zw, err := zlib.NewWriterLevel(&b, zlib.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := zw.Write(data); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func unmarshalZlib(data []byte, v any) error {
	zr, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer zr.Close()
	raw, err := io.ReadAll(zr)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}
