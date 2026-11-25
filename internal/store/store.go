package store

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
	"time"

	"ocrlab/internal/apperr"
	"ocrlab/internal/canonical"
	"ocrlab/internal/model"
	"ocrlab/internal/workspace"

	_ "modernc.org/sqlite"
)

type Store struct {
	paths workspace.Paths
	db    *sql.DB
}

type ImageRecord struct {
	ImageID       string          `json:"image_id"`
	Group         string          `json:"group"`
	File          string          `json:"file"`
	Width         int             `json:"width"`
	Height        int             `json:"height"`
	Language      string          `json:"language"`
	ContentSHA256 string          `json:"content_sha256"`
	TruthDigest   string          `json:"truth_digest"`
	Spec          model.ImageSpec `json:"spec"`
}

type Checkpoint struct {
	ImageID       string
	Stage         string
	Seq           int64
	InputVersion  string
	ConfigVersion string
	ProductDigest string
	ArtifactRel   string
	Product       []byte
}

type PublishRequest struct {
	ImageID         string
	Stage           string
	Seq             int64
	ExpectedPrevSeq int64
	SequenceToken   string
	InputVersion    string
	ConfigVersion   string
	ProductDigest   string
	ArtifactRel     string
	Product         []byte
}

type ImportSummary struct {
	ManifestDigest string `json:"manifest_digest"`
	ImportedImages int    `json:"imported_images"`
	RemovedImages  int    `json:"removed_images"`
	Groups         int    `json:"groups"`
}

func Open(paths workspace.Paths) (*Store, error) {
	dsn := paths.DBPath + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	s := &Store{paths: paths, db: db}
	if err := s.initSchema(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) initSchema(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS groups (
			name TEXT PRIMARY KEY,
			target_shards INTEGER NOT NULL,
			shard_count INTEGER NOT NULL,
			total_characters INTEGER NOT NULL,
			manifest_digest TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS images (
			image_id TEXT PRIMARY KEY,
			group_name TEXT NOT NULL,
			file TEXT NOT NULL,
			width INTEGER NOT NULL,
			height INTEGER NOT NULL,
			language TEXT NOT NULL,
			content_sha256 TEXT NOT NULL,
			truth_digest TEXT NOT NULL,
			spec_json BLOB NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			image_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			seq INTEGER NOT NULL,
			input_version TEXT NOT NULL,
			config_version TEXT NOT NULL,
			product_digest TEXT NOT NULL,
			artifact_path TEXT NOT NULL,
			product_json BLOB NOT NULL,
			published_at TEXT NOT NULL,
			PRIMARY KEY (image_id, stage)
		)`,
		`CREATE TABLE IF NOT EXISTS shard_state (
			image_id TEXT PRIMARY KEY,
			next_seq INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS query_cache (
			cache_key TEXT PRIMARY KEY,
			image_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			result_json BLOB NOT NULL,
			result_digest TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_query_cache_image ON query_cache(image_id, stage)`,
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}
	return tx.Commit()
}

func (s *Store) ImportManifest(ctx context.Context, manifest model.Manifest, manifestDigest string) (ImportSummary, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ImportSummary{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('manifest_digest',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, manifestDigest); err != nil {
		return ImportSummary{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('seed',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, fmt.Sprintf("%d", manifest.Seed)); err != nil {
		return ImportSummary{}, err
	}
	existingRows, err := tx.QueryContext(ctx, `SELECT image_id FROM images`)
	if err != nil {
		return ImportSummary{}, err
	}
	existing := make(map[string]struct{})
	for existingRows.Next() {
		var id string
		if err := existingRows.Scan(&id); err != nil {
			existingRows.Close()
			return ImportSummary{}, err
		}
		existing[id] = struct{}{}
	}
	if err := existingRows.Close(); err != nil {
		return ImportSummary{}, err
	}
	keep := make(map[string]struct{}, len(manifest.Images))
	for _, group := range manifest.Groups {
		if _, err := tx.ExecContext(ctx, `INSERT INTO groups(name,target_shards,shard_count,total_characters,manifest_digest)
			VALUES(?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET target_shards=excluded.target_shards,shard_count=excluded.shard_count,total_characters=excluded.total_characters,manifest_digest=excluded.manifest_digest`,
			group.Name, group.TargetShards, group.Observed.ShardCount, group.Observed.TotalCharacters, manifestDigest); err != nil {
			return ImportSummary{}, err
		}
	}
	for _, imageSpec := range manifest.Images {
		keep[imageSpec.ImageID] = struct{}{}
		imagePath, err := s.paths.ImagePath(imageSpec.File)
		if err != nil {
			return ImportSummary{}, err
		}
		content, err := os.ReadFile(imagePath)
		if err != nil {
			return ImportSummary{}, fmt.Errorf("read sample %s: %w", imagePath, err)
		}
		truthDigest, err := canonical.Digest(imageSpec.Truth)
		if err != nil {
			return ImportSummary{}, err
		}
		specJSON, err := canonical.Marshal(imageSpec)
		if err != nil {
			return ImportSummary{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO images(image_id,group_name,file,width,height,language,content_sha256,truth_digest,spec_json)
			VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(image_id) DO UPDATE SET group_name=excluded.group_name,file=excluded.file,width=excluded.width,height=excluded.height,language=excluded.language,content_sha256=excluded.content_sha256,truth_digest=excluded.truth_digest,spec_json=excluded.spec_json`,
			imageSpec.ImageID, imageSpec.Group, imageSpec.File, imageSpec.Width, imageSpec.Height, imageSpec.Language,
			canonical.DigestBytes(content), truthDigest, specJSON); err != nil {
			return ImportSummary{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO shard_state(image_id,next_seq) VALUES(?,1)`, imageSpec.ImageID); err != nil {
			return ImportSummary{}, err
		}
	}
	removed := 0
	ids := make([]string, 0)
	for id := range existing {
		if _, exists := keep[id]; !exists {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		removed++
		for _, statement := range []string{
			`DELETE FROM checkpoints WHERE image_id=?`,
			`DELETE FROM shard_state WHERE image_id=?`,
			`DELETE FROM query_cache WHERE image_id=?`,
			`DELETE FROM images WHERE image_id=?`,
		} {
			if _, err := tx.ExecContext(ctx, statement, id); err != nil {
				return ImportSummary{}, err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return ImportSummary{}, err
	}
	return ImportSummary{ManifestDigest: manifestDigest, ImportedImages: len(manifest.Images), RemovedImages: removed, Groups: len(manifest.Groups)}, nil
}

func (s *Store) GetImage(ctx context.Context, imageID string) (ImageRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT image_id,group_name,file,width,height,language,content_sha256,truth_digest,spec_json FROM images WHERE image_id=?`, imageID)
	var record ImageRecord
	var specJSON []byte
	if err := row.Scan(&record.ImageID, &record.Group, &record.File, &record.Width, &record.Height, &record.Language, &record.ContentSHA256, &record.TruthDigest, &specJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			e := apperr.WithImage(apperr.CodeNotFound, "image is not imported", imageID)
			return ImageRecord{}, e
		}
		return ImageRecord{}, err
	}
	if err := json.Unmarshal(specJSON, &record.Spec); err != nil {
		return ImageRecord{}, err
	}
	return record, nil
}

func (s *Store) ListImages(ctx context.Context, groups []string) ([]ImageRecord, error) {
	query := `SELECT image_id,group_name,file,width,height,language,content_sha256,truth_digest,spec_json FROM images`
	args := make([]any, 0)
	if len(groups) > 0 {
		placeholders := make([]string, len(groups))
		for i, group := range groups {
			placeholders[i] = "?"
			args = append(args, group)
		}
		query += ` WHERE group_name IN (` + strings.Join(placeholders, ",") + `)`
	}
	query += ` ORDER BY group_name,image_id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ImageRecord, 0)
	for rows.Next() {
		var record ImageRecord
		var specJSON []byte
		if err := rows.Scan(&record.ImageID, &record.Group, &record.File, &record.Width, &record.Height, &record.Language, &record.ContentSHA256, &record.TruthDigest, &specJSON); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(specJSON, &record.Spec); err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *Store) GetCheckpoint(ctx context.Context, imageID, stage string) (Checkpoint, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT image_id,stage,seq,input_version,config_version,product_digest,artifact_path,product_json FROM checkpoints WHERE image_id=? AND stage=?`, imageID, stage)
	var checkpoint Checkpoint
	if err := row.Scan(&checkpoint.ImageID, &checkpoint.Stage, &checkpoint.Seq, &checkpoint.InputVersion, &checkpoint.ConfigVersion, &checkpoint.ProductDigest, &checkpoint.ArtifactRel, &checkpoint.Product); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Checkpoint{}, false, nil
		}
		return Checkpoint{}, false, err
	}
	if canonical.DigestBytes(checkpoint.Product) != checkpoint.ProductDigest {
		return Checkpoint{}, false, staleError(imageID, stage, "digest_mismatch", "stored checkpoint product digest does not match its SQLite payload")
	}
	artifactBytes, err := os.ReadFile(filepath.Join(s.paths.Root, filepath.FromSlash(checkpoint.ArtifactRel)))
	if err != nil {
		if os.IsNotExist(err) {
			return Checkpoint{}, false, staleError(imageID, stage, "artifact_missing", "published artifact file is missing")
		}
		return Checkpoint{}, false, err
	}
	if canonical.DigestBytes(artifactBytes) != checkpoint.ProductDigest {
		return Checkpoint{}, false, staleError(imageID, stage, "digest_mismatch", "artifact file digest does not match the checkpoint")
	}
	return checkpoint, true, nil
}

func (s *Store) NextSequence(ctx context.Context, imageID string) (next int64, previous int64, token string, err error) {
	if _, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO shard_state(image_id,next_seq) VALUES(?,1)`, imageID); err != nil {
		return 0, 0, "", err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT next_seq FROM shard_state WHERE image_id=?`, imageID).Scan(&next); err != nil {
		return 0, 0, "", err
	}
	return next, next - 1, SequenceToken(imageID, next), nil
}

func SequenceToken(imageID string, seq int64) string {
	digest, _ := canonical.Digest(struct {
		ImageID string `json:"image_id"`
		Seq     int64  `json:"seq"`
	}{ImageID: imageID, Seq: seq})
	return "seq-" + digest[:24]
}

func (s *Store) Publish(ctx context.Context, request PublishRequest) error {
	if _, ok := model.StageOrder[request.Stage]; !ok {
		return apperr.New(apperr.CodeInvalidArgument, "unknown stage: "+request.Stage)
	}
	if canonical.DigestBytes(request.Product) != request.ProductDigest {
		return apperr.New(apperr.CodeInvalidArgument, "product digest does not match product bytes")
	}
	unlock, err := acquireFileLock(s.paths.LockPath)
	if err != nil {
		return apperr.Wrap(apperr.CodeStorage, "acquire publish lock", err)
	}
	defer unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return apperr.Wrap(apperr.CodeStorage, "begin checkpoint transaction", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO shard_state(image_id,next_seq) VALUES(?,1)`, request.ImageID); err != nil {
		return err
	}
	var nextSeq int64
	if err := tx.QueryRowContext(ctx, `SELECT next_seq FROM shard_state WHERE image_id=?`, request.ImageID).Scan(&nextSeq); err != nil {
		return err
	}
	if request.ExpectedPrevSeq != request.Seq-1 {
		return staleError(request.ImageID, request.Stage, "sequence_misaligned", fmt.Sprintf("expected previous sequence %d is not adjacent to publish sequence %d", request.ExpectedPrevSeq, request.Seq))
	}
	if request.SequenceToken != SequenceToken(request.ImageID, request.Seq) {
		return staleError(request.ImageID, request.Stage, "cross_shard_sequence", "sequence token belongs to a different shard or sequence")
	}
	if request.Seq < nextSeq {
		return staleError(request.ImageID, request.Stage, "sequence_rollback", fmt.Sprintf("publish sequence %d is behind next sequence %d", request.Seq, nextSeq))
	}
	if request.Seq > nextSeq {
		return staleError(request.ImageID, request.Stage, "sequence_gap", fmt.Sprintf("publish sequence %d skips expected sequence %d", request.Seq, nextSeq))
	}
	var existingSeq int64
	var existingDigest, existingInput string
	err = tx.QueryRowContext(ctx, `SELECT seq,product_digest,input_version FROM checkpoints WHERE image_id=? AND stage=?`, request.ImageID, request.Stage).Scan(&existingSeq, &existingDigest, &existingInput)
	if err == nil {
		reason := "duplicate_checkpoint"
		if existingDigest != request.ProductDigest {
			reason = "checkpoint_digest_mismatch"
		} else if existingInput != request.InputVersion {
			reason = "checkpoint_fingerprint_changed"
		}
		return staleError(request.ImageID, request.Stage, reason, "stage checkpoint already exists")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	stageOrder := model.StageOrder[request.Stage]
	rows, err := tx.QueryContext(ctx, `SELECT stage FROM checkpoints WHERE image_id=?`, request.ImageID)
	if err != nil {
		return err
	}
	existingStages := make([]string, 0)
	for rows.Next() {
		var stage string
		if err := rows.Scan(&stage); err != nil {
			rows.Close()
			return err
		}
		existingStages = append(existingStages, stage)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, stage := range existingStages {
		if model.StageOrder[stage] >= stageOrder {
			return staleError(request.ImageID, request.Stage, "stage_order_conflict", "a same-or-later stage checkpoint must be invalidated before this publish")
		}
	}
	required := []string{model.StageDetect, model.StageRecognize, model.StageStrategy, model.StageDiagnostic}
	for _, stage := range required[:stageOrder-1] {
		found := false
		for _, existing := range existingStages {
			if existing == stage {
				found = true
				break
			}
		}
		if !found {
			return staleError(request.ImageID, request.Stage, "missing_upstream_stage", "required upstream checkpoint "+stage+" is absent")
		}
	}
	artifactPath, err := safeJoin(s.paths.Root, request.ArtifactRel)
	if err != nil {
		return err
	}
	if err := writeArtifactAtomic(artifactPath, request.Product); err != nil {
		return apperr.Wrap(apperr.CodeStorage, "publish artifact file", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoints(image_id,stage,seq,input_version,config_version,product_digest,artifact_path,product_json,published_at)
		VALUES(?,?,?,?,?,?,?,?,?)`, request.ImageID, request.Stage, request.Seq, request.InputVersion, request.ConfigVersion, request.ProductDigest, request.ArtifactRel, request.Product, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE shard_state SET next_seq=? WHERE image_id=? AND next_seq=?`, request.Seq+1, request.ImageID, request.Seq)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return staleError(request.ImageID, request.Stage, "conditional_update_lost", "shard sequence conditional update did not match")
	}
	if err := tx.Commit(); err != nil {
		return staleError(request.ImageID, request.Stage, "commit_conflict", "checkpoint transaction lost a concurrent publish: "+err.Error())
	}
	return nil
}

func (s *Store) InvalidateFrom(ctx context.Context, imageID, earliestStage string, removeFiles bool) error {
	order, ok := model.StageOrder[earliestStage]
	if !ok {
		return apperr.New(apperr.CodeInvalidArgument, "unknown stage: "+earliestStage)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT stage,artifact_path FROM checkpoints WHERE image_id=?`, imageID)
	if err != nil {
		return err
	}
	type removed struct{ stage, artifact string }
	removedRows := make([]removed, 0)
	for rows.Next() {
		var stage, artifact string
		if err := rows.Scan(&stage, &artifact); err != nil {
			rows.Close()
			return err
		}
		if model.StageOrder[stage] >= order {
			removedRows = append(removedRows, removed{stage: stage, artifact: artifact})
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, item := range removedRows {
		if _, err := tx.ExecContext(ctx, `DELETE FROM checkpoints WHERE image_id=? AND stage=?`, imageID, item.stage); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM query_cache`); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if removeFiles {
		for _, item := range removedRows {
			path, err := safeJoin(s.paths.Root, item.artifact)
			if err == nil {
				_ = os.Remove(path)
			}
		}
	}
	return nil
}

func (s *Store) CacheGet(ctx context.Context, key string) ([]byte, bool, error) {
	var result []byte
	var digest string
	err := s.db.QueryRowContext(ctx, `SELECT result_json,result_digest FROM query_cache WHERE cache_key=?`, key).Scan(&result, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if canonical.DigestBytes(result) != digest {
		return nil, false, staleError("", "", "cache_digest_mismatch", "query cache payload digest is invalid")
	}
	return result, true, nil
}

func (s *Store) CacheSet(ctx context.Context, key, imageID, stage string, result []byte) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO query_cache(cache_key,image_id,stage,result_json,result_digest,created_at)
		VALUES(?,?,?,?,?,?) ON CONFLICT(cache_key) DO UPDATE SET result_json=excluded.result_json,result_digest=excluded.result_digest,created_at=excluded.created_at`,
		key, imageID, stage, result, canonical.DigestBytes(result), time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) Metadata(ctx context.Context, key string) (string, bool, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key=?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return value, err == nil, err
}

func (s *Store) AllCheckpoints(ctx context.Context) ([]Checkpoint, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT image_id,stage,seq,input_version,config_version,product_digest,artifact_path,product_json FROM checkpoints ORDER BY image_id,stage`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Checkpoint, 0)
	for rows.Next() {
		var checkpoint Checkpoint
		if err := rows.Scan(&checkpoint.ImageID, &checkpoint.Stage, &checkpoint.Seq, &checkpoint.InputVersion, &checkpoint.ConfigVersion, &checkpoint.ProductDigest, &checkpoint.ArtifactRel, &checkpoint.Product); err != nil {
			return nil, err
		}
		result = append(result, checkpoint)
	}
	return result, rows.Err()
}

func (s *Store) ClearOutputs(ctx context.Context) error {
	unlock, err := acquireFileLock(s.paths.LockPath)
	if err != nil {
		return err
	}
	defer unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range []string{`DELETE FROM checkpoints`, `DELETE FROM query_cache`, `UPDATE shard_state SET next_seq=1`} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return os.RemoveAll(s.paths.ArtifactsDir)
}

func (s *Store) Paths() workspace.Paths { return s.paths }

func staleError(imageID, stage, reason, message string) *apperr.Error {
	return &apperr.Error{Code: apperr.CodeStaleCheckpoint, Message: message, ImageID: imageID, Stage: stage, Reason: reason}
}

func safeJoin(root, relative string) (string, error) {
	if filepath.IsAbs(relative) {
		return "", apperr.New(apperr.CodeInvalidArgument, "artifact path must be relative")
	}
	clean := filepath.Clean(filepath.FromSlash(relative))
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", apperr.New(apperr.CodeInvalidArgument, "artifact path escapes workspace")
	}
	return filepath.Join(root, clean), nil
}

func writeArtifactAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".artifact-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
