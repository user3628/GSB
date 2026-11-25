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
	"sync"
	"time"

	"layoutread/internal/model"

	_ "modernc.org/sqlite"
)

const (
	ClassificationAdvance     = "advance"
	ClassificationDuplicate   = "duplicate"
	ClassificationRollback    = "rollback"
	ClassificationReplacement = "replacement"
	schemaVersion             = "1"
)

var pathLocks sync.Map

type Checkpoint struct {
	DocID               string `json:"doc_id"`
	Stage               string `json:"stage"`
	StageOrdinal        int64  `json:"stage_ordinal"`
	Seq                 int64  `json:"seq"`
	InputFingerprint    string `json:"input_fingerprint"`
	StrategyVersion     int64  `json:"strategy_version"`
	ArtifactSummary     string `json:"artifact_summary"`
	ArtifactFingerprint string `json:"artifact_fingerprint"`
	ArtifactJSON        string `json:"artifact_json"`
	ContentFingerprint  string `json:"content_fingerprint,omitempty"`
	CreatedAt           string `json:"-"`
}

type DocumentState struct {
	DocID              string `json:"doc_id"`
	ContentFingerprint string `json:"content_fingerprint"`
	StrategyVersion    int64  `json:"strategy_version"`
	Seq                int64  `json:"seq"`
	UpdatedAt          string `json:"-"`
}

type PublishResult struct {
	DocID      string `json:"doc_id"`
	Stage      string `json:"stage"`
	Seq        int64  `json:"seq"`
	Class      string `json:"class"`
	SnapDigest string `json:"snapshot_digest"`
}

type Store struct {
	db        *sql.DB
	path      string
	lockPath  string
	guardPath string

	failpoint func(stage string) error
}

type guardDoc struct {
	DocID string `json:"doc_id"`
	Seq   int64  `json:"seq"`
}

type guardState struct {
	SnapshotDigest string     `json:"snapshot_digest"`
	Documents      []guardDoc `json:"documents"`
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, model.NewError(model.CodeInvalidRequest, "database path is required", "")
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	dsn := path
	sep := "?"
	if len(path) > 0 && (contains(path, "?")) {
		sep = "&"
	}
	dsn += sep + "_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)"
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
	s := &Store{
		db:        db,
		path:      path,
		lockPath:  path + ".lock",
		guardPath: path + ".guard.json",
	}
	if err := s.ensureSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.verifyOnOpen(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Path() string { return s.path }

func (s *Store) ensureSchema(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	statements := []string{
		`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS documents (
			doc_id TEXT PRIMARY KEY,
			content_fingerprint TEXT NOT NULL,
			strategy_version INTEGER NOT NULL,
			seq INTEGER NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS stage_checkpoints (
			doc_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			stage_ordinal INTEGER NOT NULL,
			seq INTEGER NOT NULL,
			input_fingerprint TEXT NOT NULL,
			strategy_version INTEGER NOT NULL,
			artifact_summary TEXT NOT NULL,
			artifact_fingerprint TEXT NOT NULL,
			artifact_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			PRIMARY KEY (doc_id, stage)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_stage_checkpoints_doc_seq ON stage_checkpoints(doc_id, seq)`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES('schema_version',?) ON CONFLICT(key) DO NOTHING`, schemaVersion); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) withBusinessLock(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	value, _ := pathLocks.LoadOrStore(s.path, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()
	lock, err := acquireFileLock(s.lockPath)
	if err != nil {
		return err
	}
	defer lock.release()
	return fn()
}

func (s *Store) verifyOnOpen(ctx context.Context) error {
	return s.withBusinessLock(ctx, func() error {
		digest, docs, err := s.snapshotState(ctx)
		if err != nil {
			return err
		}
		guard, err := readGuard(s.guardPath)
		if errors.Is(err, os.ErrNotExist) {
			return writeGuard(s.guardPath, guardState{SnapshotDigest: digest, Documents: docs})
		}
		if err != nil {
			return model.NewError(model.CodeSnapshotReplaced, "checkpoint guard file cannot be read", "")
		}
		class := compareGuard(guard, guardState{SnapshotDigest: digest, Documents: docs})
		switch class {
		case ClassificationAdvance:
			return writeGuard(s.guardPath, guardState{SnapshotDigest: digest, Documents: docs})
		case ClassificationDuplicate:
			return nil
		case ClassificationRollback:
			return &model.Error{Code: model.CodeCheckpointRollback, Message: "checkpoint snapshot sequence moved backwards"}
		case ClassificationReplacement:
			return &model.Error{Code: model.CodeSnapshotReplaced, Message: "checkpoint snapshot was replaced without a sequence advance"}
		default:
			return &model.Error{Code: model.CodeSnapshotReplaced, Message: "unknown checkpoint snapshot state"}
		}
	})
}

func compareGuard(previous, current guardState) string {
	previousByDoc := make(map[string]int64, len(previous.Documents))
	for _, doc := range previous.Documents {
		previousByDoc[doc.DocID] = doc.Seq
	}
	advanced := false
	for _, doc := range current.Documents {
		previousSeq, ok := previousByDoc[doc.DocID]
		if !ok {
			advanced = true
			continue
		}
		if doc.Seq < previousSeq {
			return ClassificationRollback
		}
		if doc.Seq > previousSeq {
			advanced = true
		}
		delete(previousByDoc, doc.DocID)
	}
	if len(previousByDoc) > 0 {
		return ClassificationRollback
	}
	if advanced {
		return ClassificationAdvance
	}
	if previous.SnapshotDigest == current.SnapshotDigest {
		return ClassificationDuplicate
	}
	return ClassificationReplacement
}

func readGuard(path string) (guardState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return guardState{}, err
	}
	var guard guardState
	if err := json.Unmarshal(data, &guard); err != nil {
		return guardState{}, err
	}
	sort.Slice(guard.Documents, func(i, j int) bool { return guard.Documents[i].DocID < guard.Documents[j].DocID })
	return guard, nil
}

func writeGuard(path string, guard guardState) error {
	sort.Slice(guard.Documents, func(i, j int) bool { return guard.Documents[i].DocID < guard.Documents[j].DocID })
	data, err := json.Marshal(guard)
	if err != nil {
		return err
	}
	temp := path + ".tmp"
	if err := os.WriteFile(temp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(temp, path)
}

func (s *Store) NextSequence(ctx context.Context, docID string) (int64, error) {
	var seq int64
	err := s.db.QueryRowContext(ctx, `SELECT seq FROM documents WHERE doc_id=?`, docID).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	return seq + 1, nil
}

func (s *Store) Publish(ctx context.Context, checkpoint Checkpoint) (PublishResult, error) {
	if checkpoint.DocID == "" || checkpoint.Stage == "" || model.StageNumber(checkpoint.Stage) == 0 || checkpoint.Seq <= 0 {
		return PublishResult{}, model.NewError(model.CodeInvalidRequest, "checkpoint must include doc_id, known stage and positive sequence", checkpoint.DocID)
	}
	if checkpoint.ArtifactFingerprint == "" {
		checkpoint.ArtifactFingerprint = model.DigestBytes([]byte(checkpoint.ArtifactJSON))
	}
	if checkpoint.ArtifactSummary == "" {
		checkpoint.ArtifactSummary = checkpoint.ArtifactFingerprint
	}
	actualFingerprint := model.DigestBytes([]byte(checkpoint.ArtifactJSON))
	if actualFingerprint != checkpoint.ArtifactFingerprint || checkpoint.ArtifactSummary != checkpoint.ArtifactFingerprint {
		return PublishResult{}, &model.Error{
			Code: model.CodeCheckpointCorrupt, Message: "artifact summary or fingerprint does not match artifact content", DocID: checkpoint.DocID, Stage: checkpoint.Stage,
		}
	}
	if checkpoint.ContentFingerprint == "" {
		checkpoint.ContentFingerprint = checkpoint.InputFingerprint
	}
	var result PublishResult
	err := s.withBusinessLock(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var currentSeq int64
		err = tx.QueryRowContext(ctx, `SELECT seq FROM documents WHERE doc_id=?`, checkpoint.DocID).Scan(&currentSeq)
		if errors.Is(err, sql.ErrNoRows) {
			currentSeq = 0
		} else if err != nil {
			return err
		}
		var existingSeq int64
		var existingSummary, existingFingerprint, existingInput string
		existingErr := tx.QueryRowContext(ctx,
			`SELECT seq, artifact_summary, artifact_fingerprint, input_fingerprint FROM stage_checkpoints WHERE doc_id=? AND stage=?`,
			checkpoint.DocID, checkpoint.Stage).Scan(&existingSeq, &existingSummary, &existingFingerprint, &existingInput)
		hasExisting := existingErr == nil
		if existingErr != nil && !errors.Is(existingErr, sql.ErrNoRows) {
			return existingErr
		}
		if hasExisting {
			if existingSeq >= checkpoint.Seq {
				message := "checkpoint sequence is duplicate or behind the current document sequence"
				if existingSeq == checkpoint.Seq && existingSummary == checkpoint.ArtifactSummary &&
					existingFingerprint == checkpoint.ArtifactFingerprint && existingInput == checkpoint.InputFingerprint {
					message = "duplicate checkpoint publication"
				}
				return staleError(checkpoint, message, existingSeq, currentSeq, ClassificationDuplicate)
			}
		}
		if checkpoint.Seq < currentSeq+1 {
			return staleError(checkpoint, "checkpoint sequence moved backwards", 0, currentSeq, ClassificationRollback)
		}
		if checkpoint.Seq > currentSeq+1 {
			return staleError(checkpoint, "checkpoint sequence is not contiguous for this document", 0, currentSeq, ClassificationReplacement)
		}
		stale, err := s.checkExistingSnapshotIntegrity(ctx, tx)
		if err != nil {
			return err
		}
		if stale {
			return &model.Error{Code: model.CodeSnapshotReplaced, Message: "database checkpoint snapshot integrity check failed", DocID: checkpoint.DocID}
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		stageOrdinal := model.StageNumber(checkpoint.Stage)
		_, err = tx.ExecContext(ctx, `
			INSERT INTO stage_checkpoints(doc_id,stage,stage_ordinal,seq,input_fingerprint,strategy_version,artifact_summary,artifact_fingerprint,artifact_json,created_at)
			VALUES(?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(doc_id,stage) DO UPDATE SET
				stage_ordinal=excluded.stage_ordinal,
				seq=excluded.seq,
				input_fingerprint=excluded.input_fingerprint,
				strategy_version=excluded.strategy_version,
				artifact_summary=excluded.artifact_summary,
				artifact_fingerprint=excluded.artifact_fingerprint,
				artifact_json=excluded.artifact_json,
				created_at=excluded.created_at`,
			checkpoint.DocID, checkpoint.Stage, stageOrdinal, checkpoint.Seq, checkpoint.InputFingerprint,
			checkpoint.StrategyVersion, checkpoint.ArtifactSummary, checkpoint.ArtifactFingerprint, checkpoint.ArtifactJSON, now)
		if err != nil {
			return err
		}
		if currentSeq == 0 {
			_, err = tx.ExecContext(ctx, `
				INSERT INTO documents(doc_id,content_fingerprint,strategy_version,seq,updated_at)
				VALUES(?,?,?,?,?)`,
				checkpoint.DocID, checkpoint.ContentFingerprint, checkpoint.StrategyVersion, checkpoint.Seq, now)
			if err != nil {
				return err
			}
		} else {
			result, err := tx.ExecContext(ctx, `
				UPDATE documents SET content_fingerprint=?,strategy_version=?,seq=?,updated_at=?
				WHERE doc_id=? AND seq=?`,
				checkpoint.ContentFingerprint, checkpoint.StrategyVersion, checkpoint.Seq, now, checkpoint.DocID, currentSeq)
			if err != nil {
				return err
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if affected != 1 {
				return staleError(checkpoint, "conditional document sequence update was rejected", 0, currentSeq, ClassificationRollback)
			}
		}
		if s.failpoint != nil {
			if err := s.failpoint(checkpoint.Stage); err != nil {
				return err
			}
		}
		digest, docs, err := snapshotStateTx(ctx, tx)
		if err != nil {
			return err
		}
		if err := setMetaTx(ctx, tx, "snapshot_digest", digest); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		if err := writeGuard(s.guardPath, guardState{SnapshotDigest: digest, Documents: docs}); err != nil {
			return &model.Error{Code: model.CodeSnapshotReplaced, Message: "checkpoint committed but guard could not be advanced: " + err.Error(), DocID: checkpoint.DocID}
		}
		result = PublishResult{
			DocID: checkpoint.DocID, Stage: checkpoint.Stage, Seq: checkpoint.Seq, Class: ClassificationAdvance, SnapDigest: digest,
		}
		return nil
	})
	return result, err
}

func staleError(checkpoint Checkpoint, message string, existingSeq, currentSeq int64, class string) *model.Error {
	return &model.Error{
		Code:    model.CodeStaleCheckpoint,
		Message: fmt.Sprintf("%s (stage=%s requested=%d existing=%d document_head=%d class=%s)", message, checkpoint.Stage, checkpoint.Seq, existingSeq, currentSeq, class),
		DocID:   checkpoint.DocID,
		Stage:   checkpoint.Stage,
	}
}

func (s *Store) checkExistingSnapshotIntegrity(ctx context.Context, tx *sql.Tx) (bool, error) {
	var stored string
	err := tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='snapshot_digest'`).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	digest, _, err := snapshotStateTx(ctx, tx)
	if err != nil {
		return false, err
	}
	return stored != digest, nil
}

func setMetaTx(ctx context.Context, tx *sql.Tx, key, value string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

func (s *Store) snapshotState(ctx context.Context) (string, []guardDoc, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", nil, err
	}
	defer tx.Rollback()
	return snapshotStateTx(ctx, tx)
}

func snapshotStateTx(ctx context.Context, tx *sql.Tx) (string, []guardDoc, error) {
	rows, err := tx.QueryContext(ctx, `SELECT doc_id,content_fingerprint,strategy_version,seq FROM documents ORDER BY doc_id`)
	if err != nil {
		return "", nil, err
	}
	defer rows.Close()
	docs := make([]guardDoc, 0)
	digest := model.DigestBytes(nil)
	hashInput := make([]byte, 0)
	for rows.Next() {
		var docID, contentFP string
		var strategyVersion, seq int64
		if err := rows.Scan(&docID, &contentFP, &strategyVersion, &seq); err != nil {
			return "", nil, err
		}
		docs = append(docs, guardDoc{DocID: docID, Seq: seq})
		hashInput = append(hashInput, []byte(fmt.Sprintf("D|%s|%s|%d|%d\n", docID, contentFP, strategyVersion, seq))...)
	}
	if err := rows.Err(); err != nil {
		return "", nil, err
	}
	rows.Close()
	stageRows, err := tx.QueryContext(ctx, `SELECT doc_id,stage,stage_ordinal,seq,input_fingerprint,strategy_version,artifact_summary,artifact_fingerprint,artifact_json FROM stage_checkpoints ORDER BY doc_id,stage_ordinal,stage`)
	if err != nil {
		return "", nil, err
	}
	defer stageRows.Close()
	for stageRows.Next() {
		var docID, stage, inputFP, summary, artifactFP, artifactJSON string
		var stageOrdinal, seq, strategyVersion int64
		if err := stageRows.Scan(&docID, &stage, &stageOrdinal, &seq, &inputFP, &strategyVersion, &summary, &artifactFP, &artifactJSON); err != nil {
			return "", nil, err
		}
		hashInput = append(hashInput, []byte(fmt.Sprintf("S|%s|%s|%d|%d|%s|%d|%s|%s|%s\n", docID, stage, stageOrdinal, seq, inputFP, strategyVersion, summary, artifactFP, artifactJSON))...)
	}
	if err := stageRows.Err(); err != nil {
		return "", nil, err
	}
	stageRows.Close()
	metaRows, err := tx.QueryContext(ctx, `SELECT key,value FROM meta WHERE key IN ('strategy_version','strategy_fingerprint','strategy_json','analysis_pending') ORDER BY key`)
	if err != nil {
		return "", nil, err
	}
	defer metaRows.Close()
	for metaRows.Next() {
		var key, value string
		if err := metaRows.Scan(&key, &value); err != nil {
			return "", nil, err
		}
		hashInput = append(hashInput, []byte(fmt.Sprintf("M|%s|%s\n", key, value))...)
	}
	if err := metaRows.Err(); err != nil {
		return "", nil, err
	}
	if len(hashInput) > 0 {
		digest = model.DigestBytes(hashInput)
	}
	return digest, docs, nil
}

func (s *Store) SaveStrategy(ctx context.Context, strategy model.Strategy) error {
	if err := strategy.Validate(); err != nil {
		return err
	}
	fingerprint := strategy.Fingerprint()
	encoded, err := json.Marshal(strategy)
	if err != nil {
		return err
	}
	return s.withBusinessLock(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var existingVersion int64
		var existingFingerprint string
		err = tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='strategy_version'`).Scan(&existingVersion)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			_ = tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='strategy_fingerprint'`).Scan(&existingFingerprint)
			if strategy.Version < existingVersion {
				return &model.Error{Code: model.CodeStrategyInvalid, Message: "strategy version must be monotonic", Stage: "strategy"}
			}
			if strategy.Version == existingVersion && existingFingerprint != "" && existingFingerprint != fingerprint {
				return &model.Error{Code: model.CodeStrategyInvalid, Message: "strategy content changed without a version increment", Stage: "strategy"}
			}
			if strategy.Version == existingVersion && existingFingerprint == fingerprint {
				return nil
			}
		}
		if err := setMetaTx(ctx, tx, "strategy_version", fmt.Sprintf("%d", strategy.Version)); err != nil {
			return err
		}
		if err := setMetaTx(ctx, tx, "strategy_fingerprint", fingerprint); err != nil {
			return err
		}
		if err := setMetaTx(ctx, tx, "strategy_json", string(encoded)); err != nil {
			return err
		}
		digest, docs, err := snapshotStateTx(ctx, tx)
		if err != nil {
			return err
		}
		if err := setMetaTx(ctx, tx, "snapshot_digest", digest); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return writeGuard(s.guardPath, guardState{SnapshotDigest: digest, Documents: docs})
	})
}

func (s *Store) LoadStrategy(ctx context.Context) (model.Strategy, bool, error) {
	var versionText, encoded string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='strategy_version'`).Scan(&versionText)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Strategy{}, false, nil
	}
	if err != nil {
		return model.Strategy{}, false, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='strategy_json'`).Scan(&encoded); err != nil {
		return model.Strategy{}, false, err
	}
	var strategy model.Strategy
	if err := json.Unmarshal([]byte(encoded), &strategy); err != nil {
		return model.Strategy{}, false, err
	}
	return strategy, true, nil
}

func (s *Store) SetAnalysisPending(ctx context.Context, pending bool) error {
	return s.withBusinessLock(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		value := "0"
		if pending {
			value = "1"
		}
		if err := setMetaTx(ctx, tx, "analysis_pending", value); err != nil {
			return err
		}
		digest, docs, err := snapshotStateTx(ctx, tx)
		if err != nil {
			return err
		}
		if err := setMetaTx(ctx, tx, "snapshot_digest", digest); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return writeGuard(s.guardPath, guardState{SnapshotDigest: digest, Documents: docs})
	})
}

func (s *Store) AnalysisPending(ctx context.Context) (bool, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='analysis_pending'`).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return value == "1", nil
}

func (s *Store) GetCheckpoint(ctx context.Context, docID, stage string) (Checkpoint, bool, error) {
	checkpoint := Checkpoint{}
	err := s.db.QueryRowContext(ctx, `
		SELECT doc_id,stage,stage_ordinal,seq,input_fingerprint,strategy_version,artifact_summary,artifact_fingerprint,artifact_json,created_at
		FROM stage_checkpoints WHERE doc_id=? AND stage=?`, docID, stage).Scan(
		&checkpoint.DocID, &checkpoint.Stage, &checkpoint.StageOrdinal, &checkpoint.Seq,
		&checkpoint.InputFingerprint, &checkpoint.StrategyVersion, &checkpoint.ArtifactSummary,
		&checkpoint.ArtifactFingerprint, &checkpoint.ArtifactJSON, &checkpoint.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Checkpoint{}, false, nil
	}
	if err != nil {
		return Checkpoint{}, false, err
	}
	if got := model.DigestBytes([]byte(checkpoint.ArtifactJSON)); got != checkpoint.ArtifactFingerprint {
		return checkpoint, true, &model.Error{
			Code: model.CodeCheckpointCorrupt, Message: "checkpoint artifact summary does not match stored artifact", DocID: docID, Stage: stage,
		}
	}
	return checkpoint, true, nil
}

func (s *Store) ListCheckpoints(ctx context.Context, docID string) ([]Checkpoint, error) {
	query := `SELECT doc_id,stage,stage_ordinal,seq,input_fingerprint,strategy_version,artifact_summary,artifact_fingerprint,artifact_json,created_at FROM stage_checkpoints`
	args := []any{}
	if docID != "" {
		query += ` WHERE doc_id=?`
		args = append(args, docID)
	}
	query += ` ORDER BY doc_id,stage_ordinal`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Checkpoint, 0)
	for rows.Next() {
		var checkpoint Checkpoint
		if err := rows.Scan(&checkpoint.DocID, &checkpoint.Stage, &checkpoint.StageOrdinal, &checkpoint.Seq,
			&checkpoint.InputFingerprint, &checkpoint.StrategyVersion, &checkpoint.ArtifactSummary,
			&checkpoint.ArtifactFingerprint, &checkpoint.ArtifactJSON, &checkpoint.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, checkpoint)
	}
	return result, rows.Err()
}

func (s *Store) ListDocuments(ctx context.Context) ([]DocumentState, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT doc_id,content_fingerprint,strategy_version,seq,updated_at FROM documents ORDER BY doc_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]DocumentState, 0)
	for rows.Next() {
		var doc DocumentState
		if err := rows.Scan(&doc.DocID, &doc.ContentFingerprint, &doc.StrategyVersion, &doc.Seq, &doc.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, doc)
	}
	return result, rows.Err()
}

func (s *Store) Snapshot(ctx context.Context) (string, []guardDoc, error) {
	return s.snapshotState(ctx)
}

func (s *Store) VerifySnapshot(ctx context.Context) error {
	return s.withBusinessLock(ctx, func() error {
		digest, docs, err := s.snapshotState(ctx)
		if err != nil {
			return err
		}
		guard, err := readGuard(s.guardPath)
		if err != nil {
			return err
		}
		switch compareGuard(guard, guardState{SnapshotDigest: digest, Documents: docs}) {
		case ClassificationAdvance:
			return writeGuard(s.guardPath, guardState{SnapshotDigest: digest, Documents: docs})
		case ClassificationDuplicate:
			return nil
		case ClassificationRollback:
			return &model.Error{Code: model.CodeCheckpointRollback, Message: "checkpoint snapshot sequence moved backwards"}
		default:
			return &model.Error{Code: model.CodeSnapshotReplaced, Message: "checkpoint snapshot was replaced"}
		}
	})
}
