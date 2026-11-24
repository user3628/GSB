package cockpitaudio

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

type Store struct {
	db       *sql.DB
	lockPath string
}

type Checkpoint struct {
	ID               int64  `json:"id"`
	ShardID          string `json:"shard_id"`
	Stage            string `json:"stage"`
	Sequence         int    `json:"sequence"`
	ShardFingerprint string `json:"shard_fingerprint"`
	ConfigVersion    string `json:"config_version"`
	EditVersion      int    `json:"edit_version"`
	ArtifactDigest   string `json:"artifact_digest"`
	PayloadJSON      string `json:"payload_json"`
	PublisherID      string `json:"publisher_id"`
	RecordHash       string `json:"record_hash"`
	PrevHash         string `json:"prev_hash"`
	LogicalClock     int64  `json:"logical_clock"`
	IsValid          bool   `json:"is_valid"`
}

type PublishInput struct {
	ShardID          string `json:"shard_id"`
	Stage            string `json:"stage"`
	Sequence         int    `json:"sequence"`
	ExpectedShardID  string `json:"expected_shard_id,omitempty"`
	ShardFingerprint string `json:"shard_fingerprint"`
	ConfigVersion    string `json:"config_version"`
	EditVersion      int    `json:"edit_version"`
	Artifact         any    `json:"-"`
	PayloadJSON      string `json:"payload_json,omitempty"`
	ArtifactDigest   string `json:"artifact_digest,omitempty"`
	PublisherID      string `json:"publisher_id"`
}

type PublishOutcome struct {
	Checkpoint     Checkpoint `json:"checkpoint"`
	Classification string     `json:"classification"`
}

func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db, lockPath: path + ".publish.lock"}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS checkpoints (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	shard_id TEXT NOT NULL,
	stage TEXT NOT NULL,
	sequence INTEGER NOT NULL,
	shard_fingerprint TEXT NOT NULL,
	config_version TEXT NOT NULL,
	edit_version INTEGER NOT NULL,
	artifact_digest TEXT NOT NULL,
	payload_json TEXT NOT NULL,
	publisher_id TEXT NOT NULL DEFAULT '',
	record_hash TEXT NOT NULL DEFAULT '',
	prev_hash TEXT NOT NULL DEFAULT '',
	logical_clock INTEGER NOT NULL DEFAULT 0,
	is_valid INTEGER NOT NULL DEFAULT 1
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_checkpoints_shard_sequence ON checkpoints(shard_id, sequence);
CREATE UNIQUE INDEX IF NOT EXISTS uq_valid_shard_stage ON checkpoints(shard_id, stage) WHERE is_valid=1;
CREATE INDEX IF NOT EXISTS idx_checkpoints_shard_stage ON checkpoints(shard_id, stage);

CREATE TABLE IF NOT EXISTS shard_counters (
	shard_id TEXT PRIMARY KEY,
	next_sequence INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS stage_heads (
	shard_id TEXT NOT NULL,
	stage TEXT NOT NULL,
	sequence INTEGER NOT NULL,
	checkpoint_id INTEGER NOT NULL,
	shard_fingerprint TEXT NOT NULL,
	artifact_digest TEXT NOT NULL,
	record_hash TEXT NOT NULL,
	PRIMARY KEY(shard_id, stage)
);

CREATE TABLE IF NOT EXISTS snapshot_chain (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	shard_id TEXT NOT NULL,
	stage TEXT NOT NULL,
	sequence INTEGER NOT NULL,
	artifact_digest TEXT NOT NULL,
	record_hash TEXT NOT NULL,
	prev_hash TEXT NOT NULL,
	logical_clock INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chain_shard ON snapshot_chain(shard_id, sequence);
`)
	return err
}

func DigestCanonical(v any) (string, []byte, error) {
	var b []byte
	var err error
	if raw, ok := v.(json.RawMessage); ok {
		b = raw
	} else if str, ok := v.(string); ok {
		b = []byte(str)
	} else if bytes, ok := v.([]byte); ok {
		b = bytes
	} else {
		b, err = CanonicalJSON(v)
		if err != nil {
			return "", nil, err
		}
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), b, nil
}

func (s *Store) NextSequence(shardID string) (int, error) {
	var next int
	err := s.db.QueryRow(`SELECT next_sequence FROM shard_counters WHERE shard_id=?`, shardID).Scan(&next)
	if errors.Is(err, sql.ErrNoRows) {
		return 1, nil
	}
	return next, err
}

func (s *Store) WithFileLock(fn func() *Error) *Error {
	lock := NewFileLock(s.lockPath)
	if err := lock.Lock(); err != nil {
		return NewError(CodeInternal, "store", "", "acquire file lock: %v", err)
	}
	defer lock.Unlock()
	return fn()
}

func (s *Store) Publish(in PublishInput) (*PublishOutcome, *Error) {
	if in.ShardID == "" || in.Stage == "" {
		return nil, NewError(CodeInvalidParam, in.Stage, in.ShardID, "shard_id and stage are required")
	}
	if in.ExpectedShardID != "" && in.ExpectedShardID != in.ShardID {
		return nil, &Error{
			Code: CodeStaleCheckpoint, Stage: in.Stage, ShardID: in.ShardID,
			Message: "cross-shard checkpoint publication rejected",
			Diagnostics: map[string]string{
				"classification": "cross_shard", "expected_shard_id": in.ExpectedShardID,
			},
		}
	}

	payload := in.PayloadJSON
	if payload == "" && in.Artifact != nil {
		b, err := CanonicalJSON(in.Artifact)
		if err != nil {
			return nil, NewError(CodeInternal, in.Stage, in.ShardID, "marshal artifact: %v", err)
		}
		payload = string(b)
	}
	digest := in.ArtifactDigest
	if digest == "" {
		sum := sha256.Sum256([]byte(payload))
		digest = hex.EncodeToString(sum[:])
	}

	var outcome *PublishOutcome
	lockErr := s.WithFileLock(func() *Error {
		tx, err := s.db.Begin()
		if err != nil {
			return NewError(CodeInternal, in.Stage, in.ShardID, "begin tx: %v", err)
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()

		next := 1
		var counterNext int
		err = tx.QueryRow(`SELECT next_sequence FROM shard_counters WHERE shard_id=?`, in.ShardID).Scan(&counterNext)
		if err == nil {
			next = counterNext
		} else if !errors.Is(err, sql.ErrNoRows) {
			return NewError(CodeInternal, in.Stage, in.ShardID, "read counter: %v", err)
		}

		requested := in.Sequence
		if requested == 0 {
			requested = next
		}
		if requested != next {
			classification := "future_gap"
			if requested < next {
				classification = "duplicate_or_behind"
			}
			return &Error{
				Code: CodeStaleCheckpoint, Stage: in.Stage, ShardID: in.ShardID,
				Message: fmt.Sprintf("stale checkpoint sequence %d, expected %d", requested, next),
				Diagnostics: map[string]string{
					"classification":     classification,
					"requested_sequence": fmt.Sprint(requested),
					"expected_sequence":  fmt.Sprint(next),
				},
			}
		}

		var oldSeq int
		var oldHash string
		headErr := tx.QueryRow(`SELECT sequence, record_hash FROM stage_heads WHERE shard_id=? AND stage=?`, in.ShardID, in.Stage).Scan(&oldSeq, &oldHash)
		if headErr != nil && !errors.Is(headErr, sql.ErrNoRows) {
			return NewError(CodeInternal, in.Stage, in.ShardID, "read stage head: %v", headErr)
		}

		var prevHash string
		_ = tx.QueryRow(`SELECT record_hash FROM snapshot_chain WHERE shard_id=? ORDER BY sequence DESC, id DESC LIMIT 1`, in.ShardID).Scan(&prevHash)

		cp := Checkpoint{
			ShardID: in.ShardID, Stage: in.Stage, Sequence: requested,
			ShardFingerprint: in.ShardFingerprint, ConfigVersion: in.ConfigVersion,
			EditVersion: in.EditVersion, ArtifactDigest: digest, PayloadJSON: payload,
			PublisherID: in.PublisherID, PrevHash: prevHash, LogicalClock: int64(requested),
			IsValid: true,
		}
		cp.RecordHash = hashCheckpoint(cp)

		if _, err := tx.Exec(`UPDATE checkpoints SET is_valid=0 WHERE shard_id=? AND stage=? AND is_valid=1`, in.ShardID, in.Stage); err != nil {
			return NewError(CodeInternal, in.Stage, in.ShardID, "supersede old checkpoint: %v", err)
		}

		res, err := tx.Exec(`
INSERT INTO checkpoints
(shard_id,stage,sequence,shard_fingerprint,config_version,edit_version,artifact_digest,payload_json,publisher_id,record_hash,prev_hash,logical_clock,is_valid)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1)`,
			cp.ShardID, cp.Stage, cp.Sequence, cp.ShardFingerprint, cp.ConfigVersion, cp.EditVersion,
			cp.ArtifactDigest, cp.PayloadJSON, cp.PublisherID, cp.RecordHash, cp.PrevHash, cp.LogicalClock)
		if err != nil {
			return NewError(CodeStaleCheckpoint, in.Stage, in.ShardID, "insert checkpoint: %v", err)
		}
		id, _ := res.LastInsertId()
		cp.ID = id

		if errors.Is(headErr, sql.ErrNoRows) {
			if _, err := tx.Exec(`INSERT INTO stage_heads(shard_id,stage,sequence,checkpoint_id,shard_fingerprint,artifact_digest,record_hash) VALUES(?,?,?,?,?,?,?)`,
				cp.ShardID, cp.Stage, cp.Sequence, id, cp.ShardFingerprint, cp.ArtifactDigest, cp.RecordHash); err != nil {
				return NewError(CodeStaleCheckpoint, in.Stage, in.ShardID, "insert stage head: %v", err)
			}
		} else {
			res, err := tx.Exec(`UPDATE stage_heads SET sequence=?, checkpoint_id=?, shard_fingerprint=?, artifact_digest=?, record_hash=? WHERE shard_id=? AND stage=? AND sequence=?`,
				cp.Sequence, id, cp.ShardFingerprint, cp.ArtifactDigest, cp.RecordHash, cp.ShardID, cp.Stage, oldSeq)
			if err != nil {
				return NewError(CodeInternal, in.Stage, in.ShardID, "conditional stage head update: %v", err)
			}
			n, _ := res.RowsAffected()
			if n != 1 {
				return &Error{Code: CodeStaleCheckpoint, Stage: in.Stage, ShardID: in.ShardID, Message: "stage head CAS lost",
					Diagnostics: map[string]string{"classification": "duplicate_or_behind", "expected_old_sequence": fmt.Sprint(oldSeq)}}
			}
		}

		if _, err := tx.Exec(`INSERT INTO snapshot_chain(shard_id,stage,sequence,artifact_digest,record_hash,prev_hash,logical_clock) VALUES(?,?,?,?,?,?,?)`,
			cp.ShardID, cp.Stage, cp.Sequence, cp.ArtifactDigest, cp.RecordHash, cp.PrevHash, cp.LogicalClock); err != nil {
			return NewError(CodeInternal, in.Stage, in.ShardID, "insert chain: %v", err)
		}
		if _, err := tx.Exec(`INSERT INTO shard_counters(shard_id,next_sequence) VALUES(?,?) ON CONFLICT(shard_id) DO UPDATE SET next_sequence=excluded.next_sequence`,
			in.ShardID, requested+1); err != nil {
			return NewError(CodeInternal, in.Stage, in.ShardID, "update counter: %v", err)
		}
		if err := tx.Commit(); err != nil {
			return NewError(CodeInternal, in.Stage, in.ShardID, "commit checkpoint: %v", err)
		}
		committed = true
		outcome = &PublishOutcome{Checkpoint: cp, Classification: "normal_advance"}
		return nil
	})
	if lockErr != nil {
		return nil, lockErr
	}
	return outcome, nil
}

func hashCheckpoint(cp Checkpoint) string {
	sum, err := Fingerprint(struct {
		ShardID          string `json:"shard_id"`
		Stage            string `json:"stage"`
		Sequence         int    `json:"sequence"`
		ShardFingerprint string `json:"shard_fingerprint"`
		ConfigVersion    string `json:"config_version"`
		EditVersion      int    `json:"edit_version"`
		ArtifactDigest   string `json:"artifact_digest"`
		PayloadJSON      string `json:"payload_json"`
		PublisherID      string `json:"publisher_id"`
		PrevHash         string `json:"prev_hash"`
		LogicalClock     int64  `json:"logical_clock"`
	}{cp.ShardID, cp.Stage, cp.Sequence, cp.ShardFingerprint, cp.ConfigVersion, cp.EditVersion,
		cp.ArtifactDigest, cp.PayloadJSON, cp.PublisherID, cp.PrevHash, cp.LogicalClock})
	if err != nil {
		panic(err)
	}
	return sum
}

func (s *Store) LatestCheckpoint(shardID, stage string) (*Checkpoint, error) {
	row := s.db.QueryRow(`
SELECT id,shard_id,stage,sequence,shard_fingerprint,config_version,edit_version,artifact_digest,payload_json,publisher_id,record_hash,prev_hash,logical_clock,is_valid
FROM checkpoints WHERE shard_id=? AND stage=? AND is_valid=1`, shardID, stage)
	return scanCheckpoint(row)
}

type rowScanner interface{ Scan(...any) error }

func scanCheckpoint(row rowScanner) (*Checkpoint, error) {
	var cp Checkpoint
	var valid int
	err := row.Scan(&cp.ID, &cp.ShardID, &cp.Stage, &cp.Sequence, &cp.ShardFingerprint, &cp.ConfigVersion,
		&cp.EditVersion, &cp.ArtifactDigest, &cp.PayloadJSON, &cp.PublisherID, &cp.RecordHash,
		&cp.PrevHash, &cp.LogicalClock, &valid)
	if err != nil {
		return nil, err
	}
	cp.IsValid = valid == 1
	return &cp, nil
}

func (s *Store) LoadPayload(shardID, stage string, target any) (*Checkpoint, *Error) {
	cp, err := s.LatestCheckpoint(shardID, stage)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, NewError(CodeNotFound, stage, shardID, "checkpoint not found")
		}
		return nil, NewError(CodeInternal, stage, shardID, "load checkpoint: %v", err)
	}
	if err := json.Unmarshal([]byte(cp.PayloadJSON), target); err != nil {
		return cp, NewError(CodeInternal, stage, shardID, "unmarshal payload: %v", err)
	}
	return cp, nil
}

type SnapshotVerification struct {
	ShardID        string `json:"shard_id"`
	Stage          string `json:"stage"`
	Sequence       int    `json:"sequence"`
	Classification string `json:"classification"`
	Healthy        bool   `json:"healthy"`
}

func (s *Store) VerifySnapshot(shardID, stage string) (*SnapshotVerification, *Error) {
	cp, err := s.LatestCheckpoint(shardID, stage)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, NewError(CodeNotFound, stage, shardID, "valid snapshot not found")
		}
		return nil, NewError(CodeInternal, stage, shardID, "load snapshot: %v", err)
	}
	ver := &SnapshotVerification{ShardID: shardID, Stage: stage, Sequence: cp.Sequence}
	sum := sha256.Sum256([]byte(cp.PayloadJSON))
	if hex.EncodeToString(sum[:]) != cp.ArtifactDigest {
		ver.Classification = "tamper"
		return ver, &Error{Code: CodeStaleCheckpoint, Stage: stage, ShardID: shardID, Message: "artifact digest mismatch",
			Diagnostics: map[string]string{"classification": "tamper", "sequence": fmt.Sprint(cp.Sequence)}}
	}
	if hashCheckpoint(*cp) != cp.RecordHash {
		ver.Classification = "tamper"
		return ver, &Error{Code: CodeStaleCheckpoint, Stage: stage, ShardID: shardID, Message: "record hash mismatch",
			Diagnostics: map[string]string{"classification": "tamper", "sequence": fmt.Sprint(cp.Sequence)}}
	}

	var chainSeq int
	var chainDigest, chainHash string
	err = s.db.QueryRow(`SELECT sequence,artifact_digest,record_hash FROM snapshot_chain WHERE shard_id=? AND stage=? AND record_hash=?`,
		shardID, stage, cp.RecordHash).Scan(&chainSeq, &chainDigest, &chainHash)
	if err != nil || chainSeq != cp.Sequence || chainDigest != cp.ArtifactDigest {
		ver.Classification = "tamper"
		return ver, &Error{Code: CodeStaleCheckpoint, Stage: stage, ShardID: shardID, Message: "snapshot chain does not match record",
			Diagnostics: map[string]string{"classification": "tamper", "sequence": fmt.Sprint(cp.Sequence)}}
	}

	var maxStageSeq int
	if err := s.db.QueryRow(`SELECT COALESCE(MAX(sequence),0) FROM snapshot_chain WHERE shard_id=? AND stage=?`, shardID, stage).Scan(&maxStageSeq); err == nil {
		if cp.Sequence < maxStageSeq {
			ver.Classification = "rollback"
			return ver, &Error{Code: CodeStaleCheckpoint, Stage: stage, ShardID: shardID, Message: "snapshot sequence rolled back",
				Diagnostics: map[string]string{"classification": "rollback", "current_sequence": fmt.Sprint(cp.Sequence), "max_sequence": fmt.Sprint(maxStageSeq)}}
		}
	}

	var headSeq int
	var headFP, headDigest, headHash string
	err = s.db.QueryRow(`SELECT sequence,shard_fingerprint,artifact_digest,record_hash FROM stage_heads WHERE shard_id=? AND stage=?`, shardID, stage).
		Scan(&headSeq, &headFP, &headDigest, &headHash)
	if err != nil || headSeq != cp.Sequence || headFP != cp.ShardFingerprint || headDigest != cp.ArtifactDigest || headHash != cp.RecordHash {
		ver.Classification = "tamper"
		return ver, &Error{Code: CodeStaleCheckpoint, Stage: stage, ShardID: shardID, Message: "stage head was replaced inconsistently",
			Diagnostics: map[string]string{"classification": "tamper", "sequence": fmt.Sprint(cp.Sequence)}}
	}

	ver.Classification = "normal_advance"
	ver.Healthy = true
	return ver, nil
}

func (s *Store) ListCheckpoints(shardID string) ([]Checkpoint, error) {
	rows, err := s.db.Query(`SELECT id,shard_id,stage,sequence,shard_fingerprint,config_version,edit_version,artifact_digest,payload_json,publisher_id,record_hash,prev_hash,logical_clock,is_valid FROM checkpoints WHERE shard_id=? ORDER BY sequence`, shardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Checkpoint{}
	for rows.Next() {
		cp, err := scanCheckpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *cp)
	}
	return out, rows.Err()
}

func DBPathInTemp(name string) string {
	return filepath.Join(os.TempDir(), name)
}
