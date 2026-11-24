package rewardpack

import (
	"context"
	"database/sql"
	"fmt"
)

// StageState is the currently active committed checkpoint for a stage.
type StageState struct {
	Stage            string `json:"stage"`
	Sequence         int64  `json:"sequence"`
	InputFingerprint string `json:"input_fingerprint"`
	ConfigVersion    int    `json:"config_version"`
	ArtifactDigest   string `json:"artifact_digest"`
	Payload          []byte `json:"-"`
}

// PairArtifact is one independently reusable per-pair stage product.
type PairArtifact struct {
	Stage       string `json:"stage"`
	PairID      string `json:"pair_id"`
	Source      string `json:"source"`
	InputDigest string `json:"input_digest"`
	Digest      string `json:"artifact_digest"`
	Payload     []byte `json:"-"`
}

// StagePublication is a conditional, transactional checkpoint publication.
type StagePublication struct {
	Stage                  string
	ExpectedSeq            int64
	InputFingerprint       string
	ConfigVersion          int
	ArtifactDigest         string
	Payload                []byte
	PreviousArtifactDigest string
	PairArtifacts          []PairArtifact
	DeletePairIDs          []string
	Reason                 string
}

type PublishResult struct {
	Stage    string `json:"stage"`
	Sequence int64  `json:"sequence"`
}

// PublishStage atomically publishes a stage product and advances its sequence.
// A stale expected sequence, duplicate input, cross-stage predecessor mismatch,
// or illegal stage order fails with E_STALE_CHECKPOINT / E_MISSING_ARTIFACT.
func (s *Store) PublishStage(ctx context.Context, publication StagePublication) (PublishResult, error) {
	stageIndex := StageIndex(publication.Stage)
	if stageIndex < 0 {
		return PublishResult{}, &BusinessError{Code: CodeIllegalStage, Stage: publication.Stage, Context: "unknown stage", Message: fmt.Sprintf("illegal stage %q", publication.Stage)}
	}
	if publication.InputFingerprint == "" || publication.ConfigVersion < 1 {
		return PublishResult{}, &BusinessError{Code: CodeInvalidInput, Stage: publication.Stage, Message: "input fingerprint and config version are required"}
	}
	actualDigest := digestBytes(publication.Payload)
	if publication.ArtifactDigest == "" {
		publication.ArtifactDigest = actualDigest
	}
	if publication.ArtifactDigest != actualDigest {
		return PublishResult{}, &BusinessError{Code: CodeInvalidInput, Stage: publication.Stage, Expected: actualDigest, Actual: publication.ArtifactDigest, Message: "artifact digest does not match payload"}
	}
	var result PublishResult
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if stageIndex > 0 {
			previous := StageOrder[stageIndex-1]
			var previousDigest string
			err := tx.QueryRowContext(ctx, `SELECT artifact_digest FROM stage_state WHERE stage=?`, previous).Scan(&previousDigest)
			if err == sql.ErrNoRows {
				return &BusinessError{Code: CodeMissingArtifact, Stage: publication.Stage, Context: "previous stage " + previous + " has no published artifact", Message: "missing predecessor artifact"}
			}
			if err != nil {
				return errorWith(CodeInternal, err)
			}
			if publication.PreviousArtifactDigest != previousDigest {
				return &BusinessError{Code: CodeStaleCheckpoint, Stage: publication.Stage, Expected: previousDigest, Actual: publication.PreviousArtifactDigest, Context: "cross-stage predecessor mismatch", Message: "publication was computed from a different predecessor artifact"}
			}
		}
		var currentSeq int64
		var currentInput, currentDigest string
		exists := true
		err := tx.QueryRowContext(ctx, `SELECT seq,input_fingerprint,artifact_digest FROM stage_state WHERE stage=?`, publication.Stage).Scan(&currentSeq, &currentInput, &currentDigest)
		if err == sql.ErrNoRows {
			exists = false
			currentSeq = 0
		} else if err != nil {
			return errorWith(CodeInternal, err)
		}
		if publication.ExpectedSeq != currentSeq {
			return &BusinessError{Code: CodeStaleCheckpoint, Stage: publication.Stage, Expected: currentSeq, Actual: publication.ExpectedSeq, Context: "expected sequence does not match committed sequence", Message: "stale checkpoint publication"}
		}
		if exists && currentInput == publication.InputFingerprint {
			return &BusinessError{Code: CodeStaleCheckpoint, Stage: publication.Stage, Expected: currentInput, Actual: publication.InputFingerprint, Context: "duplicate stage/input publication", Message: "stage input has already been published"}
		}
		nextSeq := currentSeq + 1
		if exists {
			res, err := tx.ExecContext(ctx, `UPDATE stage_state SET seq=?,input_fingerprint=?,config_version=?,artifact_digest=?,payload=?,updated_at=? WHERE stage=? AND seq=?`,
				nextSeq, publication.InputFingerprint, publication.ConfigVersion, publication.ArtifactDigest, publication.Payload, nowUTC(), publication.Stage, currentSeq)
			if err != nil {
				return errorWith(CodeInternal, err)
			}
			affected, err := res.RowsAffected()
			if err != nil {
				return errorWith(CodeInternal, err)
			}
			if affected != 1 {
				return &BusinessError{Code: CodeStaleCheckpoint, Stage: publication.Stage, Expected: currentSeq, Context: "conditional stage sequence update lost race", Message: "stage sequence changed concurrently"}
			}
		} else {
			if publication.ExpectedSeq != 0 {
				return &BusinessError{Code: CodeStaleCheckpoint, Stage: publication.Stage, Expected: 0, Actual: publication.ExpectedSeq, Context: "missing stage state", Message: "cannot advance a non-existent stage by more than one"}
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO stage_state(stage,seq,input_fingerprint,config_version,artifact_digest,payload,updated_at) VALUES(?,1,?,?,?,?,?)`,
				publication.Stage, publication.InputFingerprint, publication.ConfigVersion, publication.ArtifactDigest, publication.Payload, nowUTC()); err != nil {
				return errorWith(CodeInternal, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE checkpoints SET valid=0 WHERE stage=? AND valid=1`, publication.Stage); err != nil {
			return errorWith(CodeInternal, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoints(stage,stage_seq,input_fingerprint,config_version,artifact_digest,valid,created_at) VALUES(?,?,?,?,?,1,?)`,
			publication.Stage, nextSeq, publication.InputFingerprint, publication.ConfigVersion, publication.ArtifactDigest, nowUTC()); err != nil {
			return errorWith(CodeInternal, err)
		}
		for _, artifact := range publication.PairArtifacts {
			if artifact.Stage == "" {
				artifact.Stage = publication.Stage
			}
			if artifact.Stage != publication.Stage {
				return &BusinessError{Code: CodeIllegalStage, Stage: publication.Stage, Actual: artifact.Stage, Message: "pair artifact belongs to a different stage"}
			}
			if artifact.Digest == "" {
				artifact.Digest = digestBytes(artifact.Payload)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO pair_artifacts(stage,pair_id,source,input_digest,artifact_digest,payload) VALUES(?,?,?,?,?,?)
ON CONFLICT(stage,pair_id) DO UPDATE SET source=excluded.source,input_digest=excluded.input_digest,artifact_digest=excluded.artifact_digest,payload=excluded.payload`,
				publication.Stage, artifact.PairID, artifact.Source, artifact.InputDigest, artifact.Digest, artifact.Payload); err != nil {
				return errorWith(CodeInternal, err)
			}
		}
		for _, pairID := range publication.DeletePairIDs {
			if _, err := tx.ExecContext(ctx, `DELETE FROM pair_artifacts WHERE stage=? AND pair_id=?`, publication.Stage, pairID); err != nil {
				return errorWith(CodeInternal, err)
			}
		}
		result = PublishResult{Stage: publication.Stage, Sequence: nextSeq}
		return nil
	})
	return result, err
}

func (s *Store) StageState(ctx context.Context, stage string) (StageState, bool, error) {
	if StageIndex(stage) < 0 {
		return StageState{}, false, &BusinessError{Code: CodeIllegalStage, Stage: stage, Message: "illegal stage"}
	}
	var state StageState
	err := s.db.QueryRowContext(ctx, `SELECT stage,seq,input_fingerprint,config_version,artifact_digest,payload FROM stage_state WHERE stage=?`, stage).
		Scan(&state.Stage, &state.Sequence, &state.InputFingerprint, &state.ConfigVersion, &state.ArtifactDigest, &state.Payload)
	if err == sql.ErrNoRows {
		return StageState{}, false, nil
	}
	if err != nil {
		return StageState{}, false, errorWith(CodeInternal, err)
	}
	return state, true, nil
}

func (s *Store) AllStageStates(ctx context.Context) ([]StageState, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT stage,seq,input_fingerprint,config_version,artifact_digest,payload FROM stage_state`)
	if err != nil {
		return nil, errorWith(CodeInternal, err)
	}
	defer rows.Close()
	result := make([]StageState, 0, len(StageOrder))
	for rows.Next() {
		var state StageState
		if err := rows.Scan(&state.Stage, &state.Sequence, &state.InputFingerprint, &state.ConfigVersion, &state.ArtifactDigest, &state.Payload); err != nil {
			return nil, errorWith(CodeInternal, err)
		}
		result = append(result, state)
	}
	if err := rows.Err(); err != nil {
		return nil, errorWith(CodeInternal, err)
	}
	for i := 1; i < len(result); i++ {
		for j := i; j > 0 && StageIndex(result[j].Stage) < StageIndex(result[j-1].Stage); j-- {
			result[j], result[j-1] = result[j-1], result[j]
		}
	}
	return result, nil
}

// LoadPairArtifacts loads all reusable artifacts for a stage.
func (s *Store) LoadPairArtifacts(ctx context.Context, stage string) (map[string]PairArtifact, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT stage,pair_id,source,input_digest,artifact_digest,payload FROM pair_artifacts WHERE stage=? ORDER BY pair_id`, stage)
	if err != nil {
		return nil, errorWith(CodeInternal, err)
	}
	defer rows.Close()
	result := map[string]PairArtifact{}
	for rows.Next() {
		var artifact PairArtifact
		if err := rows.Scan(&artifact.Stage, &artifact.PairID, &artifact.Source, &artifact.InputDigest, &artifact.Digest, &artifact.Payload); err != nil {
			return nil, errorWith(CodeInternal, err)
		}
		result[artifact.PairID] = artifact
	}
	return result, errorWith(CodeInternal, rows.Err())
}

// PairArtifactDigests returns stage/pair digest rows without payloads.
func (s *Store) PairArtifactDigests(ctx context.Context, stage string) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT pair_id,artifact_digest FROM pair_artifacts WHERE stage=? ORDER BY pair_id`, stage)
	if err != nil {
		return nil, errorWith(CodeInternal, err)
	}
	defer rows.Close()
	result := map[string]string{}
	for rows.Next() {
		var pairID, digest string
		if err := rows.Scan(&pairID, &digest); err != nil {
			return nil, errorWith(CodeInternal, err)
		}
		result[pairID] = digest
	}
	return result, errorWith(CodeInternal, rows.Err())
}

type Checkpoint struct {
	ID               int64  `json:"id"`
	Stage            string `json:"stage"`
	StageSequence    int64  `json:"stage_sequence"`
	InputFingerprint string `json:"input_fingerprint"`
	ConfigVersion    int    `json:"config_version"`
	ArtifactDigest   string `json:"artifact_digest"`
	Valid            bool   `json:"valid"`
}

func (s *Store) Checkpoints(ctx context.Context) ([]Checkpoint, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,stage,stage_seq,input_fingerprint,config_version,artifact_digest,valid FROM checkpoints ORDER BY id`)
	if err != nil {
		return nil, errorWith(CodeInternal, err)
	}
	defer rows.Close()
	var result []Checkpoint
	for rows.Next() {
		var cp Checkpoint
		var valid int
		if err := rows.Scan(&cp.ID, &cp.Stage, &cp.StageSequence, &cp.InputFingerprint, &cp.ConfigVersion, &cp.ArtifactDigest, &valid); err != nil {
			return nil, errorWith(CodeInternal, err)
		}
		cp.Valid = valid == 1
		result = append(result, cp)
	}
	return result, errorWith(CodeInternal, rows.Err())
}

// SaveManifest atomically records the input snapshot for a completed build.
func (s *Store) SaveManifest(ctx context.Context, manifest Manifest) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if err := saveManifestTx(ctx, tx, manifest); err != nil {
			return err
		}
		return consumePendingChangesTx(ctx, tx)
	})
}

// ExportStagePayload returns the exact committed stage JSON bytes.
func (s *Store) ExportStagePayload(ctx context.Context, stage string) ([]byte, error) {
	state, ok, err := s.StageState(ctx, stage)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &BusinessError{Code: CodeMissingArtifact, Stage: stage, Message: "stage has not been published"}
	}
	return state.Payload, nil
}
