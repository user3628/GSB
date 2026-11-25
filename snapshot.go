package speakerdiar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// SnapshotEnvelope is the atomic versioned file format for every stage.
type SnapshotEnvelope struct {
	SchemaVersion      int             `json:"schema_version"`
	SessionID          string          `json:"session_id"`
	ChunkID            string          `json:"chunk_id"`
	Stage              string          `json:"stage"`
	StageOrder         int             `json:"stage_order"`
	VersionFingerprint string          `json:"version_fingerprint"`
	ConfigVersion      string          `json:"config_version"`
	ArtifactDigest     string          `json:"artifact_digest"`
	Payload            json.RawMessage `json:"payload"`
}

// SnapshotStore writes and reads atomic stage snapshots.
type SnapshotStore struct {
	root string
}

// NewSnapshotStore returns <workDir>/snapshots.
func NewSnapshotStore(workDir string) *SnapshotStore {
	return &SnapshotStore{root: filepath.Join(workDir, "snapshots")}
}

// StagePath returns the atomic latest snapshot path for a chunk/stage.
func (s *SnapshotStore) StagePath(chunkID, stage string) string {
	return filepath.Join(s.root, chunkID, stage+".json")
}

// VersionedPath returns an immutable content-addressed snapshot copy.
func (s *SnapshotStore) VersionedPath(chunkID, stage, fingerprint string) string {
	short := fingerprint
	if len(short) >= 16 {
		short = short[:16]
	}
	return filepath.Join(s.root, chunkID, "versions", fmt.Sprintf("%s-%s.json", stage, short))
}

// SaveStageSnapshot atomically writes both latest and versioned files.
func (s *SnapshotStore) SaveStageSnapshot(sessionID, chunkID, stage, fingerprint, configVersion string, payload any) (*SnapshotEnvelope, error) {
	order, ok := StageOrder[stage]
	if !ok {
		return nil, newError(CodeSnapshot, "unknown stage "+stage)
	}
	payloadBytes, err := CanonicalJSON(payload)
	if err != nil {
		return nil, err
	}
	digest := DigestBytes(payloadBytes)
	envelope := SnapshotEnvelope{
		SchemaVersion: SchemaVersion, SessionID: sessionID, ChunkID: chunkID,
		Stage: stage, StageOrder: order, VersionFingerprint: fingerprint,
		ConfigVersion: configVersion, ArtifactDigest: digest, Payload: payloadBytes,
	}
	raw, err := PrettyJSON(envelope)
	if err != nil {
		return nil, err
	}
	mainPath := s.StagePath(chunkID, stage)
	if err := AtomicWriteFile(mainPath, raw); err != nil {
		return nil, err
	}
	versionedPath := s.VersionedPath(chunkID, stage, fingerprint)
	if err := AtomicWriteFile(versionedPath, raw); err != nil {
		return nil, err
	}
	return &envelope, nil
}

// LoadEnvelope reads and parses a snapshot envelope.
func (s *SnapshotStore) LoadEnvelope(path string) (*SnapshotEnvelope, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var env SnapshotEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		de := newError(CodeSnapshot, "snapshot is unreadable")
		de.Details = map[string]any{"path": path, "error": err.Error()}
		return nil, de
	}
	compacted := bytes.Buffer{}
	if err := json.Compact(&compacted, env.Payload); err != nil {
		de := newError(CodeSnapshot, "snapshot payload is not canonical JSON")
		de.ChunkID = env.ChunkID
		de.Stage = env.Stage
		de.Details = map[string]any{"path": path, "error": err.Error()}
		return &env, de
	}
	env.Payload = compacted.Bytes()
	if got := DigestBytes(env.Payload); got != env.ArtifactDigest {
		de := newError(CodeSnapshot, "snapshot payload digest mismatch")
		de.ChunkID = env.ChunkID
		de.Stage = env.Stage
		de.Details = map[string]any{"expected": env.ArtifactDigest, "actual": got, "path": path}
		return &env, de
	}
	return &env, nil
}

// LoadStagePayload loads the latest valid snapshot and decodes its payload.
func (s *SnapshotStore) LoadStagePayload(chunkID, stage string, target any) (*SnapshotEnvelope, error) {
	env, err := s.LoadEnvelope(s.StagePath(chunkID, stage))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(env.Payload, target); err != nil {
		return env, err
	}
	return env, nil
}

// VerifyCheckpointSnapshot compares committed fact and atomic snapshot.
func (s *SnapshotStore) VerifyCheckpointSnapshot(cp Checkpoint) (MutationClass, error) {
	env, err := s.LoadEnvelope(s.StagePath(cp.ChunkID, cp.Stage))
	if err != nil {
		return MutationReplaced, err
	}
	if env.VersionFingerprint != cp.Fingerprint || env.ArtifactDigest != cp.ArtifactDigest {
		return MutationReplaced, nil
	}
	return MutationUnchanged, nil
}
