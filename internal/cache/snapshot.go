package cache

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const SnapshotVersion = 1

type SnapshotEntry struct {
	EntryID               string `json:"entry_id"`
	CacheKey              string `json:"cache_key"`
	StageID               string `json:"stage_id"`
	InputFingerprint      string `json:"input_fingerprint"`
	ToolchainVersion      string `json:"toolchain_version"`
	ArtifactDigest        string `json:"artifact_digest"`
	ArtifactSize          int64  `json:"artifact_size"`
	Hits                  int64  `json:"hits"`
	RetainUntil           string `json:"retain_until"`
	CreatedAt             string `json:"created_at"`
	LastHitAt             string `json:"last_hit_at"`
	State                 State  `json:"state"`
	Version               int64  `json:"version"`
	ArtifactContentBase64 string `json:"artifact_content_base64,omitempty"`
}

type Snapshot struct {
	SnapshotVersion int             `json:"snapshot_version"`
	Sequence        int64           `json:"sequence"`
	GeneratedAt     string          `json:"generated_at"`
	Entries         []SnapshotEntry `json:"entries"`
	Checksum        string          `json:"checksum"`
}

type SnapshotSwitchResult struct {
	PreviousSequence  int64    `json:"previous_sequence"`
	Sequence          int64    `json:"sequence"`
	EntryCount        int      `json:"entry_count"`
	ArtifactCount     int      `json:"artifact_count"`
	ReleasedArtifacts int      `json:"released_artifacts"`
	CleanupWarnings   []string `json:"cleanup_warnings,omitempty"`
}

type preparedSnapshot struct {
	entries       []Entry
	artifacts     map[string]Artifact
	contents      map[string][]byte
	stagedPaths   map[string]string
	entryCount    int
	artifactCount int
}

func (s *Store) ExportSnapshot(ctx context.Context) (Snapshot, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	current := s.currentCatalog()
	dbSequence, err := s.readSequence(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	if dbSequence < current.sequence {
		dbSequence = current.sequence
	}
	sequence := dbSequence + 1
	now := s.Now()
	snapshot := Snapshot{
		SnapshotVersion: SnapshotVersion,
		Sequence:        sequence,
		GeneratedAt:     formatTime(now),
		Entries:         make([]SnapshotEntry, 0, len(current.entries)),
	}
	entries := make([]Entry, 0, len(current.entries))
	for _, entry := range current.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].CacheKey != entries[j].CacheKey {
			return entries[i].CacheKey < entries[j].CacheKey
		}
		if !entries[i].CreatedAt.Equal(entries[j].CreatedAt) {
			return entries[i].CreatedAt.Before(entries[j].CreatedAt)
		}
		return entries[i].EntryID < entries[j].EntryID
	})
	for _, entry := range entries {
		snapshotEntry := SnapshotEntry{
			EntryID:          entry.EntryID,
			CacheKey:         entry.CacheKey,
			StageID:          entry.StageID,
			InputFingerprint: entry.InputFingerprint,
			ToolchainVersion: entry.ToolchainVersion,
			ArtifactDigest:   entry.ArtifactDigest,
			ArtifactSize:     entry.ArtifactSize,
			Hits:             entry.Hits,
			RetainUntil:      formatTime(entry.RetainUntil),
			CreatedAt:        formatTime(entry.CreatedAt),
			LastHitAt:        formatTime(entry.LastHitAt),
			State:            entry.State,
			Version:          entry.Version,
		}
		if entry.State == StateAvailable || entry.State == StateStale {
			content, err := s.readVerifiedArtifact(entry)
			if err != nil {
				return Snapshot{}, err
			}
			snapshotEntry.ArtifactContentBase64 = base64.StdEncoding.EncodeToString(content)
		}
		snapshot.Entries = append(snapshot.Entries, snapshotEntry)
	}
	if err := SealSnapshot(&snapshot); err != nil {
		return Snapshot{}, err
	}
	if err := s.persistSequence(ctx, sequence); err != nil {
		return Snapshot{}, err
	}
	if _, err := s.reloadAndPublish(ctx); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func SealSnapshot(snapshot *Snapshot) error {
	if snapshot == nil {
		return newError(CodeSnapshotInvalid, "snapshot is required", nil)
	}
	snapshot.Checksum = snapshotChecksum(*snapshot)
	return nil
}

func snapshotChecksum(snapshot Snapshot) string {
	snapshot.Checksum = ""
	data, _ := json.Marshal(snapshot)
	return DigestBytes(data)
}

func (s *Store) persistSequence(ctx context.Context, sequence int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return dbError("cannot start snapshot sequence transaction", err)
	}
	if err := s.setSequenceTx(ctx, tx, sequence); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return dbError("cannot commit snapshot sequence", err)
	}
	return nil
}

func WriteSnapshotFile(path string, snapshot Snapshot) error {
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return wrapError(CodeIO, "cannot encode snapshot file", map[string]any{"path": path}, err)
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return wrapError(CodeIO, "cannot create snapshot directory", map[string]any{"path": dir}, err)
	}
	file, err := os.CreateTemp(dir, ".snapshot-*.tmp")
	if err != nil {
		return wrapError(CodeIO, "cannot create temporary snapshot file", map[string]any{"path": path}, err)
	}
	temp := file.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = file.Close()
			_ = os.Remove(temp)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return wrapError(CodeIO, "cannot write snapshot file", map[string]any{"path": path}, err)
	}
	if err := file.Sync(); err != nil {
		return wrapError(CodeIO, "cannot fsync snapshot file", map[string]any{"path": path}, err)
	}
	if err := file.Close(); err != nil {
		return wrapError(CodeIO, "cannot close snapshot file", map[string]any{"path": path}, err)
	}
	if err := os.Rename(temp, path); err != nil {
		return wrapError(CodeIO, "cannot publish snapshot file", map[string]any{"path": path}, err)
	}
	cleanup = false
	return nil
}

func LoadSnapshotFile(path string) (Snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Snapshot{}, wrapError(CodeSnapshotInvalid, "cannot read snapshot file", map[string]any{"path": path}, err)
	}
	snapshot, err := ParseSnapshot(data)
	if err != nil {
		return Snapshot{}, withContext(err, map[string]any{"path": path})
	}
	return snapshot, nil
}

func ParseSnapshot(data []byte) (Snapshot, error) {
	var snapshot Snapshot
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return Snapshot{}, wrapError(CodeSnapshotInvalid, "invalid snapshot JSON", nil, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err != nil {
			return Snapshot{}, wrapError(CodeSnapshotInvalid, "snapshot JSON contains invalid trailing data", nil, err)
		}
		return Snapshot{}, newError(CodeSnapshotInvalid, "snapshot JSON contains trailing values", nil)
	}
	if err := validateSnapshotHeader(snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func validateSnapshotHeader(snapshot Snapshot) error {
	if snapshot.SnapshotVersion != SnapshotVersion {
		return newError(CodeSnapshotInvalid, "unsupported snapshot version", map[string]any{
			"snapshot_version": snapshot.SnapshotVersion,
			"supported":        SnapshotVersion,
		})
	}
	if snapshot.Sequence <= 0 {
		return newError(CodeSnapshotInvalid, "snapshot sequence must be positive", map[string]any{
			"sequence": snapshot.Sequence,
		})
	}
	if _, err := time.Parse(time.RFC3339Nano, snapshot.GeneratedAt); err != nil {
		return wrapError(CodeSnapshotInvalid, "snapshot generated_at must be RFC3339", map[string]any{
			"generated_at": snapshot.GeneratedAt,
		}, err)
	}
	if !sha256DigestPattern.MatchString(snapshot.Checksum) {
		return newError(CodeSnapshotInvalid, "snapshot checksum format is invalid", map[string]any{
			"checksum": snapshot.Checksum,
		})
	}
	actual := snapshotChecksum(snapshot)
	if subtle.ConstantTimeCompare([]byte(actual), []byte(snapshot.Checksum)) != 1 {
		return newError(CodeSnapshotInvalid, "snapshot checksum mismatch", map[string]any{
			"expected_checksum": snapshot.Checksum,
			"actual_checksum":   actual,
		})
	}
	return nil
}

func (s *Store) prepareSnapshot(snapshot Snapshot) (*preparedSnapshot, error) {
	if err := validateSnapshotHeader(snapshot); err != nil {
		return nil, err
	}
	prepared := &preparedSnapshot{
		entries:     make([]Entry, 0, len(snapshot.Entries)),
		artifacts:   make(map[string]Artifact),
		contents:    make(map[string][]byte),
		stagedPaths: make(map[string]string),
	}
	entryIDs := make(map[string]bool)
	activeKeys := make(map[string]bool)
	for index, item := range snapshot.Entries {
		context := map[string]any{"index": index, "entry_id": item.EntryID}
		if item.EntryID == "" {
			return nil, newError(CodeSnapshotInvalid, "snapshot entry_id is required", context)
		}
		if entryIDs[item.EntryID] {
			return nil, newError(CodeSnapshotInvalid, "snapshot contains duplicate entry_id", context)
		}
		entryIDs[item.EntryID] = true
		key, cacheKey, err := NormalizeKey(Key{
			StageID:          item.StageID,
			InputFingerprint: item.InputFingerprint,
			ToolchainVersion: item.ToolchainVersion,
		})
		if err != nil {
			return nil, withContext(err, context)
		}
		if item.CacheKey != cacheKey {
			return nil, newError(CodeSnapshotInvalid, "snapshot cache_key does not match normalized identity", withContextValues(context, map[string]any{
				"cache_key":      item.CacheKey,
				"calculated_key": cacheKey,
			}))
		}
		if !isKnownState(item.State) {
			return nil, newError(CodeSnapshotInvalid, "snapshot state is invalid", withContextValues(context, map[string]any{"state": item.State}))
		}
		if item.State != StateEvicted {
			if activeKeys[cacheKey] {
				return nil, newError(CodeSnapshotInvalid, "snapshot contains more than one active entry for a cache key", withContextValues(context, map[string]any{"cache_key": cacheKey}))
			}
			activeKeys[cacheKey] = true
		}
		if item.ArtifactSize < 0 || item.Hits < 0 || item.Version < 0 {
			return nil, newError(CodeSnapshotInvalid, "snapshot counters and sizes must be non-negative", context)
		}
		if !sha256DigestPattern.MatchString(item.ArtifactDigest) {
			return nil, newError(CodeSnapshotInvalid, "snapshot artifact_digest is invalid", withContextValues(context, map[string]any{
				"artifact_digest": item.ArtifactDigest,
			}))
		}
		retainUntil, err := time.Parse(time.RFC3339Nano, item.RetainUntil)
		if err != nil {
			return nil, wrapError(CodeSnapshotInvalid, "snapshot retain_until is invalid", context, err)
		}
		createdAt, err := time.Parse(time.RFC3339Nano, item.CreatedAt)
		if err != nil {
			return nil, wrapError(CodeSnapshotInvalid, "snapshot created_at is invalid", context, err)
		}
		lastHitAt, err := time.Parse(time.RFC3339Nano, item.LastHitAt)
		if err != nil {
			return nil, wrapError(CodeSnapshotInvalid, "snapshot last_hit_at is invalid", context, err)
		}
		entry := Entry{
			EntryID:          item.EntryID,
			CacheKey:         cacheKey,
			StageID:          key.StageID,
			InputFingerprint: key.InputFingerprint,
			ToolchainVersion: key.ToolchainVersion,
			ArtifactDigest:   item.ArtifactDigest,
			ArtifactSize:     item.ArtifactSize,
			Hits:             item.Hits,
			RetainUntil:      retainUntil.UTC(),
			CreatedAt:        createdAt.UTC(),
			LastHitAt:        lastHitAt.UTC(),
			State:            item.State,
			Version:          item.Version,
		}
		if entry.State == StateAvailable || entry.State == StateStale {
			content, err := base64.StdEncoding.DecodeString(item.ArtifactContentBase64)
			if err != nil {
				return nil, wrapError(CodeSnapshotInvalid, "snapshot artifact content is not valid base64", context, err)
			}
			if err := verifyContent(content, entry.ArtifactDigest, entry.ArtifactSize, item.EntryID); err != nil {
				return nil, withContext(err, context)
			}
			if previous, ok := prepared.contents[entry.ArtifactDigest]; ok {
				if !bytes.Equal(previous, content) {
					return nil, newError(CodeSnapshotInvalid, "same digest has different snapshot content", context)
				}
			} else {
				prepared.contents[entry.ArtifactDigest] = content
			}
			ref := prepared.artifacts[entry.ArtifactDigest]
			ref.Digest = entry.ArtifactDigest
			ref.Size = entry.ArtifactSize
			ref.RefCount++
			relative, err := s.relativeArtifactPath(entry.ArtifactDigest)
			if err != nil {
				return nil, err
			}
			ref.RelativePath = relative
			prepared.artifacts[entry.ArtifactDigest] = ref
		}
		prepared.entries = append(prepared.entries, entry)
	}

	for digest, content := range prepared.contents {
		staged, err := s.stageArtifact(content, digest)
		if err != nil {
			prepared.removeStaged()
			return nil, err
		}
		prepared.stagedPaths[digest] = staged
	}
	prepared.entryCount = len(prepared.entries)
	prepared.artifactCount = len(prepared.artifacts)
	return prepared, nil
}

func (p *preparedSnapshot) removeStaged() {
	for _, path := range p.stagedPaths {
		_ = os.Remove(path)
	}
	p.stagedPaths = make(map[string]string)
}

func (s *Store) SwitchSnapshot(ctx context.Context, snapshot Snapshot) (SnapshotSwitchResult, error) {
	prepared, err := s.prepareSnapshot(snapshot)
	if err != nil {
		return SnapshotSwitchResult{}, err
	}
	defer prepared.removeStaged()

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	currentSequence, err := s.readSequence(ctx)
	if err != nil {
		return SnapshotSwitchResult{}, err
	}
	currentCatalog := s.currentCatalog()
	if currentCatalog.sequence > currentSequence {
		currentSequence = currentCatalog.sequence
	}
	if snapshot.Sequence <= currentSequence {
		return SnapshotSwitchResult{}, newError(CodeSnapshotSequence, "snapshot sequence must be greater than the active sequence", map[string]any{
			"snapshot_sequence": snapshot.Sequence,
			"current_sequence":  currentSequence,
		})
	}

	oldDigests := make(map[string]bool, len(currentCatalog.artifacts))
	for digest := range currentCatalog.artifacts {
		oldDigests[digest] = true
	}
	createdDigests := make([]string, 0)
	cleanupNew := true
	defer func() {
		if cleanupNew {
			for _, digest := range createdDigests {
				path, pathErr := s.artifactPath(digest)
				if pathErr == nil {
					_ = os.Remove(path)
				}
			}
		}
	}()

	for digest, staged := range prepared.stagedPaths {
		finalPath, err := s.artifactPath(digest)
		if err != nil {
			return SnapshotSwitchResult{}, err
		}
		if _, statErr := os.Stat(finalPath); statErr == nil {
			if err := verifyFile(finalPath, digest, prepared.artifacts[digest].Size); err != nil {
				return SnapshotSwitchResult{}, err
			}
			_ = os.Remove(staged)
			delete(prepared.stagedPaths, digest)
			continue
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return SnapshotSwitchResult{}, wrapError(CodeIO, "cannot inspect snapshot artifact target", map[string]any{"path": finalPath}, statErr)
		}
		if err := os.Rename(staged, finalPath); err != nil {
			return SnapshotSwitchResult{}, wrapError(CodeIO, "cannot install snapshot artifact", map[string]any{
				"source": staged,
				"target": finalPath,
			}, err)
		}
		delete(prepared.stagedPaths, digest)
		createdDigests = append(createdDigests, digest)
	}

	intentID, err := s.insertIntent(ctx, intentSnapshot, fmt.Sprintf("sequence:%d", snapshot.Sequence), map[string]any{
		"sequence": snapshot.Sequence,
		"checksum": snapshot.Checksum,
	})
	if err != nil {
		return SnapshotSwitchResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.markIntentAborted(ctx, intentID)
		return SnapshotSwitchResult{}, dbError("cannot start snapshot switch transaction", err)
	}
	for _, statement := range []string{"DELETE FROM entries", "DELETE FROM artifacts", "DELETE FROM evictions"} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			_ = tx.Rollback()
			s.markIntentAborted(ctx, intentID)
			return SnapshotSwitchResult{}, dbError("cannot replace snapshot catalog tables", err)
		}
	}
	for _, artifact := range prepared.artifacts {
		_, err := tx.ExecContext(ctx, `INSERT INTO artifacts(digest, size, relative_path, ref_count) VALUES (?, ?, ?, ?)`,
			artifact.Digest, artifact.Size, artifact.RelativePath, artifact.RefCount)
		if err != nil {
			_ = tx.Rollback()
			s.markIntentAborted(ctx, intentID)
			return SnapshotSwitchResult{}, dbError("cannot insert snapshot artifact occupancy", err)
		}
	}
	for _, entry := range prepared.entries {
		_, err := tx.ExecContext(ctx, `
INSERT INTO entries(entry_id, cache_key, stage_id, input_fingerprint, toolchain_version,
                    artifact_digest, artifact_size, hits, retain_until_ns, created_at_ns,
                    last_hit_ns, state, version)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			entry.EntryID, entry.CacheKey, entry.StageID, entry.InputFingerprint, entry.ToolchainVersion,
			entry.ArtifactDigest, entry.ArtifactSize, entry.Hits, ns(entry.RetainUntil), ns(entry.CreatedAt),
			ns(entry.LastHitAt), string(entry.State), entry.Version)
		if err != nil {
			_ = tx.Rollback()
			s.markIntentAborted(ctx, intentID)
			return SnapshotSwitchResult{}, dbError("cannot insert snapshot entry", err)
		}
	}
	if err := s.setSequenceTx(ctx, tx, snapshot.Sequence); err != nil {
		_ = tx.Rollback()
		s.markIntentAborted(ctx, intentID)
		return SnapshotSwitchResult{}, err
	}
	if err := markIntentDoneTx(ctx, tx, intentID, ns(s.Now())); err != nil {
		_ = tx.Rollback()
		s.markIntentAborted(ctx, intentID)
		return SnapshotSwitchResult{}, err
	}
	if err := tx.Commit(); err != nil {
		s.markIntentAborted(ctx, intentID)
		return SnapshotSwitchResult{}, dbError("cannot commit snapshot switch", err)
	}
	cleanupNew = false

	next, err := s.loadCatalog(ctx)
	if err != nil {
		return SnapshotSwitchResult{}, err
	}
	cleanupWarnings := make([]string, 0)
	released := 0
	s.visibility.Lock()
	s.generation++
	next.generation = s.generation
	s.catalog = next
	for digest := range oldDigests {
		if _, retained := prepared.artifacts[digest]; retained {
			continue
		}
		path, pathErr := s.artifactPath(digest)
		if pathErr != nil {
			cleanupWarnings = append(cleanupWarnings, pathErr.Error())
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			cleanupWarnings = append(cleanupWarnings, err.Error())
			continue
		}
		released++
	}
	s.visibility.Unlock()
	return SnapshotSwitchResult{
		PreviousSequence:  currentSequence,
		Sequence:          snapshot.Sequence,
		EntryCount:        prepared.entryCount,
		ArtifactCount:     prepared.artifactCount,
		ReleasedArtifacts: released,
		CleanupWarnings:   cleanupWarnings,
	}, nil
}

func withContextValues(base, extra map[string]any) map[string]any {
	merged := make(map[string]any, len(base)+len(extra))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range extra {
		merged[key] = value
	}
	return merged
}
