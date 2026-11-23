package cache

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"time"
)

type PutResult struct {
	Entry      Entry `json:"entry"`
	Idempotent bool  `json:"idempotent"`
}

// PutBatch validates every input before invoking the single writer. A
// structurally invalid batch therefore cannot leak a valid prefix into SQLite.
func (s *Store) PutBatch(ctx context.Context, inputs []EntryInput) ([]PutResult, error) {
	if len(inputs) == 0 {
		return nil, newError(CodeEntryInvalid, "entry batch must not be empty", nil)
	}
	normalized := make([]EntryInput, len(inputs))
	for index, input := range inputs {
		validated, _, _, err := NormalizeAndValidate(input)
		if err != nil {
			return nil, withContext(err, map[string]any{"index": index})
		}
		normalized[index] = validated
	}
	results := make([]PutResult, 0, len(normalized))
	for _, input := range normalized {
		result, err := s.Put(ctx, input)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

// Put performs one writer-serialized, content-addressed write. An identical
// active write for the same normalized key is idempotent; different content is
// rejected without changing the existing catalog.
func (s *Store) Put(ctx context.Context, input EntryInput) (result PutResult, err error) {
	normalized, key, cacheKey, err := NormalizeAndValidate(input)
	if err != nil {
		return PutResult{}, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	current := s.currentCatalog()
	if existing, ok := current.activeFor(cacheKey); ok {
		if existing.ArtifactDigest == normalized.ArtifactDigest && existing.ArtifactSize == normalized.ArtifactSize {
			return PutResult{Entry: existing, Idempotent: true}, nil
		}
		return PutResult{}, newError(CodeConflictingWrite, "the same cache key already has different artifact content", map[string]any{
			"cache_key":        cacheKey,
			"existing_digest":  existing.ArtifactDigest,
			"requested_digest": normalized.ArtifactDigest,
			"existing_state":   existing.State,
			"entry_id":         existing.EntryID,
		})
	}

	entryID, err := randomID("entry_")
	if err != nil {
		return PutResult{}, err
	}
	now := s.Now()
	entry := Entry{
		EntryID:          entryID,
		CacheKey:         cacheKey,
		StageID:          key.StageID,
		InputFingerprint: key.InputFingerprint,
		ToolchainVersion: key.ToolchainVersion,
		ArtifactDigest:   normalized.ArtifactDigest,
		ArtifactSize:     normalized.ArtifactSize,
		Hits:             0,
		RetainUntil:      normalized.RetainUntil,
		CreatedAt:        now,
		LastHitAt:        now,
		State:            StatePending,
		Version:          0,
	}

	writeIntentID, err := s.insertIntent(ctx, intentWrite, cacheKey, map[string]any{
		"entry_id": entryID,
		"digest":   normalized.ArtifactDigest,
		"size":     normalized.ArtifactSize,
	})
	if err != nil {
		return PutResult{}, err
	}
	success := false
	var stagedPath string
	defer func() {
		if stagedPath != "" {
			_ = os.Remove(stagedPath)
		}
		if !success {
			s.failWrite(ctx, writeIntentID, entryID, cacheKey, normalized.ArtifactDigest)
		}
	}()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PutResult{}, dbError("cannot start cache entry transaction", err)
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO entries(entry_id, cache_key, stage_id, input_fingerprint, toolchain_version,
                    artifact_digest, artifact_size, hits, retain_until_ns, created_at_ns,
                    last_hit_ns, state, version)
VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, 'PENDING', 0)`,
		entry.EntryID, entry.CacheKey, entry.StageID, entry.InputFingerprint, entry.ToolchainVersion,
		entry.ArtifactDigest, entry.ArtifactSize, ns(entry.RetainUntil), ns(entry.CreatedAt), ns(entry.LastHitAt))
	if err != nil {
		_ = tx.Rollback()
		return PutResult{}, dbError("cannot insert pending cache entry", err)
	}
	if err := tx.Commit(); err != nil {
		return PutResult{}, dbError("cannot commit pending cache entry", err)
	}
	if _, err := s.reloadAndPublish(ctx); err != nil {
		return PutResult{}, err
	}

	stagedPath, err = s.stageArtifact(normalized.ArtifactContent, normalized.ArtifactDigest)
	if err != nil {
		return PutResult{}, err
	}
	finalPath, err := s.commitStagedArtifact(stagedPath, normalized.ArtifactDigest, normalized.ArtifactSize)
	if err != nil {
		return PutResult{}, err
	}
	stagedPath = ""

	if err := verifyFile(finalPath, normalized.ArtifactDigest, normalized.ArtifactSize); err != nil {
		return PutResult{}, err
	}

	transitionIntentID, err := s.insertIntent(ctx, intentTransition, cacheKey, map[string]any{
		"entry_id": entryID,
		"from":     StatePending,
		"to":       StateWriting,
		"reason":   "write_started",
	})
	if err != nil {
		return PutResult{}, err
	}
	tx, err = s.db.BeginTx(ctx, nil)
	if err != nil {
		return PutResult{}, dbError("cannot start writing transition", err)
	}
	if err := s.updateEntryStateTx(ctx, tx, entryID, StatePending, StateWriting, now); err != nil {
		_ = tx.Rollback()
		return PutResult{}, err
	}
	if err := markIntentDoneTx(ctx, tx, transitionIntentID, ns(now)); err != nil {
		_ = tx.Rollback()
		return PutResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return PutResult{}, dbError("cannot commit writing transition", err)
	}
	if _, err := s.reloadAndPublish(ctx); err != nil {
		return PutResult{}, err
	}

	s.maybeCrash(CrashAfterWriting)

	if err := verifyFile(finalPath, normalized.ArtifactDigest, normalized.ArtifactSize); err != nil {
		return PutResult{}, err
	}

	availableIntentID, err := s.insertIntent(ctx, intentTransition, cacheKey, map[string]any{
		"entry_id": entryID,
		"from":     StateWriting,
		"to":       StateAvailable,
		"reason":   "digest_recheck_passed",
	})
	if err != nil {
		return PutResult{}, err
	}
	tx, err = s.db.BeginTx(ctx, nil)
	if err != nil {
		return PutResult{}, dbError("cannot start availability transaction", err)
	}
	if err := s.updateEntryStateTx(ctx, tx, entryID, StateWriting, StateAvailable, now); err != nil {
		_ = tx.Rollback()
		return PutResult{}, err
	}
	if err := s.incrementArtifactRefTx(ctx, tx, normalized.ArtifactDigest, normalized.ArtifactSize); err != nil {
		_ = tx.Rollback()
		return PutResult{}, err
	}
	if err := markIntentDoneTx(ctx, tx, availableIntentID, ns(now)); err != nil {
		_ = tx.Rollback()
		return PutResult{}, err
	}
	if err := markIntentDoneTx(ctx, tx, writeIntentID, ns(now)); err != nil {
		_ = tx.Rollback()
		return PutResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return PutResult{}, dbError("cannot commit available cache entry", err)
	}
	success = true
	catalogAfter, err := s.reloadAndPublish(ctx)
	if err != nil {
		return PutResult{}, err
	}
	entry = catalogAfter.active[cacheKey]
	return PutResult{Entry: entry, Idempotent: false}, nil
}

func (s *Store) updateEntryStateTx(ctx context.Context, tx *sql.Tx, entryID string, from, to State, now time.Time) error {
	result, err := tx.ExecContext(ctx, `
UPDATE entries SET state=?, version=version+1
WHERE entry_id=? AND state=?`, string(to), entryID, string(from))
	if err != nil {
		return dbError("cannot update cache entry state", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return newError(CodeInvalidTransition, "cache entry state changed before transition commit", map[string]any{
			"entry_id": entryID,
			"from":     from,
			"to":       to,
		})
	}
	return nil
}

func (s *Store) incrementArtifactRefTx(ctx context.Context, tx *sql.Tx, digest string, size int64) error {
	var existingSize int64
	var refCount int64
	err := tx.QueryRowContext(ctx, `SELECT size, ref_count FROM artifacts WHERE digest=?`, digest).Scan(&existingSize, &refCount)
	if err == nil {
		if existingSize != size {
			return newError(CodeDigestMismatch, "artifact digest has inconsistent persisted size", map[string]any{
				"digest":         digest,
				"expected_size":  size,
				"persisted_size": existingSize,
			})
		}
		_, err = tx.ExecContext(ctx, `UPDATE artifacts SET ref_count=ref_count+1 WHERE digest=?`, digest)
		if err != nil {
			return dbError("cannot increment artifact reference", err)
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return dbError("cannot inspect artifact reference", err)
	}
	relativePath, err := s.relativeArtifactPath(digest)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO artifacts(digest, size, relative_path, ref_count) VALUES (?, ?, ?, 1)`, digest, size, relativePath)
	if err != nil {
		return dbError("cannot insert artifact reference", err)
	}
	return nil
}

func (s *Store) stageArtifact(content []byte, digest string) (string, error) {
	file, err := os.CreateTemp(s.stagingDir, "artifact-*.tmp")
	if err != nil {
		return "", wrapError(CodeIO, "cannot create staged artifact", nil, err)
	}
	path := file.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(content); err != nil {
		return "", wrapError(CodeIO, "cannot write staged artifact", map[string]any{"digest": digest}, err)
	}
	if err := file.Sync(); err != nil {
		return "", wrapError(CodeIO, "cannot fsync staged artifact", map[string]any{"digest": digest}, err)
	}
	if err := file.Close(); err != nil {
		return "", wrapError(CodeIO, "cannot close staged artifact", map[string]any{"digest": digest}, err)
	}
	cleanup = false
	return path, nil
}

func (s *Store) commitStagedArtifact(stagedPath, digest string, size int64) (string, error) {
	finalPath, err := s.artifactPath(digest)
	if err != nil {
		return "", err
	}
	if _, statErr := os.Stat(finalPath); statErr == nil {
		if err := verifyFile(finalPath, digest, size); err != nil {
			return "", err
		}
		_ = os.Remove(stagedPath)
		return finalPath, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", wrapError(CodeIO, "cannot inspect final artifact", map[string]any{"path": finalPath}, statErr)
	}
	if err := os.Rename(stagedPath, finalPath); err != nil {
		return "", wrapError(CodeIO, "cannot commit staged artifact", map[string]any{
			"source": stagedPath,
			"target": finalPath,
		}, err)
	}
	return finalPath, nil
}

func verifyFile(path, digest string, size int64) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return wrapError(CodeDigestMismatch, "artifact content cannot be read for digest verification", map[string]any{
			"path":   path,
			"digest": digest,
		}, err)
	}
	return verifyContent(content, digest, size, path)
}

func verifyContent(content []byte, digest string, size int64, path string) error {
	actual := DigestBytes(content)
	if int64(len(content)) != size || actual != digest {
		return newError(CodeDigestMismatch, "artifact content failed digest recheck", map[string]any{
			"path":            path,
			"expected_digest": digest,
			"actual_digest":   actual,
			"expected_size":   size,
			"actual_size":     len(content),
		})
	}
	return nil
}

func (s *Store) failWrite(ctx context.Context, intentID, entryID, cacheKey, digest string) {
	s.abortRunningIntentsForTarget(ctx, cacheKey)
	if intentID != "" {
		s.markIntentAborted(ctx, intentID)
	}
	if entryID != "" {
		_, _ = s.db.ExecContext(ctx, `UPDATE entries SET state='PENDING', version=version+1 WHERE entry_id=? AND state IN ('PENDING','WRITING')`, entryID)
	}
	if _, err := s.reloadAndPublish(ctx); err == nil {
		s.removeArtifactIfUnreferenced(digest)
	}
}

func (s *Store) removeArtifactIfUnreferenced(digest string) {
	var refs int64
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM artifacts WHERE digest=? AND ref_count>0`, digest).Scan(&refs); err != nil {
		return
	}
	if refs > 0 {
		return
	}
	path, err := s.artifactPath(digest)
	if err == nil {
		_ = os.Remove(path)
	}
}
