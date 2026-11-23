package cache

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sort"
	"time"
)

type EvictedEntry struct {
	EntryID     string `json:"entry_id"`
	CacheKey    string `json:"cache_key"`
	StageID     string `json:"stage_id"`
	Digest      string `json:"artifact_digest"`
	Size        int64  `json:"artifact_size"`
	Hits        int64  `json:"hits"`
	Reason      string `json:"reason"`
	FreedBytes  int64  `json:"freed_bytes"`
	StateBefore State  `json:"state_before"`
}

type EvictionResult struct {
	TriggeredAt     string         `json:"triggered_at"`
	CapacityBytes   int64          `json:"capacity_bytes"`
	UsedBytesBefore int64          `json:"used_bytes_before"`
	UsedBytesAfter  int64          `json:"used_bytes_after"`
	ReleasedBytes   int64          `json:"released_bytes"`
	ExpiredToStale  []Entry        `json:"expired_to_stale"`
	Evicted         []EvictedEntry `json:"evicted"`
}

// Evict is the only capacity/retention reclamation entry point. Expired
// AVAILABLE entries become STALE first. All STALE entries are mandatory
// candidates; additional AVAILABLE entries are selected by the deterministic
// ordering (last_hit_at ASC, artifact_size DESC, cache_key ASC, entry_id ASC)
// until used bytes fit within capacity.
func (s *Store) Evict(ctx context.Context) (EvictionResult, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	now := s.Now()
	before := s.currentCatalog()
	result := EvictionResult{
		TriggeredAt:     formatTime(now),
		CapacityBytes:   s.capacityBytes,
		UsedBytesBefore: before.usedBytes,
		ExpiredToStale:  make([]Entry, 0),
		Evicted:         make([]EvictedEntry, 0),
	}

	expired := make([]Entry, 0)
	for _, entry := range before.active {
		if entry.State == StateAvailable && entry.Expired(now) {
			expired = append(expired, entry)
		}
	}
	sortEntries(expired)
	for _, entry := range expired {
		updated, err := s.transitionLocked(ctx, entry.CacheKey, StateStale, "retention_expired")
		if err != nil {
			return EvictionResult{}, err
		}
		result.ExpiredToStale = append(result.ExpiredToStale, updated)
	}

	current := s.currentCatalog()
	stale := make([]Entry, 0)
	available := make([]Entry, 0)
	for _, entry := range current.active {
		switch entry.State {
		case StateStale:
			stale = append(stale, entry)
		case StateAvailable:
			available = append(available, entry)
		}
	}
	sortEntries(stale)
	sortEntries(available)

	refs := make(map[string]int64, len(current.artifacts))
	for digest, artifact := range current.artifacts {
		refs[digest] = artifact.RefCount
	}
	projected := current.usedBytes
	selected := make(map[string]Entry)
	for _, entry := range stale {
		selected[entry.EntryID] = entry
		projected -= projectedRelease(entry, refs, current.artifacts)
	}
	for _, entry := range available {
		if projected <= s.capacityBytes {
			break
		}
		selected[entry.EntryID] = entry
		projected -= projectedRelease(entry, refs, current.artifacts)
	}

	ordered := make([]Entry, 0, len(selected))
	for _, entry := range selected {
		ordered = append(ordered, entry)
	}
	sortEntries(ordered)
	for _, entry := range ordered {
		evicted, err := s.evictOneLocked(ctx, entry, now)
		if err != nil {
			return result, err
		}
		result.Evicted = append(result.Evicted, evicted)
		result.ReleasedBytes += evicted.FreedBytes
	}
	result.UsedBytesAfter = s.currentCatalog().usedBytes
	return result, nil
}

func sortEntries(entries []Entry) {
	sort.Slice(entries, func(i, j int) bool {
		left, right := entries[i], entries[j]
		if !left.LastHitAt.Equal(right.LastHitAt) {
			return left.LastHitAt.Before(right.LastHitAt)
		}
		if left.ArtifactSize != right.ArtifactSize {
			return left.ArtifactSize > right.ArtifactSize
		}
		if left.CacheKey != right.CacheKey {
			return left.CacheKey < right.CacheKey
		}
		return left.EntryID < right.EntryID
	})
}

func projectedRelease(entry Entry, refs map[string]int64, artifacts map[string]Artifact) int64 {
	if refs[entry.ArtifactDigest] <= 0 {
		return 0
	}
	refs[entry.ArtifactDigest]--
	if refs[entry.ArtifactDigest] == 0 {
		return artifacts[entry.ArtifactDigest].Size
	}
	return 0
}

func (s *Store) evictOneLocked(ctx context.Context, entry Entry, now time.Time) (EvictedEntry, error) {
	stateBefore := entry.State
	if entry.State == StateAvailable {
		updated, err := s.transitionLocked(ctx, entry.CacheKey, StateStale, "capacity_limit")
		if err != nil {
			return EvictedEntry{}, err
		}
		entry = updated
	}
	if entry.State != StateStale {
		return EvictedEntry{}, newError(CodeInvalidTransition, "only STALE entries can be evicted", map[string]any{
			"cache_key": entry.CacheKey,
			"state":     entry.State,
		})
	}
	reason := "capacity"
	if entry.Expired(now) {
		reason = "retention_expired"
	}

	intentID, err := s.insertIntent(ctx, intentEvict, entry.CacheKey, map[string]any{
		"entry_id": entry.EntryID,
		"digest":   entry.ArtifactDigest,
		"reason":   reason,
	})
	if err != nil {
		return EvictedEntry{}, err
	}
	s.maybeCrash(CrashAfterEvictIntent)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.markIntentAborted(ctx, intentID)
		return EvictedEntry{}, dbError("cannot start eviction transaction", err)
	}
	var persistedState string
	err = tx.QueryRowContext(ctx, `SELECT state FROM entries WHERE entry_id=?`, entry.EntryID).Scan(&persistedState)
	if err != nil {
		_ = tx.Rollback()
		s.markIntentAborted(ctx, intentID)
		return EvictedEntry{}, dbError("cannot inspect entry for eviction", err)
	}
	if State(persistedState) != StateStale {
		_ = tx.Rollback()
		s.markIntentAborted(ctx, intentID)
		return EvictedEntry{}, newError(CodeInvalidTransition, "entry is not STALE at eviction commit", map[string]any{
			"entry_id": entry.EntryID,
			"state":    persistedState,
		})
	}

	var refCount, artifactSize int64
	var relativePath string
	err = tx.QueryRowContext(ctx, `SELECT ref_count, size, relative_path FROM artifacts WHERE digest=?`, entry.ArtifactDigest).Scan(&refCount, &artifactSize, &relativePath)
	if err != nil {
		_ = tx.Rollback()
		s.markIntentAborted(ctx, intentID)
		if errors.Is(err, sql.ErrNoRows) {
			return EvictedEntry{}, newError(CodeInternal, "STALE entry has no persisted artifact occupancy", map[string]any{
				"entry_id": entry.EntryID,
				"digest":   entry.ArtifactDigest,
			})
		}
		return EvictedEntry{}, dbError("cannot inspect artifact occupancy", err)
	}
	if refCount <= 0 {
		_ = tx.Rollback()
		s.markIntentAborted(ctx, intentID)
		return EvictedEntry{}, newError(CodeInternal, "artifact occupancy is already zero", map[string]any{"digest": entry.ArtifactDigest})
	}
	if refCount == 1 {
		_, err = tx.ExecContext(ctx, `DELETE FROM artifacts WHERE digest=?`, entry.ArtifactDigest)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE artifacts SET ref_count=ref_count-1 WHERE digest=?`, entry.ArtifactDigest)
	}
	if err != nil {
		_ = tx.Rollback()
		s.markIntentAborted(ctx, intentID)
		return EvictedEntry{}, dbError("cannot release artifact occupancy", err)
	}
	if err := s.updateEntryStateTx(ctx, tx, entry.EntryID, StateStale, StateEvicted, now); err != nil {
		_ = tx.Rollback()
		s.markIntentAborted(ctx, intentID)
		return EvictedEntry{}, err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO evictions(entry_id, cache_key, artifact_digest, artifact_size, hits, reason, evicted_at_ns)
VALUES (?, ?, ?, ?, ?, ?, ?)`, entry.EntryID, entry.CacheKey, entry.ArtifactDigest, entry.ArtifactSize, entry.Hits, reason, ns(now))
	if err != nil {
		_ = tx.Rollback()
		s.markIntentAborted(ctx, intentID)
		return EvictedEntry{}, dbError("cannot write eviction history", err)
	}
	if err := markIntentDoneTx(ctx, tx, intentID, ns(now)); err != nil {
		_ = tx.Rollback()
		s.markIntentAborted(ctx, intentID)
		return EvictedEntry{}, err
	}
	if err := tx.Commit(); err != nil {
		s.markIntentAborted(ctx, intentID)
		return EvictedEntry{}, dbError("cannot commit eviction", err)
	}

	freed := int64(0)
	if refCount == 1 {
		freed = artifactSize
	}
	next, err := s.loadCatalog(ctx)
	if err != nil {
		return EvictedEntry{}, err
	}
	s.visibility.Lock()
	s.generation++
	next.generation = s.generation
	s.catalog = next
	var removeErr error
	if refCount == 1 {
		removeErr = os.Remove(s.artifactPathForRelative(relativePath))
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
	}
	s.visibility.Unlock()
	if removeErr != nil {
		return EvictedEntry{}, wrapError(CodeIO, "eviction committed but artifact file could not be removed", map[string]any{
			"digest": entry.ArtifactDigest,
		}, removeErr)
	}
	return EvictedEntry{
		EntryID:     entry.EntryID,
		CacheKey:    entry.CacheKey,
		StageID:     entry.StageID,
		Digest:      entry.ArtifactDigest,
		Size:        entry.ArtifactSize,
		Hits:        entry.Hits,
		Reason:      reason,
		FreedBytes:  freed,
		StateBefore: stateBefore,
	}, nil
}

func (s *Store) artifactPathForRelative(relative string) string {
	if relative == "" {
		return ""
	}
	return s.dir + string(os.PathSeparator) + filepathFromSlash(relative)
}

func filepathFromSlash(value string) string {
	result := make([]byte, 0, len(value))
	for i := 0; i < len(value); i++ {
		if value[i] == '/' {
			result = append(result, os.PathSeparator)
		} else {
			result = append(result, value[i])
		}
	}
	return string(result)
}
