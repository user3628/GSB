package cache

import (
	"context"
	"os"
	"time"
)

type HitResult struct {
	Entry   Entry  `json:"entry"`
	Content []byte `json:"-"`
}

type hitCandidate struct {
	entry   Entry
	content []byte
}

// Hit performs a digest-verified lookup. The potentially expensive artifact
// read is done concurrently with other readers against one immutable catalog
// generation. Only the short statistics commit and catalog publication are
// serialized through the single writer lock.
func (s *Store) Hit(ctx context.Context, key Key, expectedDigest string) (HitResult, error) {
	_, cacheKey, err := NormalizeKey(key)
	if err != nil {
		return HitResult{}, err
	}
	if expectedDigest != "" && !sha256DigestPattern.MatchString(expectedDigest) {
		return HitResult{}, newError(CodeEntryInvalid, "expected_digest must be sha256:<64 lowercase hex>", map[string]any{
			"expected_digest": expectedDigest,
		})
	}

	preflight, err := s.preflightHit(cacheKey, expectedDigest)
	if err != nil {
		if IsCode(err, CodeEntryExpired) {
			return HitResult{}, s.markExpired(ctx, cacheKey)
		}
		return HitResult{}, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	current := s.currentCatalog()
	entry, ok := current.activeFor(cacheKey)
	if !ok {
		if latest, exists := current.latestFor(cacheKey); exists {
			if latest.State == StateEvicted {
				return HitResult{}, missError(cacheKey, "EVICTED", map[string]any{"entry_id": latest.EntryID})
			}
		}
		return HitResult{}, missError(cacheKey, "KEY_NOT_FOUND", nil)
	}
	if err := s.ensureHittable(entry, expectedDigest, s.Now()); err != nil {
		if IsCode(err, CodeEntryExpired) {
			_, transitionErr := s.transitionLocked(ctx, cacheKey, StateStale, "retention_expired")
			if transitionErr != nil && !IsCode(transitionErr, CodeInvalidTransition) {
				return HitResult{}, transitionErr
			}
			return HitResult{}, withContext(err, map[string]any{"entry_id": entry.EntryID})
		}
		return HitResult{}, err
	}

	content := preflight.content
	if preflight.entry.EntryID != entry.EntryID {
		content, err = s.readVerifiedArtifact(entry)
		if err != nil {
			return HitResult{}, err
		}
	}

	now := s.Now()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return HitResult{}, dbError("cannot start hit statistics transaction", err)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE entries SET hits=hits+1, last_hit_ns=?, version=version+1
WHERE entry_id=? AND state='AVAILABLE'`, ns(now), entry.EntryID)
	if err != nil {
		_ = tx.Rollback()
		return HitResult{}, dbError("cannot update hit statistics", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		_ = tx.Rollback()
		return HitResult{}, newError(CodeInvalidTransition, "cache entry is no longer available", map[string]any{
			"cache_key": cacheKey,
			"entry_id":  entry.EntryID,
		})
	}
	if err := tx.Commit(); err != nil {
		return HitResult{}, dbError("cannot commit hit statistics", err)
	}
	published, err := s.reloadAndPublish(ctx)
	if err != nil {
		return HitResult{}, err
	}
	updated, ok := published.activeFor(cacheKey)
	if !ok || updated.EntryID != entry.EntryID {
		return HitResult{}, newError(CodeInternal, "hit statistics committed but entry is unavailable", map[string]any{
			"cache_key": cacheKey,
			"entry_id":  entry.EntryID,
		})
	}
	return HitResult{Entry: updated, Content: content}, nil
}

func (s *Store) preflightHit(cacheKey, expectedDigest string) (hitCandidate, error) {
	s.visibility.RLock()
	defer s.visibility.RUnlock()
	current := s.catalog
	entry, ok := current.activeFor(cacheKey)
	if !ok {
		if latest, exists := current.latestFor(cacheKey); exists && latest.State == StateEvicted {
			return hitCandidate{}, missError(cacheKey, "EVICTED", map[string]any{"entry_id": latest.EntryID})
		}
		return hitCandidate{}, missError(cacheKey, "KEY_NOT_FOUND", nil)
	}
	if err := s.ensureHittable(entry, expectedDigest, s.Now()); err != nil {
		return hitCandidate{}, err
	}
	content, err := s.readVerifiedArtifact(entry)
	if err != nil {
		return hitCandidate{}, err
	}
	return hitCandidate{entry: entry, content: content}, nil
}

func (s *Store) ensureHittable(entry Entry, expectedDigest string, now time.Time) error {
	context := map[string]any{
		"cache_key": entry.CacheKey,
		"entry_id":  entry.EntryID,
		"state":     entry.State,
	}
	switch entry.State {
	case StateAvailable:
	case StateStale:
		return newError(CodeEntryExpired, "cache entry is stale", withReason(context, "EXPIRED"))
	case StateEvicted:
		return missError(entry.CacheKey, "EVICTED", context)
	case StatePending, StateWriting:
		return missError(entry.CacheKey, "NOT_AVAILABLE", context)
	default:
		return missError(entry.CacheKey, "NOT_AVAILABLE", context)
	}
	if entry.Expired(now) {
		return newError(CodeEntryExpired, "cache entry retention period has expired", withReason(context, "EXPIRED"))
	}
	if expectedDigest != "" && expectedDigest != entry.ArtifactDigest {
		return newError(CodeDigestMismatch, "expected digest does not match the cached entry", withReason(map[string]any{
			"cache_key":       entry.CacheKey,
			"entry_id":        entry.EntryID,
			"expected_digest": expectedDigest,
			"actual_digest":   entry.ArtifactDigest,
		}, "EXPECTED_DIGEST_MISMATCH"))
	}
	return nil
}

func (s *Store) readVerifiedArtifact(entry Entry) ([]byte, error) {
	path, err := s.artifactPath(entry.ArtifactDigest)
	if err != nil {
		return nil, err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, wrapError(CodeDigestMismatch, "cached artifact is unavailable for digest recheck", map[string]any{
			"cache_key": entry.CacheKey,
			"path":      path,
			"digest":    entry.ArtifactDigest,
		}, err)
	}
	if err := verifyContent(content, entry.ArtifactDigest, entry.ArtifactSize, path); err != nil {
		return nil, withContext(err, map[string]any{
			"cache_key": entry.CacheKey,
			"entry_id":  entry.EntryID,
		})
	}
	return content, nil
}

func (s *Store) markExpired(ctx context.Context, cacheKey string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	current := s.currentCatalog()
	entry, ok := current.activeFor(cacheKey)
	if !ok {
		if latest, exists := current.latestFor(cacheKey); exists && latest.State == StateEvicted {
			return missError(cacheKey, "EVICTED", map[string]any{"entry_id": latest.EntryID})
		}
		return missError(cacheKey, "KEY_NOT_FOUND", nil)
	}
	if entry.State == StateAvailable && entry.Expired(s.Now()) {
		_, err := s.transitionLocked(ctx, cacheKey, StateStale, "retention_expired")
		if err != nil {
			return err
		}
	}
	return newError(CodeEntryExpired, "cache entry retention period has expired", withReason(map[string]any{
		"cache_key": cacheKey,
		"entry_id":  entry.EntryID,
		"state":     entry.State,
	}, "EXPIRED"))
}

func missError(cacheKey, reason string, extra map[string]any) error {
	context := map[string]any{"cache_key": cacheKey, "reason": reason}
	for key, value := range extra {
		context[key] = value
	}
	return newError(CodeCacheMiss, "cache key did not produce a reusable hit", context)
}

func withReason(context map[string]any, reason string) map[string]any {
	context["reason"] = reason
	return context
}

// EntryByKey returns the active entry (or the most recent evicted entry) for
// diagnostics without changing hit statistics.
func (s *Store) EntryByKey(key Key) (Entry, bool) {
	_, cacheKey, err := NormalizeKey(key)
	if err != nil {
		return Entry{}, false
	}
	s.visibility.RLock()
	defer s.visibility.RUnlock()
	if entry, ok := s.catalog.activeFor(cacheKey); ok {
		return entry, true
	}
	entry, ok := s.catalog.latestFor(cacheKey)
	return entry, ok
}

func (s *Store) HitCount(cacheKey string) int64 {
	s.visibility.RLock()
	defer s.visibility.RUnlock()
	if entry, ok := s.catalog.activeFor(cacheKey); ok {
		return entry.Hits
	}
	return 0
}
