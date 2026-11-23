package cache

import (
	"context"
)

// Transition validates and persists one explicit state transition. Validation
// happens before an intent or update is written, so a rejected transition
// leaves both the database and published catalog untouched.
func (s *Store) Transition(ctx context.Context, key Key, to State) (Entry, error) {
	_, cacheKey, err := NormalizeKey(key)
	if err != nil {
		return Entry{}, err
	}
	if !isKnownState(to) {
		return Entry{}, newError(CodeInvalidTransition, "unknown target cache state", map[string]any{
			"to": to,
		})
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	current := s.currentCatalog()
	entry, ok := current.activeFor(cacheKey)
	if ok && to == StateEvicted {
		if err := ValidateTransition(entry.State, to); err != nil {
			return Entry{}, withContext(err, map[string]any{
				"cache_key": cacheKey,
				"entry_id":  entry.EntryID,
			})
		}
		if _, err := s.evictOneLocked(ctx, entry, s.Now()); err != nil {
			return Entry{}, err
		}
		published := s.currentCatalog()
		return published.latest[cacheKey], nil
	}
	return s.transitionLocked(ctx, cacheKey, to, "explicit_transition")
}

func (s *Store) transitionLocked(ctx context.Context, cacheKey string, to State, reason string) (Entry, error) {
	current := s.currentCatalog()
	entry, ok := current.activeFor(cacheKey)
	if !ok {
		if latest, exists := current.latestFor(cacheKey); exists {
			return Entry{}, newError(CodeInvalidTransition, "cache entry is already terminal", map[string]any{
				"cache_key": cacheKey,
				"from":      latest.State,
				"to":        to,
				"entry_id":  latest.EntryID,
			})
		}
		return Entry{}, missError(cacheKey, "KEY_NOT_FOUND", nil)
	}
	if err := ValidateTransition(entry.State, to); err != nil {
		return Entry{}, withContext(err, map[string]any{
			"cache_key": cacheKey,
			"entry_id":  entry.EntryID,
		})
	}
	if to == StateAvailable {
		if _, err := s.readVerifiedArtifact(entry); err != nil {
			return Entry{}, err
		}
	}

	now := s.Now()
	intentID, err := s.insertIntent(ctx, intentTransition, cacheKey, map[string]any{
		"entry_id": entry.EntryID,
		"from":     entry.State,
		"to":       to,
		"reason":   reason,
	})
	if err != nil {
		return Entry{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.markIntentAborted(ctx, intentID)
		return Entry{}, dbError("cannot start state transition transaction", err)
	}
	if err := s.updateEntryStateTx(ctx, tx, entry.EntryID, entry.State, to, now); err != nil {
		_ = tx.Rollback()
		s.markIntentAborted(ctx, intentID)
		return Entry{}, err
	}
	if to == StateAvailable {
		if err := s.incrementArtifactRefTx(ctx, tx, entry.ArtifactDigest, entry.ArtifactSize); err != nil {
			_ = tx.Rollback()
			s.markIntentAborted(ctx, intentID)
			return Entry{}, err
		}
	}
	if err := markIntentDoneTx(ctx, tx, intentID, ns(now)); err != nil {
		_ = tx.Rollback()
		s.markIntentAborted(ctx, intentID)
		return Entry{}, err
	}
	if err := tx.Commit(); err != nil {
		s.markIntentAborted(ctx, intentID)
		return Entry{}, dbError("cannot commit state transition", err)
	}
	published, err := s.reloadAndPublish(ctx)
	if err != nil {
		return Entry{}, err
	}
	updated, ok := published.activeFor(cacheKey)
	if !ok {
		return Entry{}, newError(CodeInternal, "transition committed but active entry disappeared", map[string]any{
			"cache_key": cacheKey,
		})
	}
	return updated, nil
}

func isKnownState(state State) bool {
	switch state {
	case StatePending, StateWriting, StateAvailable, StateStale, StateEvicted:
		return true
	default:
		return false
	}
}

func (s State) String() string { return string(s) }
