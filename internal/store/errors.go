package store

import "example.com/gsb-router/internal/errs"

func errsTableNotFound(tableID string) *errs.Error {
	return errs.New(errs.CodeTableNotFound, "logical table not found", map[string]any{"table_id": tableID})
}

func cacheInconsistent(tableID, reason string) *errs.Error {
	return errs.New(errs.CodeCacheInconsistent, "route cache is inconsistent", map[string]any{"table_id": tableID, "reason": reason})
}

func observedAhead(tableID string, observed, current int64) *errs.Error {
	return errs.New(errs.CodeObservedAhead, "observed version is ahead of the committed version", map[string]any{
		"table_id":         tableID,
		"observed_version": observed,
		"current_version":  current,
	})
}

func staleRead(tableID string, cacheVersion, expectedVersion, observed, current int64, reason string) *errs.Error {
	return errs.New(errs.CodeStaleRead, "version-bound read rejected", map[string]any{
		"table_id":         tableID,
		"cache_version":    cacheVersion,
		"expected_version": expectedVersion,
		"observed_version": observed,
		"current_version":  current,
		"reason":           reason,
	})
}
