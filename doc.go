// Package prefixcache implements a deterministic prompt-prefix cache.
//
// The package owns four explicitly versioned stages: text normalization,
// block fingerprinting, slot allocation, and hit adjudication. Policy snapshots,
// slot maps, request states, invalidations, and evictions are committed as a
// single SQLite transaction. Callers that observe an older version receive a
// typed E_STALE_VERSION error instead of a stale hit decision.
package prefixcache
