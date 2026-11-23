package cache

import (
	"context"
	"fmt"
)

type catalog struct {
	entries    map[string]Entry
	active     map[string]Entry
	latest     map[string]Entry
	artifacts  map[string]Artifact
	sequence   int64
	generation uint64
	usedBytes  int64
}

func emptyCatalog() *catalog {
	return &catalog{
		entries:   make(map[string]Entry),
		active:    make(map[string]Entry),
		latest:    make(map[string]Entry),
		artifacts: make(map[string]Artifact),
	}
}

func (c *catalog) activeFor(key string) (Entry, bool) {
	entry, ok := c.active[key]
	return entry, ok
}

func (c *catalog) latestFor(key string) (Entry, bool) {
	entry, ok := c.latest[key]
	return entry, ok
}

func (c *catalog) entryCounts() map[string]int {
	counts := map[string]int{
		string(StatePending):   0,
		string(StateWriting):   0,
		string(StateAvailable): 0,
		string(StateStale):     0,
		string(StateEvicted):   0,
	}
	for _, entry := range c.entries {
		counts[string(entry.State)]++
	}
	return counts
}

func (s *Store) loadCatalog(ctx context.Context) (*catalog, error) {
	c := emptyCatalog()
	rows, err := s.db.QueryContext(ctx, `
SELECT entry_id, cache_key, stage_id, input_fingerprint, toolchain_version,
       artifact_digest, artifact_size, hits, retain_until_ns, created_at_ns,
       last_hit_ns, state, version
FROM entries
ORDER BY created_at_ns ASC, entry_id ASC`)
	if err != nil {
		return nil, wrapError(CodeIO, "cannot load cache entries", nil, err)
	}
	defer rows.Close()
	for rows.Next() {
		entry, err := scanEntry(rows)
		if err != nil {
			return nil, wrapError(CodeIO, "cannot decode cache entry", nil, err)
		}
		c.entries[entry.EntryID] = entry
		c.latest[entry.CacheKey] = entry
		if entry.State != StateEvicted {
			c.active[entry.CacheKey] = entry
		}
	}
	if err := rows.Err(); err != nil {
		return nil, wrapError(CodeIO, "cannot iterate cache entries", nil, err)
	}

	artifactRows, err := s.db.QueryContext(ctx, `
SELECT digest, size, relative_path, ref_count
FROM artifacts
ORDER BY digest ASC`)
	if err != nil {
		return nil, wrapError(CodeIO, "cannot load artifacts", nil, err)
	}
	defer artifactRows.Close()
	for artifactRows.Next() {
		var artifact Artifact
		if err := artifactRows.Scan(&artifact.Digest, &artifact.Size, &artifact.RelativePath, &artifact.RefCount); err != nil {
			return nil, wrapError(CodeIO, "cannot decode artifact", nil, err)
		}
		c.artifacts[artifact.Digest] = artifact
		if artifact.RefCount > 0 {
			c.usedBytes += artifact.Size
		}
	}
	if err := artifactRows.Err(); err != nil {
		return nil, wrapError(CodeIO, "cannot iterate artifacts", nil, err)
	}

	sequence, err := s.readSequence(ctx)
	if err != nil {
		return nil, err
	}
	c.sequence = sequence
	return c, nil
}

func (s *Store) currentCatalog() *catalog {
	s.visibility.RLock()
	defer s.visibility.RUnlock()
	return s.catalog
}

// publish installs a fully loaded catalog. Readers either retain the previous
// pointer until their current operation ends or observe this complete version;
// they never observe a half-applied mutation.
func (s *Store) publish(c *catalog) {
	s.visibility.Lock()
	s.generation++
	c.generation = s.generation
	s.catalog = c
	s.visibility.Unlock()
}

func (s *Store) reloadAndPublish(ctx context.Context) (*catalog, error) {
	c, err := s.loadCatalog(ctx)
	if err != nil {
		return nil, err
	}
	s.publish(c)
	return c, nil
}

func (c *catalog) String() string {
	return fmt.Sprintf("catalog{gen:%d seq:%d entries:%d used:%d}", c.generation, c.sequence, len(c.entries), c.usedBytes)
}
