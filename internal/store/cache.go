package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"example.com/gsb-router/internal/model"
)

func buildRouteCache(table model.LogicalTable, cacheVersion, expectedVersion int64, builtAt string) (RouteCache, error) {
	physicalSet := make(map[string]struct{}, len(table.Ranges))
	boundaries := make([]Boundary, 0, len(table.Ranges))
	for _, shard := range table.Ranges {
		physicalSet[shard.PhysicalTable] = struct{}{}
		boundaries = append(boundaries, Boundary{
			RangeID:        shard.ID,
			PhysicalTable:  shard.PhysicalTable,
			Lower:          shard.Lower,
			Upper:          shard.Upper,
			LowerInclusive: shard.LowerInclusive,
			UpperInclusive: shard.UpperInclusive,
			Weight:         shard.Weight,
		})
	}
	physicals := make([]string, 0, len(physicalSet))
	for physical := range physicalSet {
		physicals = append(physicals, physical)
	}
	sort.Strings(physicals)
	sort.Slice(boundaries, func(i, j int) bool {
		if boundaries[i].Lower != boundaries[j].Lower {
			return boundaries[i].Lower < boundaries[j].Lower
		}
		if boundaries[i].Upper != boundaries[j].Upper {
			return boundaries[i].Upper < boundaries[j].Upper
		}
		return boundaries[i].RangeID < boundaries[j].RangeID
	})
	canonical := struct {
		TableID        string     `json:"table_id"`
		PhysicalTables []string   `json:"physical_tables"`
		Boundaries     []Boundary `json:"boundaries"`
	}{TableID: table.ID, PhysicalTables: physicals, Boundaries: boundaries}
	payload, err := json.Marshal(canonical)
	if err != nil {
		return RouteCache{}, err
	}
	sum := sha256.Sum256(payload)
	algorithmJSON, _ := json.Marshal(table.Algorithm)
	refs := make([]CacheRef, 0, len(physicals)+4)
	for _, physical := range physicals {
		refs = append(refs, CacheRef{Kind: "physical_table", Value: physical})
	}
	refs = append(refs,
		CacheRef{Kind: "version", Value: fmt.Sprintf("%d", expectedVersion)},
		CacheRef{Kind: "shard_key", Value: table.ShardKey},
		CacheRef{Kind: "algorithm", Value: string(algorithmJSON)},
		CacheRef{Kind: "boundary_index", Value: hex.EncodeToString(sum[:])},
	)
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Kind != refs[j].Kind {
			return refs[i].Kind < refs[j].Kind
		}
		return refs[i].Value < refs[j].Value
	})
	return RouteCache{
		TableID:         table.ID,
		CacheVersion:    cacheVersion,
		ExpectedVersion: expectedVersion,
		PhysicalTables:  physicals,
		BoundaryIndex:   boundaries,
		Checksum:        hex.EncodeToString(sum[:]),
		BuiltAt:         builtAt,
		Refs:            refs,
	}, nil
}

func buildAggregateCache(view model.AggregateView, snapshot model.Snapshot, tableCaches map[string]RouteCache, cacheVersion, expectedVersion int64, builtAt string) (AggregateCache, error) {
	physicalSet := make(map[string]struct{})
	sourceVersions := make(map[string]int64, len(view.DependsOn))
	refs := make([]CacheRef, 0, len(view.DependsOn)*2)
	for _, source := range view.DependsOn {
		cache, ok := tableCaches[source]
		if !ok {
			return AggregateCache{}, fmt.Errorf("aggregate %s depends on missing cache %s", view.ID, source)
		}
		for _, physical := range cache.PhysicalTables {
			physicalSet[physical] = struct{}{}
		}
		sourceVersions[source] = cache.CacheVersion
		refs = append(refs, CacheRef{Kind: "logical_table", Value: source}, CacheRef{Kind: "route_cache_version", Value: fmt.Sprintf("%s:%d", source, cache.CacheVersion)})
	}
	physicals := make([]string, 0, len(physicalSet))
	for physical := range physicalSet {
		physicals = append(physicals, physical)
	}
	sort.Strings(physicals)
	payload := map[string]any{
		"view_id":             view.ID,
		"kind":                view.Kind,
		"source_tables":       append([]string(nil), view.DependsOn...),
		"source_versions":     sourceVersions,
		"physical_tables":     physicals,
		"materialized_rollup": true,
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Kind != refs[j].Kind {
			return refs[i].Kind < refs[j].Kind
		}
		return refs[i].Value < refs[j].Value
	})
	return AggregateCache{
		ViewID:          view.ID,
		CacheVersion:    cacheVersion,
		ExpectedVersion: expectedVersion,
		DependsOn:       append([]string(nil), view.DependsOn...),
		PhysicalTables:  physicals,
		Payload:         payload,
		BuiltAt:         builtAt,
		Refs:            refs,
	}, nil
}

func loadRouteCaches(ctx context.Context, db *sql.DB) (map[string]RouteCache, error) {
	rows, err := db.QueryContext(ctx, `SELECT table_id, cache_version, expected_version, physical_tables_json, boundary_index_json, checksum, built_at FROM route_cache`)
	if err != nil {
		return nil, fmt.Errorf("load route caches: %w", err)
	}
	defer rows.Close()
	caches := map[string]RouteCache{}
	for rows.Next() {
		var cache RouteCache
		var physicalJSON, boundaryJSON string
		if err := rows.Scan(&cache.TableID, &cache.CacheVersion, &cache.ExpectedVersion, &physicalJSON, &boundaryJSON, &cache.Checksum, &cache.BuiltAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(physicalJSON), &cache.PhysicalTables); err != nil {
			return nil, fmt.Errorf("decode physical tables for %s: %w", cache.TableID, err)
		}
		if err := json.Unmarshal([]byte(boundaryJSON), &cache.BoundaryIndex); err != nil {
			return nil, fmt.Errorf("decode boundary index for %s: %w", cache.TableID, err)
		}
		caches[cache.TableID] = cache
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for tableID, cache := range caches {
		refs, err := loadRefs(ctx, db, "route_cache_refs", "table_id", tableID)
		if err != nil {
			return nil, err
		}
		cache.Refs = refs
		caches[tableID] = cache
	}
	return caches, nil
}

func loadAggregateCaches(ctx context.Context, db *sql.DB) (map[string]AggregateCache, error) {
	rows, err := db.QueryContext(ctx, `SELECT view_id, cache_version, expected_version, depends_on_json, physical_tables_json, payload_json, built_at FROM aggregate_cache`)
	if err != nil {
		return nil, fmt.Errorf("load aggregate caches: %w", err)
	}
	defer rows.Close()
	caches := map[string]AggregateCache{}
	for rows.Next() {
		var cache AggregateCache
		var dependsJSON, physicalJSON, payloadJSON string
		if err := rows.Scan(&cache.ViewID, &cache.CacheVersion, &cache.ExpectedVersion, &dependsJSON, &physicalJSON, &payloadJSON, &cache.BuiltAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(dependsJSON), &cache.DependsOn); err != nil {
			return nil, fmt.Errorf("decode aggregate dependencies for %s: %w", cache.ViewID, err)
		}
		if err := json.Unmarshal([]byte(physicalJSON), &cache.PhysicalTables); err != nil {
			return nil, fmt.Errorf("decode aggregate physical tables for %s: %w", cache.ViewID, err)
		}
		if err := json.Unmarshal([]byte(payloadJSON), &cache.Payload); err != nil {
			return nil, fmt.Errorf("decode aggregate payload for %s: %w", cache.ViewID, err)
		}
		caches[cache.ViewID] = cache
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for viewID, cache := range caches {
		refs, err := loadRefs(ctx, db, "aggregate_cache_refs", "view_id", viewID)
		if err != nil {
			return nil, err
		}
		cache.Refs = refs
		caches[viewID] = cache
	}
	return caches, nil
}

func loadRefs(ctx context.Context, db *sql.DB, table, ownerColumn, ownerID string) ([]CacheRef, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT ref_kind, ref_value FROM %s WHERE %s = ? ORDER BY ref_kind, ref_value`, table, ownerColumn), ownerID)
	if err != nil {
		return nil, fmt.Errorf("load cache refs: %w", err)
	}
	defer rows.Close()
	refs := []CacheRef{}
	for rows.Next() {
		var ref CacheRef
		if err := rows.Scan(&ref.Kind, &ref.Value); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

func replaceAllCachesTx(ctx context.Context, tx *sql.Tx, routeCaches map[string]RouteCache, aggregates map[string]AggregateCache) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM route_cache_refs`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM route_cache`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM aggregate_cache_refs`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM aggregate_cache`); err != nil {
		return err
	}
	for _, cache := range routeCaches {
		physicalJSON, err := json.Marshal(cache.PhysicalTables)
		if err != nil {
			return err
		}
		boundaryJSON, err := json.Marshal(cache.BoundaryIndex)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO route_cache(table_id, cache_version, expected_version, physical_tables_json, boundary_index_json, checksum, built_at) VALUES(?,?,?,?,?,?,?)`,
			cache.TableID, cache.CacheVersion, cache.ExpectedVersion, string(physicalJSON), string(boundaryJSON), cache.Checksum, cache.BuiltAt); err != nil {
			return err
		}
		for _, ref := range cache.Refs {
			if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO route_cache_refs(table_id, ref_kind, ref_value) VALUES(?,?,?)`, cache.TableID, ref.Kind, ref.Value); err != nil {
				return err
			}
		}
	}
	for _, cache := range aggregates {
		dependsJSON, err := json.Marshal(cache.DependsOn)
		if err != nil {
			return err
		}
		physicalJSON, err := json.Marshal(cache.PhysicalTables)
		if err != nil {
			return err
		}
		payloadJSON, err := json.Marshal(cache.Payload)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO aggregate_cache(view_id, cache_version, expected_version, depends_on_json, physical_tables_json, payload_json, built_at) VALUES(?,?,?,?,?,?,?)`,
			cache.ViewID, cache.CacheVersion, cache.ExpectedVersion, string(dependsJSON), string(physicalJSON), string(payloadJSON), cache.BuiltAt); err != nil {
			return err
		}
		for _, ref := range cache.Refs {
			if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO aggregate_cache_refs(view_id, ref_kind, ref_value) VALUES(?,?,?)`, cache.ViewID, ref.Kind, ref.Value); err != nil {
				return err
			}
		}
	}
	return nil
}
