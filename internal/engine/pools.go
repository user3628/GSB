package engine

import (
	"context"
	"database/sql"
	"sort"

	"checkpointengine/internal/model"
)

type placementResult struct {
	Shards        []model.Shard
	Evictions     []model.Eviction
	EvictionFrom  map[string]model.Shard
	VRAMUsed      int64
	PersistedUsed int64
}

// placeShards applies desired pool placements and evicts real rows when the
// VRAM limit would be exceeded. All bytes in this function are accounting
// bytes; no logical shard size is rounded or estimated.
func placeShards(all []model.Shard, incoming []model.Shard, vramLimit int64) (placementResult, error) {
	current := shardMap(all)
	protected := make(map[string]bool, len(incoming))
	for _, s := range incoming {
		protected[shardKey(s.Round, s.ShardID)] = true
	}
	var used int64
	for _, s := range current {
		if s.Location == model.PoolVRAM {
			used += s.Bytes
		}
	}
	evictions := make([]model.Eviction, 0)
	evictionFrom := make(map[string]model.Shard)

	sortedIncoming := sortedShards(incoming)
	for _, want := range sortedIncoming {
		key := shardKey(want.Round, want.ShardID)
		cur, exists := current[key]
		if !exists {
			cur = want
			cur.Location = model.PoolPersisted
			cur.WriteState = model.WritePending
		}
		if cur.Location == model.PoolVRAM {
			used -= cur.Bytes
		}
		want.DesiredPool = model.NormalizePool(want.DesiredPool)
		if !model.ValidPool(want.DesiredPool) {
			want.DesiredPool = model.DefaultTarget(want.Precision)
		}
		if want.DesiredPool == model.PoolPersisted {
			if cur.Location == model.PoolVRAM {
				evictionFrom[key] = cur
				evictions = append(evictions, evictionFor(cur, "POLICY_DEMOTION"))
			}
			want.Location = model.PoolPersisted
			current[key] = want
			continue
		}

		if used+want.Bytes > vramLimit {
			candidates := evictionCandidates(current, protected)
			reclaimable := int64(0)
			for _, c := range candidates {
				reclaimable += c.Bytes
			}
			for _, c := range candidates {
				if used+want.Bytes <= vramLimit {
					break
				}
				keyC := shardKey(c.Round, c.ShardID)
				evictionFrom[keyC] = c
				evictions = append(evictions, evictionFor(c, "LRU_FP16_FIRST"))
				c.Location = model.PoolPersisted
				c.DesiredPool = model.PoolPersisted
				current[keyC] = c
				used -= c.Bytes
			}
			if used+want.Bytes > vramLimit {
				return placementResult{}, model.WithDetails(
					model.NewAPIError(model.CodePoolExhausted, "unable to reclaim enough VRAM for shard"),
					map[string]any{
						"round":                    want.Round,
						"shard_id":                 want.ShardID,
						"required_bytes":           want.Bytes,
						"overflow_bytes":           used + want.Bytes - vramLimit,
						"reclaimable_bytes":        reclaimable,
						"available_after_eviction": used,
						"vram_limit_bytes":         vramLimit,
						"candidate_count":          len(candidates),
						"candidate_order":          "precision FP16 before FP32, then least-recently-used, lower priority, shard id",
					})
			}
		}
		want.Location = model.PoolVRAM
		current[key] = want
		used += want.Bytes
	}

	// A policy snapshot can lower the cap without submitting a new shard.
	if used > vramLimit {
		candidates := evictionCandidates(current, protected)
		for _, c := range candidates {
			if used <= vramLimit {
				break
			}
			key := shardKey(c.Round, c.ShardID)
			evictionFrom[key] = c
			evictions = append(evictions, evictionFor(c, "POLICY_LIMIT_LRU_FP16_FIRST"))
			c.Location = model.PoolPersisted
			c.DesiredPool = model.PoolPersisted
			current[key] = c
			used -= c.Bytes
		}
		if used > vramLimit {
			return placementResult{}, model.WithDetails(
				model.NewAPIError(model.CodePoolExhausted, "policy VRAM limit cannot be satisfied"),
				map[string]any{
					"overflow_bytes":   used - vramLimit,
					"vram_limit_bytes": vramLimit,
					"pinned_residency": used,
				})
		}
	}

	out := make([]model.Shard, 0, len(current))
	var persisted int64
	for _, s := range current {
		out = append(out, s)
		if s.Location == model.PoolPersisted {
			persisted += s.Bytes
		}
	}
	return placementResult{
		Shards:        sortedShards(out),
		Evictions:     evictions,
		EvictionFrom:  evictionFrom,
		VRAMUsed:      used,
		PersistedUsed: persisted,
	}, nil
}

func evictionFor(s model.Shard, reason string) model.Eviction {
	return model.Eviction{
		ShardID:      s.ShardID,
		Round:        s.Round,
		Bytes:        s.Bytes,
		Precision:    s.Precision,
		LastAccessMS: s.LastAccessMS,
		Reason:       reason,
	}
}

func evictionCandidates(current map[string]model.Shard, protected map[string]bool) []model.Shard {
	out := make([]model.Shard, 0)
	for key, s := range current {
		if s.Location != model.PoolVRAM || s.Pinned || protected[key] {
			continue
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		ri, rj := precisionEvictRank(out[i].Precision), precisionEvictRank(out[j].Precision)
		if ri != rj {
			return ri < rj
		}
		if out[i].LastAccessMS != out[j].LastAccessMS {
			return out[i].LastAccessMS < out[j].LastAccessMS
		}
		if out[i].Priority != out[j].Priority {
			return out[i].Priority < out[j].Priority
		}
		if out[i].Round != out[j].Round {
			return out[i].Round < out[j].Round
		}
		return out[i].ShardID < out[j].ShardID
	})
	return out
}

func precisionEvictRank(p string) int {
	if p == model.PrecisionFP16 {
		return 0
	}
	return 1
}

func rebuildPoolUsageTx(ctx context.Context, tx *sql.Tx, revision int64, now string) ([]model.PoolUsage, error) {
	type agg struct {
		bytes int64
		count int
	}
	values := map[string]agg{model.PoolVRAM: {}, model.PoolPersisted: {}}
	rows, err := tx.QueryContext(ctx, `SELECT location, COALESCE(SUM(bytes),0), COUNT(*) FROM shards GROUP BY location`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var pool string
		var a agg
		if err := rows.Scan(&pool, &a.bytes, &a.count); err != nil {
			rows.Close()
			return nil, err
		}
		values[pool] = a
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, pool := range []string{model.PoolVRAM, model.PoolPersisted} {
		a := values[pool]
		if _, err := tx.ExecContext(ctx, `INSERT INTO pool_usage(pool, used_bytes, shard_count, revision, updated_at)
			VALUES(?, ?, ?, ?, ?)
			ON CONFLICT(pool) DO UPDATE SET used_bytes=excluded.used_bytes,
			shard_count=excluded.shard_count, revision=excluded.revision, updated_at=excluded.updated_at`,
			pool, a.bytes, a.count, revision, now); err != nil {
			return nil, err
		}
	}
	return loadPoolsTx(ctx, tx)
}

func loadPoolsTx(ctx context.Context, tx *sql.Tx) ([]model.PoolUsage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT pool, used_bytes, shard_count, revision, updated_at FROM pool_usage ORDER BY pool`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.PoolUsage
	for rows.Next() {
		var p model.PoolUsage
		if err := rows.Scan(&p.Pool, &p.UsedBytes, &p.ShardCount, &p.Revision, &p.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
