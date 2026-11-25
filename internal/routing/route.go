package routing

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"strings"

	"example.com/gsb-router/internal/errs"
	"example.com/gsb-router/internal/model"
)

const (
	KindEqual = "eq"
	KindRange = "range"
	KindIn    = "in"
)

type Query struct {
	TableID      string   `json:"table_id"`
	Observed     int64    `json:"observed_version"`
	Kind         string   `json:"kind"`
	Value        *string  `json:"value,omitempty"`
	Values       []string `json:"values,omitempty"`
	Lower        *string  `json:"lower,omitempty"`
	Upper        *string  `json:"upper,omitempty"`
	IncludeLower *bool    `json:"include_lower,omitempty"`
	IncludeUpper *bool    `json:"include_upper,omitempty"`
}

type Hit struct {
	PhysicalTable string `json:"physical_table"`
	RangeID       string `json:"range_id"`
	Slot          *int64 `json:"slot,omitempty"`
}

type Result struct {
	TableID        string   `json:"table_id"`
	Kind           string   `json:"kind"`
	PhysicalTables []string `json:"physical_tables"`
	Hits           []Hit    `json:"hits"`
	Conservative   bool     `json:"conservative"`
	Empty          bool     `json:"empty"`
}

// Compute is pure and deterministic. It never consults wall-clock time or a
// random source. Hits and physical table IDs are both stably sorted.
func Compute(table *model.LogicalTable, q Query) (Result, error) {
	if table == nil {
		return Result{}, errs.New(errs.CodeTableNotFound, "logical table not found", map[string]any{"table_id": q.TableID})
	}
	result := Result{TableID: table.ID, Kind: q.Kind, PhysicalTables: []string{}, Hits: []Hit{}}
	switch q.Kind {
	case KindEqual:
		if q.Value == nil {
			return Result{}, errs.New(errs.CodeQueryInvalid, "eq query requires value", map[string]any{"table_id": table.ID})
		}
		hits, err := equalHits(table, *q.Value)
		if err != nil {
			return Result{}, err
		}
		result.Hits = hits
	case KindIn:
		if q.Values == nil {
			return Result{}, errs.New(errs.CodeQueryInvalid, "in query requires values", map[string]any{"table_id": table.ID})
		}
		for _, value := range q.Values {
			hits, err := equalHits(table, value)
			if err != nil {
				return Result{}, err
			}
			result.Hits = append(result.Hits, hits...)
		}
	case KindRange:
		if q.Lower == nil || q.Upper == nil || q.IncludeLower == nil || q.IncludeUpper == nil {
			return Result{}, errs.New(errs.CodeQueryInvalid, "range query requires lower, upper, include_lower and include_upper", map[string]any{"table_id": table.ID})
		}
		hits, conservative, err := rangeHits(table, *q.Lower, *q.Upper, *q.IncludeLower, *q.IncludeUpper)
		if err != nil {
			return Result{}, err
		}
		result.Hits = hits
		result.Conservative = conservative
	default:
		return Result{}, errs.New(errs.CodeQueryInvalid, "query kind must be eq, in, or range", map[string]any{"table_id": table.ID, "kind": q.Kind})
	}
	result.Hits = dedupeHits(result.Hits)
	result.PhysicalTables = uniquePhysicalTables(result.Hits)
	result.Empty = len(result.PhysicalTables) == 0
	return result, nil
}

func equalHits(table *model.LogicalTable, raw string) ([]Hit, error) {
	slot, err := slotForValue(table, raw)
	if err != nil {
		return nil, err
	}
	hits := make([]Hit, 0, 1)
	for _, shard := range table.Ranges {
		if containsInt(shard, slot) {
			hits = append(hits, Hit{PhysicalTable: shard.PhysicalTable, RangeID: shard.ID, Slot: int64Ptr(slot)})
		}
	}
	return hits, nil
}

func rangeHits(table *model.LogicalTable, lowerRaw, upperRaw string, includeLower, includeUpper bool) ([]Hit, bool, error) {
	lower, err := parseInt(lowerRaw)
	if err != nil {
		return nil, false, errs.New(errs.CodeQueryInvalid, "range lower must be an integer", map[string]any{"table_id": table.ID, "lower": lowerRaw})
	}
	upper, err := parseInt(upperRaw)
	if err != nil {
		return nil, false, errs.New(errs.CodeQueryInvalid, "range upper must be an integer", map[string]any{"table_id": table.ID, "upper": upperRaw})
	}
	queryFirst, queryLast, queryOK := integerInterval(lower, upper, includeLower, includeUpper)
	if !queryOK {
		return []Hit{}, false, nil
	}

	switch table.Algorithm.Type {
	case model.AlgorithmRange:
		hits := make([]Hit, 0, len(table.Ranges))
		for _, shard := range table.Ranges {
			shardFirst, shardLast, shardOK := integerInterval(shard.Lower, shard.Upper, shard.LowerInclusive, shard.UpperInclusive)
			if !shardOK {
				continue
			}
			if maxInt64(queryFirst, shardFirst) <= minInt64(queryLast, shardLast) {
				hits = append(hits, Hit{PhysicalTable: shard.PhysicalTable, RangeID: shard.ID})
			}
		}
		return hits, false, nil
	case model.AlgorithmMod:
		hits := make([]Hit, 0, len(table.Ranges))
		intervals := possibleModIntervals(queryFirst, queryLast, table.Algorithm.Modulus)
		for _, shard := range table.Ranges {
			for _, interval := range intervals {
				if rangeIntersectsSlots(shard, interval[0], interval[1]) {
					hits = append(hits, Hit{PhysicalTable: shard.PhysicalTable, RangeID: shard.ID})
					break
				}
			}
		}
		return hits, false, nil
	case model.AlgorithmHash:
		// A hash range has no useful bounded preimage without enumerating all
		// possible keys. Returning every physical shard is explicit and safe.
		hits := make([]Hit, 0, len(table.Ranges))
		for _, shard := range table.Ranges {
			hits = append(hits, Hit{PhysicalTable: shard.PhysicalTable, RangeID: shard.ID})
		}
		return hits, true, nil
	default:
		return nil, false, errs.New(errs.CodeQueryInvalid, "unsupported sharding algorithm", map[string]any{"table_id": table.ID, "algorithm": table.Algorithm.Type})
	}
}

func possibleModIntervals(first, last, modulus int64) [][2]int64 {
	if modulus <= 0 {
		return nil
	}
	if first > last {
		return nil
	}
	overflowSpan := first < 0 && last > math.MaxInt64+first
	if !overflowSpan {
		span := last - first
		if span >= modulus-1 {
			return [][2]int64{{0, modulus - 1}}
		}
		firstSlot := modFloor(first, modulus)
		lastSlot := modFloor(first+span, modulus)
		if firstSlot <= lastSlot {
			return [][2]int64{{firstSlot, lastSlot}}
		}
		return [][2]int64{{0, lastSlot}, {firstSlot, modulus - 1}}
	}
	return [][2]int64{{0, modulus - 1}}
}

func slotForValue(table *model.LogicalTable, raw string) (int64, error) {
	if table.Algorithm.Type == model.AlgorithmHash {
		return hashSlot(raw, table.Algorithm.Seed, table.Algorithm.Modulus), nil
	}
	value, err := parseInt(raw)
	if err != nil {
		return 0, errs.New(errs.CodeQueryInvalid, "shard key must be an integer", map[string]any{"table_id": table.ID, "value": raw})
	}
	if table.Algorithm.Type == model.AlgorithmRange {
		return value, nil
	}
	if table.Algorithm.Type == model.AlgorithmMod {
		return modFloor(value, table.Algorithm.Modulus), nil
	}
	return 0, errs.New(errs.CodeQueryInvalid, "unsupported sharding algorithm", map[string]any{"table_id": table.ID, "algorithm": table.Algorithm.Type})
}

func hashSlot(value string, seed uint64, modulus int64) int64 {
	h := fnv.New64a()
	var seedBytes [8]byte
	binary.LittleEndian.PutUint64(seedBytes[:], seed)
	_, _ = h.Write(seedBytes[:])
	_, _ = h.Write([]byte(value))
	return int64(h.Sum64() % uint64(modulus))
}

func modFloor(value, modulus int64) int64 {
	remainder := value % modulus
	if remainder < 0 {
		remainder += modulus
	}
	return remainder
}

func parseInt(raw string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
}

func integerInterval(lower, upper int64, includeLower, includeUpper bool) (int64, int64, bool) {
	first := lower
	if !includeLower {
		if lower == math.MaxInt64 {
			return 0, 0, false
		}
		first++
	}
	last := upper
	if !includeUpper {
		if upper == math.MinInt64 {
			return 0, 0, false
		}
		last--
	}
	if first > last {
		return 0, 0, false
	}
	return first, last, true
}

func containsInt(shard model.ShardRange, value int64) bool {
	if value < shard.Lower || value > shard.Upper {
		return false
	}
	if value == shard.Lower && !shard.LowerInclusive {
		return false
	}
	if value == shard.Upper && !shard.UpperInclusive {
		return false
	}
	return true
}

func rangeIntersectsSlots(shard model.ShardRange, first, last int64) bool {
	shardFirst, shardLast, ok := integerInterval(shard.Lower, shard.Upper, shard.LowerInclusive, shard.UpperInclusive)
	if !ok {
		return false
	}
	return maxInt64(first, shardFirst) <= minInt64(last, shardLast)
}

func dedupeHits(hits []Hit) []Hit {
	seen := make(map[string]struct{}, len(hits))
	out := make([]Hit, 0, len(hits))
	for _, hit := range hits {
		slot := "-"
		if hit.Slot != nil {
			slot = strconv.FormatInt(*hit.Slot, 10)
		}
		key := hit.PhysicalTable + "\x00" + hit.RangeID + "\x00" + slot
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, hit)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PhysicalTable != out[j].PhysicalTable {
			return out[i].PhysicalTable < out[j].PhysicalTable
		}
		if out[i].RangeID != out[j].RangeID {
			return out[i].RangeID < out[j].RangeID
		}
		if out[i].Slot == nil {
			return out[j].Slot != nil
		}
		if out[j].Slot == nil {
			return false
		}
		return *out[i].Slot < *out[j].Slot
	})
	return out
}

func uniquePhysicalTables(hits []Hit) []string {
	seen := make(map[string]struct{}, len(hits))
	tables := make([]string, 0, len(hits))
	for _, hit := range hits {
		if _, exists := seen[hit.PhysicalTable]; exists {
			continue
		}
		seen[hit.PhysicalTable] = struct{}{}
		tables = append(tables, hit.PhysicalTable)
	}
	sort.Strings(tables)
	return tables
}

func int64Ptr(value int64) *int64 { return &value }

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
