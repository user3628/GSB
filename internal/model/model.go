package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"example.com/gsb-router/internal/errs"
)

const (
	AlgorithmMod   = "mod"
	AlgorithmHash  = "hash"
	AlgorithmRange = "range"

	ShardKeyInt    = "int"
	ShardKeyString = "string"
)

type Algorithm struct {
	Type    string `json:"type"`
	Modulus int64  `json:"modulus,omitempty"`
	Seed    uint64 `json:"seed,omitempty"`
}

type PhysicalTable struct {
	ID        string `json:"id"`
	LogicalDB string `json:"logical_db"`
	Name      string `json:"name"`
}

type ShardRange struct {
	ID             string `json:"id"`
	Lower          int64  `json:"lower"`
	Upper          int64  `json:"upper"`
	LowerInclusive bool   `json:"lower_inclusive"`
	UpperInclusive bool   `json:"upper_inclusive"`
	PhysicalTable  string `json:"physical_table"`
	Weight         int64  `json:"weight"`
}

type LogicalTable struct {
	ID           string       `json:"id"`
	LogicalDB    string       `json:"logical_db"`
	ShardKey     string       `json:"shard_key"`
	ShardKeyType string       `json:"shard_key_type"`
	Algorithm    Algorithm    `json:"algorithm"`
	PrimaryKey   string       `json:"primary_key,omitempty"`
	Ranges       []ShardRange `json:"ranges"`
}

type AggregateView struct {
	ID        string   `json:"id"`
	DependsOn []string `json:"depends_on"`
	Kind      string   `json:"kind,omitempty"`
}

// Snapshot is both the wire format and the durable route source of truth.
// Checksum covers a canonicalized form of every other field.
type Snapshot struct {
	Version        int64           `json:"version"`
	GeneratedAt    string          `json:"generated_at"`
	Cluster        string          `json:"cluster"`
	ShardKey       string          `json:"shard_key"`
	ShardKeyType   string          `json:"shard_key_type"`
	Algorithm      Algorithm       `json:"algorithm"`
	PhysicalTables []PhysicalTable `json:"physical_tables"`
	LogicalTables  []LogicalTable  `json:"logical_tables"`
	Aggregates     []AggregateView `json:"aggregates,omitempty"`
	Checksum       string          `json:"checksum"`
}

type TableChange struct {
	Op      string        `json:"op"`
	TableID string        `json:"table_id"`
	Table   *LogicalTable `json:"table,omitempty"`
	Ranges  []ShardRange  `json:"ranges,omitempty"`
}

const (
	ChangeUpsertTable   = "upsert_table"
	ChangeReplaceRanges = "replace_ranges"
	ChangeDeleteTable   = "delete_table"
)

// Increment is deliberately a patch description. Its base version is checked
// while the single writer lock is held.
type Increment struct {
	CommitID          string          `json:"commit_id"`
	BaseVersion       int64           `json:"base_version"`
	GeneratedAt       string          `json:"generated_at"`
	AddPhysicalTables []PhysicalTable `json:"add_physical_tables,omitempty"`
	Changes           []TableChange   `json:"changes"`
	Aggregates        []AggregateView `json:"aggregates,omitempty"`
	Checksum          string          `json:"checksum,omitempty"`
	Inject            string          `json:"inject,omitempty"`
}

type ValidationOptions struct {
	ExpectedVersion int64
	SealChecksum    bool
}

func ParseTime(value string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("generated_at must be RFC3339: %w", err)
	}
	return t.UTC(), nil
}

func CanonicalizeSnapshot(in Snapshot) Snapshot {
	out := in
	out.PhysicalTables = append([]PhysicalTable(nil), in.PhysicalTables...)
	sort.Slice(out.PhysicalTables, func(i, j int) bool {
		return out.PhysicalTables[i].ID < out.PhysicalTables[j].ID
	})
	out.LogicalTables = append([]LogicalTable(nil), in.LogicalTables...)
	for i := range out.LogicalTables {
		out.LogicalTables[i].Ranges = append([]ShardRange(nil), out.LogicalTables[i].Ranges...)
		sort.Slice(out.LogicalTables[i].Ranges, func(a, b int) bool {
			left, right := out.LogicalTables[i].Ranges[a], out.LogicalTables[i].Ranges[b]
			if left.Lower != right.Lower {
				return left.Lower < right.Lower
			}
			if left.Upper != right.Upper {
				return left.Upper < right.Upper
			}
			return left.ID < right.ID
		})
	}
	sort.Slice(out.LogicalTables, func(i, j int) bool {
		return out.LogicalTables[i].ID < out.LogicalTables[j].ID
	})
	out.Aggregates = append([]AggregateView(nil), in.Aggregates...)
	for i := range out.Aggregates {
		out.Aggregates[i].DependsOn = append([]string(nil), in.Aggregates[i].DependsOn...)
		sort.Strings(out.Aggregates[i].DependsOn)
	}
	sort.Slice(out.Aggregates, func(i, j int) bool {
		return out.Aggregates[i].ID < out.Aggregates[j].ID
	})
	return out
}

func SnapshotChecksum(in Snapshot) (string, error) {
	canonical := CanonicalizeSnapshot(in)
	canonical.Checksum = ""
	payload, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func IncrementChecksum(in Increment) (string, error) {
	canonical := in
	canonical.Checksum = ""
	canonical.Changes = append([]TableChange(nil), in.Changes...)
	sort.Slice(canonical.Changes, func(i, j int) bool {
		left, right := canonical.Changes[i], canonical.Changes[j]
		if left.TableID != right.TableID {
			return left.TableID < right.TableID
		}
		return left.Op < right.Op
	})
	canonical.AddPhysicalTables = append([]PhysicalTable(nil), in.AddPhysicalTables...)
	sort.Slice(canonical.AddPhysicalTables, func(i, j int) bool {
		return canonical.AddPhysicalTables[i].ID < canonical.AddPhysicalTables[j].ID
	})
	canonical.Aggregates = append([]AggregateView(nil), in.Aggregates...)
	sort.Slice(canonical.Aggregates, func(i, j int) bool {
		return canonical.Aggregates[i].ID < canonical.Aggregates[j].ID
	})
	payload, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// ValidateSnapshot validates and returns a canonical copy. Failed validation
// never mutates caller state or any committed store state.
func ValidateSnapshot(in Snapshot, opts ValidationOptions) (Snapshot, error) {
	s := CanonicalizeSnapshot(in)
	if s.Version < 1 {
		return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "snapshot version must be >= 1", map[string]any{"version": s.Version})
	}
	if opts.ExpectedVersion > 0 && s.Version <= opts.ExpectedVersion {
		return Snapshot{}, errs.New(errs.CodeVersionNotMonotonic, "snapshot version must be greater than the committed version", map[string]any{
			"snapshot_version":  s.Version,
			"committed_version": opts.ExpectedVersion,
		})
	}
	if _, err := ParseTime(s.GeneratedAt); err != nil {
		return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "invalid generated_at", map[string]any{"generated_at": s.GeneratedAt})
	}
	if strings.TrimSpace(s.Cluster) == "" {
		return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "cluster is required", nil)
	}
	if s.ShardKeyType != ShardKeyInt && s.ShardKeyType != ShardKeyString {
		return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "shard_key_type must be int or string", map[string]any{"shard_key_type": s.ShardKeyType})
	}
	if err := validateAlgorithm(s.Algorithm, "snapshot"); err != nil {
		return Snapshot{}, err
	}

	physical := make(map[string]PhysicalTable, len(s.PhysicalTables))
	for _, pt := range s.PhysicalTables {
		if strings.TrimSpace(pt.ID) == "" {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "physical table id is required", nil)
		}
		if _, exists := physical[pt.ID]; exists {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "physical table id must be unique", map[string]any{"physical_table": pt.ID})
		}
		if strings.TrimSpace(pt.LogicalDB) == "" || strings.TrimSpace(pt.Name) == "" {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "physical table database and name are required", map[string]any{"physical_table": pt.ID})
		}
		physical[pt.ID] = pt
	}

	seenTables := make(map[string]struct{}, len(s.LogicalTables))
	for ti := range s.LogicalTables {
		table := &s.LogicalTables[ti]
		if strings.TrimSpace(table.ID) == "" {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "logical table id is required", nil)
		}
		if _, exists := seenTables[table.ID]; exists {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "logical table id must be unique", map[string]any{"table_id": table.ID})
		}
		seenTables[table.ID] = struct{}{}
		if strings.TrimSpace(table.LogicalDB) == "" {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "logical_db is required", map[string]any{"table_id": table.ID})
		}
		if strings.TrimSpace(table.ShardKey) == "" {
			table.ShardKey = s.ShardKey
		}
		if strings.TrimSpace(table.ShardKeyType) == "" {
			table.ShardKeyType = s.ShardKeyType
		}
		if table.Algorithm.Type == "" {
			table.Algorithm = s.Algorithm
		}
		if table.ShardKeyType != ShardKeyInt && table.ShardKeyType != ShardKeyString {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "logical table shard_key_type must be int or string", map[string]any{"table_id": table.ID})
		}
		if err := validateAlgorithm(table.Algorithm, table.ID); err != nil {
			return Snapshot{}, err
		}
		if table.Algorithm.Type != AlgorithmHash && table.ShardKeyType != ShardKeyInt {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "mod/range sharding requires an int shard key", map[string]any{"table_id": table.ID})
		}
		if len(table.Ranges) == 0 {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "logical table must contain at least one shard range", map[string]any{"table_id": table.ID})
		}
		rangeIDs := make(map[string]struct{}, len(table.Ranges))
		for ri := range table.Ranges {
			shard := &table.Ranges[ri]
			if strings.TrimSpace(shard.ID) == "" {
				return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "shard range id is required", map[string]any{"table_id": table.ID})
			}
			if _, exists := rangeIDs[shard.ID]; exists {
				return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "shard range id must be unique within table", map[string]any{"table_id": table.ID, "range_id": shard.ID})
			}
			rangeIDs[shard.ID] = struct{}{}
			if shard.Lower > shard.Upper {
				return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "shard range lower must not exceed upper", map[string]any{"table_id": table.ID, "range_id": shard.ID, "lower": shard.Lower, "upper": shard.Upper})
			}
			if shard.Lower == shard.Upper && (!shard.LowerInclusive || !shard.UpperInclusive) {
				return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "a singleton shard range must be closed at both ends", map[string]any{"table_id": table.ID, "range_id": shard.ID})
			}
			if shard.Weight < 0 {
				return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "shard range weight must be non-negative", map[string]any{"table_id": table.ID, "range_id": shard.ID, "weight": shard.Weight})
			}
			if _, exists := physical[shard.PhysicalTable]; !exists {
				return Snapshot{}, errs.Snapshot(errs.CodePhysicalTableMiss, "shard range references a missing physical table", map[string]any{
					"table_id":       table.ID,
					"range_id":       shard.ID,
					"physical_table": shard.PhysicalTable,
				})
			}
		}
		for i := 1; i < len(table.Ranges); i++ {
			prev, cur := table.Ranges[i-1], table.Ranges[i]
			overlap := prev.Upper > cur.Lower ||
				(prev.Upper == cur.Lower && prev.UpperInclusive && cur.LowerInclusive)
			if overlap {
				return Snapshot{}, errs.Snapshot(errs.CodeRangeOverlap, "adjacent shard ranges overlap", map[string]any{
					"table_id":           table.ID,
					"previous_range":     prev.ID,
					"current_range":      cur.ID,
					"previous_upper":     prev.Upper,
					"current_lower":      cur.Lower,
					"previous_inclusive": prev.UpperInclusive,
					"current_inclusive":  cur.LowerInclusive,
				})
			}
		}
	}

	seenViews := make(map[string]struct{}, len(s.Aggregates))
	for _, view := range s.Aggregates {
		if strings.TrimSpace(view.ID) == "" {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "aggregate view id is required", nil)
		}
		if _, exists := seenViews[view.ID]; exists {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "aggregate view id must be unique", map[string]any{"view_id": view.ID})
		}
		seenViews[view.ID] = struct{}{}
		if len(view.DependsOn) == 0 {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "aggregate view must depend on at least one logical table", map[string]any{"view_id": view.ID})
		}
		depSeen := make(map[string]struct{}, len(view.DependsOn))
		for _, dep := range view.DependsOn {
			if _, exists := seenTables[dep]; !exists {
				return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "aggregate view depends on a missing logical table", map[string]any{"view_id": view.ID, "table_id": dep})
			}
			if _, exists := depSeen[dep]; exists {
				return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "aggregate view has a duplicate dependency", map[string]any{"view_id": view.ID, "table_id": dep})
			}
			depSeen[dep] = struct{}{}
		}
	}

	if opts.SealChecksum || strings.EqualFold(s.Checksum, "auto") {
		sum, err := SnapshotChecksum(s)
		if err != nil {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "could not compute snapshot checksum", nil)
		}
		s.Checksum = sum
	} else {
		if s.Checksum == "" {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "snapshot checksum is required", nil)
		}
		sum, err := SnapshotChecksum(s)
		if err != nil {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "could not compute snapshot checksum", nil)
		}
		if !strings.EqualFold(sum, s.Checksum) {
			return Snapshot{}, errs.Snapshot(errs.CodeSnapshotInvalid, "snapshot checksum mismatch", map[string]any{"expected": sum, "actual": s.Checksum})
		}
	}
	return s, nil
}

func validateAlgorithm(alg Algorithm, owner string) error {
	switch alg.Type {
	case AlgorithmMod, AlgorithmHash:
		if alg.Modulus <= 0 {
			return errs.Snapshot(errs.CodeSnapshotInvalid, "mod/hash algorithm modulus must be positive", map[string]any{"owner": owner, "modulus": alg.Modulus})
		}
	case AlgorithmRange:
		// Range sharding compares key values directly with each shard interval.
	default:
		return errs.Snapshot(errs.CodeSnapshotInvalid, "algorithm type must be mod, hash, or range", map[string]any{"owner": owner, "type": alg.Type})
	}
	return nil
}

func (s Snapshot) Clone() Snapshot {
	return CanonicalizeSnapshot(s)
}

func (s Snapshot) LogicalTable(id string) (*LogicalTable, bool) {
	for i := range s.LogicalTables {
		if s.LogicalTables[i].ID == id {
			return &s.LogicalTables[i], true
		}
	}
	return nil, false
}

func (s Snapshot) PhysicalTableIDs() []string {
	ids := make([]string, 0, len(s.PhysicalTables))
	for _, table := range s.PhysicalTables {
		ids = append(ids, table.ID)
	}
	sort.Strings(ids)
	return ids
}
