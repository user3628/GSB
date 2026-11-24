package policy

import (
	"encoding/json"
	"fmt"
	"sort"
)

const (
	FormatLegacyJSON = "legacy_json"
	FormatJSONL      = "jsonl"
	FormatBinary     = "binary"
	FormatAuto       = "auto"

	LegacyFormatVersion  = 0
	CurrentFormatVersion = 1
	BinaryMagic          = "RSPB"

	DefaultWeightPermille = 1000
	ConcurrencyPerReplica = 64
	LatencyReferenceMS    = 100
	IndexScale            = 1_000_000
	DefaultTolerancePM    = 50
)

// NewStrategy is the hierarchical v1 policy model. Resources per replica are
// optional in the persisted formats, but keep the migration inputs explicit.
type NewStrategy struct {
	ServiceID              string `json:"service_id"`
	MinReplicas            int    `json:"min_replicas"`
	MaxReplicas            int    `json:"max_replicas"`
	TargetConcurrency      int64  `json:"target_concurrency"`
	LatencyBudgetMS        int    `json:"latency_budget_ms"`
	QuotaGroupRef          string `json:"quota_group_ref"`
	WeightPermille         int    `json:"weight_permille"`
	VRAMBytesPerReplica    uint64 `json:"vram_bytes_per_replica,omitempty"`
	ComputeMilliPerReplica uint64 `json:"compute_milli_per_replica,omitempty"`
	ElasticEnabled         bool   `json:"elastic_enabled"`
	PlacementZone          string `json:"placement_zone,omitempty"`
}

// LegacyStrategy is deliberately flat. DeprecatedFields keeps old, unknown
// fields visible to the migration decision instead of silently dropping them.
type LegacyStrategy struct {
	ServiceID              string                     `json:"service_id"`
	FixedReplicas          int                        `json:"fixed_replicas"`
	VRAMBytesPerReplica    uint64                     `json:"vram_bytes_per_replica"`
	ComputeMilliPerReplica uint64                     `json:"compute_milli_per_replica"`
	ElasticEnabled         bool                       `json:"elastic_enabled"`
	DeprecatedFields       map[string]json.RawMessage `json:"deprecated_fields,omitempty"`

	RecordIndex int    `json:"-"`
	SourceJSON  []byte `json:"-"`
}

type QuotaGroup struct {
	ID           string `json:"id"`
	VRAMBytes    uint64 `json:"vram_bytes"`
	ComputeMilli uint64 `json:"compute_milli"`
}

type Placement struct {
	ReplicaIndex int    `json:"replica_index"`
	NodeID       string `json:"node_id"`
}

type DerivedService struct {
	Strategy        NewStrategy `json:"strategy"`
	DesiredReplicas int         `json:"desired_replicas"`
	ConcurrencyNeed int64       `json:"concurrency_need"`
	ResourceCap     uint64      `json:"resource_cap"`
	Placements      []Placement `json:"placements"`
}

type IndexStep struct {
	ServiceID              string `json:"service_id"`
	DesiredReplicas        int    `json:"desired_replicas"`
	WeightPermille         int    `json:"weight_permille"`
	ExactNumerator         string `json:"exact_numerator"`
	ExactDenominator       string `json:"exact_denominator"`
	RoundedMicros          int64  `json:"rounded_micros"`
	Rounding               string `json:"rounding"`
	RoundingErrorNumerator string `json:"rounding_error_numerator"`
}

type WeightedIndex struct {
	Scale                              int         `json:"scale"`
	WeightTotal                        int64       `json:"weight_total"`
	IndexMicros                        int64       `json:"index_micros"`
	IndexDecimal                       string      `json:"index_decimal"`
	SumRoundedMicros                   int64       `json:"sum_rounded_micros"`
	CompensationMethod                 string      `json:"compensation_method"`
	CompensatedAdjustmentMicros        int64       `json:"compensated_adjustment_micros"`
	CumulativeRoundingErrorNumerator   string      `json:"cumulative_rounding_error_numerator"`
	CumulativeRoundingErrorDenominator string      `json:"cumulative_rounding_error_denominator"`
	Steps                              []IndexStep `json:"steps"`
	OrderedServiceIDs                  []string    `json:"ordered_service_ids"`
}

type ThresholdDecision struct {
	ThresholdMicros           int64    `json:"threshold_micros"`
	RelativeTolerancePermille int      `json:"relative_tolerance_permille"`
	ToleranceMicros           int64    `json:"tolerance_micros"`
	DifferenceMicros          int64    `json:"difference_micros"`
	Exceeded                  bool     `json:"exceeded"`
	WithinTolerance           bool     `json:"within_tolerance"`
	Decision                  string   `json:"decision"`
	TieBreakServices          []string `json:"tie_break_services"`
}

type VersionedState struct {
	Version     int               `json:"version"`
	Policies    []DerivedService  `json:"policies"`
	QuotaGroups []QuotaGroup      `json:"quota_groups"`
	Index       WeightedIndex     `json:"weighted_index"`
	Threshold   ThresholdDecision `json:"threshold"`
}

func SortStrategies(strategies []NewStrategy) {
	sort.Slice(strategies, func(i, j int) bool {
		return strategies[i].ServiceID < strategies[j].ServiceID
	})
}

func SortedQuotaGroups(groups []QuotaGroup) {
	sort.Slice(groups, func(i, j int) bool { return groups[i].ID < groups[j].ID })
}

func quotaMap(groups []QuotaGroup) map[string]QuotaGroup {
	out := make(map[string]QuotaGroup, len(groups))
	for _, group := range groups {
		out[group.ID] = group
	}
	return out
}

func ceilDiv(n, d int64) int64 {
	if n <= 0 {
		return 0
	}
	return (n + d - 1) / d
}

func clampInt64(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func FormatMicros(v int64) string {
	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}
	return fmt.Sprintf("%s%d.%06d", sign, v/IndexScale, v%IndexScale)
}
