package policy

import (
	"encoding/json"
	"fmt"
	"sort"

	"example.com/replicabudget/internal/apperr"
)

type Tolerance struct {
	RelativePermille int `json:"relative_permille"`
	AbsoluteReplicas int `json:"absolute_replicas"`
}

func DefaultTolerance() Tolerance {
	return Tolerance{RelativePermille: DefaultTolerancePM, AbsoluteReplicas: 0}
}

type MappingBasis struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Rule  string `json:"rule"`
	Value any    `json:"value,omitempty"`
}

type DegradationDetail struct {
	Field  string `json:"field"`
	Value  any    `json:"value,omitempty"`
	Reason string `json:"reason"`
}

type EquivalenceCheck struct {
	FixedReplicas     int     `json:"fixed_replicas"`
	ExpectedReplicas  int     `json:"expected_replicas"`
	Difference        int     `json:"difference"`
	AllowedDifference int     `json:"allowed_difference"`
	WithinTolerance   bool    `json:"within_tolerance"`
	QuotaGroupRef     string  `json:"quota_group_ref"`
	QuotaExists       bool    `json:"quota_exists"`
	ResourceCapacity  *uint64 `json:"resource_capacity_replicas,omitempty"`
	ConcurrencyNeed   int64   `json:"concurrency_need"`
	MinReplicas       int     `json:"min_replicas"`
	MaxReplicas       int     `json:"max_replicas"`
}

type MigrationDecision struct {
	ServiceID    string              `json:"service_id"`
	Decision     string              `json:"decision"`
	Reasons      []string            `json:"reasons"`
	Mappings     []MappingBasis      `json:"mapping_basis"`
	Degradations []DegradationDetail `json:"degradations"`
	Equivalence  EquivalenceCheck    `json:"equivalence"`
}

type WeightedReplicaCheck struct {
	OldIndexMicros          int64 `json:"old_index_micros"`
	ExpectedIndexMicros     int64 `json:"expected_index_micros"`
	DifferenceMicros        int64 `json:"difference_micros"`
	AllowedDifferenceMicros int64 `json:"allowed_difference_micros"`
	WithinTolerance         bool  `json:"within_tolerance"`
}

type MigrationPlan struct {
	Decision                 string               `json:"decision"`
	Tolerance                Tolerance            `json:"tolerance"`
	Decisions                []MigrationDecision  `json:"decisions"`
	Strategies               []NewStrategy        `json:"strategies"`
	NumericMismatches        []MigrationDecision  `json:"numeric_mismatches"`
	WeightedIndexEquivalence WeightedReplicaCheck `json:"weighted_index_equivalence"`
	GlobalReasons            []string             `json:"global_reasons"`
}

var knownDeprecatedFields = map[string]string{
	"quota_group_ref": "mapped_to_quota_group_ref",
	"placement_zone":  "mapped_to_placement_zone",
	"owner":           "dropped_known_obsolete",
	"legacy_note":     "dropped_known_obsolete",
}

// BuildMigrationPlan evaluates the old record against the quota snapshot. It is
// deterministic and never mutates an existing committed snapshot.
func BuildMigrationPlan(legacy []LegacyStrategy, quotas []QuotaGroup, tolerance Tolerance) MigrationPlan {
	qmap := quotaMap(quotas)
	if tolerance.RelativePermille < 0 {
		tolerance.RelativePermille = 0
	}
	if tolerance.AbsoluteReplicas < 0 {
		tolerance.AbsoluteReplicas = 0
	}
	ordered := append([]LegacyStrategy(nil), legacy...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ServiceID < ordered[j].ServiceID })
	plan := MigrationPlan{Decision: "accept", Tolerance: tolerance, Decisions: []MigrationDecision{}, Strategies: []NewStrategy{}, NumericMismatches: []MigrationDecision{}, GlobalReasons: []string{}}
	for _, old := range ordered {
		decision := MigrationDecision{
			ServiceID: old.ServiceID, Decision: "accepted", Reasons: []string{},
			Mappings: []MappingBasis{}, Degradations: []DegradationDetail{},
		}
		quotaRef := "q-default"
		placementZone := ""
		manualReasons := []string{}
		for key, raw := range old.DeprecatedFields {
			rule, known := knownDeprecatedFields[key]
			if !known {
				manualReasons = append(manualReasons, "unmapped_deprecated_field:"+key)
				continue
			}
			switch key {
			case "quota_group_ref":
				value, err := DecodeDeprecatedString(raw)
				if err != nil {
					manualReasons = append(manualReasons, "invalid_deprecated_field:quota_group_ref")
					continue
				}
				quotaRef = value
				decision.Mappings = append(decision.Mappings, MappingBasis{From: "deprecated_fields.quota_group_ref", To: "quota_group_ref", Rule: rule, Value: value})
			case "placement_zone":
				value, err := DecodeDeprecatedString(raw)
				if err != nil {
					manualReasons = append(manualReasons, "invalid_deprecated_field:placement_zone")
					continue
				}
				placementZone = value
				decision.Mappings = append(decision.Mappings, MappingBasis{From: "deprecated_fields.placement_zone", To: "placement_zone", Rule: rule, Value: value})
			default:
				var value any
				_ = json.Unmarshal(raw, &value)
				decision.Mappings = append(decision.Mappings, MappingBasis{From: "deprecated_fields." + key, To: "", Rule: rule, Value: value})
			}
		}
		minReplicas, maxReplicas := deriveLegacyBounds(old.FixedReplicas, old.ElasticEnabled)
		decision.Equivalence = EquivalenceCheck{
			FixedReplicas: old.FixedReplicas, QuotaGroupRef: quotaRef,
			ConcurrencyNeed: int64(old.FixedReplicas) * ConcurrencyPerReplica,
			MinReplicas:     minReplicas, MaxReplicas: maxReplicas,
		}
		if !old.ElasticEnabled {
			decision.Mappings = append(decision.Mappings,
				MappingBasis{From: "fixed_replicas", To: "min_replicas", Rule: "elastic disabled: min=max=fixed", Value: old.FixedReplicas},
				MappingBasis{From: "fixed_replicas", To: "max_replicas", Rule: "elastic disabled: min=max=fixed", Value: old.FixedReplicas},
			)
		} else {
			decision.Mappings = append(decision.Mappings,
				MappingBasis{From: "fixed_replicas", To: "min_replicas", Rule: "elastic enabled: floor(fixed*0.9), at least 1", Value: minReplicas},
				MappingBasis{From: "fixed_replicas", To: "max_replicas", Rule: "elastic enabled: fixed+ceil(fixed*0.2)", Value: maxReplicas},
			)
		}
		decision.Degradations = append(decision.Degradations,
			DegradationDetail{Field: "weight_permille", Value: DefaultWeightPermille, Reason: "legacy model has no cross-service weight; default per-mille weight applied"},
		)
		strategy := NewStrategy{
			ServiceID: old.ServiceID, MinReplicas: minReplicas, MaxReplicas: maxReplicas,
			TargetConcurrency: int64(old.FixedReplicas) * ConcurrencyPerReplica,
			LatencyBudgetMS:   LatencyReferenceMS, QuotaGroupRef: quotaRef,
			WeightPermille: DefaultWeightPermille, VRAMBytesPerReplica: old.VRAMBytesPerReplica,
			ComputeMilliPerReplica: old.ComputeMilliPerReplica, ElasticEnabled: old.ElasticEnabled,
			PlacementZone: placementZone,
		}
		quota, quotaExists := qmap[quotaRef]
		decision.Equivalence.QuotaExists = quotaExists
		expected := int64(strategy.MinReplicas)
		var capacity *uint64
		if quotaExists {
			capValue := resourceCapacity(strategy, quota)
			capacity = &capValue
			decision.Equivalence.ResourceCapacity = capacity
			expected = ConcurrencyNeed(strategy)
			if int64(capValue) < expected {
				expected = int64(capValue)
			}
			if expected > int64(strategy.MaxReplicas) {
				expected = int64(strategy.MaxReplicas)
			}
		} else {
			manualReasons = append(manualReasons, "quota_group_ref_not_found:"+quotaRef)
			expected = ConcurrencyNeed(strategy)
		}
		decision.Equivalence.ExpectedReplicas = int(expected)
		decision.Equivalence.Difference = absInt(int(expected) - old.FixedReplicas)
		allowed := tolerance.AbsoluteReplicas
		relative := (old.FixedReplicas*tolerance.RelativePermille + 999) / 1000
		if relative > allowed {
			allowed = relative
		}
		decision.Equivalence.AllowedDifference = allowed
		decision.Equivalence.WithinTolerance = decision.Equivalence.Difference <= allowed
		if !decision.Equivalence.WithinTolerance {
			manualReasons = append(manualReasons, fmt.Sprintf("replica_budget_mismatch:expected=%d fixed=%d allowed=%d", expected, old.FixedReplicas, allowed))
			plan.NumericMismatches = append(plan.NumericMismatches, decision)
		}
		if len(manualReasons) > 0 {
			decision.Decision = "manual_confirmation"
			decision.Reasons = append(decision.Reasons, manualReasons...)
			plan.Decision = "manual_confirmation"
		}
		plan.Decisions = append(plan.Decisions, decision)
		plan.Strategies = append(plan.Strategies, strategy)
	}
	oldServices := make([]DerivedService, 0, len(ordered))
	expectedServices := make([]DerivedService, 0, len(ordered))
	for _, decision := range plan.Decisions {
		oldServices = append(oldServices, DerivedService{
			Strategy:        NewStrategy{ServiceID: decision.ServiceID, WeightPermille: DefaultWeightPermille},
			DesiredReplicas: decision.Equivalence.FixedReplicas,
		})
		expectedServices = append(expectedServices, DerivedService{
			Strategy:        NewStrategy{ServiceID: decision.ServiceID, WeightPermille: DefaultWeightPermille},
			DesiredReplicas: decision.Equivalence.ExpectedReplicas,
		})
	}
	if len(oldServices) > 0 {
		oldIndex, oldErr := ComputeWeightedIndex(oldServices)
		expectedIndex, expectedErr := ComputeWeightedIndex(expectedServices)
		if oldErr == nil && expectedErr == nil {
			check := WeightedReplicaCheck{OldIndexMicros: oldIndex.IndexMicros, ExpectedIndexMicros: expectedIndex.IndexMicros}
			check.DifferenceMicros = absInt64(check.ExpectedIndexMicros - check.OldIndexMicros)
			check.AllowedDifferenceMicros = ceilDiv(absInt64(check.OldIndexMicros)*int64(tolerance.RelativePermille), 1000) + int64(tolerance.AbsoluteReplicas)*IndexScale
			check.WithinTolerance = check.DifferenceMicros <= check.AllowedDifferenceMicros
			plan.WeightedIndexEquivalence = check
			if !check.WithinTolerance {
				plan.Decision = "manual_confirmation"
				plan.GlobalReasons = append(plan.GlobalReasons, fmt.Sprintf("weighted_index_mismatch:old=%d expected=%d allowed=%d", check.OldIndexMicros, check.ExpectedIndexMicros, check.AllowedDifferenceMicros))
			}
		}
	}
	return plan
}

func deriveLegacyBounds(fixed int, elastic bool) (int, int) {
	if !elastic {
		return fixed, fixed
	}
	minReplicas := fixed - fixed*10/100
	if minReplicas < 1 {
		minReplicas = 1
	}
	maxReplicas := fixed + (fixed*20+99)/100
	if maxReplicas < fixed {
		maxReplicas = fixed
	}
	return minReplicas, maxReplicas
}

func (p MigrationPlan) AmbiguousError() error {
	return apperr.WithDetails(apperr.MigrationAmbiguous, "legacy migration requires manual confirmation", map[string]any{
		"decision": p.Decision, "tolerance": p.Tolerance, "decisions": p.Decisions,
		"numeric_mismatches":         p.NumericMismatches,
		"weighted_index_equivalence": p.WeightedIndexEquivalence,
		"global_reasons":             p.GlobalReasons,
	})
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
