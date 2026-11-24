package policy

import (
	"fmt"
	"hash/fnv"
	"math/big"
	"sort"

	"example.com/replicabudget/internal/apperr"
)

const DefaultThresholdMicros int64 = 5_000_000
const DefaultRelativeTolerancePermille = 1

func ConcurrencyNeed(s NewStrategy) int64 {
	base := ceilDiv(s.TargetConcurrency, ConcurrencyPerReplica)
	latency := ceilDiv(s.TargetConcurrency*int64(LatencyReferenceMS), int64(ConcurrencyPerReplica)*int64(s.LatencyBudgetMS))
	return max(base, latency)
}

func DeriveAll(strategies []NewStrategy, quotas []QuotaGroup, version int) ([]DerivedService, WeightedIndex, error) {
	qmap := quotaMap(quotas)
	ordered := append([]NewStrategy(nil), strategies...)
	SortStrategies(ordered)
	derived := make([]DerivedService, 0, len(ordered))
	for i, strategy := range ordered {
		if err := ValidateStrategy(strategy, qmap, i+1); err != nil {
			return nil, WeightedIndex{}, err
		}
		need := ConcurrencyNeed(strategy)
		bounded := clampInt64(need, int64(strategy.MinReplicas), int64(strategy.MaxReplicas))
		quota := qmap[strategy.QuotaGroupRef]
		capacity := resourceCapacity(strategy, quota)
		desired := bounded
		if capacity < uint64(desired) {
			desired = int64(capacity)
		}
		placements := placementsFor(strategy.ServiceID, desired)
		derived = append(derived, DerivedService{
			Strategy: strategy, DesiredReplicas: int(desired), ConcurrencyNeed: need,
			ResourceCap: capacity, Placements: placements,
		})
	}
	index, err := ComputeWeightedIndex(derived)
	if err != nil {
		return nil, WeightedIndex{}, err
	}
	_ = version
	return derived, index, nil
}

func placementsFor(serviceID string, desired int64) []Placement {
	if desired <= 0 {
		return []Placement{}
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(serviceID))
	base := h.Sum32() % 8
	out := make([]Placement, 0, desired)
	for i := int64(0); i < desired; i++ {
		node := (base + uint32(i*3)) % 8
		out = append(out, Placement{ReplicaIndex: int(i), NodeID: fmt.Sprintf("node-%02d", node)})
	}
	return out
}

// ComputeWeightedIndex uses exact integer arithmetic. Each service contribution
// is rounded half-up at micro precision, residuals are accumulated as an exact
// fixed-point compensation, and the final index is recomputed from the exact
// numerator. No float operation participates in the contract.
func ComputeWeightedIndex(services []DerivedService) (WeightedIndex, error) {
	if len(services) == 0 {
		return WeightedIndex{Scale: IndexScale, Steps: []IndexStep{}, OrderedServiceIDs: []string{}}, nil
	}
	ordered := append([]DerivedService(nil), services...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Strategy.ServiceID < ordered[j].Strategy.ServiceID })
	weightTotal := int64(0)
	for _, service := range ordered {
		if service.Strategy.WeightPermille <= 0 {
			return WeightedIndex{}, apperr.WithDetails(apperr.SpecInvalid, "weight must be positive for weighted index", map[string]any{"service_id": service.Strategy.ServiceID})
		}
		if weightTotal > 1_000_000_000_000-int64(service.Strategy.WeightPermille) {
			return WeightedIndex{}, apperr.New(apperr.SpecInvalid, "weight total overflow")
		}
		weightTotal += int64(service.Strategy.WeightPermille)
	}
	if weightTotal <= 0 {
		return WeightedIndex{}, apperr.New(apperr.SpecInvalid, "weight total must be positive")
	}
	den := big.NewInt(weightTotal)
	scale := big.NewInt(IndexScale)
	totalExact := big.NewInt(0)
	steps := make([]IndexStep, 0, len(ordered))
	orderedIDs := make([]string, 0, len(ordered))
	sumRounded := int64(0)
	for _, service := range ordered {
		orderedIDs = append(orderedIDs, service.Strategy.ServiceID)
		exact := big.NewInt(int64(service.DesiredReplicas))
		exact.Mul(exact, big.NewInt(int64(service.Strategy.WeightPermille)))
		exact.Mul(exact, scale)
		totalExact.Add(totalExact, exact)

		q, r := new(big.Int), new(big.Int)
		q.QuoRem(exact, den, r)
		rounded := new(big.Int).Set(q)
		rounding := "exact"
		if r.Sign() != 0 {
			rounding = "floor"
			twiceR := new(big.Int).Lsh(r, 1)
			if twiceR.Cmp(den) >= 0 {
				rounded.Add(rounded, big.NewInt(1))
				rounding = "half_up"
			}
		}
		roundedInt := rounded.Int64()
		if sumRounded > 1<<62-roundedInt {
			return WeightedIndex{}, apperr.New(apperr.SpecInvalid, "rounded index sum overflow")
		}
		sumRounded += roundedInt
		errorNumerator := new(big.Int).Mul(rounded, den)
		errorNumerator.Sub(errorNumerator, exact)
		steps = append(steps, IndexStep{
			ServiceID: service.Strategy.ServiceID, DesiredReplicas: service.DesiredReplicas,
			WeightPermille: service.Strategy.WeightPermille,
			ExactNumerator: exact.String(), ExactDenominator: den.String(),
			RoundedMicros: roundedInt, Rounding: rounding,
			RoundingErrorNumerator: errorNumerator.String(),
		})
	}
	finalQ, finalR := new(big.Int), new(big.Int)
	finalQ.QuoRem(totalExact, den, finalR)
	twiceFinalR := new(big.Int).Lsh(finalR, 1)
	if twiceFinalR.Cmp(den) >= 0 {
		finalQ.Add(finalQ, big.NewInt(1))
	}
	final := finalQ.Int64()
	cumulativeError := new(big.Int).Mul(big.NewInt(sumRounded), den)
	cumulativeError.Sub(cumulativeError, totalExact)
	return WeightedIndex{
		Scale: IndexScale, WeightTotal: weightTotal, IndexMicros: final,
		IndexDecimal: FormatMicros(final), SumRoundedMicros: sumRounded,
		CompensationMethod:                 "exact_integer_residual",
		CompensatedAdjustmentMicros:        final - sumRounded,
		CumulativeRoundingErrorNumerator:   cumulativeError.String(),
		CumulativeRoundingErrorDenominator: den.String(),
		Steps:                              steps, OrderedServiceIDs: orderedIDs,
	}, nil
}

func CompareThreshold(index WeightedIndex, thresholdMicros int64, tolerancePermille int) ThresholdDecision {
	if thresholdMicros < 0 {
		thresholdMicros = 0
	}
	diff := index.IndexMicros - thresholdMicros
	tolerance := ceilDiv(absInt64(thresholdMicros)*int64(tolerancePermille), 1000)
	exceeded := diff > tolerance
	within := absInt64(diff) <= tolerance
	decision := "exceeded"
	if !exceeded {
		if within && diff != 0 {
			decision = "within_tolerance"
		} else if diff <= 0 {
			decision = "not_exceeded"
		}
	}
	return ThresholdDecision{
		ThresholdMicros: thresholdMicros, RelativeTolerancePermille: tolerancePermille,
		ToleranceMicros: tolerance, DifferenceMicros: diff, Exceeded: exceeded,
		WithinTolerance: within, Decision: decision,
		TieBreakServices: append([]string(nil), index.OrderedServiceIDs...),
	}
}

func absInt64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
