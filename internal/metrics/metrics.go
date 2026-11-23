package metrics

import (
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"

	"blocksnap/internal/fixed"
)

var ErrCorrupt = errors.New("snapshot chain is internally inconsistent")

type Snapshot struct {
	ID             string
	Kind           string
	ParentID       string
	ConsistencySeq int64
	Weight         int64
	Ordinal        int64
	CreatedAt      string
}

type BlockRef struct {
	BlockNo int64
	Version int64
	Digest  string
}

type VersionKey struct {
	BlockNo int64
	Version int64
}

type State struct {
	VolumeID      string
	BlockCount    int64
	BlockSize     int64
	OverheadBytes int64
	LastSeq       int64
	Live          map[int64]BlockRef
	Snapshots     map[string]Snapshot
	Refs          map[string]map[int64]int64
	Versions      map[VersionKey]string
}

type Number struct {
	Units      string `json:"units"`
	Scale      int    `json:"scale"`
	Decimal    string `json:"decimal"`
	Rounding   string `json:"rounding"`
	Remainder  string `json:"remainder"`
	ErrorUpper string `json:"error_upper_units"`
}

type CapacityUsage struct {
	AllocatedBlocks int64  `json:"allocated_blocks"`
	UsedBytes       string `json:"used_bytes"`
	CapacityBytes   string `json:"capacity_bytes"`
	Ratio           Number `json:"ratio"`
}

type RollbackMargin struct {
	ChainStorageBytes     string `json:"chain_storage_bytes"`
	ChainDistinctVersions int64  `json:"chain_distinct_versions"`
	WritebackBlocks       int64  `json:"writeback_blocks"`
	WritebackBytes        string `json:"writeback_bytes"`
	ReserveBytes          string `json:"reserve_bytes"`
	Ratio                 Number `json:"ratio"`
}

type ConsistencyTerm struct {
	SnapshotID     string `json:"snapshot_id"`
	Weight         string `json:"weight"`
	MatchingBlocks int64  `json:"matching_blocks"`
	BlockCount     int64  `json:"block_count"`
	Ratio          Number `json:"ratio"`
}

type WeightedConsistency struct {
	TotalWeight           string            `json:"total_weight"`
	ExactNumerator        string            `json:"exact_numerator"`
	Ratio                 Number            `json:"ratio"`
	NaiveUnits            string            `json:"naive_units"`
	NaiveErrorNumerator   string            `json:"naive_error_numerator"`
	NaiveErrorUpperUnits  string            `json:"naive_error_upper_units"`
	CompensatedErrorUnits string            `json:"compensated_error_upper_units"`
	Terms                 []ConsistencyTerm `json:"terms"`
}

type RollbackCost struct {
	WritebackBlocks       int64  `json:"writeback_blocks"`
	WritebackBytes        string `json:"writeback_bytes"`
	OverheadPerBlockBytes string `json:"overhead_per_block_bytes"`
	CostBytes             string `json:"cost_bytes"`
}

type RoundingStep struct {
	Metric           string `json:"metric"`
	ExactNumerator   string `json:"exact_numerator"`
	ExactDenominator string `json:"exact_denominator"`
	Units            string `json:"units"`
	Decimal          string `json:"decimal"`
	Remainder        string `json:"remainder"`
	Rounding         string `json:"rounding"`
	ErrorUpperUnits  string `json:"error_upper_units"`
}

type RoundingReport struct {
	Scale                     int            `json:"scale"`
	Mode                      string         `json:"mode"`
	Steps                     []RoundingStep `json:"steps"`
	CumulativeErrorUpperUnits string         `json:"cumulative_error_upper_units"`
	Note                      string         `json:"note"`
}

type AnalysisMetrics struct {
	CapacityUsage       CapacityUsage       `json:"capacity_usage"`
	RollbackMargin      RollbackMargin      `json:"rollback_margin"`
	SnapshotConsistency Number              `json:"snapshot_consistency"`
	WeightedConsistency WeightedConsistency `json:"weighted_consistency"`
	RollbackCost        RollbackCost        `json:"rollback_cost"`
	Rounding            RoundingReport      `json:"rounding"`
}

type SnapshotSummary struct {
	ID             string `json:"id"`
	Kind           string `json:"kind"`
	ParentID       string `json:"parent_id,omitempty"`
	ConsistencySeq int64  `json:"consistency_seq"`
	Weight         int64  `json:"weight"`
	Ordinal        int64  `json:"ordinal"`
	CreatedAt      string `json:"created_at"`
}

type Analysis struct {
	VolumeID string          `json:"volume_id"`
	Snapshot SnapshotSummary `json:"snapshot"`
	Chain    []string        `json:"chain"`
	Metrics  AnalysisMetrics `json:"metrics"`
}

type DecisionRequest struct {
	MarginFloorUnits  *big.Int
	CostBudgetBytes   *big.Int
	RelativeTolerance *big.Int
}

type Decision struct {
	Accepted                  bool   `json:"accepted"`
	Reason                    string `json:"reason"`
	MarginFloor               string `json:"margin_floor"`
	MarginFloorEffectiveLower string `json:"margin_floor_effective_lower"`
	MarginBelowFloor          bool   `json:"margin_below_floor"`
	CostBudgetBytes           string `json:"cost_budget_bytes"`
	CostBudgetEffectiveUpper  string `json:"cost_budget_effective_upper"`
	CostAboveBudget           bool   `json:"cost_above_budget"`
	RelativeTolerance         string `json:"relative_tolerance"`
	ToleranceComparison       string `json:"tolerance_comparison"`
	SelectedSnapshotID        string `json:"selected_snapshot_id,omitempty"`
}

type Evaluation struct {
	SnapshotID string   `json:"snapshot_id"`
	Analysis   Analysis `json:"analysis"`
	Decision   Decision `json:"decision"`
}

// Analyze computes all derived metrics for the chain ending at snapshotID. All
// iteration over blocks and snapshots is order-stable; counts are exact and
// ratios pass through the fixed-point layer exactly once.
func Analyze(state *State, snapshotID string) (Analysis, error) {
	snap, ok := state.Snapshots[snapshotID]
	if !ok {
		return Analysis{}, fmt.Errorf("%w: snapshot %s missing", ErrCorrupt, snapshotID)
	}
	chain, err := chainFor(state, snapshotID)
	if err != nil {
		return Analysis{}, err
	}
	effective := make(map[string]map[int64]int64, len(chain))
	for _, id := range chain {
		cur := state.Snapshots[id]
		var base map[int64]int64
		if cur.ParentID != "" {
			parent, exists := effective[cur.ParentID]
			if !exists {
				return Analysis{}, fmt.Errorf("%w: parent %s of %s not resolved", ErrCorrupt, cur.ParentID, id)
			}
			base = parent
		}
		merged := make(map[int64]int64, len(base)+len(state.Refs[id]))
		for blockNo, version := range base {
			merged[blockNo] = version
		}
		for blockNo, version := range state.Refs[id] {
			merged[blockNo] = version
		}
		if int64(len(merged)) != state.BlockCount {
			return Analysis{}, fmt.Errorf("%w: snapshot %s covers %d of %d blocks", ErrCorrupt, id, len(merged), state.BlockCount)
		}
		effective[id] = merged
	}
	target := effective[snapshotID]

	blockNoSet := make(map[int64]struct{}, len(target))
	for no := range target {
		blockNoSet[no] = struct{}{}
	}
	blockNos := sortedKeys(blockNoSet)

	// Validate references and precompute per-snapshot matching blocks.
	terms := make([]ConsistencyTerm, 0, len(chain))
	weightValues := make([]*big.Int, 0, len(chain))
	weightValuesI := make([]*big.Int, 0, len(chain))
	steps := make([]RoundingStep, 0, len(chain)+3)
	for _, id := range chain {
		refs := effective[id]
		matching := int64(0)
		for _, no := range blockNos {
			ver, ok := refs[no]
			if !ok {
				return Analysis{}, fmt.Errorf("%w: snapshot %s missing block %d", ErrCorrupt, id, no)
			}
			digest, ok := state.Versions[VersionKey{BlockNo: no, Version: ver}]
			if !ok || digest == "" {
				return Analysis{}, fmt.Errorf("%w: block %d version %d content missing", ErrCorrupt, no, ver)
			}
			live, ok := state.Live[no]
			if !ok {
				return Analysis{}, fmt.Errorf("%w: live block %d missing", ErrCorrupt, no)
			}
			if live.Digest == digest {
				matching++
			}
		}
		units, rem, err := fixed.Ratio(big.NewInt(matching), big.NewInt(state.BlockCount))
		if err != nil {
			return Analysis{}, err
		}
		snapMeta := state.Snapshots[id]
		terms = append(terms, ConsistencyTerm{
			SnapshotID: id, Weight: strconv.FormatInt(snapMeta.Weight, 10),
			MatchingBlocks: matching, BlockCount: state.BlockCount,
			Ratio: makeNumber(units, rem),
		})
		steps = append(steps, roundingStep("snapshot_consistency:"+id, big.NewInt(matching), big.NewInt(state.BlockCount), units, rem))
		weightValues = append(weightValues, big.NewInt(snapMeta.Weight))
		weightValuesI = append(weightValuesI, units)
	}

	// Chain storage is the number of distinct (block,version) extents. It is
	// the "used capacity" denominator for the rollback reserve ratio.
	distinct := make(map[VersionKey]struct{})
	for _, id := range chain {
		for no, ver := range effective[id] {
			distinct[VersionKey{BlockNo: no, Version: ver}] = struct{}{}
		}
	}
	chainStorageSum := fixed.NewCompensatedSum()
	for range sortedVersionKeys(distinct) {
		chainStorageSum.Add(big.NewInt(state.BlockSize))
	}
	chainStorage := chainStorageSum.Value()

	writebackBlocks := int64(0)
	for _, no := range blockNos {
		targetVer := target[no]
		targetDigest, ok := state.Versions[VersionKey{BlockNo: no, Version: targetVer}]
		if !ok {
			return Analysis{}, fmt.Errorf("%w: target digest for block %d version %d missing", ErrCorrupt, no, targetVer)
		}
		liveDigest := state.Live[no].Digest
		if targetDigest != liveDigest {
			writebackBlocks++
		}
	}
	writebackSum := fixed.NewCompensatedSum()
	for i := int64(0); i < writebackBlocks; i++ {
		writebackSum.Add(big.NewInt(state.BlockSize))
	}
	writebackBytes := writebackSum.Value()
	reserveBytes := new(big.Int).Sub(new(big.Int).Set(chainStorage), writebackBytes)
	if reserveBytes.Sign() < 0 {
		reserveBytes.SetInt64(0)
	}
	marginUnits, marginRem, err := fixed.Ratio(reserveBytes, chainStorage)
	if err != nil {
		return Analysis{}, err
	}
	steps = append(steps, roundingStep("rollback_margin", reserveBytes, chainStorage, marginUnits, marginRem))

	allocatedBlocks := int64(0)
	usedSum := fixed.NewCompensatedSum()
	for _, no := range sortedLiveKeys(state.Live) {
		if state.Live[no].Version > 0 {
			allocatedBlocks++
			usedSum.Add(big.NewInt(state.BlockSize))
		}
	}
	usedBytes := usedSum.Value()
	capacityBytes := new(big.Int).Mul(big.NewInt(state.BlockCount), big.NewInt(state.BlockSize))
	capacityUnits, capacityRem, err := fixed.Ratio(usedBytes, capacityBytes)
	if err != nil {
		return Analysis{}, err
	}
	steps = append(steps, roundingStep("used_capacity_ratio", usedBytes, capacityBytes, capacityUnits, capacityRem))

	weighted, err := fixed.WeightedAverage(weightValues, weightValuesI)
	if err != nil {
		return Analysis{}, err
	}
	weightedRemainder := new(big.Int)
	if weighted.TotalWeight.Sign() > 0 {
		_, weightedRemainder = new(big.Int).QuoRem(new(big.Int).Set(weighted.ExactWeightedNumerator), weighted.TotalWeight, new(big.Int))
		steps = append(steps, roundingStep("weighted_consistency", weighted.ExactWeightedNumerator, weighted.TotalWeight, weighted.Units, weightedRemainder))
	}

	snapshotConsistency := terms[len(terms)-1].Ratio
	cumulativeBound := new(big.Rat).SetFrac(big.NewInt(int64(len(steps))), big.NewInt(2)).FloatString(6)
	compensatedBound := "0.500000"
	if weighted.TotalWeight.Sign() == 0 {
		compensatedBound = "0.000000"
	}

	analysis := Analysis{
		VolumeID: state.VolumeID,
		Snapshot: SnapshotSummary{
			ID: snap.ID, Kind: snap.Kind, ParentID: snap.ParentID,
			ConsistencySeq: snap.ConsistencySeq, Weight: snap.Weight,
			Ordinal: snap.Ordinal, CreatedAt: snap.CreatedAt,
		},
		Chain: chain,
		Metrics: AnalysisMetrics{
			CapacityUsage: CapacityUsage{
				AllocatedBlocks: allocatedBlocks,
				UsedBytes:       usedBytes.String(), CapacityBytes: capacityBytes.String(),
				Ratio: makeNumber(capacityUnits, capacityRem),
			},
			RollbackMargin: RollbackMargin{
				ChainStorageBytes: chainStorage.String(), ChainDistinctVersions: int64(len(distinct)),
				WritebackBlocks: writebackBlocks, WritebackBytes: writebackBytes.String(),
				ReserveBytes: reserveBytes.String(),
				Ratio:        makeNumber(marginUnits, marginRem),
			},
			SnapshotConsistency: snapshotConsistency,
			WeightedConsistency: WeightedConsistency{
				TotalWeight:           weighted.TotalWeight.String(),
				ExactNumerator:        weighted.ExactWeightedNumerator.String(),
				Ratio:                 makeNumber(weighted.Units, weightedRemainder),
				NaiveUnits:            weighted.NaiveUnits.String(),
				NaiveErrorNumerator:   weighted.NaiveErrorNumerator.String(),
				NaiveErrorUpperUnits:  formatHalfBound(int64(len(chain))),
				CompensatedErrorUnits: compensatedBound,
				Terms:                 terms,
			},
			RollbackCost: RollbackCost{
				WritebackBlocks: writebackBlocks, WritebackBytes: writebackBytes.String(),
				OverheadPerBlockBytes: strconv.FormatInt(state.OverheadBytes, 10),
				CostBytes:             new(big.Int).Add(new(big.Int).Set(writebackBytes), new(big.Int).Mul(big.NewInt(writebackBlocks), big.NewInt(state.OverheadBytes))).String(),
			},
			Rounding: RoundingReport{
				Scale: fixed.Scale, Mode: "half-away-from-zero",
				Steps:                     steps,
				CumulativeErrorUpperUnits: cumulativeBound,
				Note:                      "All ratios use 10^-6 fixed-point integers. Cumulative bound is the sum of one half-unit per division; weighted consistency additionally reports the deliberately naive per-term path for diagnosis only.",
			},
		},
	}
	return analysis, nil
}

func chainFor(state *State, snapshotID string) ([]string, error) {
	var reverse []string
	seen := make(map[string]struct{})
	cur := snapshotID
	for cur != "" {
		if _, ok := seen[cur]; ok {
			return nil, fmt.Errorf("%w: snapshot cycle at %s", ErrCorrupt, cur)
		}
		snap, ok := state.Snapshots[cur]
		if !ok {
			return nil, fmt.Errorf("%w: snapshot %s missing", ErrCorrupt, cur)
		}
		seen[cur] = struct{}{}
		reverse = append(reverse, cur)
		cur = snap.ParentID
	}
	chain := make([]string, len(reverse))
	for i := range reverse {
		chain[len(reverse)-1-i] = reverse[i]
	}
	return chain, nil
}

func sortedKeys(m map[int64]struct{}) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sortedLiveKeys(m map[int64]BlockRef) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sortedVersionKeys(m map[VersionKey]struct{}) []VersionKey {
	out := make([]VersionKey, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].BlockNo != out[j].BlockNo {
			return out[i].BlockNo < out[j].BlockNo
		}
		return out[i].Version < out[j].Version
	})
	return out
}

func makeNumber(units, remainder *big.Int) Number {
	return Number{
		Units: units.String(), Scale: fixed.Scale, Decimal: fixed.Format(units),
		Rounding: "half-away-from-zero", Remainder: remainder.String(),
		ErrorUpper: "0.500000",
	}
}

func roundingStep(metric string, numerator, denominator, units, remainder *big.Int) RoundingStep {
	return RoundingStep{
		Metric: metric, ExactNumerator: numerator.String(), ExactDenominator: denominator.String(),
		Units: units.String(), Decimal: fixed.Format(units), Remainder: remainder.String(),
		Rounding: "half-away-from-zero", ErrorUpperUnits: "0.500000",
	}
}

func formatHalfBound(n int64) string {
	r := new(big.Rat).SetFrac(big.NewInt(n), big.NewInt(2))
	return r.FloatString(6)
}

// Decide applies the inclusive relative-tolerance contract. Values inside the
// widened interval satisfy the predicate; only a strict excursion rejects.
func Decide(analysis Analysis, req DecisionRequest) (Decision, error) {
	if req.MarginFloorUnits == nil || req.CostBudgetBytes == nil || req.RelativeTolerance == nil {
		return Decision{}, fmt.Errorf("decision request is incomplete")
	}
	lower, err := fixed.RelativeToleranceLower(req.MarginFloorUnits, req.RelativeTolerance)
	if err != nil {
		return Decision{}, err
	}
	upper, err := fixed.RelativeToleranceUpper(req.CostBudgetBytes, req.RelativeTolerance)
	if err != nil {
		return Decision{}, err
	}
	marginUnits, ok := new(big.Int).SetString(analysis.Metrics.RollbackMargin.Ratio.Units, 10)
	if !ok {
		return Decision{}, fmt.Errorf("invalid stored margin units")
	}
	cost, ok := new(big.Int).SetString(analysis.Metrics.RollbackCost.CostBytes, 10)
	if !ok {
		return Decision{}, fmt.Errorf("invalid stored rollback cost")
	}
	marginBelow := marginUnits.Cmp(lower) < 0
	costAbove := cost.Cmp(upper) > 0
	reason := "accepted"
	if marginBelow && costAbove {
		reason = "margin_below_floor_and_cost_above_budget"
	} else if marginBelow {
		reason = "margin_below_floor"
	} else if costAbove {
		reason = "cost_above_budget"
	}
	return Decision{
		Accepted:                  !marginBelow && !costAbove,
		Reason:                    reason,
		MarginFloor:               fixed.Format(req.MarginFloorUnits),
		MarginFloorEffectiveLower: fixed.Format(lower),
		MarginBelowFloor:          marginBelow,
		CostBudgetBytes:           req.CostBudgetBytes.String(),
		CostBudgetEffectiveUpper:  upper.String(),
		CostAboveBudget:           costAbove,
		RelativeTolerance:         fixed.Format(req.RelativeTolerance),
		ToleranceComparison:       "inclusive: reject only when margin < floor - ceil(floor*relative_tolerance) or cost > budget + ceil(budget*relative_tolerance)",
	}, nil
}

// EvaluateCandidates sorts by snapshot ID before deciding. When multiple
// candidates satisfy the contract, the lexicographically smallest ID wins,
// independent of request order.
func EvaluateCandidates(candidates []Analysis, req DecisionRequest) ([]Evaluation, *Evaluation, error) {
	sorted := append([]Analysis(nil), candidates...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Snapshot.ID < sorted[j].Snapshot.ID })
	evaluations := make([]Evaluation, 0, len(sorted))
	var selected *Evaluation
	for _, analysis := range sorted {
		decision, err := Decide(analysis, req)
		if err != nil {
			return nil, nil, err
		}
		eval := Evaluation{SnapshotID: analysis.Snapshot.ID, Analysis: analysis, Decision: decision}
		if selected == nil && decision.Accepted {
			copyEval := eval
			selected = &copyEval
		}
		evaluations = append(evaluations, eval)
	}
	if selected != nil {
		for i := range evaluations {
			if evaluations[i].SnapshotID == selected.SnapshotID {
				evaluations[i].Decision.SelectedSnapshotID = selected.SnapshotID
			}
		}
	}
	return evaluations, selected, nil
}

// Chain returns the root-to-target snapshot IDs.
func Chain(state *State, snapshotID string) ([]string, error) {
	return chainFor(state, snapshotID)
}

// EffectiveRefs resolves a snapshot's write-on-copy chain into a complete
// block->version map. It is used by mutation paths that need the same view as
// the metric engine without duplicating chain semantics.
func EffectiveRefs(state *State, snapshotID string) (map[int64]int64, error) {
	chain, err := chainFor(state, snapshotID)
	if err != nil {
		return nil, err
	}
	effective := make(map[string]map[int64]int64, len(chain))
	for _, id := range chain {
		cur := state.Snapshots[id]
		if cur.Kind == "full" && cur.ParentID != "" {
			return nil, fmt.Errorf("%w: full snapshot %s has a parent", ErrCorrupt, id)
		}
		merged := make(map[int64]int64)
		if cur.ParentID != "" {
			parent, ok := effective[cur.ParentID]
			if !ok {
				return nil, fmt.Errorf("%w: parent %s unresolved", ErrCorrupt, cur.ParentID)
			}
			for no, ver := range parent {
				merged[no] = ver
			}
		}
		for no, ver := range state.Refs[id] {
			merged[no] = ver
		}
		if int64(len(merged)) != state.BlockCount {
			return nil, fmt.Errorf("%w: snapshot %s covers %d of %d blocks", ErrCorrupt, id, len(merged), state.BlockCount)
		}
		effective[id] = merged
	}
	return effective[snapshotID], nil
}
