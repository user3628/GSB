package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"sort"

	"example.com/joinshuffle/internal/model"
	"example.com/joinshuffle/internal/snapshot"
	"example.com/joinshuffle/internal/sum"
)

const (
	MergePartitionAscending = "partition_ascending"
	MergeMagnitudeBucket    = "magnitude_bucket_partition_asc"
)

type Options struct {
	JoinType       model.JoinType
	PartitionCount int
	SkewThreshold  float64
	Algorithm      model.SumAlgorithm
	FixedScaleBits int
	MergeRule      string
}

type Distribution struct {
	TotalRecords      int64
	KeyCount          int
	MaxRecordShare    float64
	SkewedPartitions  []int
	MaxAbsMeasure     float64
	SumAbsMeasure     float64
	MinPositiveAbs    float64
	MagnitudeRatio    float64
	EstimatedJoinRows int64
}

type Result struct {
	Partitions         []model.PartitionStat
	Groups             []model.GroupResult
	Partials           []sum.Partial
	Skew               model.SkewReport
	MergeOrderBasis    string
	CompensationDigest string
}

type recordRef struct {
	index     int
	key       int64
	measure   float64
	null      bool
	sideOrder int
}

type partitionWork struct {
	left  []recordRef
	right []recordRef
}

type groupWork struct {
	count int64
	acc   sum.Accumulator
}

func PartitionForKey(key int64, partitions int) int {
	if partitions <= 1 {
		return 0
	}
	x := uint64(key) + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x = x ^ (x >> 31)
	return int(x % uint64(partitions))
}

func Analyze(left, right *snapshot.Snapshot, partitionCount int, skewThreshold float64, joinType model.JoinType) Distribution {
	if partitionCount < 1 {
		partitionCount = 1
	}
	counts := make([]int64, partitionCount)
	leftCounts := make(map[int64]int64)
	rightCounts := make(map[int64]int64)
	dist := Distribution{MinPositiveAbs: math.Inf(1)}
	for i := 0; i < left.Meta.Rows; i++ {
		p := PartitionForKey(left.Keys[i], partitionCount)
		counts[p]++
		leftCounts[left.Keys[i]]++
		dist.TotalRecords++
		if !left.Nulls[i] {
			abs := math.Abs(left.Measures[i])
			dist.MaxAbsMeasure = math.Max(dist.MaxAbsMeasure, abs)
			dist.SumAbsMeasure += abs
			if abs > 0 {
				dist.MinPositiveAbs = math.Min(dist.MinPositiveAbs, abs)
			}
		}
	}
	for i := 0; i < right.Meta.Rows; i++ {
		p := PartitionForKey(right.Keys[i], partitionCount)
		counts[p]++
		rightCounts[right.Keys[i]]++
		dist.TotalRecords++
		if !right.Nulls[i] {
			abs := math.Abs(right.Measures[i])
			dist.MaxAbsMeasure = math.Max(dist.MaxAbsMeasure, abs)
			dist.SumAbsMeasure += abs
			if abs > 0 {
				dist.MinPositiveAbs = math.Min(dist.MinPositiveAbs, abs)
			}
		}
	}
	dist.KeyCount = len(leftCounts)
	for key := range rightCounts {
		if _, ok := leftCounts[key]; !ok {
			dist.KeyCount++
		}
	}
	if dist.TotalRecords > 0 {
		for p, count := range counts {
			share := float64(count) / float64(dist.TotalRecords)
			dist.MaxRecordShare = math.Max(dist.MaxRecordShare, share)
			if share > skewThreshold {
				dist.SkewedPartitions = append(dist.SkewedPartitions, p)
			}
		}
	}
	switch joinType {
	case model.JoinInner:
		for key, lc := range leftCounts {
			dist.EstimatedJoinRows += lc * rightCounts[key]
		}
	case model.JoinLeft:
		for key, lc := range leftCounts {
			matches := rightCounts[key]
			if matches == 0 {
				dist.EstimatedJoinRows += lc
			} else {
				dist.EstimatedJoinRows += lc * matches
			}
		}
	}
	if math.IsInf(dist.MinPositiveAbs, 1) {
		dist.MinPositiveAbs = 0
	}
	if dist.MinPositiveAbs > 0 {
		dist.MagnitudeRatio = dist.MaxAbsMeasure / dist.MinPositiveAbs
	}
	return dist
}

func Run(left, right *snapshot.Snapshot, opts Options) (*Result, error) {
	if opts.PartitionCount < 1 || opts.PartitionCount > 256 {
		return nil, model.NewError(model.CodeInvalidRequest, "partition_count must be between 1 and 256", "partition_count", opts.PartitionCount)
	}
	if opts.JoinType != model.JoinInner && opts.JoinType != model.JoinLeft {
		return nil, model.NewError(model.CodeInvalidRequest, "join_type must be inner or left", "join_type", opts.JoinType)
	}
	if opts.MergeRule == "" {
		opts.MergeRule = MergePartitionAscending
	}
	parts := make([]partitionWork, opts.PartitionCount)
	for i := 0; i < left.Meta.Rows; i++ {
		p := PartitionForKey(left.Keys[i], opts.PartitionCount)
		parts[p].left = append(parts[p].left, recordRef{
			index: i, key: left.Keys[i], measure: left.Measures[i], null: left.Nulls[i], sideOrder: 0,
		})
	}
	for i := 0; i < right.Meta.Rows; i++ {
		p := PartitionForKey(right.Keys[i], opts.PartitionCount)
		parts[p].right = append(parts[p].right, recordRef{
			index: i, key: right.Keys[i], measure: right.Measures[i], null: right.Nulls[i], sideOrder: 1,
		})
	}

	stats := make([]model.PartitionStat, opts.PartitionCount)
	partials := make([]sum.Partial, 0)
	totalRecords := int64(left.Meta.Rows + right.Meta.Rows)
	skew := model.SkewReport{Threshold: opts.SkewThreshold}
	for pi := range parts {
		part := &parts[pi]
		sortRecordRefs(part.left)
		sortRecordRefs(part.right)
		rightIndex := make(map[int64][]recordRef)
		for _, ref := range part.right {
			rightIndex[ref.key] = append(rightIndex[ref.key], ref)
		}
		groups := make(map[int64]*groupWork)
		var joinedRecords int64
		for _, lref := range part.left {
			matches := rightIndex[lref.key]
			if len(matches) == 0 {
				if opts.JoinType == model.JoinLeft {
					g := groups[lref.key]
					if g == nil {
						acc, err := sum.NewAccumulator(opts.Algorithm, opts.FixedScaleBits)
						if err != nil {
							return nil, err
						}
						g = &groupWork{acc: acc}
						groups[lref.key] = g
					}
					g.count++
					joinedRecords++
					if !lref.null {
						g.acc.Add(lref.measure)
					}
				}
				continue
			}
			for _, rref := range matches {
				g := groups[lref.key]
				if g == nil {
					acc, err := sum.NewAccumulator(opts.Algorithm, opts.FixedScaleBits)
					if err != nil {
						return nil, err
					}
					g = &groupWork{acc: acc}
					groups[lref.key] = g
				}
				g.count++
				joinedRecords++
				value, valid := sum.JoinMeasure(lref.measure, !lref.null, rref.measure, !rref.null)
				if valid {
					g.acc.Add(value)
				}
			}
		}
		keys := sortedGroupKeys(groups)
		for _, key := range keys {
			g := groups[key]
			p := g.acc.Partial(key, pi)
			p.Count = g.count
			partials = append(partials, p)
		}
		recordCount := len(part.left) + len(part.right)
		share := 0.0
		if totalRecords > 0 {
			share = float64(recordCount) / float64(totalRecords)
		}
		affectedSet := make(map[int64]struct{}, len(part.left)+len(part.right))
		for _, ref := range part.left {
			affectedSet[ref.key] = struct{}{}
		}
		for _, ref := range part.right {
			affectedSet[ref.key] = struct{}{}
		}
		affected := sortedInt64Keys(affectedSet)
		skewed := share > opts.SkewThreshold
		if skewed {
			skew.SkewedPartitions = append(skew.SkewedPartitions, pi)
		}
		skew.MaxRecordShare = math.Max(skew.MaxRecordShare, share)
		stats[pi] = model.PartitionStat{
			Partition:     pi,
			LeftRecords:   len(part.left),
			RightRecords:  len(part.right),
			RecordCount:   recordCount,
			JoinedRecords: int(joinedRecords),
			BytesEstimate: int64(recordCount)*16 + joinedRecords*8 + int64(len(keys))*8,
			RecordShare:   share,
			Skewed:        skewed,
			AffectedKeys:  affected,
		}
	}
	skew.Detected = len(skew.SkewedPartitions) > 0

	byKey := make(map[int64][]sum.Partial)
	countsByKey := make(map[int64]int64)
	for _, p := range partials {
		byKey[p.Key] = append(byKey[p.Key], p)
		countsByKey[p.Key] += p.Count
	}
	keys := sortedPartialKeys(byKey)
	groups := make([]model.GroupResult, 0, len(keys))
	groupDigests := make([]string, 0, len(keys))
	for _, key := range keys {
		merged, err := sum.Merge(byKey[key], opts.Algorithm, opts.FixedScaleBits, opts.MergeRule)
		if err != nil {
			return nil, err
		}
		var sumPtr, avgPtr *float64
		if merged.ValueCount > 0 {
			s := merged.Sum
			a := merged.Sum / float64(merged.ValueCount)
			sumPtr = &s
			avgPtr = &a
		}
		gr := model.GroupResult{
			Key:                  key,
			Sum:                  sumPtr,
			Average:              avgPtr,
			GroupCount:           countsByKey[key],
			ValueCount:           merged.ValueCount,
			AbsSum:               merged.AbsSum,
			AbsoluteErrorBound:   merged.AbsErrorBound,
			RelativeErrorBound:   merged.RelErrorBound,
			AverageAbsErrorBound: merged.AverageAbsErrorBound,
			AverageRelErrorBound: merged.AverageRelErrorBound,
			CompensationDigest:   merged.CompensationHash,
		}
		groups = append(groups, gr)
		groupDigests = append(groupDigests, gr.CompensationDigest)
	}
	return &Result{
		Partitions:         stats,
		Groups:             groups,
		Partials:           partials,
		Skew:               skew,
		MergeOrderBasis:    mergeOrderBasis(opts.MergeRule),
		CompensationDigest: digestStrings(groupDigests),
	}, nil
}

func GroupMap(result *Result) map[int64]*model.GroupResult {
	out := make(map[int64]*model.GroupResult, len(result.Groups))
	for i := range result.Groups {
		out[result.Groups[i].Key] = &result.Groups[i]
	}
	return out
}

func sortRecordRefs(refs []recordRef) {
	sort.SliceStable(refs, func(i, j int) bool {
		a, b := refs[i], refs[j]
		if a.null != b.null {
			return !a.null
		}
		if !a.null {
			aa, ab := math.Abs(a.measure), math.Abs(b.measure)
			if aa != ab {
				return aa < ab
			}
		}
		if a.key != b.key {
			return a.key < b.key
		}
		if a.sideOrder != b.sideOrder {
			return a.sideOrder < b.sideOrder
		}
		return a.index < b.index
	})
}

func mergeOrderBasis(rule string) string {
	switch rule {
	case MergeMagnitudeBucket:
		return "Sort partials by base-2 magnitude bucket ascending, then absolute partial value ascending, then partition id; stable for a fixed committed snapshot."
	case MergePartitionAscending:
		return "Sort partials by partition id ascending, then group key; independent of partition completion order."
	default:
		return "Unknown merge order: " + rule
	}
}

func sortedGroupKeys(groups map[int64]*groupWork) []int64 {
	keys := make([]int64, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func sortedPartialKeys(groups map[int64][]sum.Partial) []int64 {
	keys := make([]int64, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func sortedInt64Keys(set map[int64]struct{}) []int64 {
	keys := make([]int64, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func digestStrings(values []string) string {
	h := sha256.New()
	for _, value := range values {
		h.Write([]byte(value))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func FormatRelativeDeviation(value, reference float64) float64 {
	denom := math.Abs(reference)
	if denom < 1e-300 {
		if value == reference {
			return 0
		}
		return math.Inf(1)
	}
	return math.Abs(value-reference) / denom
}

func ValidatePartitionInvariance(groupsByCount map[int]*Result, tolerance float64) (map[int64]float64, map[int64]*float64, *model.BusinessError) {
	if tolerance <= 0 {
		return nil, nil, model.NewError(model.CodeInvalidRequest, "tolerance must be positive")
	}
	ref, ok := groupsByCount[1]
	if !ok {
		return nil, nil, model.NewError(model.CodeInternal, "single-partition reference result is missing")
	}
	reference := GroupMap(ref)
	maxByKey := make(map[int64]float64, len(reference))
	refValues := make(map[int64]*float64, len(reference))
	refKeys := make([]int64, 0, len(reference))
	for key, group := range reference {
		refKeys = append(refKeys, key)
		if group.Sum != nil {
			copyValue := *group.Sum
			refValues[key] = &copyValue
		} else {
			refValues[key] = nil
		}
		maxByKey[key] = 0
	}
	sort.Slice(refKeys, func(i, j int) bool { return refKeys[i] < refKeys[j] })
	counts := make([]int, 0, len(groupsByCount))
	for count := range groupsByCount {
		counts = append(counts, count)
	}
	sort.Ints(counts)
	for _, count := range counts {
		actualGroups := GroupMap(groupsByCount[count])
		if len(actualGroups) != len(reference) {
			return nil, nil, model.NewError(model.CodePrecisionToleranceExceeded,
				"group cardinality differs across partition counts",
				"partition_count", count,
				"reference_group_count", len(reference),
				"actual_group_count", len(actualGroups),
				"tolerance", tolerance)
		}
		for _, key := range refKeys {
			refGroup := reference[key]
			actual := actualGroups[key]
			if actual == nil {
				return nil, nil, model.NewError(model.CodePrecisionToleranceExceeded,
					"group presence differs across partition counts",
					"key", key, "partition_count", count, "tolerance", tolerance)
			}
			if actual.GroupCount != refGroup.GroupCount {
				return nil, nil, model.NewError(model.CodePrecisionToleranceExceeded,
					"group count differs across partition counts",
					"key", key, "partition_count", count,
					"group_count", actual.GroupCount,
					"reference_group_count", refGroup.GroupCount,
					"tolerance", tolerance)
			}
			if actual.ValueCount != refGroup.ValueCount {
				return nil, nil, model.NewError(model.CodePrecisionToleranceExceeded,
					"value count differs across partition counts",
					"key", key, "partition_count", count,
					"value_count", actual.ValueCount,
					"reference_value_count", refGroup.ValueCount,
					"tolerance", tolerance)
			}
			if refGroup.Sum == nil || actual.Sum == nil {
				if (refGroup.Sum == nil) != (actual.Sum == nil) {
					return nil, nil, model.NewError(model.CodePrecisionToleranceExceeded,
						"group nullness differs across partition counts",
						"key", key, "partition_count", count, "tolerance", tolerance)
				}
				continue
			}
			dev := FormatRelativeDeviation(*actual.Sum, *refGroup.Sum)
			if math.IsInf(dev, 1) {
				return nil, nil, model.NewError(model.CodePrecisionToleranceExceeded,
					"non-finite relative deviation",
					"key", key,
					"partition_count", count,
					"value", *actual.Sum,
					"reference", *refGroup.Sum,
					"tolerance", tolerance)
			}
			if dev > maxByKey[key] {
				maxByKey[key] = dev
			}
			if dev > tolerance {
				return nil, nil, model.NewError(model.CodePrecisionToleranceExceeded,
					"aggregate result exceeds the declared partition-independent tolerance",
					"key", key,
					"partition_count", count,
					"value", *actual.Sum,
					"reference", *refGroup.Sum,
					"relative_deviation", dev,
					"tolerance", tolerance)
			}
		}
	}
	return maxByKey, refValues, nil
}
