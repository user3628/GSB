package edgeconfig

import (
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

func EvaluateConsistency(scope string, commitVersion int64, roundID string, nodes []Node, matched map[string]bool, policy NumericPolicy) Consistency {
	p := withDefaultPolicy(Policy{Numeric: policy}).Numeric
	ordered := make([]Node, len(nodes))
	copy(ordered, nodes)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Region != ordered[j].Region {
			return ordered[i].Region < ordered[j].Region
		}
		return ordered[i].ID < ordered[j].ID
	})

	result := Consistency{
		Scope:                scope,
		CommitVersion:        commitVersion,
		RoundID:              roundID,
		Scale:                p.Scale,
		ThresholdUnits:       p.ThresholdUnits,
		Threshold:            FormatFixed(p.ThresholdUnits, p.Scale),
		RelativeTolerancePPM: p.RelativeTolerancePPM,
		RoundingMode:         p.Rounding,
		NodeOrder:            make([]string, 0, len(ordered)),
		TieBreak: TieBreak{
			Policy:       p.TieBreak,
			OrderedNodes: make([]string, 0, len(ordered)),
		},
	}

	weights := make(map[string]int64, len(ordered))
	for _, node := range ordered {
		weight := p.DefaultWeight
		if declared, ok := p.NodeWeights[node.ID]; ok && declared > 0 {
			weight = declared
		}
		weights[node.ID] = weight
		result.TotalWeight += weight
		if matched[node.ID] {
			result.MatchedWeight += weight
		}
	}

	floorSum := int64(0)
	remainderCandidates := make([]int, 0, len(ordered))
	for _, node := range ordered {
		result.NodeOrder = append(result.NodeOrder, node.Region+"/"+node.ID)
		result.TieBreak.OrderedNodes = append(result.TieBreak.OrderedNodes, node.ID)
		weight := weights[node.ID]
		isMatched := matched[node.ID]
		step := RoundingStep{
			NodeID:     node.ID,
			Region:     node.Region,
			Weight:     weight,
			Matched:    isMatched,
			Rounding:   "floor_then_largest_remainder:" + p.Rounding,
			ErrorDenom: 0,
		}
		if isMatched && result.TotalWeight > 0 {
			numer := new(big.Int).Mul(big.NewInt(weight), big.NewInt(p.Scale))
			denom := big.NewInt(result.TotalWeight)
			q, r := new(big.Int).QuoRem(numer, denom, new(big.Int))
			step.ExactNumer = weight * p.Scale
			step.Quotient = q.Int64()
			step.Remainder = r.Int64()
			step.RoundedUnits = step.Quotient
			step.ErrorDenom = result.TotalWeight
			floorSum += step.Quotient
			remainderCandidates = append(remainderCandidates, len(result.Steps))
		}
		result.Steps = append(result.Steps, step)
	}

	if result.TotalWeight > 0 {
		denom := big.NewInt(result.TotalWeight)
		globalNumer := new(big.Int).Mul(big.NewInt(result.MatchedWeight), big.NewInt(p.Scale))
		globalQ, globalR := new(big.Int).QuoRem(globalNumer, denom, new(big.Int))
		globalRounded := globalQ.Int64()
		if twiceR := new(big.Int).Mul(globalR, big.NewInt(2)); twiceR.Cmp(denom) >= 0 {
			globalRounded++
		}
		extras := int(globalRounded - floorSum)
		sort.Slice(remainderCandidates, func(i, j int) bool {
			left := result.Steps[remainderCandidates[i]]
			right := result.Steps[remainderCandidates[j]]
			if left.Remainder != right.Remainder {
				return left.Remainder > right.Remainder
			}
			if left.Region != right.Region {
				return left.Region < right.Region
			}
			return left.NodeID < right.NodeID
		})
		for i := 0; i < extras && i < len(remainderCandidates); i++ {
			result.Steps[remainderCandidates[i]].RoundedUnits++
		}
		result.ScoreUnits = globalRounded
		for index := range result.Steps {
			step := &result.Steps[index]
			if !step.Matched {
				continue
			}
			step.ErrorNumer = (step.RoundedUnits-step.Quotient)*result.TotalWeight - step.Remainder
			result.CumulativeRoundingError.Numerator += step.ErrorNumer
		}
	}

	result.Score = FormatFixed(result.ScoreUnits, p.Scale)
	errorDen := big.NewInt(result.TotalWeight)
	errorDen.Mul(errorDen, big.NewInt(p.Scale))
	result.CumulativeRoundingError.Denominator = errorDen.Int64()
	result.CumulativeRoundingError.Scale = 1_000_000_000
	if result.TotalWeight > 0 && result.CumulativeRoundingError.Numerator != 0 {
		displayNum := new(big.Int).Mul(big.NewInt(result.CumulativeRoundingError.Numerator), big.NewInt(result.CumulativeRoundingError.Scale))
		den := new(big.Int).Mul(big.NewInt(result.TotalWeight), big.NewInt(p.Scale))
		q, r := new(big.Int).QuoRem(displayNum, den, new(big.Int))
		if result.CumulativeRoundingError.Numerator >= 0 {
			if twice := new(big.Int).Mul(r, big.NewInt(2)); twice.Cmp(den) >= 0 {
				q.Add(q, big.NewInt(1))
			}
		} else {
			absNum := new(big.Int).Abs(displayNum)
			absQ, absR := new(big.Int).QuoRem(absNum, den, new(big.Int))
			if twice := new(big.Int).Mul(absR, big.NewInt(2)); twice.Cmp(den) >= 0 {
				absQ.Add(absQ, big.NewInt(1))
			}
			q = absQ.Neg(absQ)
		}
		result.CumulativeRoundingError.FixedUnits = q.Int64()
	}

	result.Decision, result.TieBreak.Applied, result.TieBreak.BoundaryNode = CompareThreshold(
		result.ScoreUnits, p.ThresholdUnits, p.RelativeTolerancePPM, result.TieBreak.OrderedNodes, matched,
	)
	return result
}

// CompareThreshold performs integer fixed-point comparison. Relative tolerance is
// expressed in parts per million. When the values are inside tolerance, the first
// node in stable region/id order is the declared deterministic tie-breaker.
func CompareThreshold(scoreUnits, thresholdUnits, tolerancePPM int64, orderedNodeIDs []string, matched map[string]bool) (decision string, tieApplied bool, boundaryNode string) {
	diff := scoreUnits - thresholdUnits
	if diff < 0 {
		diff = -diff
	}
	maxUnits := scoreUnits
	if thresholdUnits > maxUnits {
		maxUnits = thresholdUnits
	}
	within := false
	if maxUnits >= 0 {
		within = diff*1_000_000 <= tolerancePPM*maxUnits
	}
	if !within {
		if scoreUnits > thresholdUnits {
			return "PASS", false, ""
		}
		return "FAIL", false, ""
	}

	for _, id := range orderedNodeIDs {
		boundaryNode = id
		break
	}
	if boundaryNode == "" {
		if scoreUnits >= thresholdUnits {
			return "PASS_AT_TIE", true, ""
		}
		return "FAIL_AT_TIE", true, ""
	}
	if matched[boundaryNode] {
		return "TIE_BROKEN_PASS", true, boundaryNode
	}
	return "TIE_BROKEN_FAIL", true, boundaryNode
}

func FormatFixed(units, scale int64) string {
	if scale <= 0 {
		return strconv.FormatInt(units, 10)
	}
	digits := len(strconv.FormatInt(scale, 10)) - 1
	if digits <= 0 {
		return strconv.FormatInt(units, 10)
	}
	negative := units < 0
	if negative {
		units = -units
	}
	whole := units / scale
	frac := units % scale
	s := fmt.Sprintf("%d.%0*d", whole, digits, frac)
	if negative {
		s = "-" + s
	}
	return s
}

func ParseFixedDecimal(raw string, scale int64) (int64, error) {
	if scale <= 0 {
		scale = 1_000_000
	}
	raw = strings.TrimSpace(raw)
	negative := strings.HasPrefix(raw, "-")
	raw = strings.TrimPrefix(raw, "-")
	parts := strings.SplitN(raw, ".", 2)
	whole, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid fixed decimal %q", raw)
	}
	digits := len(strconv.FormatInt(scale, 10)) - 1
	frac := ""
	if len(parts) == 2 {
		frac = parts[1]
	}
	if len(frac) > digits {
		frac = frac[:digits]
	}
	frac += strings.Repeat("0", digits-len(frac))
	fracUnits, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, err
	}
	units := whole*scale + fracUnits
	if negative {
		units = -units
	}
	return units, nil
}
