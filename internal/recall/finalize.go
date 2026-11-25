package recall

import (
	"math"
	"sort"
)

func roundDivNearest(n, d int64) int64 {
	if d == 0 {
		return 0
	}
	if n >= 0 {
		return (n + d/2) / d
	}
	return -((-n + d/2) / d)
}

func finalizeRoute(route string, requestedDepth, effectiveDepth int, cands []Candidate) RouteResult {
	result := RouteResult{Route: route, Enabled: effectiveDepth > 0, RequestedDepth: requestedDepth, EffectiveDepth: effectiveDepth}
	if effectiveDepth <= 0 {
		result.Candidates = []Candidate{}
		return result
	}
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].DomainScoreFP != cands[j].DomainScoreFP {
			return cands[i].DomainScoreFP > cands[j].DomainScoreFP
		}
		return cands[i].ItemID < cands[j].ItemID
	})
	if len(cands) > effectiveDepth {
		cands = append([]Candidate(nil), cands[:effectiveDepth]...)
	}

	var maxScore float64
	var maxBound float64
	for _, c := range cands {
		if c.DomainScore > maxScore {
			maxScore = c.DomainScore
			maxBound = c.RawErrorBound
		}
	}
	for i := range cands {
		cands[i].Route = route
		cands[i].Rank = i + 1
		if maxScore > 0 && cands[i].DomainScoreFP > 0 {
			cands[i].NormalizedScoreFP = int64(math.Round(
				float64(cands[i].DomainScoreFP) / float64(cands[0].DomainScoreFP) * float64(ScaleNormal),
			))
			if cands[i].NormalizedScoreFP > ScaleNormal {
				cands[i].NormalizedScoreFP = ScaleNormal
			}
			cands[i].NormalizedScore = float64(cands[i].NormalizedScoreFP) / float64(ScaleNormal)
			cands[i].NormalizedErrorBound = (cands[i].RawErrorBound+cands[i].NormalizedScore*maxBound)/maxScore +
				0.5/float64(ScaleNormal)
		}
	}
	result.Candidates = cands
	result.Returned = len(cands)
	return result
}
