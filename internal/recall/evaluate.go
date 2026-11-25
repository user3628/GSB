package recall

import (
	"sort"

	"localsearch/multirecall/internal/model"
)

func Evaluate(bundle model.RecallBundle, fused model.FusionResult, judgments []model.Judgment, evalConfig model.EvaluationConfig) model.EvaluationResult {
	result := model.EvaluationResult{
		QueryID:           bundle.QueryID,
		RouteHits:         make([]model.RouteHit, 0, len(model.RouteOrder)),
		RelevantItemCount: len(judgments),
	}
	relevant := make(map[string]struct{}, len(judgments))
	for _, judgment := range judgments {
		if judgment.Relevance > 0 {
			relevant[judgment.ItemID] = struct{}{}
		}
	}
	result.RelevantItemCount = len(relevant)
	routeRank := make(map[string]map[string]int, len(bundle.Routes))
	for _, route := range bundle.Routes {
		rankMap := make(map[string]int, len(route.Candidates))
		hits := 0
		for rank, candidate := range route.Candidates {
			if _, ok := rankMap[candidate.ItemID]; !ok {
				rankMap[candidate.ItemID] = rank + 1
			}
			if _, ok := relevant[candidate.ItemID]; ok {
				hits++
			}
		}
		routeRank[route.Route] = rankMap
		result.RouteHits = append(result.RouteHits, model.RouteHit{Route: route.Route, HitCount: hits})
	}
	if len(relevant) == 0 {
		result.FusedTop10Overlap = notApplicable("no_reference_labels")
		result.CandidateCoverage = notApplicable("no_reference_labels")
		result.AverageRankDeviation = notApplicable("no_reference_labels")
		return result
	}

	topK := 10
	if len(fused.Results) < topK {
		topK = len(fused.Results)
	}
	overlapHits := 0
	fusedRank := make(map[string]int, len(fused.Results))
	for rank, item := range fused.Results {
		if _, exists := fusedRank[item.ItemID]; !exists {
			fusedRank[item.ItemID] = rank + 1
		}
		if rank < topK {
			if _, ok := relevant[item.ItemID]; ok {
				overlapHits++
			}
		}
	}
	overlapDenom := min(10, len(relevant))
	result.FusedTop10Overlap = applicable(overlapHits, overlapDenom, "no_relevant_items_in_top_window")

	union := make(map[string]struct{})
	bestRouteRank := make(map[string]int)
	for _, route := range bundle.Routes {
		for rank, candidate := range route.Candidates {
			union[candidate.ItemID] = struct{}{}
			if _, exists := bestRouteRank[candidate.ItemID]; !exists {
				bestRouteRank[candidate.ItemID] = rank + 1
			}
		}
	}
	covered := 0
	for itemID := range relevant {
		if _, ok := union[itemID]; ok {
			covered++
		}
	}
	result.CandidateCoverage = applicable(covered, len(relevant), "no_reference_labels")

	deviations := make([]int, 0)
	for itemID := range relevant {
		fRank, okF := fusedRank[itemID]
		rRank, okR := bestRouteRank[itemID]
		if okF && okR {
			deviations = append(deviations, absInt(fRank-rRank))
		}
	}
	sort.Ints(deviations)
	if len(deviations) == 0 {
		result.AverageRankDeviation = notApplicable("no_ranked_relevant_overlap")
	} else {
		sum := 0
		for _, deviation := range deviations {
			sum += deviation
		}
		result.AverageRankDeviation = model.Metric{
			Applicable:  true,
			Value:       float64(sum) / float64(len(deviations)),
			Numerator:   sum,
			Denominator: len(deviations),
		}
	}
	return result
}

func applicable(numerator, denominator int, reason string) model.Metric {
	if denominator == 0 {
		return notApplicable(reason)
	}
	return model.Metric{
		Applicable:  true,
		Value:       float64(numerator) / float64(denominator),
		Numerator:   numerator,
		Denominator: denominator,
	}
}

func notApplicable(reason string) model.Metric {
	return model.Metric{Applicable: false, NotApplicableReason: reason}
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}
