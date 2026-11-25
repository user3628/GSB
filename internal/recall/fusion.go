package recall

import (
	"math"
	"sort"

	"localsearch/multirecall/internal/model"
)

func Fuse(bundle model.RecallBundle, config model.FusionConfig) (model.FusionResult, error) {
	result := model.FusionResult{
		QueryID:       bundle.QueryID,
		Strategy:      config.Strategy,
		RequestedTopK: config.TopK,
		RouteDetails:  make([]model.RouteDetail, 0, len(bundle.Routes)),
		Results:       []model.FusedItem{},
	}
	type accumulator struct {
		item model.FusedItem
		seen map[string]struct{}
	}
	byItem := make(map[string]*accumulator)
	for _, route := range bundle.Routes {
		details := model.RouteDetail{Route: route.Route, CandidateCount: len(route.Candidates), ItemIDs: make([]string, 0, len(route.Candidates))}
		seenRoute := make(map[string]struct{}, len(route.Candidates))
		for rank, candidate := range route.Candidates {
			if _, duplicate := seenRoute[candidate.ItemID]; duplicate {
				return model.FusionResult{}, model.NewError(model.CodeInternal, "route contains duplicate item_id", bundle.QueryID)
			}
			seenRoute[candidate.ItemID] = struct{}{}
			details.ItemIDs = append(details.ItemIDs, candidate.ItemID)
			contribution := routeContribution(config, route.Route, candidate.Score, rank+1)
			acc, ok := byItem[candidate.ItemID]
			if !ok {
				acc = &accumulator{
					item: model.FusedItem{ItemID: candidate.ItemID, Sources: []model.SourceContribution{}},
					seen: make(map[string]struct{}),
				}
				byItem[candidate.ItemID] = acc
			}
			if _, duplicate := acc.seen[route.Route]; duplicate {
				return model.FusionResult{}, model.NewError(model.CodeInternal, "item merged twice from same route", bundle.QueryID)
			}
			acc.seen[route.Route] = struct{}{}
			acc.item.Score += contribution
			acc.item.Sources = append(acc.item.Sources, model.SourceContribution{
				Route:        route.Route,
				Rank:         rank + 1,
				RouteScore:   candidate.Score,
				Contribution: contribution,
			})
		}
		sort.Strings(details.ItemIDs)
		result.RouteDetails = append(result.RouteDetails, details)
	}
	result.CandidateCount = len(byItem)
	unique := make([]model.FusedItem, 0, len(byItem))
	for _, acc := range byItem {
		sort.Slice(acc.item.Sources, func(i, j int) bool {
			return model.RouteIndex(acc.item.Sources[i].Route) < model.RouteIndex(acc.item.Sources[j].Route)
		})
		unique = append(unique, acc.item)
	}
	sort.Slice(unique, func(i, j int) bool {
		if unique[i].Score == unique[j].Score {
			return unique[i].ItemID < unique[j].ItemID
		}
		return unique[i].Score > unique[j].Score
	})
	if config.TopK > len(unique) {
		result.TopKExceedsCandidates = true
		result.Truncated = true
		result.TruncationReason = "top_k_exceeds_candidates"
	} else if config.TopK < len(unique) {
		result.Truncated = true
		result.TruncationReason = "top_k_reached"
	}
	if config.TopK < len(unique) {
		unique = unique[:config.TopK]
	}
	result.Results = unique
	result.ReturnedCount = len(unique)
	return result, nil
}

func routeContribution(config model.FusionConfig, route string, score float64, rank int) float64 {
	weight := config.Weights.Get(route)
	if config.Strategy == "rrf" {
		denom := config.RRFK + float64(rank)
		if denom == 0 || math.IsInf(denom, 0) || math.IsNaN(denom) {
			return 0
		}
		return weight / denom
	}
	return weight * score
}

func VerifyFusion(bundle model.RecallBundle, config model.FusionConfig, fused model.FusionResult) bool {
	expected, err := Fuse(bundle, config)
	if err != nil {
		return false
	}
	gotData, err := model.CanonicalJSON(fused)
	if err != nil {
		return false
	}
	wantData, err := model.CanonicalJSON(expected)
	if err != nil {
		return false
	}
	return string(gotData) == string(wantData)
}

func SortedRouteCandidateIDs(route model.RouteResult) []string {
	ids := make([]string, 0, len(route.Candidates))
	for _, candidate := range route.Candidates {
		ids = append(ids, candidate.ItemID)
	}
	sort.Strings(ids)
	return ids
}
