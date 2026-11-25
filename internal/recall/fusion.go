package recall

import (
	"math"
	"sort"

	"multirecall/internal/configx"
	"multirecall/internal/model"
)

func weightOf(w configx.RouteWeights, route string) float64 {
	switch route {
	case "lexical":
		return w.Lexical
	case "category":
		return w.Category
	default:
		return w.Vector
	}
}

func mergeFusion(q model.Query, cfg configx.Snapshot, routes map[string]RouteResult) ([]Hit, TruncationRecord, *model.APIError) {
	w := cfg.Fusion.Weights
	if err := configx.ValidateWeights(w); err != nil {
		apiErr, ok := err.(*model.APIError)
		if ok {
			apiErr.QueryID = q.QueryID
			return nil, TruncationRecord{}, apiErr
		}
		return nil, TruncationRecord{}, &model.APIError{Code: model.CodeNoWeight, QueryID: q.QueryID, Message: err.Error()}
	}

	merged := map[string]*Hit{}
	for _, route := range routeNames() {
		rr, ok := routes[route]
		if !ok || !rr.Enabled {
			continue
		}
		weight := weightOf(w, route)
		weightFP := int64(math.Round(weight * float64(ScaleWeight)))
		for _, cand := range rr.Candidates {
			hit := merged[cand.ItemID]
			if hit == nil {
				hit = &Hit{ItemID: cand.ItemID, Derivation: []string{}, Contributions: []RouteContribution{}, RouteErrorBounds: []RouteErrorBound{}}
				merged[cand.ItemID] = hit
			}
			var contributionFP int64
			formula := ""
			switch cfg.Fusion.Method {
			case "reciprocal_rank", "rrf":
				k := cfg.Fusion.RRFK
				if k <= 0 {
					k = 60
				}
				contributionFP = roundDivNearest(weightFP, int64(k+cand.Rank))
				formula = "rrf_contribution_fp=round(weight_fp/(rrf_k+source_rank))"
			default:
				contributionFP = roundDivNearest(weightFP*cand.NormalizedScoreFP, ScaleWeight)
				formula = "weighted_contribution_fp=round(weight_fp*normalized_score_fp/1e9)"
			}
			contribution := float64(contributionFP) / float64(ScaleFusion)
			logContribution := 0.0
			if contribution > 0 {
				logContribution = math.Log(math.Max(contribution, 1.0/float64(ScaleFusion)))
			}
			hit.Contributions = append(hit.Contributions, RouteContribution{
				Route: route, SourceRank: cand.Rank, RouteScore: cand.DomainScore, RouteScoreFP: cand.DomainScoreFP,
				NormalizedScore: cand.NormalizedScore, Weight: weight, WeightFP: weightFP,
				Contribution: contribution, ContributionFP: contributionFP, LogContribution: logContribution, Formula: formula,
			})
			hit.FusionScoreFP += contributionFP
			hit.RouteErrorBounds = append(hit.RouteErrorBounds, RouteErrorBound{
				Route: route, QuantizationBound: cand.RawErrorBound,
				PropagationBound: math.Abs(weight) * cand.NormalizedErrorBound,
			})
		}
	}

	hits := make([]Hit, 0, len(merged))
	for _, hit := range merged {
		sort.SliceStable(hit.Contributions, func(i, j int) bool {
			return routeOrder(hit.Contributions[i].Route) < routeOrder(hit.Contributions[j].Route)
		})
		sort.SliceStable(hit.RouteErrorBounds, func(i, j int) bool {
			return routeOrder(hit.RouteErrorBounds[i].Route) < routeOrder(hit.RouteErrorBounds[j].Route)
		})
		hit.FusionScore = float64(hit.FusionScoreFP) / float64(ScaleFusion)
		hit.Derivation = append(hit.Derivation,
			"route order is lexical,category,vector; source ranks are 1-based within each adjudicated route depth",
			"fusion score is exact int64 accumulation of contribution_fp divided by 1e9",
			"log_contribution is ln(contribution), with zero contribution recorded as 0",
		)
		hits = append(hits, *hit)
	}
	sort.SliceStable(hits, func(i, j int) bool {
		if hits[i].FusionScoreFP != hits[j].FusionScoreFP {
			return hits[i].FusionScoreFP > hits[j].FusionScoreFP
		}
		return hits[i].ItemID < hits[j].ItemID
	})
	for i := range hits {
		hits[i].Rank = i + 1
	}

	record := BuildTruncationRecord(len(hits), cfg)
	if hits == nil {
		hits = []Hit{}
	}
	return hits, record, nil
}

func BuildTruncationRecord(mergedCount int, cfg configx.Snapshot) TruncationRecord {
	tr := cfg.Fusion.Truncation
	limit := tr.TopK
	if tr.MaxCandidates > 0 && (limit <= 0 || tr.MaxCandidates < limit) {
		limit = tr.MaxCandidates
	}
	record := TruncationRecord{RequestedTopK: tr.TopK, MaxCandidates: tr.MaxCandidates, MergedCandidates: mergedCount, EffectiveLimit: limit}
	if limit <= 0 || limit > mergedCount {
		limit = mergedCount
		record.EffectiveLimit = limit
		if tr.RecordClamp {
			record.ClampedToCandidate = true
			record.Reason = "requested truncation position exceeds merged candidate count; truncated to candidate count"
		}
	}
	if tr.MaxCandidates > 0 && tr.MaxCandidates < mergedCount && limit == tr.MaxCandidates {
		record.Truncated = true
		if record.Reason == "" {
			record.Reason = "max_candidates truncation applied"
		}
	} else if tr.TopK > 0 && tr.TopK < mergedCount {
		record.Truncated = true
	}
	record.ReturnedHits = limit
	return record
}

func ApplyTruncation(hits []Hit, record TruncationRecord) []Hit {
	if record.EffectiveLimit < len(hits) {
		hits = append([]Hit(nil), hits[:record.EffectiveLimit]...)
	}
	if hits == nil {
		return []Hit{}
	}
	return hits
}

func routeOrder(route string) int {
	switch route {
	case "lexical":
		return 0
	case "category":
		return 1
	case "vector":
		return 2
	default:
		return 9
	}
}
