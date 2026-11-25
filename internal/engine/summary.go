package engine

import "localsearch/multirecall/internal/model"

func buildSummary(rc *runContext, results []model.QueryResult, unprocessed []model.UnprocessedQuery, stopped bool) model.Summary {
	summary := model.Summary{
		ConfigVersion:           rc.config.Version,
		ConfigFingerprint:       rc.configFingerprint,
		Seed:                    rc.config.Seed,
		DataFingerprint:         rc.data.DataFingerprint,
		QueryCount:              len(rc.data.Queries),
		ProcessedCount:          len(results),
		UnprocessedCount:        len(unprocessed),
		FusionTopK:              rc.config.Fusion.TopK,
		EvaluationTopK:          10,
		NotApplicableCounts:     make(map[string]int),
		UnprocessedReasonCounts: make(map[string]int),
		Stopped:                 stopped,
	}
	routeAggregates := make(map[string]*model.RouteAggregate, len(model.RouteOrder))
	for _, route := range model.RouteOrder {
		routeAggregates[route] = &model.RouteAggregate{Route: route}
	}
	processedIDs := make(map[string]struct{}, len(results))
	for _, result := range results {
		processedIDs[result.QueryID] = struct{}{}
		for _, route := range result.Recall.Routes {
			aggregate := routeAggregates[route.Route]
			aggregate.CandidateCount += len(route.Candidates)
			if route.Enabled {
				aggregate.EnabledQueries++
			}
		}
		for _, hit := range result.Evaluation.RouteHits {
			if aggregate, ok := routeAggregates[hit.Route]; ok {
				aggregate.HitCount += hit.HitCount
			}
		}
		addMetricAggregate(&summary.FusedTop10Overlap, result.Evaluation.FusedTop10Overlap)
		addMetricAggregate(&summary.CandidateCoverage, result.Evaluation.CandidateCoverage)
		addMetricAggregate(&summary.AverageRankDeviation, result.Evaluation.AverageRankDeviation)
		addNotApplicable(&summary, "fused_top10_overlap", result.Evaluation.FusedTop10Overlap)
		addNotApplicable(&summary, "candidate_coverage", result.Evaluation.CandidateCoverage)
		addNotApplicable(&summary, "average_rank_deviation", result.Evaluation.AverageRankDeviation)
	}
	for _, work := range rc.jobs {
		if _, ok := processedIDs[work.query.QueryID]; ok && work.decision.Processed {
			summary.PredictedEvaluations += work.decision.PredictedEvaluations
		}
	}
	for _, route := range model.RouteOrder {
		aggregate := routeAggregates[route]
		summary.Routes = append(summary.Routes, *aggregate)
	}
	finalizeMetric(&summary.FusedTop10Overlap)
	finalizeMetric(&summary.CandidateCoverage)
	finalizeMetric(&summary.AverageRankDeviation)
	for _, value := range unprocessed {
		summary.UnprocessedReasonCounts[value.Reason]++
	}
	summary.SummaryFingerprint = ""
	data, _ := model.CanonicalJSON(summary)
	summary.SummaryFingerprint = model.SHA256(data)
	return summary
}

func addMetricAggregate(target *model.AggregateMetric, metric model.Metric) {
	if !metric.Applicable {
		target.NotApplicableQueries++
		return
	}
	target.ApplicableQueries++
	target.NumeratorSum += metric.Numerator
	target.DenominatorSum += metric.Denominator
}

func finalizeMetric(target *model.AggregateMetric) {
	if target.DenominatorSum > 0 {
		target.Mean = float64(target.NumeratorSum) / float64(target.DenominatorSum)
	}
}

func addNotApplicable(summary *model.Summary, name string, metric model.Metric) {
	if metric.Applicable {
		return
	}
	summary.NotApplicableCounts[name]++
	reason := metric.NotApplicableReason
	if reason == "" {
		reason = "unspecified"
	}
	summary.NotApplicableCounts[name+":"+reason]++
}
