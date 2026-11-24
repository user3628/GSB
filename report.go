package rewardpack

import "sort"

// BuildReport creates the final auditable attribution artifact.
func BuildReport(cfg Config, scores []ScoreResult, features []FeatureResult, budget BudgetResult) Report {
	featureByPair := make(map[string]string, len(features))
	for _, feature := range features {
		featureByPair[feature.PairID] = feature.Digest
	}
	retained := make(map[string]bool, len(budget.Retained))
	for _, pair := range budget.Retained {
		retained[pair.PairID] = true
	}
	eliminated := make(map[string]EliminatedPair, len(budget.Eliminated))
	for _, pair := range budget.Eliminated {
		eliminated[pair.PairID] = pair
	}
	ordered := append([]ScoreResult(nil), scores...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].PairID < ordered[j].PairID })
	pairs := make([]ReportPair, 0, len(ordered))
	for _, score := range ordered {
		decision := "RETAINED"
		if !retained[score.PairID] {
			if why, ok := eliminated[score.PairID]; ok {
				decision = "ELIMINATED:" + why.Reason
			} else {
				decision = "ELIMINATED:UNKNOWN"
			}
		}
		pairs = append(pairs, ReportPair{
			PairID:        score.PairID,
			Source:        score.Source,
			Lang:          score.Lang,
			Strength:      score.Strength,
			TokenCost:     score.TokenCost,
			Factors:       append([]Factor(nil), score.Factors...),
			FeatureDigest: featureByPair[score.PairID],
			Retained:      retained[score.PairID],
			Decision:      decision,
		})
	}
	unmet := make([]string, 0)
	for _, coverage := range budget.LanguageCoverage {
		if !coverage.Met {
			unmet = append(unmet, coverage.Language)
		}
	}
	report := Report{
		ConfigVersion:    cfg.Version,
		ConfigDigest:     cfg.Digest(),
		GlobalTokens:     budget.GlobalTokens,
		UsedTokens:       budget.UsedTokens,
		RetainedPairs:    len(budget.Retained),
		EliminatedPairs:  len(budget.Eliminated),
		Pairs:            pairs,
		SourceLayers:     append([]SourceLayer(nil), budget.SourceLayers...),
		LanguageCoverage: append([]LanguageCoverage(nil), budget.LanguageCoverage...),
		UnmetLanguages:   unmet,
	}
	report.InputDigest = hashBytes(
		[]byte("report\x00"),
		[]byte(cfg.Digest()),
		[]byte(budget.Digest),
		[]byte(CanonicalString(featureByPair)),
	)
	report.Digest = digestJSON(report)
	return report
}

// Bundle returns the canonical comparison artifact.
func Bundle(cfg Config, normalized []NormalizedPair, scores []ScoreResult, features []FeatureResult, budget BudgetResult, report Report) ArtifactBundle {
	return ArtifactBundle{
		ConfigVersion: cfg.Version,
		ConfigDigest:  cfg.Digest(),
		Normalized:    normalized,
		Scores:        scores,
		Features:      features,
		Budget:        budget,
		Report:        report,
	}
}
