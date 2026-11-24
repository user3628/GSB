package rewardpack

import (
	"fmt"
	"sort"
)

const (
	ReasonSourceQuota       = "SOURCE_QUOTA"
	ReasonGlobalBudget      = "GLOBAL_TOKEN_BUDGET"
	ReasonBothLimits        = "GLOBAL_TOKEN_BUDGET_AND_SOURCE_QUOTA"
	ReasonQuotaBelowMinimum = "QUOTA_BELOW_MIN_RETENTION_UNIT"
	ReasonNoPairs           = "NO_AVAILABLE_PAIRS"
	ReasonZeroQuota         = "ZERO_SOURCE_QUOTA"
)

// DecideBudget applies deterministic per-source quota allocation and retention.
func DecideBudget(scores []ScoreResult, cfg Config, observedSources []string) (BudgetResult, error) {
	orderedScores := append([]ScoreResult(nil), scores...)
	sort.Slice(orderedScores, func(i, j int) bool { return orderedScores[i].PairID < orderedScores[j].PairID })

	sourceSet := map[string]bool{}
	for _, source := range observedSources {
		sourceSet[source] = true
	}
	for _, score := range orderedScores {
		sourceSet[score.Source] = true
	}
	for source := range cfg.Budget.SourceQuotas {
		sourceSet[source] = true
	}
	sources := sortedStringKeys(sourceSet)

	availableBySource := map[string]int{}
	for _, score := range orderedScores {
		availableBySource[score.Source]++
	}
	requested := make(map[string]int, len(sources))
	activeRequested := make(map[string]int, len(sources))
	for _, source := range sources {
		if quota, ok := cfg.Budget.SourceQuotas[source]; ok {
			requested[source] = quota
		} else {
			requested[source] = cfg.Budget.DefaultSourceQuota
		}
		if availableBySource[source] > 0 && requested[source] >= cfg.Budget.MinRetentionUnit {
			activeRequested[source] = requested[source]
		}
	}
	effective := allocateQuotas(activeRequested, cfg.Budget.GlobalTokens)
	remainingSource := make(map[string]int, len(sources))
	layers := make(map[string]*SourceLayer, len(sources))
	for _, source := range sources {
		quota := effective[source]
		if availableBySource[source] == 0 {
			quota = 0
		}
		remainingSource[source] = quota
		layer := &SourceLayer{
			Source:          source,
			AvailablePairs:  availableBySource[source],
			ConfiguredQuota: requested[source],
			EffectiveQuota:  quota,
		}
		switch {
		case availableBySource[source] == 0:
			layer.Status = "EMPTY"
			layer.Note = "no available preference pairs for this source; empty layer retained"
		case quota < cfg.Budget.MinRetentionUnit:
			layer.Status = "QUOTA_BELOW_MIN_UNIT"
			layer.Note = fmt.Sprintf("configured quota %d and effective quota %d are below min_retention_unit %d; empty layer retained", requested[source], quota, cfg.Budget.MinRetentionUnit)
		default:
			layer.Status = "PENDING"
		}
		layers[source] = layer
	}

	byID := make(map[string]ScoreResult, len(orderedScores))
	selected := map[string]RetainedPair{}
	eliminated := map[string]EliminatedPair{}
	globalRemaining := cfg.Budget.GlobalTokens

	candidateOrder := append([]ScoreResult(nil), orderedScores...)
	sort.Slice(candidateOrder, func(i, j int) bool {
		a, b := candidateOrder[i], candidateOrder[j]
		if a.Strength != b.Strength {
			return a.Strength > b.Strength
		}
		if a.TokenCost != b.TokenCost {
			return a.TokenCost < b.TokenCost
		}
		return a.PairID < b.PairID
	})
	for _, score := range candidateOrder {
		byID[score.PairID] = score
	}

	selectPair := func(score ScoreResult, purpose string) bool {
		layer := layers[score.Source]
		if layer == nil || layer.EffectiveQuota < cfg.Budget.MinRetentionUnit {
			return false
		}
		if score.TokenCost > layer.EffectiveQuota || score.TokenCost > globalRemaining {
			return false
		}
		if remainingSource[score.Source] < score.TokenCost || globalRemaining < score.TokenCost {
			return false
		}
		remainingSource[score.Source] -= score.TokenCost
		globalRemaining -= score.TokenCost
		selected[score.PairID] = RetainedPair{
			PairID: score.PairID, Source: score.Source, Lang: score.Lang,
			Strength: score.Strength, TokenCost: score.TokenCost, Rank: 0, Purpose: purpose,
		}
		return true
	}

	requiredOrder, minima := languageRequirements(cfg)
	for _, language := range requiredOrder {
		minimum := minima[language]
		for count := 0; count < minimum; count++ {
			found := false
			for _, score := range candidateOrder {
				if score.Lang != language {
					continue
				}
				if _, ok := selected[score.PairID]; ok {
					continue
				}
				if selectPair(score, "LANGUAGE_COVERAGE") {
					found = true
					break
				}
			}
			if !found {
				break
			}
		}
	}

	for _, score := range candidateOrder {
		if _, ok := selected[score.PairID]; ok {
			continue
		}
		if selectPair(score, "STRENGTH_FILL") {
			continue
		}
		layer := layers[score.Source]
		reason := ReasonSourceQuota
		detail := "source quota cannot accommodate pair"
		if layer == nil || layer.EffectiveQuota < cfg.Budget.MinRetentionUnit {
			reason = ReasonQuotaBelowMinimum
			detail = "source effective quota is below the minimum retention unit"
		} else if remainingSource[score.Source] < score.TokenCost && globalRemaining < score.TokenCost {
			reason = ReasonBothLimits
			detail = fmt.Sprintf("source remaining=%d, global remaining=%d, pair cost=%d", remainingSource[score.Source], globalRemaining, score.TokenCost)
		} else if globalRemaining < score.TokenCost {
			reason = ReasonGlobalBudget
			detail = fmt.Sprintf("global remaining=%d, pair cost=%d", globalRemaining, score.TokenCost)
		} else if remainingSource[score.Source] < score.TokenCost {
			detail = fmt.Sprintf("source remaining=%d, pair cost=%d", remainingSource[score.Source], score.TokenCost)
		}
		eliminated[score.PairID] = EliminatedPair{
			PairID: score.PairID, Source: score.Source, Lang: score.Lang,
			Strength: score.Strength, TokenCost: score.TokenCost,
			Reason: reason, Detail: detail,
		}
	}

	retained := make([]RetainedPair, 0, len(selected))
	for _, pair := range selected {
		retained = append(retained, pair)
	}
	sort.Slice(retained, func(i, j int) bool {
		a, b := retained[i], retained[j]
		if a.Strength != b.Strength {
			return a.Strength > b.Strength
		}
		if a.TokenCost != b.TokenCost {
			return a.TokenCost < b.TokenCost
		}
		return a.PairID < b.PairID
	})
	for i := range retained {
		retained[i].Rank = i + 1
		layer := layers[retained[i].Source]
		layer.KeptPairs++
		layer.UsedTokens += retained[i].TokenCost
	}
	eliminatedList := make([]EliminatedPair, 0, len(eliminated))
	for _, pair := range eliminated {
		eliminatedList = append(eliminatedList, pair)
	}
	sort.Slice(eliminatedList, func(i, j int) bool { return eliminatedList[i].PairID < eliminatedList[j].PairID })

	layerList := make([]SourceLayer, 0, len(sources))
	for _, source := range sources {
		layer := layers[source]
		if layer.Status == "PENDING" {
			switch {
			case layer.KeptPairs > 0 && remainingSource[source] < layer.EffectiveQuota:
				layer.Status = "SELECTED"
				layer.Note = fmt.Sprintf("kept %d pair(s) using %d/%d tokens", layer.KeptPairs, layer.UsedTokens, layer.EffectiveQuota)
			case layer.KeptPairs > 0:
				layer.Status = "SELECTED"
				layer.Note = fmt.Sprintf("kept %d pair(s) using %d/%d tokens", layer.KeptPairs, layer.UsedTokens, layer.EffectiveQuota)
			default:
				layer.Status = "NO_PAIR_FIT"
				layer.Note = fmt.Sprintf("effective quota %d could not fit any available pair", layer.EffectiveQuota)
			}
		}
		layerList = append(layerList, *layer)
	}

	coverage := make([]LanguageCoverage, 0, len(requiredOrder))
	unmet := make([]string, 0)
	keptByLanguage := map[string]int{}
	for _, pair := range retained {
		keptByLanguage[pair.Lang]++
	}
	for _, language := range requiredOrder {
		required := minima[language]
		kept := keptByLanguage[language]
		met := kept >= required
		note := ""
		if !met {
			note = fmt.Sprintf("required %d, retained %d", required, kept)
			unmet = append(unmet, language)
		}
		coverage = append(coverage, LanguageCoverage{Language: language, Required: required, Kept: kept, Met: met, Note: note})
	}

	result := BudgetResult{
		GlobalTokens:     cfg.Budget.GlobalTokens,
		UsedTokens:       cfg.Budget.GlobalTokens - globalRemaining,
		MinRetentionUnit: cfg.Budget.MinRetentionUnit,
		Retained:         retained,
		Eliminated:       eliminatedList,
		SourceLayers:     layerList,
		LanguageCoverage: coverage,
		InputDigest:      budgetInputDigest(orderedScores, cfg),
	}
	result.Digest = digestJSON(result)
	return result, nil
}

func budgetInputDigest(scores []ScoreResult, cfg Config) string {
	digests := make([]string, len(scores))
	for i, score := range scores {
		digests[i] = score.Digest
	}
	sort.Strings(digests)
	return hashBytes([]byte("budget\x00"), []byte(cfg.BudgetDigest()), []byte(CanonicalString(digests)))
}

func languageRequirements(cfg Config) ([]string, map[string]int) {
	minima := map[string]int{}
	order := make([]string, 0)
	seen := map[string]bool{}
	for _, language := range cfg.Budget.RequiredLanguages {
		language = normalizeLanguage(language)
		if language == "" || seen[language] {
			continue
		}
		seen[language] = true
		minimum := cfg.Budget.LanguageMinimums[language]
		if minimum < 1 {
			minimum = 1
		}
		order = append(order, language)
		minima[language] = minimum
	}
	extra := make([]string, 0)
	for language, minimum := range cfg.Budget.LanguageMinimums {
		language = normalizeLanguage(language)
		if language == "" || seen[language] || minimum <= 0 {
			continue
		}
		seen[language] = true
		extra = append(extra, language)
		minima[language] = minimum
	}
	sort.Strings(extra)
	order = append(order, extra...)
	return order, minima
}

type quotaRemainder struct {
	source    string
	base      int
	remainder int64
}

func allocateQuotas(requested map[string]int, global int) map[string]int {
	out := make(map[string]int, len(requested))
	if global <= 0 {
		for source := range requested {
			out[source] = 0
		}
		return out
	}
	sum := 0
	for _, quota := range requested {
		sum += quota
	}
	if sum <= global {
		for source, quota := range requested {
			out[source] = quota
		}
		return out
	}
	rows := make([]quotaRemainder, 0, len(requested))
	assigned := 0
	for _, source := range sortedStringKeys(requested) {
		quota := requested[source]
		numerator := int64(quota) * int64(global)
		base := int(numerator / int64(sum))
		remainder := numerator % int64(sum)
		out[source] = base
		assigned += base
		rows = append(rows, quotaRemainder{source: source, base: base, remainder: remainder})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].remainder != rows[j].remainder {
			return rows[i].remainder > rows[j].remainder
		}
		return rows[i].source < rows[j].source
	})
	for i := 0; assigned < global && i < len(rows); i++ {
		out[rows[i].source]++
		assigned++
	}
	return out
}
