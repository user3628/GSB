package recall

import (
	"strings"

	"multirecall/internal/model"
)

func splitCategoryPath(p string) []string {
	parts := strings.Split(p, "/")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func commonPrefixDepth(a, b []string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	d := 0
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			break
		}
		d++
	}
	return d
}

func categoryPrefixDepth(history string, item model.Item) (int, []string) {
	hp := splitCategoryPath(history)
	if len(hp) == 0 {
		return 0, nil
	}
	if len(hp) == 1 {
		if hp[0] == item.Category || (len(item.CategoryPath) > 0 && hp[0] == item.CategoryPath[len(item.CategoryPath)-1]) {
			return 1, hp
		}
	}
	return commonPrefixDepth(hp, item.CategoryPath), hp
}

func SearchCategory(q model.Query, items []model.Item, requestedDepth, effectiveDepth int) RouteResult {
	var cands []Candidate
	for _, item := range items {
		ev := CategoryEvidence{Formula: "sum(recency_index_plus_1 * common_path_prefix_depth); exact int64 count domain"}
		var score int64
		for i, history := range q.HistoryClickCategories {
			depth, prefix := categoryPrefixDepth(history, item)
			if depth == 0 {
				continue
			}
			recency := int64(i + 1)
			contribution := recency * int64(depth)
			score += contribution
			ev.PrefixCount += depth
			ev.Matches = append(ev.Matches, CategoryMatch{
				HistoryPath: history, HistoryPrefix: prefix, ItemPrefix: append([]string(nil), item.CategoryPath...),
				PrefixDepth: depth, RecencyWeight: recency, Contribution: contribution,
			})
		}
		if score == 0 {
			continue
		}
		ev.Score = score
		cands = append(cands, Candidate{
			ItemID: item.ItemID, DomainScore: float64(score), DomainScoreFP: score,
			RawErrorBound: 0, Category: &ev,
		})
	}
	return finalizeRoute("category", requestedDepth, effectiveDepth, cands)
}
