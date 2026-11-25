package recall

import (
	"math"

	"localsearch/multirecall/internal/model"
)

type Index struct {
	items          []model.Item
	itemByID       map[string]model.Item
	categoryCounts map[string]int
	titleTerms     [][]string
	itemNorms      []float64
	vectorDim      int
}

type RoutePlan struct {
	Enabled bool
	Depth   int
}

func NewIndex(items []model.Item) *Index {
	items = model.NormalizeItems(items)
	idx := &Index{
		items:          items,
		itemByID:       make(map[string]model.Item, len(items)),
		categoryCounts: make(map[string]int),
		titleTerms:     make([][]string, len(items)),
		itemNorms:      make([]float64, len(items)),
	}
	for i, item := range items {
		if idx.vectorDim == 0 && len(item.Vector) > 0 {
			idx.vectorDim = len(item.Vector)
		}
		idx.itemByID[item.ItemID] = item
		idx.categoryCounts[item.Category]++
		terms := tokenize(item.Title)
		idx.titleTerms[i] = terms
		idx.itemNorms[i] = norm(item.Vector)
	}
	return idx
}

func (idx *Index) ItemCount() int { return len(idx.items) }

func (idx *Index) Item(itemID string) (model.Item, bool) {
	item, ok := idx.itemByID[itemID]
	return item, ok
}

func (idx *Index) RecallRoute(query model.Query, route string, plan RoutePlan) (model.RouteResult, error) {
	result := model.RouteResult{
		Route:          route,
		Enabled:        plan.Enabled,
		RequestedDepth: plan.Depth,
		AppliedDepth:   plan.Depth,
		Candidates:     []model.Candidate{},
	}
	if len(idx.items) == 0 || IsEmptyQuery(query.Text) {
		return result, model.NewError(model.CodeEmptyInput, "query text and item library must not be empty", query.QueryID)
	}
	if !plan.Enabled || plan.Depth <= 0 {
		result.DisabledReason = "route_disabled_by_budget"
		return result, nil
	}
	switch route {
	case model.RouteLexical:
		result.Candidates = idx.lexical(query, plan.Depth)
	case model.RouteCategory:
		result.Candidates = idx.category(query, plan.Depth)
	case model.RouteVector:
		result.Candidates = idx.vector(query, plan.Depth)
	default:
		return result, model.NewError(model.CodeInvalidConfig, "unknown route: "+route, query.QueryID)
	}
	model.SortCandidates(result.Candidates)
	result.EvaluatedCount = plan.Depth
	if result.EvaluatedCount > len(idx.items) {
		result.EvaluatedCount = len(idx.items)
		result.AppliedDepth = len(idx.items)
	}
	return result, nil
}

func (idx *Index) RecallAll(query model.Query, plans map[string]RoutePlan) (model.RecallBundle, error) {
	if len(idx.items) == 0 || IsEmptyQuery(query.Text) {
		return model.RecallBundle{QueryID: query.QueryID}, model.NewError(model.CodeEmptyInput, "query text and item library must not be empty", query.QueryID)
	}
	bundle := model.RecallBundle{QueryID: query.QueryID, Routes: make([]model.RouteResult, 0, len(model.RouteOrder))}
	for _, route := range model.RouteOrder {
		result, err := idx.RecallRoute(query, route, plans[route])
		if err != nil {
			return model.RecallBundle{}, err
		}
		bundle.Routes = append(bundle.Routes, result)
	}
	bundle.Sort()
	return bundle, nil
}

func (idx *Index) lexical(query model.Query, depth int) []model.Candidate {
	queryTerms := tokenize(query.Text)
	querySet := make(map[string]struct{}, len(queryTerms))
	for _, term := range queryTerms {
		querySet[term] = struct{}{}
	}
	out := make([]model.Candidate, 0, depth)
	for i, item := range idx.items {
		matched := make([]string, 0)
		for _, term := range idx.titleTerms[i] {
			if _, ok := querySet[term]; ok {
				matched = append(matched, term)
			}
		}
		if len(matched) == 0 {
			continue
		}
		denom := len(queryTerms) + len(idx.titleTerms[i])
		score := 0.0
		if denom > 0 {
			score = 2.0 * float64(len(matched)) / float64(denom)
		}
		out = append(out, model.Candidate{
			ItemID: item.ItemID,
			Score:  score,
			Components: model.ScoreComponents{Lexical: &model.LexicalComponents{
				MatchedTerms:    matched,
				QueryTermCount:  len(queryTerms),
				TitleTermCount:  len(idx.titleTerms[i]),
				OverlapCount:    len(matched),
				DiceCoefficient: score,
			}},
		})
	}
	model.SortCandidates(out)
	if len(out) > depth {
		out = out[:depth]
	}
	return out
}

func (idx *Index) category(query model.Query, depth int) []model.Candidate {
	totalClicks := len(query.ClickedCategories)
	if totalClicks == 0 {
		return []model.Candidate{}
	}
	clickCounts := make(map[string]int, totalClicks)
	for _, category := range query.ClickedCategories {
		clickCounts[category]++
	}
	out := make([]model.Candidate, 0, depth)
	for _, item := range idx.items {
		clicks, ok := clickCounts[item.Category]
		if !ok || clicks == 0 {
			continue
		}
		itemCount := idx.categoryCounts[item.Category]
		affinity := float64(clicks) / float64(totalClicks)
		score := affinity * math.Log1p(float64(itemCount))
		out = append(out, model.Candidate{
			ItemID: item.ItemID,
			Score:  score,
			Components: model.ScoreComponents{Category: &model.CategoryComponents{
				MatchedCategories: []string{item.Category},
				ClickCount:        clicks,
				TotalClicks:       totalClicks,
				CategoryItemCount: itemCount,
				Affinity:          affinity,
			}},
		})
	}
	model.SortCandidates(out)
	if len(out) > depth {
		out = out[:depth]
	}
	return out
}

func (idx *Index) vector(query model.Query, depth int) []model.Candidate {
	queryVector := QueryVector(query, idx.vectorDim)
	queryNorm := norm(queryVector)
	if queryNorm == 0 {
		return []model.Candidate{}
	}
	out := make([]model.Candidate, 0, depth)
	for i, item := range idx.items {
		itemNorm := idx.itemNorms[i]
		score := 0.0
		if itemNorm > 0 {
			score = dot(queryVector, item.Vector) / (queryNorm * itemNorm)
			if score > 1 {
				score = 1
			}
			if score < -1 {
				score = -1
			}
		}
		if score <= 0 {
			continue
		}
		out = append(out, model.Candidate{
			ItemID: item.ItemID,
			Score:  score,
			Components: model.ScoreComponents{Vector: &model.VectorComponents{
				CosineSimilarity: score,
				QueryNorm:        queryNorm,
				ItemNorm:         itemNorm,
			}},
		})
	}
	model.SortCandidates(out)
	if len(out) > depth {
		out = out[:depth]
	}
	return out
}

func norm(vector []float64) float64 {
	sum := 0.0
	for _, value := range vector {
		sum += value * value
	}
	return math.Sqrt(sum)
}

func dot(a, b []float64) float64 {
	n := min(len(a), len(b))
	sum := 0.0
	for i := 0; i < n; i++ {
		sum += a[i] * b[i]
	}
	return sum
}
