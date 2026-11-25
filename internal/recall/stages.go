package recall

import (
	"multirecall/internal/model"
	"multirecall/internal/textnorm"
)

func (e *Engine) ApplyStage(stage string, report *QueryReport, globalPlan *BudgetPlan) *model.APIError {
	if report.Error != nil {
		return report.Error
	}
	q := report.Query
	switch stage {
	case StageBudget:
		if globalPlan != nil {
			report.Budget = globalPlan
		} else if report.Budget == nil {
			plan := AdjudicateBudget(e.Config, len(e.Dataset.Queries))
			report.Budget = &plan
		}
		return nil
	case StageNormalize:
		norm := textnorm.Tokenize(q.RawQuery)
		report.Normalization = &norm
		if norm.Normalized == "" || len(norm.UniqueTerms) == 0 || e.Lex == nil || e.Lex.IndexedTerms == 0 {
			report.Error = &model.APIError{
				Code: model.CodeEmptyQuery, QueryID: q.QueryID,
				Message: "query normalizes to zero terms or lexical inverted index is empty",
			}
			return report.Error
		}
		return nil
	case StageLexical:
		depths := EffectiveDepths(*report.Budget)
		result, err := e.Lex.Search(q, depths.Lexical)
		if err != nil {
			report.Error = err
			return err
		}
		report.Lexical = &result
		return nil
	case StageCategory:
		depths := EffectiveDepths(*report.Budget)
		result := SearchCategory(q, e.Dataset.Items, depths.Category, depths.Category)
		report.Category = &result
		return nil
	case StageVector:
		depths := EffectiveDepths(*report.Budget)
		result, err := SearchVector(q, e.Dataset.Items, e.Dataset.VectorDim, depths.Vector, depths.Vector)
		if err != nil {
			err.QueryID = q.QueryID
			report.Error = err
			return err
		}
		report.Vector = &result
		return nil
	case StageFuse:
		routes := reportRoutes(report)
		hits, trunc, err := mergeFusion(q, e.Config, routes)
		if err != nil {
			report.Error = err
			return err
		}
		lookups := candidateLookup(routes)
		computeFusionBounds(e.Config, routes, lookups, hits)
		report.Hits = hits
		report.Truncation = &trunc
		return nil
	case StageAudit:
		routes := reportRoutes(report)
		lookups := candidateLookup(routes)
		audit, err := auditNumeric(e, q, e.Config, routes, lookups, report.Hits)
		if err != nil {
			report.Error = err
			return err
		}
		report.Audit = audit
		if report.Truncation != nil {
			report.Hits = ApplyTruncation(report.Hits, *report.Truncation)
		}
		depths := EffectiveDepths(*report.Budget)
		var lexN, catN, vecN int
		if report.Lexical != nil {
			lexN = report.Lexical.Returned
		}
		if report.Category != nil {
			catN = report.Category.Returned
		}
		if report.Vector != nil {
			vecN = report.Vector.Returned
		}
		merged := 0
		if report.Truncation != nil {
			merged = report.Truncation.MergedCandidates
		}
		costs := e.Config.Budget.RouteCostMicroseconds
		report.Metrics = QueryMetrics{
			CandidateEvaluations: depths.Lexical + depths.Category + depths.Vector,
			LexicalReturned:      lexN,
			CategoryReturned:     catN,
			VectorReturned:       vecN,
			MergedItems:          merged,
			EstimatedMicroseconds: int64(depths.Lexical)*costs["lexical"] +
				int64(depths.Category)*costs["category"] +
				int64(depths.Vector)*costs["vector"],
		}
		return nil
	default:
		err := model.NewError(model.CodeInvalidRequest, "unknown pipeline stage: "+stage)
		report.Error = err
		return err
	}
}

func reportRoutes(report *QueryReport) map[string]RouteResult {
	routes := map[string]RouteResult{}
	if report.Lexical != nil {
		routes["lexical"] = *report.Lexical
	}
	if report.Category != nil {
		routes["category"] = *report.Category
	}
	if report.Vector != nil {
		routes["vector"] = *report.Vector
	}
	return routes
}
