package budget

import (
	"localsearch/multirecall/internal/model"
)

const (
	ReasonEnabled            = "enabled_within_quota_and_budget"
	ReasonConfiguredZero     = "configured_depth_zero"
	ReasonQuotaExhausted     = "route_evaluation_quota_exhausted"
	ReasonFairShareTrimmed   = "fair_share_trimmed_for_remaining_queries"
	ReasonRoundBudgetTrimmed = "per_round_time_budget_trimmed"
	ReasonGlobalExhausted    = "global_candidate_evaluation_budget_exhausted"
	ReasonRoundExhausted     = "per_round_time_budget_exhausted"
	ReasonNoRouteUnderBudget = "no_route_under_remaining_budget"
)

type Plan struct {
	Decisions   []model.BudgetDecision
	Unprocessed []model.UnprocessedQuery
	ByQuery     map[string]model.BudgetDecision
	Predicted   int
}

type workingRoute struct {
	route      string
	configured int
	depth      int
	reason     string
	eliminated string
}

type planner struct {
	config          model.Config
	itemCount       int
	remainingGlobal int
	remainingQuota  map[string]int
	roundCapacity   int
	roundPredicted  int
	roundQueries    int
	currentRound    int
	decisions       []model.BudgetDecision
	unprocessed     []model.UnprocessedQuery
}

func Build(config model.Config, itemCount int, queries []model.Query) Plan {
	config = config.Normalize()
	queries = model.NormalizeQueries(queries)
	p := &planner{
		config:          config,
		itemCount:       itemCount,
		remainingGlobal: config.Budget.GlobalCandidateEvaluations,
		remainingQuota:  make(map[string]int, len(model.RouteOrder)),
		roundCapacity:   roundCapacity(config),
		decisions:       make([]model.BudgetDecision, 0, len(queries)),
		unprocessed:     make([]model.UnprocessedQuery, 0),
	}
	for _, route := range model.RouteOrder {
		p.remainingQuota[route] = config.Routes.Get(route).EvaluationQuota
	}
	for i, query := range queries {
		if p.currentRound == 0 ||
			p.roundQueries >= config.Execution.MaxConcurrentQueries ||
			(p.roundCapacity > 0 && p.roundPredicted >= p.roundCapacity) {
			p.currentRound++
			p.roundPredicted = 0
			p.roundQueries = 0
		}
		p.decide(p.currentRound, query.QueryID, len(queries)-i)
		p.roundQueries++
	}
	return p.result()
}

func BuildForQueries(config model.Config, itemCount int, queries []model.Query) Plan {
	return Build(config, itemCount, queries)
}

func (p *planner) result() Plan {
	byQuery := make(map[string]model.BudgetDecision, len(p.decisions))
	predicted := 0
	for _, decision := range p.decisions {
		byQuery[decision.QueryID] = decision
		predicted += decision.PredictedEvaluations
	}
	return Plan{Decisions: p.decisions, Unprocessed: p.unprocessed, ByQuery: byQuery, Predicted: predicted}
}

func (p *planner) decide(round int, queryID string, remainingQueries int) {
	decision := model.BudgetDecision{
		Round:            round,
		QueryID:          queryID,
		Routes:           make([]model.RouteDecision, 0, len(model.RouteOrder)),
		BudgetAtDecision: p.remainingGlobal,
		RemainingQueries: remainingQueries,
	}
	routes := make([]workingRoute, 0, len(model.RouteOrder))
	for _, route := range model.RouteOrder {
		configured := p.config.Routes.Get(route).CandidateDepth
		capped := configured
		if capped > p.itemCount {
			capped = p.itemCount
		}
		work := workingRoute{route: route, configured: configured, depth: capped}
		switch {
		case capped <= 0:
			work.depth = 0
			work.eliminated = ReasonConfiguredZero
		case p.remainingQuota[route] <= 0:
			work.depth = 0
			work.eliminated = ReasonQuotaExhausted
		case work.depth > p.remainingQuota[route]:
			work.depth = p.remainingQuota[route]
			work.reason = "trimmed_to_route_quota"
		}
		routes = append(routes, work)
	}

	total := routeTotal(routes)
	fairShare := 0
	if p.remainingGlobal > 0 && remainingQueries > 0 {
		fairShare = p.remainingGlobal / remainingQueries
		if fairShare == 0 {
			fairShare = 1
		}
	}
	roundRemaining := p.roundCapacity - p.roundPredicted
	target := total
	trimReason := ""
	if fairShare < target {
		target = fairShare
		trimReason = ReasonFairShareTrimmed
	}
	if roundRemaining < target {
		target = roundRemaining
		trimReason = ReasonRoundBudgetTrimmed
	}
	if target < 0 {
		target = 0
	}
	if target < total {
		total = trimToTarget(routes, target)
		for i := range routes {
			if routes[i].depth == 0 && routes[i].eliminated == "" {
				routes[i].eliminated = trimReason
			}
		}
	}

	predicted := 0
	for _, work := range routes {
		rd := model.RouteDecision{
			Route:                work.route,
			ConfiguredDepth:      work.configured,
			AppliedDepth:         work.depth,
			PredictedEvaluations: work.depth,
			RemainingRouteQuota:  p.remainingQuota[work.route],
			Reason:               work.reason,
			EliminatedReason:     work.eliminated,
		}
		if work.depth > 0 {
			rd.Enabled = true
			if rd.Reason == "" {
				rd.Reason = ReasonEnabled
			}
		} else {
			rd.Enabled = false
			if rd.Reason == "" {
				rd.Reason = "route_disabled"
			}
			if rd.EliminatedReason == "" {
				rd.EliminatedReason = ReasonNoRouteUnderBudget
			}
		}
		predicted += work.depth
		decision.Routes = append(decision.Routes, rd)
	}
	decision.PredictedEvaluations = predicted
	decision.Processed = predicted > 0
	switch {
	case decision.Processed:
		decision.Reason = "adaptive_route_plan_selected"
	case p.remainingGlobal <= 0:
		decision.Reason = ReasonGlobalExhausted
	case roundRemaining <= 0:
		decision.Reason = ReasonRoundExhausted
	default:
		decision.Reason = ReasonNoRouteUnderBudget
	}

	p.remainingGlobal -= predicted
	p.roundPredicted += predicted
	for _, route := range decision.Routes {
		p.remainingQuota[route.Route] -= route.PredictedEvaluations
	}
	if !decision.Processed {
		p.unprocessed = append(p.unprocessed, model.UnprocessedQuery{QueryID: queryID, Reason: decision.Reason})
	}
	p.decisions = append(p.decisions, decision)
}

func routeTotal(routes []workingRoute) int {
	total := 0
	for _, route := range routes {
		total += route.depth
	}
	return total
}

func trimToTarget(routes []workingRoute, target int) int {
	total := routeTotal(routes)
	for total > target {
		selected := -1
		for i := range routes {
			if routes[i].depth <= 0 {
				continue
			}
			if selected == -1 || routes[i].depth > routes[selected].depth {
				selected = i
			}
		}
		if selected == -1 {
			break
		}
		routes[selected].depth--
		total--
		if routes[selected].reason == "" {
			routes[selected].reason = ReasonFairShareTrimmed
		}
	}
	return total
}

func roundCapacity(config model.Config) int {
	if config.Budget.PerRoundTimeBudgetMS <= 0 || config.Budget.EstimatedCandidateMicros <= 0 {
		return 0
	}
	capacity := int(float64(config.Budget.PerRoundTimeBudgetMS) * 1000.0 / config.Budget.EstimatedCandidateMicros)
	if capacity < 0 {
		return 0
	}
	return capacity
}
