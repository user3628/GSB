package recall

import (
	"math"

	"multirecall/internal/configx"
)

func routeNames() []string { return []string{"lexical", "category", "vector"} }

func depthOf(d configx.RouteDepths, route string) int {
	switch route {
	case "lexical":
		return d.Lexical
	case "category":
		return d.Category
	default:
		return d.Vector
	}
}

func withDepth(d configx.RouteDepths, route string, v int) configx.RouteDepths {
	switch route {
	case "lexical":
		d.Lexical = v
	case "category":
		d.Category = v
	default:
		d.Vector = v
	}
	return d
}

func AdjudicateBudget(cfg configx.Snapshot, queryCount int) BudgetPlan {
	if queryCount < 0 {
		queryCount = 0
	}
	concurrency := cfg.Budget.QueryConcurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	costs := cfg.Budget.RouteCostMicroseconds
	if costs == nil {
		costs = map[string]int64{}
	}

	plan := BudgetPlan{
		QueryCount:            queryCount,
		GlobalLimit:           cfg.Budget.GlobalEvaluationLimit,
		EstimatedTimeBudgetUS: cfg.Budget.RoundTimeBudgetMS * 1000,
		Concurrency:           concurrency,
	}
	depths := cfg.CandidateDepth
	permilles := map[string]int{"lexical": cfg.PermilleBudget.Lexical, "category": cfg.PermilleBudget.Category, "vector": cfg.PermilleBudget.Vector}

	for _, route := range routeNames() {
		requested := depthOf(cfg.CandidateDepth, route)
		if requested < 0 {
			requested = 0
		}
		permille := permilles[route]
		if permille < 0 {
			permille = 0
		}
		if permille > 1000 {
			permille = 1000
		}
		total := 0
		permilleDepth := 0
		if queryCount > 0 {
			total = (queryCount*requested*permille + 999) / 1000
			permilleDepth = total / queryCount
		}
		effective := requested
		if permilleDepth < effective {
			effective = permilleDepth
			plan.Reductions = append(plan.Reductions, BudgetReduction{
				Route: route, Kind: "permille_budget", FromDepth: requested, ToDepth: effective,
				Eliminated: effective == 0,
				Reason:     "floor(ceil(query_count*depth*permille/1000)/query_count) lowers uniform per-query depth",
			})
		}
		depths = withDepth(depths, route, effective)
		evalCount := queryCount * effective
		costUS := int64(evalCount) * costs[route]
		plan.Routes = append(plan.Routes, RouteBudget{
			Route: route, RequestedDepth: requested, Permille: permille,
			PermilleEvaluationTotal: total, PermilleDepth: permilleDepth,
			EffectiveDepth: effective, PlannedEvaluations: evalCount,
			EstimatedMicroseconds: costUS, Enabled: effective > 0,
		})
		plan.PlannedEvaluations += evalCount
		plan.EstimatedTimeUS += costUS
	}
	plan.EstimatedTimeUS = int64(math.Ceil(float64(plan.EstimatedTimeUS) / float64(concurrency)))

	order := cfg.Budget.ReductionOrder
	if len(order) == 0 {
		order = []string{"vector", "category", "lexical"}
	}

	if plan.GlobalLimit >= 0 && plan.PlannedEvaluations > plan.GlobalLimit && queryCount > 0 {
		excess := plan.PlannedEvaluations - plan.GlobalLimit
		for _, route := range order {
			if excess <= 0 {
				break
			}
			current := depthOf(depths, route)
			if current <= 0 {
				continue
			}
			cutDepth := int(math.Ceil(float64(excess) / float64(queryCount)))
			if cutDepth > current {
				cutDepth = current
			}
			from := current
			to := current - cutDepth
			depths = withDepth(depths, route, to)
			plan.PlannedEvaluations -= cutDepth * queryCount
			excess -= cutDepth * queryCount
			plan.Reductions = append(plan.Reductions, BudgetReduction{
				Route: route, Kind: "global_evaluation_budget", FromDepth: from, ToDepth: to,
				Eliminated: to == 0,
				Reason:     "deterministic reduction order protects lexical first; global candidate evaluation budget exceeded",
			})
		}
	}

	if plan.EstimatedTimeBudgetUS > 0 && plan.EstimatedTimeUS > plan.EstimatedTimeBudgetUS && queryCount > 0 {
		for _, route := range order {
			if plan.EstimatedTimeUS <= plan.EstimatedTimeBudgetUS {
				break
			}
			current := depthOf(depths, route)
			if current <= 0 {
				continue
			}
			costPerDepth := costs[route] * int64(queryCount) / int64(concurrency)
			if costPerDepth <= 0 {
				costPerDepth = 1
			}
			overUS := plan.EstimatedTimeUS - plan.EstimatedTimeBudgetUS
			cutDepth := int(math.Ceil(float64(overUS) / float64(costPerDepth)))
			if cutDepth > current {
				cutDepth = current
			}
			from := current
			to := current - cutDepth
			depths = withDepth(depths, route, to)
			removedEval := cutDepth * queryCount
			plan.PlannedEvaluations -= removedEval
			removedTime := int64(math.Ceil(float64(removedEval) * float64(costs[route]) / float64(concurrency)))
			plan.EstimatedTimeUS -= removedTime
			plan.Reductions = append(plan.Reductions, BudgetReduction{
				Route: route, Kind: "round_time_budget", FromDepth: from, ToDepth: to,
				Eliminated: to == 0,
				Reason:     "deterministic cost model exceeds per-round time budget; cancellation is only checked at query boundaries",
			})
		}
	}

	for i := range plan.Routes {
		route := plan.Routes[i].Route
		effective := depthOf(depths, route)
		plan.Routes[i].EffectiveDepth = effective
		plan.Routes[i].Enabled = effective > 0
		plan.Routes[i].PlannedEvaluations = queryCount * effective
		plan.Routes[i].EstimatedMicroseconds = int64(queryCount*effective) * costs[route]
	}
	plan.ConsumedEvaluations = plan.PlannedEvaluations
	return plan
}

func EffectiveDepths(plan BudgetPlan) configx.RouteDepths {
	var out configx.RouteDepths
	for _, rb := range plan.Routes {
		switch rb.Route {
		case "lexical":
			out.Lexical = rb.EffectiveDepth
		case "category":
			out.Category = rb.EffectiveDepth
		case "vector":
			out.Vector = rb.EffectiveDepth
		}
	}
	return out
}

func ReducedConfig(cfg configx.Snapshot, plan BudgetPlan) configx.Snapshot {
	next := cfg
	next.CandidateDepth = EffectiveDepths(plan)
	next.PermilleBudget = configx.PermilleBudgets{Lexical: 1000, Category: 1000, Vector: 1000}
	next.Budget.GlobalEvaluationLimit = 1_000_000
	next.Budget.RoundTimeBudgetMS = 60_000
	configx.RecalcDigest(&next)
	return next
}
