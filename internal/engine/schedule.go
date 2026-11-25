package engine

import (
	"fmt"
	"sort"

	"queryrewrite/internal/spec"
)

// Stage4 schedules a turn under three constraints: strategy per-mille budget,
// per-turn evaluation budget and cache capacity. Every give-up is recorded with
// a reason, and the ledger makes consumption fully recomputable afterwards.
func Stage4(sessionID string, turn int, cands []spec.Candidate, hits []spec.HitResult,
	adjs []spec.AdjResult, p spec.Policy, cache *Cache) spec.BudgetLedger {

	hitByID := map[string]spec.HitResult{}
	for _, h := range hits {
		hitByID[h.CandidateID] = h
	}
	adjByID := map[string]spec.AdjResult{}
	for _, a := range adjs {
		adjByID[a.CandidateID] = a
	}

	ledger := spec.BudgetLedger{
		Turn:           turn,
		EvalBudget:     p.EvalBudgetPerTurn,
		PerMilleBudget: cloneMille(p.StrategyPerMille),
		PerMilleUsed:   map[string]int{},
		CapacityTotal:  p.CacheCapacity,
		Accepted:       []string{},
		Skips:          []spec.Skip{},
		DegradeReasons: []string{},
	}
	for k := range p.StrategyPerMille {
		ledger.PerMilleUsed[k] = 0
	}
	// Per-turn accept cap per strategy = perMille/1000 * eval budget, rounded
	// deterministically. This makes the per-mille allocation a real constraint.
	acceptCap := map[string]int{}
	for k, mille := range ledger.PerMilleBudget {
		acceptCap[k] = mille * p.EvalBudgetPerTurn / 1000
	}

	// Explicit per-mille sum validation: anything != 1000 is reported and the
	// turn degrades to proportional normalized budgets (never silent truncation).
	if sumMille(p.StrategyPerMille) != 1000 {
		ledger.Degraded = true
		ledger.DegradeReasons = append(ledger.DegradeReasons,
			fmt.Sprintf("%s: per_mille sum=%d != 1000; proportional rebalance applied",
				spec.CodeBudgetMille, sumMille(p.StrategyPerMille)))
		ledger.PerMilleBudget = rebalanceMille(p.StrategyPerMille)
	}

	// Deterministic priority: benefit desc, failure upper asc, candidate id asc.
	order := append([]spec.Candidate(nil), cands...)
	sort.SliceStable(order, func(i, j int) bool {
		ai, aj := adjByID[order[i].ID], adjByID[order[j].ID]
		if ai.BenefitScore != aj.BenefitScore {
			return ai.BenefitScore > aj.BenefitScore
		}
		if ai.FailureUpper != aj.FailureUpper {
			return ai.FailureUpper < aj.FailureUpper
		}
		return order[i].ID < order[j].ID
	})

	evalUsed := 0
	for _, cand := range order {
		// Evaluation budget counts every assessed candidate.
		if evalUsed >= p.EvalBudgetPerTurn {
			ledger.Skips = append(ledger.Skips, spec.Skip{
				CandidateID: cand.ID, Strategy: cand.Strategy,
				Reason: fmt.Sprintf("%s: eval budget exhausted %d/%d",
					spec.CodeBudgetExceeded, evalUsed, p.EvalBudgetPerTurn),
			})
			ledger.Degraded = true
			ledger.DegradeReasons = append(ledger.DegradeReasons,
				spec.CodeBudgetMille.String()+"-turn-eval-exhausted")
			continue
		}
		evalUsed++

		adj := adjByID[cand.ID]
		if adj.NumericError != nil || adj.Decision != "ACCEPT" {
			reason := "stage3 decision REJECT"
			if adj.NumericError != nil {
				reason = string(adj.NumericError.Code) + ":" + adj.NumericError.Message
			}
			ledger.Skips = append(ledger.Skips, spec.Skip{
				CandidateID: cand.ID, Strategy: cand.Strategy, Reason: reason,
			})
			continue
		}

		// Strategy per-mille budget: each accept costs 1 per-mille point.
		used := ledger.PerMilleUsed[cand.Strategy]
		allow := acceptCap[cand.Strategy]
		if used+1 > allow {
			ledger.Skips = append(ledger.Skips, spec.Skip{
				CandidateID: cand.ID, Strategy: cand.Strategy,
				Reason: fmt.Sprintf("%s: strategy %s per-mille used %d >= budget %d",
					spec.CodeBudgetExceeded, cand.Strategy, used, allow),
			})
			ledger.Degraded = true
			continue
		}

		// Capacity constraint with deterministic eviction. Stage 2 already made
		// the slot tentative; here we evict a durable victim if needed and
		// promote the tentative slot to durable.
		hr := hitByID[cand.ID]
		if hr.Cacheable {
			fp := CandidateFingerprint(hr)
			if sl, live := cache.Lookup(fp); live {
				// Exact or already-present slot: refresh LRU age only.
				sl.LastUsedTurn = turn
			} else {
				if cache.durableSize() >= p.CacheCapacity {
					victim := cache.evictionVictim(turn)
					if victim == nil || !cache.ApplyEviction(victim.SlotID) {
						ledger.Skips = append(ledger.Skips, spec.Skip{
							CandidateID: cand.ID, Strategy: cand.Strategy,
							Reason: fmt.Sprintf("%s: cache capacity %d reached and no evictable slot",
								spec.CodeCacheFull, p.CacheCapacity),
						})
						ledger.Degraded = true
						continue
					}
				}
				if cache.durableSize() < p.CacheCapacity {
					cache.Put(fp, len(hr.Blocks), sessionID, turn, turn)
				}
			}
		}

		ledger.PerMilleUsed[cand.Strategy] = used + 1
		ledger.Accepted = append(ledger.Accepted, cand.ID)
	}
	ledger.EvalUsed = evalUsed
	ledger.CapacityUsed = cache.durableSize()

	// Defensive explicit report if cumulative consumption ever exceeds budget.
	for strat, used := range ledger.PerMilleUsed {
		if used > ledger.PerMilleBudget[strat] {
			ledger.Degraded = true
			ledger.DegradeReasons = append(ledger.DegradeReasons,
				fmt.Sprintf("%s: cumulative strategy %s used %d > budget %d",
					spec.CodeBudgetExceeded, strat, used, ledger.PerMilleBudget[strat]))
		}
	}
	return ledger
}

func sumMille(m map[string]int) int {
	s := 0
	for _, v := range m {
		s += v
	}
	return s
}

func cloneMille(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// rebalanceMille scales budgets so they sum to 1000 deterministically,
// distributing the rounding remainder by sorted strategy key.
func rebalanceMille(m map[string]int) map[string]int {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	total := sumMille(m)
	out := map[string]int{}
	if total <= 0 {
		share := 1000 / len(keys)
		for i, k := range keys {
			out[k] = share
			if i == len(keys)-1 {
				out[k] += 1000 - share*len(keys)
			}
		}
		return out
	}
	allocated := 0
	scaled := map[string]int{}
	for _, k := range keys {
		v := m[k] * 1000 / total
		scaled[k] = v
		allocated += v
	}
	for i := 0; allocated < 1000; i++ {
		k := keys[i%len(keys)]
		scaled[k]++
		allocated++
	}
	for k, v := range scaled {
		out[k] = v
	}
	return out
}
