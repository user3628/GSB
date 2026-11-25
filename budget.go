package feedbackstream

import "sort"

// PlanRound evaluates candidates in the required deterministic order:
// partition ID, window start and then batch ID as the stable tie breaker.
// PartitionConcurrency bounds distinct active partitions. Requirement 7 makes
// each partition a serial single-writer, so at most one new batch per
// partition is admitted in one plan.
func PlanRound(roundID string, candidates []Candidate, policy *PolicySnapshot, activeByPartition map[int]int, alreadyConsumedMS int64) *RoundPlan {
	ordered := append([]Candidate(nil), candidates...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].PartitionID != ordered[j].PartitionID {
			return ordered[i].PartitionID < ordered[j].PartitionID
		}
		if ordered[i].WindowStart != ordered[j].WindowStart {
			return ordered[i].WindowStart < ordered[j].WindowStart
		}
		return ordered[i].BatchID < ordered[j].BatchID
	})

	plan := &RoundPlan{
		RoundID: roundID, BudgetMS: policy.RoundTimeBudgetMS,
		PartitionConcurrency: policy.PartitionConcurrency,
		Admissions:           []AdmissionDecision{}, Shed: []ShedDecision{},
		ConsumedBudgetMS: alreadyConsumedMS,
	}
	admittedByPartition := map[int]bool{}
	activePartitions := 0
	for _, active := range activeByPartition {
		if active > 0 {
			activePartitions++
		}
	}
	shedRank := 0
	for _, candidate := range ordered {
		cost := candidate.CostMS
		if cost == 0 {
			cost = EstimateCost(candidate, policy.CostModel)
		}
		consumedBefore := plan.ConsumedBudgetMS
		remaining := policy.RoundTimeBudgetMS - consumedBefore
		partitionBusy := activeByPartition[candidate.PartitionID] > 0 || admittedByPartition[candidate.PartitionID]
		globalPartitionsUsed := activePartitions + len(admittedByPartition)
		decision := AdmissionDecision{
			BatchID: candidate.BatchID, PartitionID: candidate.PartitionID,
			WindowStartMS: candidate.WindowStart, CostMS: cost,
		}

		if partitionBusy {
			shedRank++
			reason := "partition single-writer slot is already active or admitted"
			decision.Admitted = false
			decision.Reason = reason
			plan.Admissions = append(plan.Admissions, decision)
			plan.Shed = append(plan.Shed, ShedDecision{
				BatchID: candidate.BatchID, PartitionID: candidate.PartitionID,
				WindowStartMS: candidate.WindowStart, Rank: shedRank,
				EstimatedCostMS: cost, ConsumedBudgetBefore: consumedBefore,
				RemainingBudgetMS: remaining,
				ReasonCode:        "PARTITION_CONCURRENCY_LIMIT", Reason: reason,
			})
			continue
		}
		if globalPartitionsUsed >= policy.PartitionConcurrency {
			shedRank++
			reason := "global partition concurrency limit is reached"
			decision.Admitted = false
			decision.Reason = reason
			plan.Admissions = append(plan.Admissions, decision)
			plan.Shed = append(plan.Shed, ShedDecision{
				BatchID: candidate.BatchID, PartitionID: candidate.PartitionID,
				WindowStartMS: candidate.WindowStart, Rank: shedRank,
				EstimatedCostMS: cost, ConsumedBudgetBefore: consumedBefore,
				RemainingBudgetMS: remaining,
				ReasonCode:        "PARTITION_CONCURRENCY_LIMIT", Reason: reason,
			})
			continue
		}
		if cost > remaining {
			shedRank++
			reason := "estimated cost exceeds remaining round time budget"
			decision.Admitted = false
			decision.Reason = reason
			plan.Admissions = append(plan.Admissions, decision)
			plan.Shed = append(plan.Shed, ShedDecision{
				BatchID: candidate.BatchID, PartitionID: candidate.PartitionID,
				WindowStartMS: candidate.WindowStart, Rank: shedRank,
				EstimatedCostMS: cost, ConsumedBudgetBefore: consumedBefore,
				RemainingBudgetMS: remaining,
				ReasonCode:        "ROUND_TIME_BUDGET_EXHAUSTED", Reason: reason,
			})
			continue
		}

		decision.Admitted = true
		plan.Admissions = append(plan.Admissions, decision)
		admittedByPartition[candidate.PartitionID] = true
		plan.ConsumedBudgetMS += cost
	}
	plan.RemainingBudgetMS = policy.RoundTimeBudgetMS - plan.ConsumedBudgetMS
	if plan.RemainingBudgetMS < 0 {
		plan.RemainingBudgetMS = 0
	}
	return plan
}
