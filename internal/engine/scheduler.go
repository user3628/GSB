package engine

import (
	"context"
	"fmt"
	"sort"
	"time"

	"driftconverge/internal/apperr"
	"driftconverge/internal/fixed"
	"driftconverge/internal/plan"
)

type scheduleState struct {
	accepted  map[int]bool
	skipped   map[int]SkippedBatch
	decisions []BudgetDecision
	spent     int64
	nextSkip  int
}

func (s *Session) executePlannedRound(ctx context.Context, roundID, endpoint string) (*ExecutionReport, error) {
	start := time.Now()
	policy := s.Samples.Policy
	costPlan := s.Analysis.Plan
	batchReport := s.Analysis.Batches
	report := &ExecutionReport{
		RoundID: roundID, SnapshotVersion: s.Analysis.SnapshotVersion, BatchVersionID: s.Analysis.BatchVersionID,
		Endpoint: endpoint, State: "running", StartedAt: start.UTC().Format(time.RFC3339Nano),
		ExecutedBatches: []BatchExecutionResult{}, SkippedBatches: []SkippedBatch{}, SkippedOrder: []string{},
	}
	limitMicros, err := fixed.MicrosFromMs(policy.RoundTimeBudgetMs)
	if err != nil {
		return report, apperr.New(apperr.InvalidArgument, "round time budget: %v", err)
	}
	toleranceComparison, err := fixed.CompareTimeBudget(0, limitMicros, policy.TimeBudgetRelativeTolerancePPM, 0)
	if err != nil {
		return report, apperr.New(apperr.InvalidArgument, "time budget tolerance: %v", err)
	}
	report.Budget = BudgetReport{
		RoundTimeBudgetMs: policy.RoundTimeBudgetMs, RoundTimeBudgetMicros: limitMicros,
		TolerancePPM: policy.TimeBudgetRelativeTolerancePPM, Decisions: []BudgetDecision{},
		OrderingRule: "dependency_ready -> severity_rank_desc -> batch_index_asc",
	}
	if batchReport.ErrorCode != "" {
		report.State = "failed"
		report.ErrorCode = string(apperr.DependencyCycle)
		report.DurationMicros = time.Since(start).Microseconds()
		return report, apperr.WithDetails(apperr.DependencyCycle, batchReport.Cycles, "dependency cycle prevents safe writeback")
	}
	schedule, err := s.buildSchedule(costPlan, limitMicros)
	if err != nil {
		report.State = "failed"
		report.ErrorCode = string(apperr.CodeOf(err))
		report.DurationMicros = time.Since(start).Microseconds()
		return report, err
	}
	report.Budget.Decisions = schedule.decisions
	report.Budget.EstimatedAcceptedCost = fixed.FormatMicros(schedule.spent) + "ms"
	remaining := limitMicros - schedule.spent
	if remaining < 0 {
		remaining = 0
	}
	report.Budget.EstimatedRemaining = fixed.FormatMicros(remaining) + "ms"
	for _, skipped := range schedule.skipped {
		report.SkippedBatches = append(report.SkippedBatches, skipped)
	}
	sortSkipped(report.SkippedBatches)
	report.SkippedOrder = skippedIDs(report.SkippedBatches)
	for _, skipped := range report.SkippedBatches {
		if skipped.Order > schedule.nextSkip {
			schedule.nextSkip = skipped.Order
		}
	}

	batchByIndex := make(map[int]plan.BatchPlan, len(costPlan.Batches))
	dependents := make(map[int][]int)
	ready := []int{}
	status := make(map[int]string, len(costPlan.Batches))
	for _, bp := range costPlan.Batches {
		batchByIndex[bp.Index] = bp
		status[bp.Index] = "unresolved"
	}
	for _, bp := range costPlan.Batches {
		for _, dep := range bp.DependsOnBatchIndexes {
			if _, ok := batchByIndex[dep]; !ok {
				return report, apperr.New(apperr.PersistenceFailed, "batch %d depends on unknown batch %d", bp.Index, dep)
			}
			dependents[dep] = append(dependents[dep], bp.Index)
		}
	}
	for idx, bp := range batchByIndex {
		if len(bp.DependsOnBatchIndexes) == 0 && schedule.accepted[idx] {
			ready = append(ready, idx)
			status[idx] = "ready"
		}
	}
	sortReady(ready, batchByIndex)

	timeoutMicros := limitMicros + toleranceComparison.ToleranceMicros
	ctx = context.WithValue(ctx, endpointContextKey{}, endpoint)
	roundCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutMicros)*time.Microsecond)
	defer cancel()
	doneCh := make(chan BatchExecutionResult, policy.MaxParallelBatches)
	running := 0
	queueFull := false
	wallExhausted := false
	completed := make(map[int]bool)
	doneChValue := doneCh
	var doneCtx <-chan struct{} = roundCtx.Done()
	for {
		sortReady(ready, batchByIndex)
		if !queueFull && !wallExhausted {
			for running < policy.MaxParallelBatches && len(ready) > 0 {
				if roundCtx.Err() != nil {
					wallExhausted = true
					break
				}
				idx := ready[0]
				ready = ready[1:]
				status[idx] = "running"
				running++
				bp := batchByIndex[idx]
				go func() {
					doneCh <- s.executeBatch(roundCtx, roundID, endpoint, bp)
				}()
			}
			if running >= policy.MaxParallelBatches && len(ready) > policy.WriteQueueCapacity {
				queueFull = true
			}
		}
		if running == 0 {
			if len(ready) == 0 || queueFull || wallExhausted {
				break
			}
			continue
		}
		select {
		case result := <-doneChValue:
			running--
			completed[result.Index] = true
			status[result.Index] = "done"
			report.ExecutedBatches = append(report.ExecutedBatches, result)
			for _, dependent := range dependents[result.Index] {
				if !schedule.accepted[dependent] || status[dependent] != "unresolved" {
					continue
				}
				allDone := true
				for _, dep := range batchByIndex[dependent].DependsOnBatchIndexes {
					if !completed[dep] {
						allDone = false
						break
					}
				}
				if allDone {
					status[dependent] = "ready"
					ready = append(ready, dependent)
				}
			}
		case <-doneCtx:
			wallExhausted = true
			doneCtx = nil
		}
	}
	// If the wait loop exits while work is still in flight (only possible for a
	// queue/deadline stop), drain already-started batches so their conclusions
	// remain durable and are not recomputed.
	for running > 0 {
		result := <-doneChValue
		running--
		completed[result.Index] = true
		report.ExecutedBatches = append(report.ExecutedBatches, result)
	}
	sort.Slice(report.ExecutedBatches, func(i, j int) bool { return report.ExecutedBatches[i].Index < report.ExecutedBatches[j].Index })

	var finishErr error
	if queueFull {
		finishErr = apperr.WithDetails(apperr.WriteQueueFull, map[string]any{
			"maxParallelBatches": policy.MaxParallelBatches, "queueCapacity": policy.WriteQueueCapacity,
		}, "write queue is full; no additional batch was admitted")
	} else if wallExhausted {
		finishErr = apperr.WithDetails(apperr.RoundBudgetExhausted, map[string]any{
			"roundTimeBudgetMicros": limitMicros, "toleranceMicros": toleranceComparison.ToleranceMicros,
		}, "round wall-time budget was exhausted before all admitted batches started")
	}
	for _, bp := range costPlan.Batches {
		if !schedule.accepted[bp.Index] || completed[bp.Index] {
			continue
		}
		reason := "write_queue_full"
		detail := "batch remained in the bounded waiting queue when capacity was exhausted"
		if queueFull && wallExhausted {
			reason = "write_queue_full"
		} else if wallExhausted {
			reason = "round_wall_budget_exhausted"
			detail = "round deadline expired before this batch was dispatched"
		}
		schedule.nextSkip++
		report.SkippedBatches = append(report.SkippedBatches, SkippedBatch{
			BatchID: bp.BatchID, Index: bp.Index, Order: schedule.nextSkip, Reason: reason, Detail: detail,
		})
	}
	sortSkipped(report.SkippedBatches)
	report.SkippedOrder = skippedIDs(report.SkippedBatches)

	for _, batch := range report.ExecutedBatches {
		for _, object := range batch.Objects {
			switch object.State {
			case "converged":
				report.ConvergedObjects++
			case "unresolved":
				report.UnresolvedObjects++
			default:
				report.FailedObjects++
			}
		}
	}
	report.DurationMicros = time.Since(start).Microseconds()
	if finishErr != nil {
		report.State = "failed"
		report.ErrorCode = string(apperr.CodeOf(finishErr))
		return report, finishErr
	}
	if report.FailedObjects > 0 {
		report.State = "failed"
		report.ErrorCode = string(overallFailureCode(report))
		return report, apperr.WithDetails(apperr.Code(report.ErrorCode), map[string]int{
			"failedObjects": report.FailedObjects, "unresolvedObjects": report.UnresolvedObjects,
		}, "writeback failed for %d object(s)", report.FailedObjects)
	}
	if report.UnresolvedObjects > 0 || len(report.SkippedBatches) > 0 {
		report.State = "partial"
		report.ErrorCode = string(apperr.ConvergenceIncomplete)
		return report, apperr.WithDetails(apperr.ConvergenceIncomplete, map[string]int{
			"unresolvedObjects": report.UnresolvedObjects, "skippedBatches": len(report.SkippedBatches),
		}, "round completed without eliminating every drift")
	}
	report.State = "completed"
	return report, nil
}

func (s *Session) buildSchedule(costPlan *plan.Report, limitMicros int64) (*scheduleState, error) {
	state := &scheduleState{accepted: make(map[int]bool), skipped: make(map[int]SkippedBatch), decisions: []BudgetDecision{}, nextSkip: 0}
	batchByIndex := make(map[int]plan.BatchPlan, len(costPlan.Batches))
	dependents := make(map[int][]int)
	status := make(map[int]string, len(costPlan.Batches))
	unresolvedDeps := make(map[int]int, len(costPlan.Batches))
	for _, bp := range costPlan.Batches {
		batchByIndex[bp.Index] = bp
		status[bp.Index] = "unresolved"
		unresolvedDeps[bp.Index] = len(bp.DependsOnBatchIndexes)
	}
	for _, bp := range costPlan.Batches {
		for _, dep := range bp.DependsOnBatchIndexes {
			if _, ok := batchByIndex[dep]; !ok {
				return nil, apperr.New(apperr.PersistenceFailed, "batch %d depends on unknown batch %d", bp.Index, dep)
			}
			dependents[dep] = append(dependents[dep], bp.Index)
		}
	}
	ready := []int{}
	for idx, bp := range batchByIndex {
		if len(bp.DependsOnBatchIndexes) == 0 {
			ready = append(ready, idx)
			status[idx] = "ready"
		}
	}
	resolve := func(idx int) {
		for _, dependent := range dependents[idx] {
			if status[dependent] != "unresolved" {
				continue
			}
			unresolvedDeps[dependent]--
			if unresolvedDeps[dependent] == 0 {
				status[dependent] = "ready"
				ready = append(ready, dependent)
			}
		}
	}
	decisionOrder := 0
	for len(ready) > 0 {
		sortReady(ready, batchByIndex)
		idx := ready[0]
		ready = ready[1:]
		bp := batchByIndex[idx]
		if status[idx] != "ready" {
			continue
		}
		dependencySkipped := false
		for _, dep := range bp.DependsOnBatchIndexes {
			if !state.accepted[dep] {
				dependencySkipped = true
				break
			}
		}
		if dependencySkipped {
			state.nextSkip++
			state.skipped[idx] = SkippedBatch{
				BatchID: bp.BatchID, Index: idx, Order: state.nextSkip, Reason: "dependency_skipped",
				Detail: "an earlier batch was skipped by budget or queue policy",
			}
			status[idx] = "skipped"
			resolve(idx)
			continue
		}
		candidate := state.spent + bp.EstimatedCostMicros
		if candidate < state.spent {
			return nil, apperr.New(apperr.SpecInvalid, "round cost accumulation overflows int64")
		}
		cmp, err := fixed.CompareTimeBudget(candidate, limitMicros, s.Samples.Policy.TimeBudgetRelativeTolerancePPM, idx)
		if err != nil {
			return nil, err
		}
		decisionOrder++
		decision := BudgetDecision{
			BatchID: bp.BatchID, Index: idx, DecisionOrder: decisionOrder, PrioritySeverity: bp.MaxSeverityRank,
			CandidateMicros: cmp.CandidateMicros, Candidate: cmp.Candidate, LimitMicros: cmp.LimitMicros, Limit: cmp.Limit,
			TolerancePPM: cmp.TolerancePPM, ToleranceMicros: cmp.ToleranceMicros, Tolerance: cmp.Tolerance,
			Decision: cmp.Decision, WithinTolerance: cmp.WithinTolerance, Explanation: cmp.Explanation,
		}
		state.decisions = append(state.decisions, decision)
		if cmp.Decision == "skip" {
			state.nextSkip++
			state.skipped[idx] = SkippedBatch{
				BatchID: bp.BatchID, Index: idx, Order: state.nextSkip, Reason: "round_time_budget",
				Detail: fmt.Sprintf("candidate=%s limit=%s tolerance=%s", cmp.Candidate, cmp.Limit, cmp.Tolerance),
			}
			status[idx] = "skipped"
		} else {
			state.accepted[idx] = true
			state.spent = candidate
			status[idx] = "accepted"
		}
		resolve(idx)
	}
	for idx := range batchByIndex {
		if status[idx] == "unresolved" {
			state.nextSkip++
			state.skipped[idx] = SkippedBatch{
				BatchID: batchByIndex[idx].BatchID, Index: idx, Order: state.nextSkip, Reason: "dependency_skipped",
				Detail: "batch dependencies could not be admitted",
			}
		}
	}
	return state, nil
}

func sortReady(ready []int, batches map[int]plan.BatchPlan) {
	sort.Slice(ready, func(i, j int) bool {
		left, right := batches[ready[i]], batches[ready[j]]
		if left.MaxSeverityRank != right.MaxSeverityRank {
			return left.MaxSeverityRank > right.MaxSeverityRank
		}
		return left.Index < right.Index
	})
}

func sortSkipped(skipped []SkippedBatch) {
	sort.Slice(skipped, func(i, j int) bool {
		if skipped[i].Order != skipped[j].Order {
			return skipped[i].Order < skipped[j].Order
		}
		return skipped[i].Index < skipped[j].Index
	})
	for i := range skipped {
		skipped[i].Order = i + 1
	}
}

func skippedIDs(skipped []SkippedBatch) []string {
	ids := make([]string, len(skipped))
	for i := range skipped {
		ids[i] = skipped[i].BatchID
	}
	return ids
}

func overallFailureCode(report *ExecutionReport) apperr.Code {
	rank := map[apperr.Code]int{
		apperr.EndpointRefused: 4,
		apperr.WriteTimeout:    3,
		apperr.ReadbackFailed:  2,
		apperr.WriteFailed:     1,
	}
	found := apperr.WriteFailed
	best := 0
	for _, batch := range report.ExecutedBatches {
		for _, object := range batch.Objects {
			if object.ErrorCode == "" {
				continue
			}
			code := apperr.Code(object.ErrorCode)
			if rank[code] > best {
				best = rank[code]
				found = code
			}
		}
	}
	return found
}
