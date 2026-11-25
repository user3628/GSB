package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"gpusched/internal/domain"
	"gpusched/internal/scheduler"
	"gpusched/internal/store"
)

// RoundReport is the deterministic conclusion of one scheduling round.
type RoundReport struct {
	RoundID           int64       `json:"round_id"`
	PolicyVersion     int64       `json:"policy_version"`
	BudgetMs          int64       `json:"budget_ms"`
	ElapsedMs         int64       `json:"elapsed_ms"`
	RunningBatches    []string    `json:"running_batches"`
	QueuedBatches     []string    `json:"queued_batches"`
	DowngradedBatches []string    `json:"downgraded_batches"`
	LateViolations    []string    `json:"latency_violations"`
	QueueFull         bool        `json:"queue_full"`
	Batches           []BatchView `json:"batches"`
}

// BatchView is the externally visible batch organization conclusion.
type BatchView struct {
	BatchID      string   `json:"batch_id"`
	RoundID      int64    `json:"round_id"`
	ModelID      string   `json:"model_id"`
	NodeID       string   `json:"node_id"`
	GroupID      string   `json:"group_id"`
	Priority     int      `json:"priority"`
	Status       string   `json:"status"`
	Members      []string `json:"members"`
	TotalSamples int64    `json:"total_samples"`
	TotalMemory  int64    `json:"total_memory_bytes"`
	EstLatencyMs int64    `json:"est_latency_ms"`
}

// virtualClock makes budget consumption deterministic while still defaulting to
// wall time when no test clock is supplied.
type virtualClock struct {
	now    int64
	active bool
}

func (c *virtualClock) millis() int64 {
	if c.active {
		return c.now
	}
	return time.Now().UnixMilli()
}

// RunRound executes one scheduling round under the global concurrency cap and a
// per-round time budget. Batching and all accounting changes commit together.
func (a *App) RunRound(ctx context.Context, virtualBudgetConsumedMs int64) (*RoundReport, error) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()

	clock := &virtualClock{now: virtualBudgetConsumedMs, active: virtualBudgetConsumedMs > 0}
	report := &RoundReport{
		RunningBatches:    []string{},
		QueuedBatches:     []string{},
		DowngradedBatches: []string{},
		LateViolations:    []string{},
		Batches:           []BatchView{},
	}
	var waitQueueCapacity int

	err := a.store.Tx(ctx, func(tx *sql.Tx) error {
		snap, err := a.store.LatestSnapshot(ctx)
		if err == sql.ErrNoRows {
			return domain.NewError(domain.ESpecInvalid, "no policy snapshot loaded", nil)
		}
		if err != nil {
			return err
		}
		policy := snap.Params
		waitQueueCapacity = policy.WaitQueueCapacity
		report.PolicyVersion = snap.Version
		report.BudgetMs = policy.RoundBudgetMs

		intentID, err := store.InsertIntent(tx, "RUN_ROUND", 0, "", "",
			fmt.Sprintf(`{"policy_version":%d}`, snap.Version))
		if err != nil {
			return err
		}

		startWall := time.Now().UTC().Format(time.RFC3339Nano)
		roundID, err := a.store.CreateRoundTx(tx, snap.Version, policy.RoundBudgetMs, startWall)
		if err != nil {
			return err
		}
		report.RoundID = roundID

		// Admitted requests that are not already bound to a live batch.
		admitted, err := a.store.ListRequestsByStateTx(tx, []string{domain.StateAdmitted})
		if err != nil {
			return err
		}
		sort.SliceStable(admitted, func(i, j int) bool { return admitted[i].Seq < admitted[j].Seq })

		nodes, err := a.store.ListNodes(ctx)
		if err != nil {
			return err
		}
		freeMem := map[string]int64{}
		for _, n := range nodes {
			freeMem[n.ID] = n.MemoryTotal - n.MemoryUsed
		}

		planned, lateViolations := scheduler.Organize(
			admitted, freeMem, policy.MaxSamplesPerModel,
			policy.LatencyBaseMs, policy.LatencyPerSampleMs)
		report.LateViolations = lateViolations

		// Deterministic scheduling order: priority asc, group round-robin, latency asc, seq.
		groupRR := groupRotationOrder(admitted)
		groupRank := map[string]int{}
		for i, g := range groupRR {
			groupRank[g] = i
		}
		sort.SliceStable(planned, func(i, j int) bool {
			pi, pj := planned[i], planned[j]
			if pi.Priority != pj.Priority {
				return pi.Priority < pj.Priority
			}
			if groupRank[pi.GroupID] != groupRank[pj.GroupID] {
				return groupRank[pi.GroupID] < groupRank[pj.GroupID]
			}
			if pi.EstLatencyMs != pj.EstLatencyMs {
				return pi.EstLatencyMs < pj.EstLatencyMs
			}
			return pi.MemberIDs[0] < pj.MemberIDs[0]
		})

		runningCount, err := store.CountBatchesByStatusTx(tx,
			[]string{domain.BatchRunning, domain.BatchQueued})
		if err != nil {
			return err
		}
		queuedCount, err := store.CountQueuedBatchesTx(tx)
		if err != nil {
			return err
		}

		elapsed := int64(0)
		for idx, pb := range planned {
			batchID := fmt.Sprintf("b-r%d-%03d", roundID, idx)
			// Time budget: deterministic virtual cost per scheduling decision.
			stepCost := pb.EstLatencyMs
			if clock.active {
				clock.now += stepCost
				elapsed = clock.now
			} else {
				elapsed = time.Since(parseWall(startWall)).Milliseconds()
			}
			if elapsed > policy.RoundBudgetMs {
				report.DowngradedBatches = append(report.DowngradedBatches,
					downgradeIDs(roundID, idx, planned[idx:])...)
				if err := recordDowngraded(tx, a, roundID, idx, planned[idx:]); err != nil {
					return err
				}
				break
			}

			if runningCount < policy.GlobalMaxConcurrent {
				status := domain.BatchRunning
				if err := persistBatch(tx, a, roundID, batchID, pb, status); err != nil {
					return err
				}
				if err := store.BumpPickCounter(tx, pb.GroupID); err != nil {
					return err
				}
				runningCount++
				report.RunningBatches = append(report.RunningBatches, batchID)
			} else if queuedCount < int64(policy.WaitQueueCapacity) {
				if err := persistBatch(tx, a, roundID, batchID, pb, domain.BatchQueued); err != nil {
					return err
				}
				if err := store.BumpPickCounter(tx, pb.GroupID); err != nil {
					return err
				}
				queuedCount++
				report.QueuedBatches = append(report.QueuedBatches, batchID)
			} else {
				// Bounded wait queue is full. We still COMMIT the running/queued
				// batches and record the overflow as deterministic downgrades; the
				// E_EXEC_QUEUE_FULL signal is returned after commit so no work is
				// rolled back or silently dropped.
				report.QueueFull = true
				report.DowngradedBatches = append(report.DowngradedBatches,
					downgradeIDs(roundID, idx, planned[idx:])...)
				if err := recordDowngraded(tx, a, roundID, idx, planned[idx:]); err != nil {
					return err
				}
				break
			}
		}
		report.ElapsedMs = elapsed

		// Build the read view from what was actually persisted.
		saved, err := a.store.ListBatchesByRoundTx(tx, roundID)
		if err != nil {
			return err
		}
		for _, b := range saved {
			members, merr := store.MembersOfBatchTx(tx, b.ID)
			if merr != nil {
				return merr
			}
			report.Batches = append(report.Batches, BatchView{
				BatchID: b.ID, RoundID: b.RoundID, ModelID: b.ModelID,
				NodeID: b.NodeID, GroupID: b.GroupID, Priority: b.Priority,
				Status: b.Status, Members: members, TotalSamples: b.TotalSamples,
				TotalMemory: b.TotalMemory, EstLatencyMs: b.EstLatencyMs,
			})
		}

		a.hook("round:before_commit")
		finished, _ := json.Marshal(report)
		if err := a.store.FinishRoundTx(tx, roundID, domain.RoundCommitted,
			time.Now().UTC().Format(time.RFC3339Nano), string(finished)); err != nil {
			return err
		}
		return store.MarkIntent(tx, intentID, domain.IntentCommitted)
	})
	if err != nil {
		return nil, err
	}
	// Post-commit crash point: the round is durable here, so recovery must find
	// a COMMITTED round (no rollback) if killed now.
	a.hook("round:after_commit")
	if report.QueueFull {
		return report, domain.NewError(domain.EExecQueueFull,
			"global wait queue is full; overflow batches downgraded and committed",
			map[string]any{"capacity": waitQueueCapacity, "downgraded": report.DowngradedBatches})
	}
	return report, nil
}

// persistBatch writes the batch header and members and flips member requests to
// BATCHED, all inside the caller's transaction. Node memory and group
// reservation were already deducted at admission time, so no extra resource
// movement happens here (batching is an organization step, not new allocation).
func persistBatch(tx *sql.Tx, a *App, roundID int64, batchID string,
	pb scheduler.PlannedBatch, status string) error {
	b := domain.Batch{
		ID: batchID, RoundID: roundID, ModelID: pb.ModelID, NodeID: pb.NodeID,
		GroupID: pb.GroupID, Priority: pb.Priority, Status: status,
		MemberIDs: pb.MemberIDs, TotalSamples: pb.TotalSamples,
		TotalMemory: pb.TotalMemory, EstLatencyMs: pb.EstLatencyMs,
		Seq: int64(len(pb.MemberIDs)),
	}
	if err := a.store.InsertBatchTx(tx, b); err != nil {
		return err
	}
	for pos, id := range pb.MemberIDs {
		if err := a.store.AddBatchMemberTx(tx, batchID, id, pos); err != nil {
			return err
		}
		req, err := a.store.GetRequestTx(tx, id)
		if err != nil {
			return err
		}
		ok, err := store.UpdateRequestState(tx, id, req.State, domain.StateBatched)
		if err != nil {
			return err
		}
		if !ok {
			return domain.NewError(domain.EInvalidTransition,
				"request cannot be batched", map[string]any{"request_id": id, "state": req.State})
		}
	}
	return nil
}

// recordDowngraded persists downgraded batches for the audit view. Member
// requests stay ADMITTED (their rows are not touched), so the next round can
// re-attempt them and nothing is silently dropped.
func recordDowngraded(tx *sql.Tx, a *App, roundID int64, startIdx int, planned []scheduler.PlannedBatch) error {
	for off, pb := range planned {
		batchID := fmt.Sprintf("b-r%d-d%03d", roundID, startIdx+off)
		if err := a.store.InsertBatchTx(tx, domain.Batch{
			ID: batchID, RoundID: roundID, ModelID: pb.ModelID, NodeID: pb.NodeID,
			GroupID: pb.GroupID, Priority: pb.Priority, Status: domain.BatchDowngraded,
			MemberIDs: pb.MemberIDs, TotalSamples: pb.TotalSamples, TotalMemory: pb.TotalMemory,
			EstLatencyMs: pb.EstLatencyMs, Seq: int64(startIdx + off),
		}); err != nil {
			return err
		}
		for pos, id := range pb.MemberIDs {
			if err := a.store.AddBatchMemberTx(tx, batchID, id, pos); err != nil {
				return err
			}
		}
	}
	return nil
}

func groupRotationOrder(reqs []domain.Request) []string {
	seen := map[string]bool{}
	var order []string
	for _, r := range reqs {
		if !seen[r.GroupID] {
			seen[r.GroupID] = true
			order = append(order, r.GroupID)
		}
	}
	sort.Strings(order)
	return order
}

func downgradeIDs(roundID int64, startIdx int, planned []scheduler.PlannedBatch) []string {
	out := make([]string, 0, len(planned))
	for i := range planned {
		out = append(out, fmt.Sprintf("b-r%d-d%03d", roundID, startIdx+i))
	}
	return out
}

func parseWall(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}
