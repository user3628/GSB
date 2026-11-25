package service

import (
	"context"
	"database/sql"
	"sort"

	"gpusched/internal/domain"
	"gpusched/internal/store"
)

// PreemptResult reports a completed preemption and the released resources.
type PreemptResult struct {
	VictimID     string `json:"victim_id"`
	GroupID      string `json:"group_id"`
	FromState    string `json:"from_state"`
	ToState      string `json:"to_state"`
	FreedMemory  int64  `json:"freed_memory_bytes"`
	FreedCompute int64  `json:"freed_compute_milli"`
	NodeID       string `json:"node_id"`
}

// Preempt moves a lower-priority victim request to PREEMPTED and releases its
// node usage and group reservation in the SAME transaction. If the state change
// cannot be paired with the quota release the transaction aborts with
// E_QUOTA_LEAK, so a "status only" update can never be observed.
func (a *App) Preempt(ctx context.Context, victimID string) (*PreemptResult, error) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()

	var result *PreemptResult
	err := a.store.Tx(ctx, func(tx *sql.Tx) error {
		intentID, err := store.InsertIntent(tx, "PREEMPT", 0, victimID, "", "")
		if err != nil {
			return err
		}
		victim, err := a.store.GetRequestTx(tx, victimID)
		if err == sql.ErrNoRows {
			return domain.NewError(domain.ENotFound, "request not found",
				map[string]any{"request_id": victimID})
		}
		if err != nil {
			return err
		}
		from := victim.State
		if from != domain.StateAdmitted && from != domain.StateBatched &&
			from != domain.StateRunning && from != domain.StatePending {
			return domain.NewError(domain.EInvalidTransition,
				"request cannot be preempted from state "+from,
				map[string]any{"request_id": victimID, "state": from})
		}

		// A batched/running victim must be detached from its batch first.
		if from == domain.StateBatched || from == domain.StateRunning {
			batchID, berr := store.BatchOfRequestTx(tx, victimID)
			if berr != nil {
				return berr
			}
			if batchID != "" {
				if _, derr := tx.Exec(`DELETE FROM batch_members WHERE request_id=?`, victimID); derr != nil {
					return derr
				}
			}
		}

		ok, err := store.UpdateRequestState(tx, victimID, from, domain.StatePreempted)
		if err != nil {
			return err
		}
		if !ok {
			return domain.NewError(domain.EInvalidTransition,
				"state changed concurrently", map[string]any{"request_id": victimID})
		}

		// Real release of node usage and group reservation.
		if victim.AssignedNodeID != "" {
			if err := store.AdjustNodeUsage(tx, victim.AssignedNodeID,
				-victim.MemoryNeed, -victim.ComputeNeedM); err != nil {
				return domain.NewError(domain.EQuotaLeak,
					"failed to release node usage", map[string]any{"node_id": victim.AssignedNodeID})
			}
		}
		if err := store.AdjustGroupReservation(tx, victim.GroupID,
			-victim.MemoryNeed, -victim.ComputeNeedM); err != nil {
			return domain.NewError(domain.EQuotaLeak,
				"failed to release group reservation", map[string]any{"group_id": victim.GroupID})
		}

		// The leak guard proves reservation and node usage still agree.
		if err := assertGroupInvariantTx(tx, a, victim.GroupID); err != nil {
			return err
		}

		a.hook("preempt:after_release")
		if err := store.MarkIntent(tx, intentID, domain.IntentCommitted); err != nil {
			return err
		}
		result = &PreemptResult{
			VictimID: victimID, GroupID: victim.GroupID, FromState: from,
			ToState: domain.StatePreempted, FreedMemory: victim.MemoryNeed,
			FreedCompute: victim.ComputeNeedM, NodeID: victim.AssignedNodeID,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ChoosePreemptionVictims deterministically selects lower-priority requests in a
// group that together free at least the needed memory and compute. Selection is
// stable: lowest priority first, then earliest submission (smallest seq).
func ChoosePreemptionVictims(candidates []domain.Request, needMem, needCompute int64, beneficiaryPriority int) []domain.Request {
	eligible := make([]domain.Request, 0, len(candidates))
	for _, r := range candidates {
		if r.Priority > beneficiaryPriority &&
			(r.State == domain.StateAdmitted || r.State == domain.StateBatched || r.State == domain.StateRunning) {
			eligible = append(eligible, r)
		}
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		if eligible[i].Priority != eligible[j].Priority {
			return eligible[i].Priority > eligible[j].Priority // lower priority number = higher prio
		}
		return eligible[i].Seq < eligible[j].Seq
	})
	var freedMem, freedCompute int64
	out := []domain.Request{}
	for _, r := range eligible {
		if freedMem >= needMem && freedCompute >= needCompute {
			break
		}
		out = append(out, r)
		freedMem += r.MemoryNeed
		freedCompute += r.ComputeNeedM
	}
	if freedMem < needMem || freedCompute < needCompute {
		return nil
	}
	return out
}
