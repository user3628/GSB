package service

import (
	"context"
	"database/sql"

	"gpusched/internal/domain"
	"gpusched/internal/store"
)

// AdmitResult describes the outcome of admitting one request.
type AdmitResult struct {
	RequestID     string `json:"request_id"`
	State         string `json:"state"`
	NodeID        string `json:"node_id"`
	GroupID       string `json:"group_id"`
	PolicyVersion int64  `json:"policy_version"`
}

// AdmitOne admits a single PENDING request. All accounting changes happen in
// one transaction: an intent is written first, then request state, node usage
// and group reservation move together, and finally the intent is marked
// committed. Over-quota returns E_QUOTA_EXCEEDED with current/need/shortfall.
func (a *App) AdmitOne(ctx context.Context, requestID string) (*AdmitResult, error) {
	// Concurrent identical admissions collapse onto one execution; waiters
	// reuse the result, and a different payload for the same id is a conflict.
	out, err := a.doOnce("admit:"+requestID, map[string]string{"request_id": requestID},
		func() (any, error) { return a.admitOneTx(ctx, requestID) })
	if err != nil {
		return nil, err
	}
	return out.(*AdmitResult), nil
}

func (a *App) admitOneTx(ctx context.Context, requestID string) (*AdmitResult, error) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()

	var result *AdmitResult
	err := a.store.Tx(ctx, func(tx *sql.Tx) error {
		intentID, err := store.InsertIntent(tx, "ADMIT", 0, requestID, "", "")
		if err != nil {
			return err
		}
		req, err := a.store.GetRequestTx(tx, requestID)
		if err == sql.ErrNoRows {
			return domain.NewError(domain.ENotFound, "request not found",
				map[string]any{"request_id": requestID})
		}
		if err != nil {
			return err
		}
		// Idempotent: a request already admitted (or further) is returned as-is.
		if req.State != domain.StatePending {
			result = &AdmitResult{RequestID: req.ID, State: req.State,
				NodeID: req.AssignedNodeID, GroupID: req.GroupID, PolicyVersion: req.PolicyVersion}
			return store.MarkIntent(tx, intentID, domain.IntentCommitted)
		}

		snap, err := a.store.LatestSnapshot(ctx)
		if err == sql.ErrNoRows {
			return domain.NewError(domain.ESpecInvalid, "no policy snapshot loaded", nil)
		}
		if err != nil {
			return err
		}

		group, err := a.store.GetGroupTx(tx, req.GroupID)
		if err != nil {
			return domain.NewError(domain.ESpecInvalid, "unknown quota group",
				map[string]any{"group_id": req.GroupID})
		}

		// Group-level capacity gate (reservation must stay within group limit).
		if detail := quotaShortfall(group, req); detail != nil {
			return domain.NewError(domain.EQuotaExceeded,
				"group reservation cannot satisfy request", map[string]any{"shortfall": detail})
		}

		// Choose a healthy member node with enough free memory and compute.
		nodes, err := a.store.ListNodesByGroupTx(tx, req.GroupID)
		if err != nil {
			return err
		}
		var chosen *domain.Node
		for i := range nodes {
			n := nodes[i]
			if n.Health != domain.HealthHealthy && n.Health != domain.HealthDegraded {
				continue
			}
			if n.MemoryTotal-n.MemoryUsed >= req.MemoryNeed &&
				n.ComputeTotalM-n.ComputeUsedM >= req.ComputeNeedM {
				chosen = &nodes[i]
				break
			}
		}
		if chosen == nil {
			return domain.NewError(domain.EQuotaExceeded,
				"no member node has enough free resources",
				map[string]any{
					"shortfall": domain.QuotaExceedDetail{
						GroupID: req.GroupID, Resource: "node_capacity",
						CurrentUsed: group.MemoryReserved, Limit: group.MemoryLimit,
						Requested: req.MemoryNeed,
						Shortfall: req.MemoryNeed,
					},
				})
		}

		// State transition guarded by expected previous state.
		ok, err := store.UpdateRequestState(tx, req.ID, domain.StatePending, domain.StateAdmitted)
		if err != nil {
			return err
		}
		if !ok {
			return domain.NewError(domain.EInvalidTransition,
				"request is not PENDING", map[string]any{"request_id": req.ID, "state": req.State})
		}
		if err := store.AssignNodeTx(tx, req.ID, chosen.ID, snap.Version); err != nil {
			return err
		}
		// Node usage and group reservation move in the same transaction.
		if err := store.AdjustNodeUsage(tx, chosen.ID, req.MemoryNeed, req.ComputeNeedM); err != nil {
			return err
		}
		if err := store.AdjustGroupReservation(tx, req.GroupID, req.MemoryNeed, req.ComputeNeedM); err != nil {
			return err
		}

		// Invariant guard: no leaked or over-counted quota after the change.
		if err := assertGroupInvariantTx(tx, a, req.GroupID); err != nil {
			return err
		}

		a.hook("admit:after_reserve")
		if err := store.MarkIntent(tx, intentID, domain.IntentCommitted); err != nil {
			return err
		}
		result = &AdmitResult{RequestID: req.ID, State: domain.StateAdmitted,
			NodeID: chosen.ID, GroupID: req.GroupID, PolicyVersion: snap.Version}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// quotaShortfall returns the first violated resource detail, or nil if feasible.
func quotaShortfall(g domain.Group, req domain.Request) *domain.QuotaExceedDetail {
	if g.MemoryReserved+req.MemoryNeed > g.MemoryLimit {
		return &domain.QuotaExceedDetail{
			GroupID: g.ID, Resource: "memory",
			CurrentUsed: g.MemoryReserved, Limit: g.MemoryLimit,
			Requested: req.MemoryNeed,
			Shortfall: g.MemoryReserved + req.MemoryNeed - g.MemoryLimit,
		}
	}
	if g.ComputeReserved+req.ComputeNeedM > g.ComputeLimitM {
		return &domain.QuotaExceedDetail{
			GroupID: g.ID, Resource: "compute_milli",
			CurrentUsed: g.ComputeReserved, Limit: g.ComputeLimitM,
			Requested: req.ComputeNeedM,
			Shortfall: g.ComputeReserved + req.ComputeNeedM - g.ComputeLimitM,
		}
	}
	return nil
}

// assertGroupInvariantTx verifies reservation/usage consistency before commit.
// A mismatch means an accounting bug (quota leak) and must abort the whole tx.
func assertGroupInvariantTx(tx *sql.Tx, a *App, groupID string) error {
	group, err := a.store.GetGroupTx(tx, groupID)
	if err != nil {
		return err
	}
	if group.MemoryReserved < 0 || group.ComputeReserved < 0 {
		return domain.NewError(domain.EQuotaLeak, "negative group reservation",
			map[string]any{"group_id": groupID})
	}
	if group.MemoryReserved > group.MemoryLimit || group.ComputeReserved > group.ComputeLimitM {
		return domain.NewError(domain.EQuotaLeak, "group reservation exceeds limit",
			map[string]any{"group_id": groupID,
				"memory_reserved": group.MemoryReserved, "memory_limit": group.MemoryLimit})
	}
	nodes, err := a.store.ListNodesByGroupTx(tx, groupID)
	if err != nil {
		return err
	}
	var nodeMem, nodeCompute int64
	for _, n := range nodes {
		if n.MemoryUsed < 0 || n.ComputeUsedM < 0 ||
			n.MemoryUsed > n.MemoryTotal || n.ComputeUsedM > n.ComputeTotalM {
			return domain.NewError(domain.EQuotaLeak, "node usage out of bounds",
				map[string]any{"node_id": n.ID})
		}
		nodeMem += n.MemoryUsed
		nodeCompute += n.ComputeUsedM
	}
	if nodeMem != group.MemoryReserved || nodeCompute != group.ComputeReserved {
		return domain.NewError(domain.EQuotaLeak,
			"group reservation diverges from aggregate node usage",
			map[string]any{
				"group_id":              groupID,
				"group_memory_reserved": group.MemoryReserved, "node_memory_used": nodeMem,
				"group_compute_reserved": group.ComputeReserved, "node_compute_used": nodeCompute,
			})
	}
	return nil
}
