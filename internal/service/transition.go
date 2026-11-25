package service

import (
	"context"
	"database/sql"

	"gpusched/internal/domain"
	"gpusched/internal/store"
)

// TransitionResult is returned for every state machine operation.
type TransitionResult struct {
	RequestID string `json:"request_id"`
	FromState string `json:"from_state"`
	ToState   string `json:"to_state"`
	Applied   bool   `json:"applied"`
}

// Transition applies a state machine transition. Illegal transitions return
// E_INVALID_TRANSITION without touching persisted state. Repeating the same
// transition is idempotent: it is reported once and subsequent identical calls
// return Applied=false while reusing the current state.
func (a *App) Transition(ctx context.Context, requestID, to string) (*TransitionResult, error) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()

	result := &TransitionResult{RequestID: requestID, ToState: to}
	err := a.store.Tx(ctx, func(tx *sql.Tx) error {
		intentID, err := store.InsertIntent(tx, "TRANSITION:"+to, 0, requestID, "", "")
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
		result.FromState = req.State

		// Idempotent no-op: already in the requested state.
		if req.State == to {
			result.Applied = false
			return store.MarkIntent(tx, intentID, domain.IntentCommitted)
		}
		if !domain.CanTransition(req.State, to) {
			return domain.NewError(domain.EInvalidTransition,
				"illegal transition", map[string]any{
					"request_id": requestID, "from": req.State, "to": to,
				})
		}

		// BATCHED -> RUNNING / RUNNING -> COMPLETED release no extra reservation
		// here; completion releases resources through releaseResourcesTx.
		if to == domain.StateCompleted {
			if err := releaseResourcesTx(tx, a, req); err != nil {
				return err
			}
		}

		ok, err := store.UpdateRequestState(tx, requestID, req.State, to)
		if err != nil {
			return err
		}
		if !ok {
			return domain.NewError(domain.EInvalidTransition,
				"state changed concurrently", map[string]any{"request_id": requestID})
		}

		// A downgrade/un-batch (BATCHED -> ADMITTED) detaches the batch.
		if to == domain.StateAdmitted && req.State == domain.StateBatched {
			if _, err := tx.Exec(`DELETE FROM batch_members WHERE request_id=?`, requestID); err != nil {
				return err
			}
		}

		if err := assertGroupInvariantTx(tx, a, req.GroupID); err != nil {
			return err
		}
		result.Applied = true
		return store.MarkIntent(tx, intentID, domain.IntentCommitted)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// releaseResourcesTx gives back node usage and group reservation for a request
// that is reaching a terminal completed state.
func releaseResourcesTx(tx *sql.Tx, a *App, req domain.Request) error {
	if req.AssignedNodeID != "" {
		if err := store.AdjustNodeUsage(tx, req.AssignedNodeID,
			-req.MemoryNeed, -req.ComputeNeedM); err != nil {
			return domain.NewError(domain.EQuotaLeak,
				"failed to release node usage on completion",
				map[string]any{"node_id": req.AssignedNodeID})
		}
	}
	if err := store.AdjustGroupReservation(tx, req.GroupID,
		-req.MemoryNeed, -req.ComputeNeedM); err != nil {
		return domain.NewError(domain.EQuotaLeak,
			"failed to release group reservation on completion",
			map[string]any{"group_id": req.GroupID})
	}
	return nil
}
