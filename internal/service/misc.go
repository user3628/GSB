package service

import (
	"context"
	"database/sql"
	"os"

	"gpusched/internal/domain"
	"gpusched/internal/store"
)

// SubmitRequest validates and persists one normalized PENDING request inside an
// intent-guarded transaction.
func (a *App) SubmitRequest(ctx context.Context, r domain.Request) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()

	return a.store.Tx(ctx, func(tx *sql.Tx) error {
		intentID, err := store.InsertIntent(tx, "SUBMIT_REQUEST", 0, r.ID, "", "")
		if err != nil {
			return err
		}
		if _, gerr := a.store.GetGroupTx(tx, r.GroupID); gerr != nil {
			return domain.NewError(domain.ESpecInvalid, "unknown quota group",
				map[string]any{"group_id": r.GroupID})
		}
		if existing, gerr := a.store.GetRequestTx(tx, r.ID); gerr == nil {
			return domain.NewError(domain.ESpecInvalid, "duplicate request id",
				map[string]any{"request_id": existing.ID})
		}
		seq, err := store.NextRequestSeqTx(tx)
		if err != nil {
			return err
		}
		r.State = domain.StatePending
		r.Seq = seq
		if err := a.store.InsertRequestTx(tx, r); err != nil {
			return err
		}
		return store.MarkIntent(tx, intentID, domain.IntentCommitted)
	})
}

func (a *App) ListGroups(ctx context.Context) ([]domain.Group, error) {
	return a.store.ListGroups(ctx)
}

func (a *App) ListNodes(ctx context.Context) ([]domain.Node, error) {
	return a.store.ListNodes(ctx)
}

func (a *App) GetRequest(ctx context.Context, id string) (domain.Request, error) {
	req, err := a.store.GetRequest(ctx, id)
	if err == sql.ErrNoRows {
		return req, domain.NewError(domain.ENotFound, "request not found",
			map[string]any{"request_id": id})
	}
	return req, err
}

func (a *App) LatestPolicy(ctx context.Context) (domain.PolicySnapshot, error) {
	snap, err := a.store.LatestSnapshot(ctx)
	if err == sql.ErrNoRows {
		return snap, domain.NewError(domain.ENotFound, "no policy snapshot", nil)
	}
	return snap, err
}

// ArmCrash installs a one-shot hook that terminates the process at the first
// matching durability point, simulating SIGKILL mid-arbitration.
func (a *App) ArmCrash(point string) {
	a.SetCrashHook(func(hit string) {
		if hit == point {
			// Flush nothing and exit immediately, mimicking kill -9.
			os.Exit(137)
		}
	})
}
