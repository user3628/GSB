package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"time"

	"cdn-edge-consistency/internal/cache"
	"cdn-edge-consistency/internal/model"
	"cdn-edge-consistency/internal/store"
)

// DebugCrash prepares a durable, intentionally incomplete purge transaction.
// The HTTP adapter then terminates the process without marking the intent or
// purge committed. Startup recovery must undo the partially visible entry
// changes and report E_PURGE_INCOMPLETE from persisted facts.
func (e *Engine) DebugCrash(ctx context.Context, nodeID, rawPath, requestID string) error {
	if !e.cfg.AllowDebugCrash {
		return model.NewBiz(model.CodeBadRequest, "debug crash fixture is disabled")
	}
	if nodeID == "" {
		return model.NewBiz(model.CodeBadRequest, "node query parameter is required")
	}
	if _, ok := e.cfg.Node(nodeID); !ok {
		return model.NewBiz(model.CodeNodeUnknown, "unknown node")
	}
	p := e.CurrentPolicy()
	if !p.CoversNode(nodeID) {
		return model.NewBiz(model.CodeNodeUnknown, "node is not covered by current policy")
	}
	target, err := cache.NormalizePath(rawPath, p.KeyRules)
	if err != nil {
		return err
	}
	if requestID == "" {
		requestID = e.newRequestID("")
	}
	sum := sha256.Sum256([]byte("debug-crash\x00" + requestID))
	purgeID := "purge-crash-" + hex.EncodeToString(sum[:8])
	nowMs := e.clock.Now().UnixMilli()
	nodes := append([]string(nil), p.Nodes...)
	var intentID int64
	err = e.store.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		undo, err := e.collectPurgeUndoTx(ctx, tx, p, "path", target)
		if err != nil {
			return err
		}
		intentID, err = store.CreateIntentTx(ctx, tx, "purge_crash_fixture", requestID, p.Version, store.IntentPayload{
			Undo:    undo,
			Details: map[string]any{"purge_id": purgeID, "path": target, "simulated_forced_termination": true},
		}, nowMs)
		return err
	})
	if err != nil {
		return err
	}
	err = e.store.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return store.CreatePurgeSessionTx(ctx, tx, store.PurgeRecord{
			PurgeID: purgeID, Kind: "path", Target: target, RequestID: requestID,
			SnapshotVersion: p.Version, DeadlineMs: nowMs + p.PurgeWindowMs, PurgeWindowMs: p.PurgeWindowMs,
			Details: map[string]any{"intent_id": intentID, "simulated_forced_termination": true},
		}, nodes, nowMs)
	})
	if err != nil {
		return err
	}
	// This transaction deliberately commits cache/index changes while leaving
	// both the intent and purge header pending. Recovery must undo it.
	return e.store.Do(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		if _, err := store.PurgeEntriesTx(ctx, tx, purgeID, "path", target, nodes, requestID, nowMs); err != nil {
			return err
		}
		for _, node := range nodes {
			if err := store.SetDeliveryStatusTx(ctx, tx, purgeID, node, "complete", "", 0, nowMs); err != nil {
				return err
			}
		}
		// Give the HTTP adapter a moment to flush behavior, then it exits.
		time.Sleep(10 * time.Millisecond)
		return nil
	})
}
