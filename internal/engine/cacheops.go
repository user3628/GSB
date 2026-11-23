package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"

	"cdn-edge-consistency/internal/cache"
	"cdn-edge-consistency/internal/model"
	"cdn-edge-consistency/internal/policy"
	"cdn-edge-consistency/internal/scheduler"
	"cdn-edge-consistency/internal/store"
)

type flightOutcome struct {
	Origin   *OriginResult
	Entries  int
	IntentID int64
	Snapshot int64
}

func (e *Engine) revalidationFn(p *policy.Snapshot, identity cache.Identity, headers http.Header, fault FaultSpec, requestID, nodeID string) func(context.Context) (any, error) {
	windowStarted := time.Now()
	return func(ctx context.Context) (any, error) {
		workCtx := ctx
		cancelWindow := func() {}
		if p.RevalidateWindowMs > 0 {
			workCtx, cancelWindow = context.WithDeadline(ctx, windowStarted.Add(time.Duration(p.RevalidateWindowMs)*time.Millisecond))
		}
		defer cancelWindow()

		started := e.clock.Now()
		intentID, epoch, err := e.beginRevalidation(workCtx, p, identity, requestID, nodeID)
		if err != nil {
			if windowErr := revalidateWindowError(ctx, workCtx, p, identity); windowErr != nil {
				return nil, windowErr
			}
			return nil, err
		}
		res, _, fetchErr := e.fetchOrigin(workCtx, identity, headers, fault, policy.EffectiveOriginTimeout(p.OriginTimeoutMs, e.cfg.OriginTimeoutMs))
		durationMs := e.clock.Now().Sub(started).Milliseconds()
		if fetchErr != nil {
			if windowErr := revalidateWindowError(ctx, workCtx, p, identity); windowErr != nil {
				fetchErr = windowErr
			}
			_ = e.finishOriginFailure(context.Background(), p, identity, requestID, nodeID, intentID, epoch, fetchErr, durationMs)
			return nil, fetchErr
		}
		if windowErr := revalidateWindowError(ctx, workCtx, p, identity); windowErr != nil {
			_ = e.finishOriginFailure(context.Background(), p, identity, requestID, nodeID, intentID, epoch, windowErr, durationMs)
			return nil, windowErr
		}
		entries, err := e.finishOriginSuccess(workCtx, p, identity, requestID, nodeID, intentID, epoch, res, durationMs)
		if err != nil {
			if windowErr := revalidateWindowError(ctx, workCtx, p, identity); windowErr != nil {
				_ = e.finishOriginFailure(context.Background(), p, identity, requestID, nodeID, intentID, epoch, windowErr, durationMs)
				return nil, windowErr
			}
			return nil, err
		}
		return &flightOutcome{Origin: res, Entries: entries, IntentID: intentID, Snapshot: p.Version}, nil
	}
}

func revalidateWindowError(parent, work context.Context, p *policy.Snapshot, identity cache.Identity) error {
	if p.RevalidateWindowMs <= 0 || parent.Err() != nil || !errors.Is(work.Err(), context.DeadlineExceeded) {
		return nil
	}
	return model.WithDetails(model.NewBiz(model.CodeRevalidateTimeout, "version propagation exceeded the policy revalidate window"), map[string]any{
		"cache_key":         identity.Key,
		"revalidate_window": p.RevalidateWindowMs,
	})
}

func (e *Engine) beginRevalidation(ctx context.Context, p *policy.Snapshot, identity cache.Identity, requestID, nodeID string) (int64, int64, error) {
	nowMs := e.clock.Now().UnixMilli()
	var intentID int64
	var epoch int64
	err := e.store.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		epoch, err = store.KeyEpochTx(ctx, tx, identity.Key)
		if err != nil {
			return err
		}
		undo, err := e.collectUndo(ctx, tx, p, identity)
		if err != nil {
			return err
		}
		payload := store.IntentPayload{
			Undo: undo,
			Details: map[string]any{
				"cache_key": identity.Key,
				"node_id":   nodeID,
				"path":      identity.NormPath,
				"epoch":     epoch,
			},
		}
		intentID, err = store.CreateIntentTx(ctx, tx, "revalidate", requestID, p.Version, payload, nowMs)
		return err
	})
	if err != nil {
		return 0, 0, err
	}
	err = e.store.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		currentEpoch, err := store.KeyEpochTx(ctx, tx, identity.Key)
		if err != nil {
			return err
		}
		if currentEpoch != epoch {
			return model.WithDetails(model.NewBiz(model.CodeVersionRetired, "cache key was purged before revalidation intent applied"), map[string]any{
				"cache_key": identity.Key,
				"epoch":     epoch,
				"current":   currentEpoch,
			})
		}
		entry, err := store.GetEntryTx(ctx, tx, nodeID, identity.Key)
		if err != nil {
			return err
		}
		if entry == nil {
			entry = &store.Entry{
				NodeID: nodeID, CacheKey: identity.Key, NormPath: identity.NormPath, NormQuery: identity.NormQuery,
				Vary: identity.Vary, State: cache.StateMiss, SnapshotVersion: p.Version, UpdatedAtMs: nowMs,
				LastRequestID: requestID, LastIntentID: intentID,
			}
			id, err := store.UpsertEntryTx(ctx, tx, *entry)
			if err != nil {
				return err
			}
			entry.ID = id
		}
		if err := transitionToRevalidatingTx(ctx, tx, entry, requestID, nowMs); err != nil {
			return err
		}
		entry.SnapshotVersion = p.Version
		entry.LastIntentID = intentID
		entry.LastRequestID = requestID
		entry.UpdatedAtMs = nowMs
		if err := updateEntryMetaTx(ctx, tx, *entry); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		_ = e.abortIntent(ctx, intentID, model.AsBiz(err).Code, nowMs)
		return 0, 0, err
	}
	return intentID, epoch, nil
}

func (e *Engine) collectUndo(ctx context.Context, tx *sql.Tx, p *policy.Snapshot, identity cache.Identity) ([]store.EntryUndo, error) {
	undo := make([]store.EntryUndo, 0, len(p.Nodes))
	for _, node := range p.Nodes {
		entry, err := store.GetEntryTx(ctx, tx, node, identity.Key)
		if err != nil {
			return nil, err
		}
		if entry == nil {
			undo = append(undo, store.EntryUndo{Exists: false, Entry: store.Entry{NodeID: node, CacheKey: identity.Key}})
			continue
		}
		undo = append(undo, store.EntryUndo{Exists: true, Entry: *entry})
	}
	return undo, nil
}

func transitionToRevalidatingTx(ctx context.Context, tx *sql.Tx, entry *store.Entry, requestID string, nowMs int64) error {
	if entry.State == cache.StatePurged {
		if err := store.TransitionTx(ctx, tx, *entry, cache.StateMiss, requestID, nowMs); err != nil {
			return err
		}
		entry.State = cache.StateMiss
	}
	if entry.State != cache.StateRevalidating {
		if err := store.TransitionTx(ctx, tx, *entry, cache.StateRevalidating, requestID, nowMs); err != nil {
			return err
		}
		entry.State = cache.StateRevalidating
	}
	return nil
}

func updateEntryMetaTx(ctx context.Context, tx *sql.Tx, entry store.Entry) error {
	_, err := tx.ExecContext(ctx, `UPDATE cache_entries SET snapshot_version=?,last_request_id=?,last_intent_id=?,updated_at_ms=? WHERE id=?`,
		entry.SnapshotVersion, entry.LastRequestID, entry.LastIntentID, entry.UpdatedAtMs, entry.ID)
	return err
}

func (e *Engine) finishOriginSuccess(ctx context.Context, p *policy.Snapshot, identity cache.Identity, requestID, nodeID string, intentID, epoch int64, res *OriginResult, durationMs int64) (int, error) {
	nowMs := e.clock.Now().UnixMilli()
	freshUntil, staleUntil := cache.ComputeWindows(nowMs, p.TTLMs, p.SWRMs)
	entriesUpdated := 0
	var retiredErr error
	err := e.store.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		currentEpoch, err := store.KeyEpochTx(ctx, tx, identity.Key)
		if err != nil {
			return err
		}
		if currentEpoch != epoch {
			// Purge won the race. Do not resurrect the old origin result.
			entry, _ := store.GetEntryTx(ctx, tx, nodeID, identity.Key)
			if entry != nil && entry.State == cache.StateRevalidating {
				to := cache.StateMiss
				if entry.ETag != "" {
					to = cache.StateStale
				}
				_ = store.TransitionTx(ctx, tx, *entry, to, requestID, nowMs)
			}
			if err := store.AbortIntentTx(ctx, tx, intentID, model.CodeVersionRetired, nowMs); err != nil {
				return err
			}
			if err := store.RecordOriginAttemptTx(ctx, tx, requestID, nodeID, identity.Key, "retired", model.CodeVersionRetired, res.ETag, durationMs, nowMs); err != nil {
				return err
			}
			retiredErr = model.WithDetails(model.NewBiz(model.CodeVersionRetired, "origin result was retired by a concurrent purge"), map[string]any{
				"cache_key": identity.Key,
				"epoch":     epoch,
				"current":   currentEpoch,
			})
			return nil
		}
		if err := store.RecordOriginSuccessTx(ctx, tx, store.OriginObject{
			CacheKey: identity.Key, ETag: res.ETag, Body: res.Body, BodyLength: int64(len(res.Body)),
			ContentType: res.ContentType, Tags: res.Tags, VersionSeq: 1, UpdatedAtMs: nowMs,
		}); err != nil {
			return err
		}
		for _, node := range p.Nodes {
			entry, err := store.GetEntryTx(ctx, tx, node, identity.Key)
			if err != nil {
				return err
			}
			if entry == nil {
				entry = &store.Entry{
					NodeID: node, CacheKey: identity.Key, NormPath: identity.NormPath, NormQuery: identity.NormQuery,
					Vary: identity.Vary, State: cache.StateMiss, SnapshotVersion: p.Version, UpdatedAtMs: nowMs,
					LastRequestID: requestID, LastIntentID: intentID,
				}
				id, err := store.UpsertEntryTx(ctx, tx, *entry)
				if err != nil {
					return err
				}
				entry.ID = id
			}
			if err := transitionToRevalidatingTx(ctx, tx, entry, requestID, nowMs); err != nil {
				return err
			}
			entry.ETag = res.ETag
			entry.Body = append([]byte(nil), res.Body...)
			entry.BodyLength = int64(len(res.Body))
			entry.ContentType = res.ContentType
			entry.Tags = append([]string(nil), res.Tags...)
			entry.SnapshotVersion = p.Version
			entry.WrittenAtMs = nowMs
			entry.FreshUntilMs = freshUntil
			entry.StaleUntilMs = staleUntil
			entry.UpdatedAtMs = nowMs
			entry.LastRequestID = requestID
			entry.LastIntentID = intentID
			entry.State = cache.StateRevalidating
			id, err := store.UpsertEntryTx(ctx, tx, *entry)
			if err != nil {
				return err
			}
			entry.ID = id
			if err := store.TransitionTx(ctx, tx, *entry, cache.StateFresh, requestID, nowMs); err != nil {
				return err
			}
			entry.State = cache.StateFresh
			if err := store.ReplaceIndexesTx(ctx, tx, *entry, true); err != nil {
				return err
			}
			entriesUpdated++
		}
		if err := store.RecordOriginAttemptTx(ctx, tx, requestID, nodeID, identity.Key, "success", "", res.ETag, durationMs, nowMs); err != nil {
			return err
		}
		return store.MarkIntentCommittedTx(ctx, tx, intentID, nowMs)
	})
	if err != nil {
		_ = e.abortIntent(context.Background(), intentID, model.AsBiz(err).Code, nowMs)
		return 0, err
	}
	if retiredErr != nil {
		return 0, retiredErr
	}
	return entriesUpdated, nil
}

func (e *Engine) finishOriginFailure(ctx context.Context, p *policy.Snapshot, identity cache.Identity, requestID, nodeID string, intentID, epoch int64, originErr error, durationMs int64) error {
	nowMs := e.clock.Now().UnixMilli()
	be := model.AsBiz(originErr)
	return e.store.Do(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		currentEpoch, err := store.KeyEpochTx(ctx, tx, identity.Key)
		if err != nil {
			return err
		}
		if currentEpoch == epoch {
			entry, err := store.GetEntryTx(ctx, tx, nodeID, identity.Key)
			if err != nil {
				return err
			}
			if entry != nil && entry.State == cache.StateRevalidating {
				to := cache.StateMiss
				if entry.ETag != "" {
					to = cache.StateStale
				}
				if err := store.TransitionTx(ctx, tx, *entry, to, requestID, nowMs); err != nil {
					return err
				}
			}
		}
		if err := store.RecordOriginAttemptTx(ctx, tx, requestID, nodeID, identity.Key, "failed", be.Code, "", durationMs, nowMs); err != nil {
			return err
		}
		return store.AbortIntentTx(ctx, tx, intentID, be.Code, nowMs)
	})
}

func (e *Engine) abortIntent(ctx context.Context, intentID int64, code string, nowMs int64) error {
	if intentID == 0 {
		return nil
	}
	return e.store.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return store.AbortIntentTx(ctx, tx, intentID, code, nowMs)
	})
}

func (e *Engine) markStale(ctx context.Context, p *policy.Snapshot, entry *store.Entry, requestID string) error {
	if entry == nil || entry.State != cache.StateFresh {
		return nil
	}
	nowMs := e.clock.Now().UnixMilli()
	var intentID int64
	err := e.store.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		undo := []store.EntryUndo{{Exists: true, Entry: *entry}}
		var err error
		intentID, err = store.CreateIntentTx(ctx, tx, "ttl_transition", requestID, p.Version, store.IntentPayload{Undo: undo}, nowMs)
		return err
	})
	if err != nil {
		return err
	}
	err = e.store.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		fresh, err := store.GetEntryTx(ctx, tx, entry.NodeID, entry.CacheKey)
		if err != nil {
			return err
		}
		if fresh == nil || fresh.State != cache.StateFresh {
			if err := store.AbortIntentTx(ctx, tx, intentID, model.CodeInvalidTransition, nowMs); err != nil {
				return err
			}
			return model.NewBiz(model.CodeInvalidTransition, "entry is no longer FRESH")
		}
		if err := store.TransitionTx(ctx, tx, *fresh, cache.StateStale, requestID, nowMs); err != nil {
			return err
		}
		return store.MarkIntentCommittedTx(ctx, tx, intentID, nowMs)
	})
	if err != nil {
		_ = e.abortIntent(context.Background(), intentID, model.AsBiz(err).Code, nowMs)
		return err
	}
	entry.State = cache.StateStale
	return nil
}

func (e *Engine) originFlightMeta(p *policy.Snapshot, identity cache.Identity, nodeID string, kind scheduler.TaskKind) scheduler.FlightMeta {
	nodeCfg, _ := e.cfg.Node(nodeID)
	heat := e.Heat(identity.Key)
	return scheduler.FlightMeta{
		FlightKey: fmt.Sprintf("%s:%d:%s", kind, p.Version, identity.Key),
		TaskID:    fmt.Sprintf("%s:%d:%s", kind, p.Version, identity.Key),
		OriginID:  nodeCfg.OriginID,
		NodeID:    nodeID,
		Key:       identity.Key,
		Priority:  nodeCfg.Priority,
		Heat:      heat,
		CostMs:    p.Scheduler.DefaultTaskCostMs,
		Kind:      kind,
	}
}

func (e *Engine) coalescedOrigin(ctx context.Context, p *policy.Snapshot, identity cache.Identity, headers http.Header, fault FaultSpec, requestID, nodeID string) (*flightOutcome, bool, error) {
	meta := e.originFlightMeta(p, identity, nodeID, scheduler.KindOrigin)
	waitMs := policy.EffectiveRevalidateTimeout(p.RevalidateTimeoutMs, e.cfg.RevalidateTimeout)
	value, coalesced, err := e.coalescer.Do(ctx, meta, time.Duration(waitMs)*time.Millisecond, e.revalidationFn(p, identity, headers, fault, requestID, nodeID))
	if err != nil {
		return nil, coalesced, err
	}
	out, ok := value.(*flightOutcome)
	if !ok {
		return nil, coalesced, model.NewBiz(model.CodeInternal, "coalesced origin returned unexpected result")
	}
	if coalesced {
		e.counters.coalesced.Add(1)
	}
	return out, coalesced, nil
}

func (e *Engine) asyncOrigin(p *policy.Snapshot, identity cache.Identity, headers http.Header, fault FaultSpec, requestID, nodeID string) (bool, error) {
	meta := e.originFlightMeta(p, identity, nodeID, scheduler.KindOrigin)
	return e.coalescer.DoAsync(meta, e.revalidationFn(p, identity, headers, fault, requestID, nodeID))
}

func EntryErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var be *model.BizError
	if errors.As(err, &be) {
		return be.Code
	}
	return model.CodeInternal
}
