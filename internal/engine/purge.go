package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"cdn-edge-consistency/internal/cache"
	"cdn-edge-consistency/internal/model"
	"cdn-edge-consistency/internal/policy"
	"cdn-edge-consistency/internal/scheduler"
	"cdn-edge-consistency/internal/store"
)

type PurgeRequest struct {
	Kind            string   `json:"kind,omitempty"`
	Path            string   `json:"path,omitempty"`
	Tag             string   `json:"tag,omitempty"`
	SnapshotVersion int64    `json:"snapshot_version,omitempty"`
	InterruptNodes  []string `json:"interrupt_nodes,omitempty"`
	RequestID       string   `json:"request_id,omitempty"`
}

type PurgeResult struct {
	PurgeID         string                `json:"purge_id"`
	Status          string                `json:"status"`
	Kind            string                `json:"kind"`
	Target          string                `json:"target"`
	SnapshotVersion int64                 `json:"snapshot_version"`
	AffectedEntries int64                 `json:"affected_entries"`
	Deliveries      []store.PurgeDelivery `json:"deliveries"`
	IncompleteNodes []string              `json:"incomplete_nodes,omitempty"`
	ErrorCode       string                `json:"error_code,omitempty"`
}

func (e *Engine) Purge(ctx context.Context, req PurgeRequest) (*PurgeResult, error) {
	kind := strings.ToLower(strings.TrimSpace(req.Kind))
	if kind == "" {
		if req.Path != "" {
			kind = "path"
		} else if req.Tag != "" {
			kind = "tag"
		}
	}
	if kind != "path" && kind != "tag" {
		return nil, model.NewBiz(model.CodeBadRequest, "purge kind must be path or tag")
	}
	p, err := e.SnapshotFor(ctx, req.SnapshotVersion)
	if err != nil {
		return nil, err
	}
	target := strings.TrimSpace(req.Path)
	if kind == "path" {
		target, err = cache.NormalizePath(req.Path, p.KeyRules)
		if err != nil {
			return nil, err
		}
	} else {
		target = strings.TrimSpace(req.Tag)
		if target == "" {
			return nil, model.NewBiz(model.CodeBadRequest, "tag must not be empty")
		}
	}
	requestID := strings.TrimSpace(req.RequestID)
	if requestID == "" {
		requestID = e.newRequestID("")
	}
	sum := sha256.Sum256([]byte("purge\x00" + requestID))
	purgeID := "purge-" + hex.EncodeToString(sum[:8])
	if existing, err := e.store.GetPurge(ctx, purgeID); err != nil {
		return nil, err
	} else if existing != nil {
		res := purgeResultFromRecord(existing)
		if existing.Status == "committed" {
			return res, nil
		}
		return res, purgeIncompleteError(existing)
	}
	if err := e.validateInterruptNodes(p, req.InterruptNodes); err != nil {
		return nil, err
	}

	nowMs := e.clock.Now().UnixMilli()
	deadlineWall := time.Now().Add(time.Duration(p.PurgeWindowMs) * time.Millisecond)
	deadline := deadlineWall.UnixMilli()
	nodes := append([]string(nil), p.Nodes...)
	sort.Strings(nodes)
	interrupt := map[string]bool{}
	for _, n := range req.InterruptNodes {
		interrupt[n] = true
	}

	var intentID int64
	err = e.store.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		undo, err := e.collectPurgeUndoTx(ctx, tx, p, kind, target)
		if err != nil {
			return err
		}
		payload := store.IntentPayload{
			Undo: undo,
			Details: map[string]any{
				"kind": kind, "target": target, "nodes": nodes, "purge_id": purgeID,
			},
		}
		intentID, err = store.CreateIntentTx(ctx, tx, "purge", requestID, p.Version, payload, nowMs)
		return err
	})
	if err != nil {
		return nil, err
	}
	err = e.store.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rec := store.PurgeRecord{
			PurgeID: purgeID, Kind: kind, Target: target, RequestID: requestID,
			SnapshotVersion: p.Version, DeadlineMs: deadline, PurgeWindowMs: p.PurgeWindowMs,
			Details: map[string]any{"intent_id": intentID},
		}
		return store.CreatePurgeSessionTx(ctx, tx, rec, nodes, nowMs)
	})
	if err != nil {
		_ = e.abortIntent(context.Background(), intentID, model.AsBiz(err).Code, nowMs)
		return nil, err
	}

	counts := map[string]int64{}
	for _, node := range nodes {
		filter := store.EntryFilter{NodeID: node, NormPath: target}
		if kind == "tag" {
			filter = store.EntryFilter{NodeID: node, Tag: target}
		}
		entries, err := e.store.ListEntries(ctx, filter)
		if err != nil {
			_ = e.failPurge(context.Background(), purgeID, intentID, nodes, model.AsBiz(err).Code, nowMs)
			return nil, err
		}
		counts[node] = int64(len(entries))
	}

	type deliveryResult struct {
		node string
		err  error
	}
	results := make(chan deliveryResult, len(nodes))
	purgeCtx, cancel := context.WithDeadline(ctx, time.UnixMilli(deadline))
	defer cancel()
	for _, node := range nodes {
		node := node
		nodeCfg, _ := e.cfg.Node(node)
		task := scheduler.Task{
			ID: fmt.Sprintf("%s:%s", purgeID, node), Kind: scheduler.KindPurge, OriginID: nodeCfg.OriginID,
			NodeID: node, Key: target, Priority: nodeCfg.Priority, Heat: e.Heat(target), CostMs: p.Scheduler.DefaultTaskCostMs,
			Run: func(taskCtx context.Context) error {
				select {
				case <-taskCtx.Done():
					return model.WithDetails(model.NewBiz(model.CodePurgeIncomplete, "purge propagation canceled before delivery"), map[string]any{"node_id": node})
				default:
				}
				if time.Now().UnixMilli() > deadline {
					return model.WithDetails(model.NewBiz(model.CodePurgeIncomplete, "purge propagation window exhausted"), map[string]any{"node_id": node, "deadline_ms": deadline})
				}
				if interrupt[node] {
					return model.WithDetails(model.NewBiz(model.CodePurgeIncomplete, "injected purge propagation interruption"), map[string]any{"node_id": node, "injected": true})
				}
				return nil
			},
		}
		ch, err := e.sched.SubmitTask(ctx, task)
		if err != nil {
			results <- deliveryResult{node: node, err: err}
			continue
		}
		go func(node string, ch <-chan error) {
			select {
			case err := <-ch:
				results <- deliveryResult{node: node, err: err}
			case <-purgeCtx.Done():
				results <- deliveryResult{node: node, err: purgeCtx.Err()}
			}
		}(node, ch)
	}

	failed := map[string]error{}
	done := map[string]bool{}
	timedOut := false
	for len(done) < len(nodes) {
		select {
		case r := <-results:
			done[r.node] = true
			if r.err != nil {
				failed[r.node] = r.err
			}
		case <-purgeCtx.Done():
			timedOut = true
		}
		if timedOut {
			break
		}
	}
	if timedOut {
		for _, node := range nodes {
			if !done[node] {
				failed[node] = context.DeadlineExceeded
			}
		}
	}

	if len(failed) > 0 {
		incomplete := make([]string, 0, len(failed))
		for n := range failed {
			incomplete = append(incomplete, n)
		}
		sort.Strings(incomplete)
		details := map[string]any{"purge_id": purgeID, "incomplete_nodes": incomplete, "affected_entries": 0}
		failureByNode := map[string]string{}
		for n, ferr := range failed {
			code := model.AsBiz(ferr).Code
			if ferr == context.DeadlineExceeded || ferr == context.Canceled || code == model.CodeInternal {
				code = model.CodePurgeIncomplete
			}
			failureByNode[n] = code
		}
		_ = e.failPurgeWithNodes(context.Background(), purgeID, intentID, nodes, failureByNode, nowMs)
		return &PurgeResult{
			PurgeID: purgeID, Status: "aborted", Kind: kind, Target: target, SnapshotVersion: p.Version,
			Deliveries: e.loadDeliveries(context.Background(), purgeID), IncompleteNodes: incomplete, ErrorCode: model.CodePurgeIncomplete,
		}, model.WithDetails(model.NewBiz(model.CodePurgeIncomplete, "purge propagation did not complete; no node was purged"), details)
	}

	// All simulated deliveries completed. Re-evaluate the target and purge all
	// applicable node entries in one writer transaction. This is the only
	// visibility-changing purge step.
	var affected int64
	err = e.store.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		affected, err = store.PurgeEntriesTx(ctx, tx, purgeID, kind, target, nodes, requestID, nowMs)
		if err != nil {
			return err
		}
		if err := store.CompletePurgeTx(ctx, tx, purgeID, affected, nowMs); err != nil {
			return err
		}
		for node, count := range counts {
			if _, err := tx.ExecContext(ctx, `UPDATE purge_deliveries SET entry_count=? WHERE purge_id=? AND node_id=?`, count, purgeID, node); err != nil {
				return err
			}
		}
		if err := store.MarkIntentCommittedTx(ctx, tx, intentID, nowMs); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		_ = e.failPurgeWithNodes(context.Background(), purgeID, intentID, nodes, map[string]string{}, nowMs)
		return nil, err
	}
	e.counters.purges.Add(1)
	rec, _ := e.store.GetPurge(ctx, purgeID)
	if rec == nil {
		return &PurgeResult{PurgeID: purgeID, Status: "committed", Kind: kind, Target: target, SnapshotVersion: p.Version, AffectedEntries: affected}, nil
	}
	return purgeResultFromRecord(rec), nil
}

func (e *Engine) validateInterruptNodes(p *policy.Snapshot, nodes []string) error {
	for _, n := range nodes {
		if !p.CoversNode(n) {
			return model.WithDetails(model.NewBiz(model.CodeNodeUnknown, "interrupt node is not covered by the policy snapshot"), map[string]any{"node_id": n})
		}
	}
	return nil
}

func (e *Engine) collectPurgeUndoTx(ctx context.Context, tx *sql.Tx, p *policy.Snapshot, kind, target string) ([]store.EntryUndo, error) {
	undo := []store.EntryUndo{}
	for _, node := range p.Nodes {
		var rows *sql.Rows
		var err error
		switch kind {
		case "path":
			rows, err = tx.QueryContext(ctx, `SELECT `+entryColumnList()+` FROM cache_entries WHERE node_id=? AND norm_path=? ORDER BY id`, node, target)
		case "tag":
			rows, err = tx.QueryContext(ctx, `SELECT `+entryColumnList()+` FROM cache_entries WHERE node_id=? AND id IN (SELECT entry_id FROM entry_tags WHERE tag=? AND active=1) ORDER BY id`, node, target)
		default:
			return nil, model.NewBiz(model.CodeBadRequest, "invalid purge kind")
		}
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			entry, err := scanEntryRows(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			undo = append(undo, store.EntryUndo{Exists: true, Entry: entry})
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return undo, nil
}

func entryColumnList() string {
	return `id,node_id,cache_key,norm_path,norm_query,vary_json,etag,body,body_length,content_type,tags_json,state,snapshot_version,written_at_ms,fresh_until_ms,stale_until_ms,updated_at_ms,last_request_id,last_intent_id`
}

func scanEntryRows(rows *sql.Rows) (store.Entry, error) {
	var e store.Entry
	var varyJSON, tagsJSON, state string
	err := rows.Scan(&e.ID, &e.NodeID, &e.CacheKey, &e.NormPath, &e.NormQuery, &varyJSON, &e.ETag, &e.Body, &e.BodyLength, &e.ContentType, &tagsJSON, &state,
		&e.SnapshotVersion, &e.WrittenAtMs, &e.FreshUntilMs, &e.StaleUntilMs, &e.UpdatedAtMs, &e.LastRequestID, &e.LastIntentID)
	if err != nil {
		return e, err
	}
	_ = json.Unmarshal([]byte(varyJSON), &e.Vary)
	_ = json.Unmarshal([]byte(tagsJSON), &e.Tags)
	e.State = cache.State(state)
	return e, nil
}

func (e *Engine) failPurge(ctx context.Context, purgeID string, intentID int64, nodes []string, code string, nowMs int64) error {
	m := map[string]string{}
	for _, n := range nodes {
		m[n] = code
	}
	return e.failPurgeWithNodes(ctx, purgeID, intentID, nodes, m, nowMs)
}

func (e *Engine) failPurgeWithNodes(ctx context.Context, purgeID string, intentID int64, nodes []string, failures map[string]string, nowMs int64) error {
	incomplete := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if _, failed := failures[n]; failed || len(failures) == 0 {
			incomplete = append(incomplete, n)
		}
	}
	return e.store.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		for _, n := range nodes {
			code, failed := failures[n]
			status := "complete"
			if failed || len(failures) == 0 {
				status = "aborted"
				if code == "" {
					code = model.CodePurgeIncomplete
				}
			}
			if err := store.SetDeliveryStatusTx(ctx, tx, purgeID, n, status, code, 0, nowMs); err != nil {
				return err
			}
		}
		if err := store.AbortPurgeTx(ctx, tx, purgeID, model.CodePurgeIncomplete, incomplete, nowMs); err != nil {
			return err
		}
		return store.AbortIntentTx(ctx, tx, intentID, model.CodePurgeIncomplete, nowMs)
	})
}

func purgeIncompleteError(p *store.PurgeRecord) error {
	return model.WithDetails(model.NewBiz(model.CodePurgeIncomplete, "purge previously aborted; no node was purged"), map[string]any{
		"purge_id": p.PurgeID, "incomplete_nodes": p.IncompleteNodes, "error_code": p.ErrorCode,
	})
}

func purgeResultFromRecord(p *store.PurgeRecord) *PurgeResult {
	return &PurgeResult{
		PurgeID: p.PurgeID, Status: p.Status, Kind: p.Kind, Target: p.Target,
		SnapshotVersion: p.SnapshotVersion, AffectedEntries: p.AffectedEntries,
		Deliveries: p.Deliveries, IncompleteNodes: p.IncompleteNodes, ErrorCode: p.ErrorCode,
	}
}

func (e *Engine) loadDeliveries(ctx context.Context, purgeID string) []store.PurgeDelivery {
	rec, err := e.store.GetPurge(ctx, purgeID)
	if err != nil || rec == nil {
		return []store.PurgeDelivery{}
	}
	return rec.Deliveries
}

func (e *Engine) ListPurges(ctx context.Context, limit int) ([]store.PurgeRecord, error) {
	return e.store.ListPurges(ctx, limit)
}

func (e *Engine) GetPurge(ctx context.Context, id string) (*store.PurgeRecord, error) {
	rec, err := e.store.GetPurge(ctx, id)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, model.NewBiz(model.CodeNotFound, "purge record not found")
	}
	return rec, nil
}
