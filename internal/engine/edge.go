package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cdn-edge-consistency/internal/cache"
	"cdn-edge-consistency/internal/model"
	"cdn-edge-consistency/internal/policy"
	"cdn-edge-consistency/internal/store"
)

type errorEnvelope struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Details   map[string]any `json:"details,omitempty"`
	RequestID string         `json:"request_id,omitempty"`
	TraceID   string         `json:"trace_id,omitempty"`
}

func errorResponse(code, message string, details map[string]any, requestID, traceID string) Response {
	body, _ := json.Marshal(errorEnvelope{Code: code, Message: message, Details: details, RequestID: requestID, TraceID: traceID})
	return Response{
		Status: model.StatusForCode(code),
		Body:   body,
		Headers: map[string]string{
			"Content-Type": "application/json; charset=utf-8",
		},
	}
}

func (e *Engine) HandleEdge(ctx context.Context, r *http.Request, nodeID, rawPath, rawQuery string) Response {
	fault, cleanQuery, err := ExtractControls(r, nodeID)
	if err != nil {
		requestID := e.newRequestID(firstNonEmpty(fault.RequestID, r.Header.Get("X-Request-ID"), r.Header.Get("Idempotency-Key")))
		return e.standaloneError(ctx, r, requestID, nodeID, rawPath, rawQuery, 0, model.AsBiz(err))
	}
	requestID := e.newRequestID(fault.RequestID)
	nowMs := e.clock.Now().UnixMilli()

	var requestedVersion int64
	if v := strings.TrimSpace(r.Header.Get("X-Policy-Version")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return e.standaloneError(ctx, r, requestID, nodeID, rawPath, cleanQuery, 0, model.NewBiz(model.CodeBadRequest, "X-Policy-Version must be a positive integer"))
		}
		requestedVersion = n
	}
	p, err := e.SnapshotFor(ctx, requestedVersion)
	if err != nil {
		return e.standaloneError(ctx, r, requestID, nodeID, rawPath, cleanQuery, requestedVersion, model.AsBiz(err))
	}
	if _, ok := e.cfg.Node(nodeID); !ok || !p.CoversNode(nodeID) {
		return e.standaloneError(ctx, r, requestID, nodeID, rawPath, cleanQuery, p.Version, model.WithDetails(model.NewBiz(model.CodeNodeUnknown, "edge node is not covered by the bound policy snapshot"), map[string]any{
			"node_id": nodeID, "snapshot_version": p.Version,
		}))
	}

	trace := NewTrace(requestID, requestID, nodeID, r.Method, rawPath, cleanQuery, p.Version, nowMs)
	trace.CacheState = string(cache.StateMiss)
	trace.Step("request", "accepted", "", "", "", map[string]any{"node_id": nodeID, "snapshot_version": p.Version})

	owner, replay, waitErr := e.claimOrRegister(ctx, requestID, r.Method, nodeID, p.Version)
	if waitErr != nil {
		return e.standaloneError(ctx, r, requestID, nodeID, rawPath, cleanQuery, p.Version, model.AsBiz(waitErr))
	}
	if !owner {
		if replay == nil {
			replay = &Response{Status: model.StatusForCode(model.CodeRequestAborted), Headers: map[string]string{"Content-Type": "application/json"}}
			replay.Body, _ = json.Marshal(errorEnvelope{Code: model.CodeRequestAborted, Message: "an earlier request with this id did not produce a response", RequestID: requestID})
		}
		replay.Headers["X-Idempotent-Replay"] = "true"
		return *replay
	}

	if fault.Kind == "purge_interrupt" {
		be := model.NewBiz(model.CodeBadRequest, "purge_interrupt is only valid on purge admin requests")
		trace.Decision = "ERROR"
		return e.completeOwned(ctx, r, requestID, p, trace, e.failTrace(trace, be, requestID))
	}

	identity, err := cache.BuildIdentity(rawPath, cleanQuery, r.Header, p.Vary, p.KeyRules)
	if err != nil {
		return e.completeOwned(ctx, r, requestID, p, trace, e.failTrace(trace, model.AsBiz(err), requestID))
	}
	trace.NormalizedPath = identity.NormPath
	trace.NormalizedQuery = identity.NormQuery
	trace.Vary = identity.Vary
	trace.CacheKey = identity.Key
	trace.Step("key", "normalized", "", "", "", map[string]any{"path": identity.NormPath, "query": identity.NormQuery, "vary": identity.Vary})

	e.bumpHeat(identity.Key)
	resp := e.serveEdgeOwned(ctx, r, nodeID, p, identity, fault, requestID, trace)
	return e.completeOwned(ctx, r, requestID, p, trace, resp)
}

func (e *Engine) standaloneError(ctx context.Context, r *http.Request, requestID, nodeID, rawPath, rawQuery string, version int64, be *model.BizError) Response {
	trace := NewTrace(requestID, requestID, nodeID, r.Method, rawPath, rawQuery, version, e.clock.Now().UnixMilli())
	trace.Decision = "ERROR"
	trace.SetError(be.Code, be.HTTPStatus, be.Details)
	trace.CacheState = string(cache.StateMiss)
	trace.Step("request", "rejected", "", "", be.Code, nil)
	trace.Step("response", "business_error", "", "", be.Code, nil)
	resp := errorResponse(be.Code, be.Message, be.Details, requestID, requestID)
	trace.Seal(e.clock.Now())
	resp.Headers["X-Request-ID"] = requestID
	resp.Headers["X-Trace-ID"] = requestID
	resp.Headers["X-Trace-Hash"] = trace.DeterministicHash
	if version > 0 {
		resp.Headers["X-Snapshot-Version"] = strconv.FormatInt(version, 10)
	}
	writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = e.store.Do(writeCtx, func(ctx context.Context, tx *sql.Tx) error {
		claimed, err := store.InsertRequestClaimTx(ctx, tx, store.RequestRecord{
			RequestID: requestID, Method: r.Method, NodeID: nodeID, SnapshotVersion: version, CreatedAtMs: e.clock.Now().UnixMilli(),
		})
		if err != nil || !claimed {
			return err
		}
		return store.CompleteRequestTx(ctx, tx, store.RequestRecord{
			RequestID: requestID, Status: "completed", ResponseStatus: resp.Status, ResponseBody: resp.Body,
			ResponseHeaders: resp.Headers, BusinessCode: be.Code, TraceJSON: trace.JSON(), CompletedAtMs: e.clock.Now().UnixMilli(),
		})
	})
	return resp
}

func (e *Engine) serveEdgeOwned(ctx context.Context, r *http.Request, nodeID string, p *policy.Snapshot, identity cache.Identity, fault FaultSpec, requestID string, trace *Trace) Response {
	entry, err := e.store.GetEntry(ctx, nodeID, identity.Key)
	if err != nil {
		be := model.AsBiz(err)
		trace.Step("cache_lookup", "error", "", "", be.Code, nil)
		return e.failTrace(trace, be, requestID)
	}
	nowMs := e.clock.Now().UnixMilli()
	if entry == nil {
		trace.CacheState = string(cache.StateMiss)
		trace.Step("cache_lookup", "miss", "", string(cache.StateMiss), "", nil)
		return e.fetchAndRespond(ctx, r, nodeID, p, identity, fault, requestID, trace, cache.TTLDecision{Class: cache.ClassMiss, State: cache.StateMiss, Expired: true})
	}

	decision := cache.Classify(entry.ETag, entry.State, entry.WrittenAtMs, entry.FreshUntilMs, entry.StaleUntilMs, nowMs, p.SWRMs)
	trace.CacheState = string(entry.State)
	trace.ContentVersion = entry.ETag
	trace.Step("cache_lookup", string(decision.Class), string(entry.State), "", "", map[string]any{"reason": decision.Reason})

	switch decision.Class {
	case cache.ClassFresh:
		e.counters.cacheHits.Add(1)
		trace.Decision = "HIT_FRESH"
		trace.HTTPStatus = http.StatusOK
		trace.Step("response", "fresh_hit", string(cache.StateFresh), string(cache.StateFresh), "", nil)
		return entryResponse(entry, "HIT", p, requestID, trace, false)

	case cache.ClassWaitFlight:
		if decision.ServeStale && entry.ETag != "" {
			e.counters.staleServed.Add(1)
			e.counters.cacheHits.Add(1)
			trace.Decision = "STALE_SERVED_REVALIDATING"
			trace.HTTPStatus = http.StatusOK
			trace.CacheState = string(cache.StateRevalidating)
			trace.Step("response", "stale_while_revalidating", string(cache.StateRevalidating), "", "", nil)
			return entryResponse(entry, "STALE-SWR", p, requestID, trace, true)
		}
		out, coalesced, err := e.coalescedOrigin(ctx, p, identity, r.Header, fault, requestID, nodeID)
		if err != nil {
			return e.originErrorTrace(trace, err, coalesced, requestID)
		}
		return e.outcomeResponse(out, p, requestID, trace, coalesced, "COALESCED_REVALIDATED")

	case cache.ClassStaleSWR:
		if entry.State == cache.StateFresh {
			if err := e.markStale(ctx, p, entry, requestID); err != nil {
				trace.Step("state_transition", "error", string(entry.State), string(cache.StateStale), model.AsBiz(err).Code, nil)
			} else {
				trace.Step("state_transition", "ttl_expired", string(cache.StateFresh), string(cache.StateStale), "", nil)
			}
		}
		e.counters.staleServed.Add(1)
		e.counters.cacheHits.Add(1)
		trace.Decision = "STALE_SERVED_SWR"
		trace.HTTPStatus = http.StatusOK
		trace.CacheState = string(cache.StateStale)
		coalesced, asyncErr := e.asyncOrigin(p, identity, r.Header, fault, requestID, nodeID)
		if asyncErr != nil {
			be := model.AsBiz(asyncErr)
			trace.Step("async_revalidate", "schedule_failed", string(cache.StateStale), "", be.Code, be.Details)
		} else {
			trace.Step("async_revalidate", "scheduled", string(cache.StateStale), "", "", map[string]any{"coalesced": coalesced})
		}
		trace.Step("response", "stale_while_revalidate", string(cache.StateStale), "", "", nil)
		return entryResponse(entry, "STALE-SWR", p, requestID, trace, true)

	case cache.ClassStaleWait, cache.ClassMiss:
		if entry.State == cache.StateFresh {
			_ = e.markStale(ctx, p, entry, requestID)
		}
		return e.fetchAndRespond(ctx, r, nodeID, p, identity, fault, requestID, trace, decision)
	}
	return e.failTrace(trace, model.NewBiz(model.CodeInternal, "unhandled cache decision"), requestID)
}

func (e *Engine) fetchAndRespond(ctx context.Context, r *http.Request, nodeID string, p *policy.Snapshot, identity cache.Identity, fault FaultSpec, requestID string, trace *Trace, decision cache.TTLDecision) Response {
	e.counters.cacheMisses.Add(1)
	out, coalesced, err := e.coalescedOrigin(ctx, p, identity, r.Header, fault, requestID, nodeID)
	if err != nil {
		return e.originErrorTrace(trace, err, coalesced, requestID)
	}
	decisionName := "MISS_FETCH"
	if decision.Class == cache.ClassStaleWait {
		decisionName = "STALE_REVALIDATE"
	}
	if coalesced {
		decisionName = "COALESCED_WAIT"
	}
	return e.outcomeResponse(out, p, requestID, trace, coalesced, decisionName)
}

func (e *Engine) outcomeResponse(out *flightOutcome, p *policy.Snapshot, requestID string, trace *Trace, coalesced bool, decision string) Response {
	trace.Decision = decision
	trace.HTTPStatus = http.StatusOK
	trace.Coalesced = coalesced
	trace.CacheState = string(cache.StateFresh)
	trace.ContentVersion = out.Origin.ETag
	if !coalesced {
		trace.OriginFetches = 1
	}
	trace.Step("origin", "success", "", "", "", map[string]any{
		"etag": out.Origin.ETag, "entries_updated": out.Entries, "coalesced": coalesced,
	})
	trace.Step("cache_commit", "replicated", string(cache.StateRevalidating), string(cache.StateFresh), "", map[string]any{"nodes": len(p.Nodes)})
	trace.Step("response", "origin_body", string(cache.StateFresh), "", "", nil)
	headers := baseHeaders(p, requestID, trace)
	headers["ETag"] = out.Origin.ETag
	headers["Content-Type"] = out.Origin.ContentType
	headers["X-Content-Version"] = strings.Trim(out.Origin.ETag, `"`)
	cacheStatus := "MISS"
	if coalesced {
		cacheStatus = "COALESCED"
		headers["X-Coalesced"] = "true"
	}
	headers["X-Cache-Status"] = cacheStatus
	headers["X-Cache-State"] = string(cache.StateFresh)
	headers["Content-Length"] = strconv.Itoa(len(out.Origin.Body))
	return Response{Status: http.StatusOK, Body: append([]byte(nil), out.Origin.Body...), Headers: headers}
}

func entryResponse(entry *store.Entry, status string, p *policy.Snapshot, requestID string, trace *Trace, stale bool) Response {
	headers := baseHeaders(p, requestID, trace)
	headers["ETag"] = entry.ETag
	headers["Content-Type"] = entry.ContentType
	headers["X-Content-Version"] = strings.Trim(entry.ETag, `"`)
	headers["X-Cache-Status"] = status
	headers["X-Cache-State"] = string(entry.State)
	headers["Content-Length"] = strconv.Itoa(len(entry.Body))
	if stale {
		headers["Warning"] = `110 - "Response is stale"`
	}
	return Response{Status: http.StatusOK, Body: append([]byte(nil), entry.Body...), Headers: headers}
}

func baseHeaders(p *policy.Snapshot, requestID string, trace *Trace) map[string]string {
	return map[string]string{
		"X-Request-ID":       requestID,
		"X-Trace-ID":         trace.TraceID,
		"X-Snapshot-Version": strconv.FormatInt(p.Version, 10),
		"X-Cache-Key":        trace.CacheKey,
	}
}

func (e *Engine) originErrorTrace(trace *Trace, err error, coalesced bool, requestID string) Response {
	be := model.AsBiz(err)
	trace.Decision = "ERROR"
	trace.Coalesced = coalesced
	trace.SetError(be.Code, be.HTTPStatus, be.Details)
	trace.Step("origin", "error", "", "", be.Code, be.Details)
	switch be.Code {
	case model.CodeOriginTimeout, model.CodeOriginRefused, model.CodeBodyCorrupt, model.CodeOriginStatus, model.CodeVersionRetired, model.CodeRevalidateTimeout:
		if !coalesced {
			trace.OriginFetches = 1
		}
	}
	trace.Step("response", "business_error", "", "", be.Code, nil)
	return errorResponse(be.Code, be.Message, be.Details, requestID, trace.TraceID)
}

func (e *Engine) failTrace(trace *Trace, be *model.BizError, requestID string) Response {
	trace.Decision = "ERROR"
	trace.SetError(be.Code, be.HTTPStatus, be.Details)
	trace.Step("response", "business_error", "", "", be.Code, nil)
	return errorResponse(be.Code, be.Message, be.Details, requestID, trace.TraceID)
}

func (e *Engine) claimOrRegister(ctx context.Context, requestID, method, nodeID string, version int64) (bool, *Response, error) {
	e.idemMu.Lock()
	if call, ok := e.idem[requestID]; ok {
		e.idemMu.Unlock()
		select {
		case <-call.done:
			resp := call.response
			resp.Headers = cloneHeaders(call.response.Headers)
			return false, &resp, nil
		case <-ctx.Done():
			return false, nil, model.WithDetails(model.NewBiz(model.CodeRevalidateTimeout, "timed out waiting for duplicate request result"), map[string]any{"request_id": requestID})
		}
	}
	call := &idemCall{done: make(chan struct{}), owner: true}
	e.idem[requestID] = call
	e.idemMu.Unlock()

	nowMs := e.clock.Now().UnixMilli()
	claimed := false
	err := e.store.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		claimed, err = store.InsertRequestClaimTx(ctx, tx, store.RequestRecord{
			RequestID: requestID, Method: method, NodeID: nodeID, SnapshotVersion: version, CreatedAtMs: nowMs,
		})
		return err
	})
	if err != nil {
		e.idemMu.Lock()
		delete(e.idem, requestID)
		close(call.done)
		e.idemMu.Unlock()
		return false, nil, err
	}
	if claimed {
		return true, nil, nil
	}
	rec, err := e.store.GetRequestRecord(ctx, requestID)
	if err != nil {
		e.idemMu.Lock()
		delete(e.idem, requestID)
		close(call.done)
		e.idemMu.Unlock()
		return false, nil, err
	}
	var resp Response
	if rec == nil || rec.Status == "inflight" {
		resp = errorResponse(model.CodeRequestAborted, "request id is already in flight but has no completed result", nil, requestID, requestID)
	} else {
		resp = Response{Status: rec.ResponseStatus, Body: append([]byte(nil), rec.ResponseBody...), Headers: cloneHeaders(rec.ResponseHeaders)}
	}
	e.idemMu.Lock()
	call.response = resp
	call.owner = false
	delete(e.idem, requestID)
	close(call.done)
	e.idemMu.Unlock()
	return false, &resp, nil
}

func (e *Engine) completeOwned(ctx context.Context, r *http.Request, requestID string, p *policy.Snapshot, trace *Trace, resp Response) Response {
	if resp.Headers == nil {
		resp.Headers = map[string]string{}
	}
	for k, v := range baseHeaders(p, requestID, trace) {
		if _, exists := resp.Headers[k]; !exists {
			resp.Headers[k] = v
		}
	}
	trace.Seal(e.clock.Now())
	resp.Headers["X-Trace-Hash"] = trace.DeterministicHash
	resp.Headers["X-Trace-ID"] = trace.TraceID
	if trace.HTTPStatus == 0 {
		trace.HTTPStatus = resp.Status
	}
	rec := store.RequestRecord{
		RequestID: requestID, Method: r.Method, NodeID: trace.NodeID, SnapshotVersion: p.Version,
		Status: "completed", ResponseStatus: resp.Status, ResponseBody: resp.Body,
		ResponseHeaders: resp.Headers, BusinessCode: trace.BusinessCode, TraceJSON: trace.JSON(),
		CompletedAtMs: e.clock.Now().UnixMilli(),
	}
	writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.store.Do(writeCtx, func(ctx context.Context, tx *sql.Tx) error {
		return store.CompleteRequestTx(ctx, tx, rec)
	}); err != nil {
		resp = errorResponse(model.CodeInternal, "failed to persist request completion: "+err.Error(), nil, requestID, trace.TraceID)
	}
	resp.Headers["X-Trace-ID"] = trace.TraceID
	e.idemMu.Lock()
	if call, ok := e.idem[requestID]; ok {
		call.response = cloneResponse(resp)
		delete(e.idem, requestID)
		close(call.done)
	}
	e.idemMu.Unlock()
	return resp
}

func cloneHeaders(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneResponse(in Response) Response {
	return Response{Status: in.Status, Body: append([]byte(nil), in.Body...), Headers: cloneHeaders(in.Headers)}
}

func (e *Engine) TraceFor(ctx context.Context, requestID string) (*Trace, error) {
	rec, err := e.store.GetRequestRecord(ctx, requestID)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, model.NewBiz(model.CodeNotFound, "request trace not found")
	}
	var t Trace
	if err := json.Unmarshal([]byte(rec.TraceJSON), &t); err != nil {
		return nil, fmt.Errorf("decode trace: %w", err)
	}
	return &t, nil
}
