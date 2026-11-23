package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
)

const maxTraceRequests = 10000

type TraceRequest struct {
	Sequence       int    `json:"sequence"`
	RequestID      string `json:"request_id"`
	ArrivalMS      int64  `json:"arrival_ms"`
	ModelID        string `json:"model_id"`
	Samples        int64  `json:"samples"`
	LatencyLimitUS int64  `json:"max_batch_latency_us"`
	Priority       int64  `json:"priority"`
}

type Trace struct {
	TraceID     string         `json:"trace_id"`
	Version     string         `json:"version"`
	ContentHash string         `json:"content_hash"`
	Requests    []TraceRequest `json:"requests"`
}

type rawTrace struct {
	TraceID  string            `json:"trace_id"`
	Version  string            `json:"version"`
	Requests []json.RawMessage `json:"requests"`
}

type rawTraceRequest struct {
	RequestID         string          `json:"request_id"`
	ArrivalMS         json.RawMessage `json:"arrival_ms"`
	ModelID           string          `json:"model_id"`
	Samples           json.RawMessage `json:"samples"`
	MaxBatchLatency   json.RawMessage `json:"max_batch_latency"`
	MaxBatchLatencyMS json.RawMessage `json:"max_batch_latency_ms"`
	LatencyLimit      json.RawMessage `json:"latency_limit"`
	ExpectedLatency   json.RawMessage `json:"expected_batch_latency"`
	Priority          json.RawMessage `json:"priority"`
}

func ParseTrace(data []byte, catalog *ModelCatalog) (*Trace, error) {
	if catalog == nil {
		return nil, NewBizError(CodeSpecInvalid, "model catalog is not initialized", nil)
	}
	var raw rawTrace
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, traceError("trace is not valid JSON", nil, err)
	}
	if err := validateIdentifier("trace_id", raw.TraceID); err != nil {
		return nil, traceError("invalid trace_id", map[string]any{"trace_id": raw.TraceID}, err)
	}
	if err := validateIdentifier("version", raw.Version); err != nil {
		return nil, traceError("invalid trace version", map[string]any{"version": raw.Version}, err)
	}
	if len(raw.Requests) == 0 {
		return nil, traceError("trace must contain at least one request", map[string]any{"trace_id": raw.TraceID}, nil)
	}
	if len(raw.Requests) > maxTraceRequests {
		return nil, traceError("trace contains too many requests", map[string]any{"trace_id": raw.TraceID, "limit": maxTraceRequests, "actual": len(raw.Requests)}, nil)
	}

	seen := make(map[string]struct{}, len(raw.Requests))
	requests := make([]TraceRequest, 0, len(raw.Requests))
	var previousArrival int64
	for i, rawReq := range raw.Requests {
		var r rawTraceRequest
		if err := json.Unmarshal(rawReq, &r); err != nil {
			return nil, traceError("request is not valid JSON", map[string]any{"index": i}, err)
		}
		ctx := map[string]any{"index": i, "request_id": r.RequestID}
		if !identifierRE.MatchString(r.RequestID) {
			return nil, traceError("request_id is missing or invalid", ctx, nil)
		}
		if _, exists := seen[r.RequestID]; exists {
			return nil, traceError("duplicate request_id in trace", ctx, nil)
		}
		seen[r.RequestID] = struct{}{}
		arrival, err := ParseNonNegativeIntRaw(r.ArrivalMS, "arrival_ms")
		if err != nil {
			return nil, traceError("invalid arrival_ms", ctx, err)
		}
		if i > 0 && arrival < previousArrival {
			return nil, traceError("arrival_ms must be non-decreasing", map[string]any{
				"index": i, "request_id": r.RequestID, "previous_arrival_ms": previousArrival, "arrival_ms": arrival,
			}, nil)
		}
		previousArrival = arrival
		model, ok := catalog.ByID[r.ModelID]
		if !ok {
			return nil, traceError("unknown model_id", map[string]any{"index": i, "request_id": r.RequestID, "model_id": r.ModelID}, nil)
		}
		samples, err := ParsePositiveIntRaw(r.Samples, "samples")
		if err != nil {
			return nil, traceError("invalid samples", ctx, err)
		}
		if samples > math.MaxInt32 {
			return nil, traceError("samples is too large for deterministic batching", map[string]any{"request_id": r.RequestID, "samples": samples}, nil)
		}
		latencyRaw := firstRaw(r.MaxBatchLatency, r.MaxBatchLatencyMS, r.LatencyLimit, r.ExpectedLatency)
		if len(latencyRaw) == 0 {
			// max_batch_latency_ms is milliseconds, while the other aliases are
			// explicit durations. The canonical sample uses max_batch_latency.
			return nil, traceError("max_batch_latency is required", ctx, nil)
		}
		latencyUS, err := ParseLatencyLimitRaw(latencyRaw)
		if err != nil {
			return nil, traceError("invalid max_batch_latency", ctx, err)
		}
		if latencyUS <= 0 {
			return nil, traceError("max_batch_latency must be positive", ctx, nil)
		}
		priority, err := ParseNonNegativeIntRaw(r.Priority, "priority")
		if err != nil {
			return nil, traceError("invalid priority", ctx, err)
		}
		if priority > math.MaxInt32 {
			return nil, traceError("priority is too large", ctx, nil)
		}
		requests = append(requests, TraceRequest{
			Sequence: i + 1, RequestID: r.RequestID, ArrivalMS: arrival,
			ModelID: model.ModelID, Samples: samples, LatencyLimitUS: latencyUS, Priority: priority,
		})
	}
	canonical := struct {
		TraceID  string         `json:"trace_id"`
		Version  string         `json:"version"`
		Requests []TraceRequest `json:"requests"`
	}{TraceID: raw.TraceID, Version: raw.Version, Requests: requests}
	canonicalJSON, _ := json.Marshal(canonical)
	sum := sha256.Sum256(canonicalJSON)
	return &Trace{TraceID: raw.TraceID, Version: raw.Version, Requests: requests, ContentHash: hex.EncodeToString(sum[:])}, nil
}

func traceError(message string, context map[string]any, cause error) error {
	if context == nil {
		context = make(map[string]any)
	}
	if cause != nil {
		context["reason"] = cause.Error()
	}
	return NewBizError(CodeTraceInvalid, message, context)
}

func (t *Trace) CanonicalJSON() []byte {
	b, _ := json.Marshal(struct {
		TraceID     string         `json:"trace_id"`
		Version     string         `json:"version"`
		ContentHash string         `json:"content_hash"`
		Requests    []TraceRequest `json:"requests"`
	}{t.TraceID, t.Version, t.ContentHash, t.Requests})
	return b
}

func (t *Trace) RequestByID(id string) (TraceRequest, bool) {
	for _, r := range t.Requests {
		if r.RequestID == id {
			return r, true
		}
	}
	return TraceRequest{}, false
}

func (t *Trace) CloneRequests() []TraceRequest {
	out := make([]TraceRequest, len(t.Requests))
	copy(out, t.Requests)
	return out
}

func (t *Trace) ValidateAgainst(catalog *ModelCatalog) error {
	for _, r := range t.Requests {
		if _, ok := catalog.ByID[r.ModelID]; !ok {
			return traceError("unknown model_id", map[string]any{"request_id": r.RequestID, "model_id": r.ModelID}, nil)
		}
		if r.Samples <= 0 || r.LatencyLimitUS <= 0 || r.ArrivalMS < 0 {
			return traceError("invalid normalized request", map[string]any{"request_id": r.RequestID}, fmt.Errorf("non-positive value"))
		}
	}
	return nil
}
