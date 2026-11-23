package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

type TraceStep struct {
	Seq     int            `json:"seq"`
	Phase   string         `json:"phase"`
	Outcome string         `json:"outcome"`
	From    string         `json:"from,omitempty"`
	To      string         `json:"to,omitempty"`
	Code    string         `json:"code,omitempty"`
	Detail  map[string]any `json:"detail,omitempty"`
}

type Trace struct {
	TraceID           string         `json:"trace_id"`
	RequestID         string         `json:"request_id"`
	NodeID            string         `json:"node_id"`
	Method            string         `json:"method"`
	RawPath           string         `json:"raw_path"`
	RawQuery          string         `json:"raw_query"`
	NormalizedPath    string         `json:"normalized_path"`
	NormalizedQuery   string         `json:"normalized_query"`
	Vary              any            `json:"vary"`
	CacheKey          string         `json:"cache_key"`
	SnapshotVersion   int64          `json:"snapshot_version"`
	Decision          string         `json:"decision"`
	BusinessCode      string         `json:"business_code,omitempty"`
	HTTPStatus        int            `json:"http_status"`
	CacheState        string         `json:"cache_state,omitempty"`
	ContentVersion    string         `json:"content_version,omitempty"`
	Coalesced         bool           `json:"coalesced"`
	OriginFetches     int            `json:"origin_fetches"`
	Steps             []TraceStep    `json:"steps"`
	CreatedAtMs       int64          `json:"created_at_ms"`
	LatencyMs         int64          `json:"latency_ms"`
	DeterministicHash string         `json:"deterministic_hash"`
	ErrorDetails      map[string]any `json:"error_details,omitempty"`
}

func NewTrace(traceID, requestID, nodeID, method, rawPath, rawQuery string, snapshotVersion, nowMs int64) *Trace {
	return &Trace{
		TraceID: traceID, RequestID: requestID, NodeID: nodeID, Method: method,
		RawPath: rawPath, RawQuery: rawQuery, SnapshotVersion: snapshotVersion,
		Steps: []TraceStep{}, CreatedAtMs: nowMs, Vary: []any{},
	}
}

func (t *Trace) Step(phase, outcome, from, to, code string, detail map[string]any) {
	t.Steps = append(t.Steps, TraceStep{Seq: len(t.Steps), Phase: phase, Outcome: outcome, From: from, To: to, Code: code, Detail: detail})
}

func (t *Trace) SetError(code string, status int, details map[string]any) {
	t.BusinessCode = code
	t.HTTPStatus = status
	t.ErrorDetails = details
}

func (t *Trace) Seal(now time.Time) {
	t.LatencyMs = now.UnixMilli() - t.CreatedAtMs
	if t.LatencyMs < 0 {
		t.LatencyMs = 0
	}
	t.DeterministicHash = t.deterministicHash()
}

func (t *Trace) deterministicHash() string {
	type canonical struct {
		NodeID          string         `json:"node_id"`
		Method          string         `json:"method"`
		NormalizedPath  string         `json:"normalized_path"`
		NormalizedQuery string         `json:"normalized_query"`
		Vary            any            `json:"vary"`
		CacheKey        string         `json:"cache_key"`
		Snapshot        int64          `json:"snapshot_version"`
		Decision        string         `json:"decision"`
		BusinessCode    string         `json:"business_code"`
		HTTPStatus      int            `json:"http_status"`
		CacheState      string         `json:"cache_state"`
		ContentVersion  string         `json:"content_version"`
		Coalesced       bool           `json:"coalesced"`
		OriginFetches   int            `json:"origin_fetches"`
		Steps           []TraceStep    `json:"steps"`
		ErrorDetails    map[string]any `json:"error_details"`
	}
	c := canonical{
		NodeID: t.NodeID, Method: t.Method, NormalizedPath: t.NormalizedPath, NormalizedQuery: t.NormalizedQuery,
		Vary: t.Vary, CacheKey: t.CacheKey, Snapshot: t.SnapshotVersion, Decision: t.Decision,
		BusinessCode: t.BusinessCode, HTTPStatus: t.HTTPStatus, CacheState: t.CacheState,
		ContentVersion: t.ContentVersion, Coalesced: t.Coalesced, OriginFetches: t.OriginFetches,
		Steps: t.Steps, ErrorDetails: t.ErrorDetails,
	}
	raw, _ := json.Marshal(c)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (t *Trace) JSON() string {
	raw, _ := json.Marshal(t)
	return string(raw)
}
