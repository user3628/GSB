package cockpitaudio

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type QueryType string

const (
	QueryActiveZones      QueryType = "active_zones"
	QueryDelayDetails     QueryType = "delay_details"
	QueryAttributionBasis QueryType = "attribution_basis"
)

type QueryRequest struct {
	QueryID   string    `json:"query_id,omitempty"`
	Type      QueryType `json:"type"`
	AtMS      int       `json:"at_ms,omitempty"`
	StartMS   int       `json:"start_ms,omitempty"`
	EndMS     int       `json:"end_ms,omitempty"`
	CommandID string    `json:"command_id,omitempty"`
	ShardID   string    `json:"shard_id,omitempty"`
}

type QueryResponse struct {
	QueryID           string          `json:"query_id"`
	Type              QueryType       `json:"type"`
	ConfigVersion     string          `json:"config_version"`
	ConfigFingerprint string          `json:"config_fingerprint"`
	EditVersion       int             `json:"edit_version"`
	CacheKey          string          `json:"cache_key"`
	Cached            bool            `json:"cached"`
	ElapsedMS         float64         `json:"elapsed_ms"`
	Result            json.RawMessage `json:"result"`
}

type activeZonesResult struct {
	AtMS      int                `json:"at_ms"`
	Zones     []string           `json:"zones"`
	Intervals []TimelineInterval `json:"intervals"`
}

type delayDetailsResult struct {
	CommandIDs []string      `json:"command_ids"`
	PairDelays []DelayDetail `json:"pair_delays"`
}

type attributionBasisResult struct {
	Attributions []Attribution `json:"attributions"`
}

type QueryService struct {
	cfg    ResolvedConfig
	result *SessionResult

	mu    sync.RWMutex
	cache map[string]json.RawMessage
	seq   atomic.Uint64
}

func NewQueryService(cfg Config, result *SessionResult) (*QueryService, *Error) {
	rc, err := cfg.ResolveVAD("")
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, NewError(CodeInternal, StageQuery, "", "nil session result")
	}
	return &QueryService{cfg: rc, result: result, cache: map[string]json.RawMessage{}}, nil
}

func (q *QueryService) Query(ctx context.Context, req QueryRequest) (*QueryResponse, *Error) {
	start := time.Now()
	if req.QueryID == "" {
		req.QueryID = q.nextQueryID()
	}
	if ctx != nil {
		select {
		case <-ctx.Done():
			return nil, &Error{Code: CodeStopped, Stage: StageQuery, QueryID: req.QueryID, Message: "query context stopped"}
		default:
		}
	}

	key, keyJSON, err := q.cacheKey(req)
	if err != nil {
		return nil, &Error{Code: CodeInternal, Stage: StageQuery, QueryID: req.QueryID, Message: err.Error()}
	}
	budget := q.cfg.LatencyBudgetMS
	if budget <= 0 {
		elapsed := float64(time.Since(start).Nanoseconds()) / 1e6
		return nil, &Error{
			Code: CodeLatencyBudget, Stage: StageQuery, QueryID: req.QueryID,
			Message: "latency budget is non-positive", ElapsedMS: Round(elapsed, 6), BudgetMS: budget,
			Diagnostics: map[string]string{"cache_key": key},
		}
	}

	q.mu.RLock()
	cached, ok := q.cache[key]
	q.mu.RUnlock()

	var resultBytes json.RawMessage
	wasCached := false
	if ok {
		resultBytes = append(json.RawMessage(nil), cached...)
		wasCached = true
	} else {
		computed, perr := q.computeCanonical(req)
		if perr != nil {
			perr.QueryID = req.QueryID
			perr.Stage = StageQuery
			return nil, perr
		}
		resultBytes = computed
		q.mu.Lock()
		if existing, ok := q.cache[key]; ok {
			resultBytes = append(json.RawMessage(nil), existing...)
			wasCached = true
		} else {
			q.cache[key] = append(json.RawMessage(nil), computed...)
		}
		q.mu.Unlock()
	}

	elapsed := float64(time.Since(start).Nanoseconds()) / 1e6
	if elapsed > budget {
		return nil, &Error{
			Code: CodeLatencyBudget, Stage: StageQuery, QueryID: req.QueryID,
			Message: "query exceeded latency budget", ElapsedMS: elapsed, BudgetMS: budget,
			Diagnostics: map[string]string{"cache_key": key, "cache_key_json": string(keyJSON)},
		}
	}
	return &QueryResponse{
		QueryID: req.QueryID, Type: req.Type,
		ConfigVersion: q.cfg.ConfigVersion, ConfigFingerprint: q.cfg.ConfigFingerprint,
		EditVersion: q.result.EditVersion, CacheKey: key, Cached: wasCached,
		ElapsedMS: Round(elapsed, 6), Result: resultBytes,
	}, nil
}

func (q *QueryService) nextQueryID() string {
	n := q.seq.Add(1)
	return "q-" + itoa64(n)
}

func itoa64(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

type queryCacheKey struct {
	Type          QueryType `json:"type"`
	AtMS          int       `json:"at_ms,omitempty"`
	StartMS       int       `json:"start_ms,omitempty"`
	EndMS         int       `json:"end_ms,omitempty"`
	CommandID     string    `json:"command_id,omitempty"`
	ShardID       string    `json:"shard_id,omitempty"`
	ConfigVersion string    `json:"config_version"`
	EditVersion   int       `json:"edit_version"`
}

func (q *QueryService) cacheKey(req QueryRequest) (string, []byte, error) {
	keyStruct := queryCacheKey{
		Type: req.Type, AtMS: req.AtMS, StartMS: req.StartMS, EndMS: req.EndMS,
		CommandID: req.CommandID, ShardID: req.ShardID,
		ConfigVersion: q.cfg.ConfigVersion, EditVersion: q.result.EditVersion,
	}
	b, err := CanonicalJSON(keyStruct)
	if err != nil {
		return "", nil, err
	}
	digest := FingerprintBytes("query-cache:", b)
	key := fmt.Sprintf(
		"type=%s;at=%d;range=%d-%d;command=%s;shard=%s;cfg=%s;edit=%d;digest=%s",
		keyStruct.Type, keyStruct.AtMS, keyStruct.StartMS, keyStruct.EndMS,
		keyStruct.CommandID, keyStruct.ShardID, keyStruct.ConfigVersion,
		keyStruct.EditVersion, digest[:16],
	)
	return key, b, nil
}

func (q *QueryService) computeCanonical(req QueryRequest) (json.RawMessage, *Error) {
	var value any
	switch req.Type {
	case QueryActiveZones:
		value = q.activeZones(req.AtMS)
	case QueryDelayDetails:
		v, err := q.delayDetails(req)
		if err != nil {
			return nil, err
		}
		value = v
	case QueryAttributionBasis:
		v, err := q.attributionBasis(req)
		if err != nil {
			return nil, err
		}
		value = v
	default:
		return nil, NewError(CodeInvalidParam, StageQuery, "", "unknown query type %q", req.Type)
	}
	b, err := CanonicalJSON(value)
	if err != nil {
		return nil, NewError(CodeInternal, StageQuery, "", "marshal query result: %v", err)
	}
	return b, nil
}

func (q *QueryService) activeZones(atMS int) activeZonesResult {
	zones := ActiveZonesAt(q.result.Timeline, atMS)
	intervals := []TimelineInterval{}
	for _, in := range q.result.Timeline.Intervals {
		if in.StartMS <= atMS && atMS < in.EndMS {
			intervals = append(intervals, in)
		}
	}
	return activeZonesResult{AtMS: atMS, Zones: zones, Intervals: intervals}
}

func (q *QueryService) allAttributions() []Attribution {
	out := []Attribution{}
	for _, s := range q.result.Shards {
		if s.Attribution != nil {
			out = append(out, s.Attribution.Items...)
		}
	}
	return out
}

func (q *QueryService) delayDetails(req QueryRequest) (delayDetailsResult, *Error) {
	out := delayDetailsResult{CommandIDs: []string{}, PairDelays: []DelayDetail{}}
	for _, a := range q.allAttributions() {
		if req.CommandID != "" && a.CommandID != req.CommandID {
			continue
		}
		if req.ShardID != "" && a.ShardID != req.ShardID {
			continue
		}
		if req.EndMS > req.StartMS && (a.EndMS <= req.StartMS || a.StartMS >= req.EndMS) {
			continue
		}
		out.CommandIDs = append(out.CommandIDs, a.CommandID)
		out.PairDelays = append(out.PairDelays, a.PairDelays...)
	}
	if req.CommandID != "" && len(out.CommandIDs) == 0 {
		return out, NewError(CodeNotFound, StageQuery, "", "command %s not found", req.CommandID)
	}
	return out, nil
}

func (q *QueryService) attributionBasis(req QueryRequest) (attributionBasisResult, *Error) {
	out := attributionBasisResult{Attributions: []Attribution{}}
	for _, a := range q.allAttributions() {
		if req.CommandID != "" && a.CommandID != req.CommandID {
			continue
		}
		if req.ShardID != "" && a.ShardID != req.ShardID {
			continue
		}
		if req.EndMS > req.StartMS && (a.EndMS <= req.StartMS || a.StartMS >= req.EndMS) {
			continue
		}
		out.Attributions = append(out.Attributions, a)
	}
	if req.CommandID != "" && len(out.Attributions) == 0 {
		return out, NewError(CodeNotFound, StageQuery, "", "command %s not found", req.CommandID)
	}
	return out, nil
}

func RecomputeQueryCanonical(cfg Config, result *SessionResult, req QueryRequest) (json.RawMessage, *Error) {
	svc, err := NewQueryService(cfg, result)
	if err != nil {
		return nil, err
	}
	return svc.computeCanonical(req)
}
