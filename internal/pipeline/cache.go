package pipeline

import (
	"context"
	"time"
)

// QueryCache caches query results. The cache key always contains the scene
// content fingerprint, config version and camera fingerprint, so a cached
// answer and a recomputed answer are byte-identical (they are the same
// canonical bytes) (requirement 4).
type QueryCache struct {
	entries map[string]cacheEntry
}

type cacheEntry struct {
	payload []byte
}

func NewQueryCache() *QueryCache {
	return &QueryCache{entries: map[string]cacheEntry{}}
}

// CacheKey builds the composite key for a query.
func CacheKey(kind string, sceneFP, cfgVer, cameraFP string) string {
	return MustDigest(struct {
		Kind     string
		SceneFP  string
		CfgVer   string
		CameraFP string
	}{kind, sceneFP, cfgVer, cameraFP})
}

func (c *QueryCache) get(key string) ([]byte, bool) {
	e, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	return e.payload, true
}

func (c *QueryCache) put(key string, payload []byte) {
	c.entries[key] = cacheEntry{payload: append([]byte(nil), payload...)}
}

// QueryResult is what a query endpoint returns.
type QueryResult struct {
	QueryID   string `json:"query_id"`
	Kind      string `json:"kind"`
	CacheHit  bool   `json:"cache_hit"`
	ElapsedMs int64  `json:"elapsed_ms"`
	Payload   []byte `json:"payload"`
}

// RunQuery executes compute under a latency budget. If the deadline is
// exceeded it returns E_LATENCY_BUDGET with the query id and elapsed time
// instead of blocking (requirement 4).
func (c *QueryCache) RunQuery(kind, queryID string, sceneFP SceneFingerprints, cfgVer string,
	budgetMs int64, compute func() (any, error)) (*QueryResult, error) {
	start := time.Now()
	key := CacheKey(kind, sceneFP.ContentFP, cfgVer, sceneFP.CameraFP)
	if payload, ok := c.get(key); ok {
		return &QueryResult{
			QueryID:   queryID,
			Kind:      kind,
			CacheHit:  true,
			ElapsedMs: time.Since(start).Milliseconds(),
			Payload:   payload,
		}, nil
	}

	if budgetMs <= 0 {
		budgetMs = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(budgetMs)*time.Millisecond)
	defer cancel()

	type res struct {
		payload []byte
		err     error
	}
	ch := make(chan res, 1)
	go func() {
		v, err := compute()
		if err != nil {
			ch <- res{err: err}
			return
		}
		b, encErr := CanonicalEncode(v)
		ch <- res{payload: b, err: encErr}
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		c.put(key, r.payload)
		return &QueryResult{
			QueryID:   queryID,
			Kind:      kind,
			CacheHit:  false,
			ElapsedMs: time.Since(start).Milliseconds(),
			Payload:   r.payload,
		}, nil
	case <-ctx.Done():
		elapsed := time.Since(start).Milliseconds()
		return nil, &BizError{
			Code:      CodeLatencyBudget,
			Message:   "query exceeded latency budget",
			QueryID:   queryID,
			ElapsedMs: elapsed,
			Field:     kind,
			Details:   map[string]string{"budget_ms": intToString(budgetMs)},
		}
	}
}

func intToString(i int64) string {
	return floatToString(float64(i))
}
