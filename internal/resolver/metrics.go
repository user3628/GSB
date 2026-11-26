package resolver

import (
	"sync"
	"sync/atomic"
	"time"

	"example.com/gsb/dnscache/internal/errs"
)

type ErrorEvent struct {
	AtMS    int64  `json:"at_ms"`
	Path    string `json:"path"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Metrics struct {
	start time.Time

	Queries     atomic.Uint64
	UDPQueries  atomic.Uint64
	TCPQueries  atomic.Uint64
	CacheHits   atomic.Uint64
	CacheMisses atomic.Uint64
	Malformed   atomic.Uint64
	TCPFrameBad atomic.Uint64
	Responses   atomic.Uint64
	WriteErrors atomic.Uint64

	mu     sync.Mutex
	codes  map[string]uint64
	events []ErrorEvent
}

func NewMetrics() *Metrics {
	return &Metrics{start: time.Now(), codes: make(map[string]uint64)}
}

func (m *Metrics) RecordQuery(transport string) {
	m.Queries.Add(1)
	if transport == "tcp" {
		m.TCPQueries.Add(1)
	} else {
		m.UDPQueries.Add(1)
	}
}

func (m *Metrics) RecordCache(hit bool) {
	if hit {
		m.CacheHits.Add(1)
	} else {
		m.CacheMisses.Add(1)
	}
}

func (m *Metrics) RecordCode(code string) {
	if code == "" {
		code = "OK"
	}
	m.mu.Lock()
	m.codes[code]++
	m.mu.Unlock()
}

func (m *Metrics) RecordError(path string, err error) {
	if err == nil {
		return
	}
	code := string(errs.CodeOf(err))
	if code == "E_INTERNAL" && errs.MessageOf(err) == "" {
		code = "E_INTERNAL"
	}
	m.RecordCode(code)
	m.mu.Lock()
	m.events = append(m.events, ErrorEvent{AtMS: time.Now().UnixMilli(), Path: path, Code: code, Message: errs.MessageOf(err)})
	if len(m.events) > 100 {
		m.events = append([]ErrorEvent(nil), m.events[len(m.events)-100:]...)
	}
	m.mu.Unlock()
}

func (m *Metrics) Snapshot() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	codes := make(map[string]uint64, len(m.codes))
	for k, v := range m.codes {
		codes[k] = v
	}
	events := append([]ErrorEvent(nil), m.events...)
	return map[string]any{
		"uptime_seconds":     time.Since(m.start).Seconds(),
		"queries":            m.Queries.Load(),
		"udp_queries":        m.UDPQueries.Load(),
		"tcp_queries":        m.TCPQueries.Load(),
		"cache_hits":         m.CacheHits.Load(),
		"cache_misses":       m.CacheMisses.Load(),
		"malformed":          m.Malformed.Load(),
		"tcp_frame_invalid":  m.TCPFrameBad.Load(),
		"responses":          m.Responses.Load(),
		"cache_write_errors": m.WriteErrors.Load(),
		"codes":              codes,
		"recent_errors":      events,
	}
}
