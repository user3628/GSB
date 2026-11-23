package scheduler

import (
	"context"
	"sort"
	"sync"

	"vpnbench/internal/model"
)

type Conflict struct {
	SessionID  string     `json:"session_id"`
	RequestSeq int64      `json:"request_seq"`
	Code       model.Code `json:"code"`
}

type Metrics struct {
	ConcurrencyLimit int        `json:"concurrency_limit"`
	InFlight         int        `json:"in_flight"`
	PeakInFlight     int        `json:"peak_in_flight"`
	Queued           int        `json:"queued"`
	PeakQueued       int        `json:"peak_queued"`
	Admitted         int        `json:"admitted"`
	Conflicts        []Conflict `json:"conflicts,omitempty"`
}

// Manager implements a bounded session gate. Sessions beyond the concurrency
// limit wait rather than being rejected. Session IDs are unique for the life of
// an active or queued session; a duplicate is a recorded conflict.
type Manager struct {
	limit      int
	slots      chan struct{}
	mu         sync.Mutex
	active     map[string]struct{}
	queue      int
	flight     int
	peakQueue  int
	peakFlight int
	admitted   int
	conflicts  []Conflict
}

func New(limit int) *Manager {
	if limit < 1 {
		limit = 1
	}
	return &Manager{
		limit:  limit,
		slots:  make(chan struct{}, limit),
		active: make(map[string]struct{}),
	}
}

func (m *Manager) Acquire(ctx context.Context, sessionID string, requestSeq int64) (func(), error) {
	m.mu.Lock()
	if _, exists := m.active[sessionID]; exists {
		conflict := Conflict{SessionID: sessionID, RequestSeq: requestSeq, Code: model.CodeSessionConflict}
		m.conflicts = append(m.conflicts, conflict)
		m.mu.Unlock()
		return nil, model.NewError(model.CodeSessionConflict, "session is already active or queued", 0).
			Locate(sessionID, "", requestSeq)
	}
	m.active[sessionID] = struct{}{}
	m.mu.Unlock()

	// Fast path: if capacity exists, this session never enters the queue.
	select {
	case m.slots <- struct{}{}:
		m.mu.Lock()
		m.flight++
		m.admitted++
		if m.flight > m.peakFlight {
			m.peakFlight = m.flight
		}
		m.mu.Unlock()
		return m.release(sessionID), nil
	default:
	}

	m.mu.Lock()
	m.queue++
	if m.queue > m.peakQueue {
		m.peakQueue = m.queue
	}
	m.mu.Unlock()

	select {
	case m.slots <- struct{}{}:
		m.mu.Lock()
		m.queue--
		m.flight++
		m.admitted++
		if m.flight > m.peakFlight {
			m.peakFlight = m.flight
		}
		m.mu.Unlock()
		return m.release(sessionID), nil
	case <-ctx.Done():
		m.mu.Lock()
		m.queue--
		delete(m.active, sessionID)
		m.mu.Unlock()
		return nil, model.NewError(model.CodeInternal, "session acquisition cancelled", 0).Locate(sessionID, "", requestSeq)
	}
}

func (m *Manager) release(sessionID string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			m.flight--
			delete(m.active, sessionID)
			m.mu.Unlock()
			<-m.slots
		})
	}
}

func (m *Manager) InFlight() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.flight
}

func (m *Manager) Queued() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.queue
}

func (m *Manager) Metrics() Metrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	conflicts := append([]Conflict(nil), m.conflicts...)
	sort.SliceStable(conflicts, func(i, j int) bool {
		if conflicts[i].SessionID != conflicts[j].SessionID {
			return conflicts[i].SessionID < conflicts[j].SessionID
		}
		return conflicts[i].RequestSeq < conflicts[j].RequestSeq
	})
	return Metrics{
		ConcurrencyLimit: m.limit,
		InFlight:         m.flight,
		PeakInFlight:     m.peakFlight,
		Queued:           m.queue,
		PeakQueued:       m.peakQueue,
		Admitted:         m.admitted,
		Conflicts:        conflicts,
	}
}
