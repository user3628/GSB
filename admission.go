package tokenforge

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics is a snapshot of the in-process admission and transaction counters.
// Durations are integer nanoseconds in reports and never converted to float.
type Metrics struct {
	MaxInFlightConfigured int   `json:"max_in_flight_configured"`
	InFlight              int64 `json:"in_flight"`
	PeakInFlight          int64 `json:"peak_in_flight"`
	Admitted              int64 `json:"admitted"`
	Contended             int64 `json:"contended"`
	QueueObservations     int64 `json:"queue_observations"`
	QueueTotalNanos       int64 `json:"queue_total_nanos"`
	QueueMaxNanos         int64 `json:"queue_max_nanos"`
}

type metricsCollector struct {
	configured int64
	inFlight   atomic.Int64
	peak       atomic.Int64
	admitted   atomic.Int64
	contended  atomic.Int64
	queueCount atomic.Int64
	queueTotal atomic.Int64
	queueMax   atomic.Int64
}

func newMetrics(configured int) *metricsCollector {
	return &metricsCollector{configured: int64(configured)}
}

func (m *metricsCollector) enter() int64 {
	n := m.inFlight.Add(1)
	for {
		old := m.peak.Load()
		if n <= old || m.peak.CompareAndSwap(old, n) {
			break
		}
	}
	return n
}

func (m *metricsCollector) leave() { m.inFlight.Add(-1) }

func (m *metricsCollector) observeQueue(d time.Duration) {
	if d < 0 {
		d = 0
	}
	n := d.Nanoseconds()
	m.queueCount.Add(1)
	m.queueTotal.Add(n)
	for {
		old := m.queueMax.Load()
		if n <= old || m.queueMax.CompareAndSwap(old, n) {
			break
		}
	}
}

func (m *metricsCollector) snapshot() Metrics {
	return Metrics{
		MaxInFlightConfigured: int(m.configured),
		InFlight:              m.inFlight.Load(),
		PeakInFlight:          m.peak.Load(),
		Admitted:              m.admitted.Load(),
		Contended:             m.contended.Load(),
		QueueObservations:     m.queueCount.Load(),
		QueueTotalNanos:       m.queueTotal.Load(),
		QueueMaxNanos:         m.queueMax.Load(),
	}
}

type admissionLimiter struct {
	slots   chan struct{}
	wait    time.Duration
	metrics *metricsCollector
}

func newAdmissionLimiter(max int, wait time.Duration, metrics *metricsCollector) *admissionLimiter {
	return &admissionLimiter{slots: make(chan struct{}, max), wait: wait, metrics: metrics}
}

func (a *admissionLimiter) acquire(ctx context.Context) (release func(), inFlight int64, err error) {
	select {
	case a.slots <- struct{}{}:
		a.metrics.admitted.Add(1)
		inFlight = a.metrics.enter()
		return a.releaseFunc(), inFlight, nil
	default:
	}

	if a.wait == 0 {
		a.metrics.contended.Add(1)
		return nil, 0, codeError(CodeQuotaContention, "transaction admission limit reached")
	}
	timer := time.NewTimer(a.wait)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case a.slots <- struct{}{}:
		a.metrics.admitted.Add(1)
		inFlight = a.metrics.enter()
		return a.releaseFunc(), inFlight, nil
	case <-timer.C:
		a.metrics.contended.Add(1)
		return nil, 0, codeError(CodeQuotaContention, "transaction admission wait threshold exceeded")
	case <-ctx.Done():
		return nil, 0, domainError(CodeCancelled, "transaction admission cancelled", ctx.Err())
	}
}

func (a *admissionLimiter) releaseFunc() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			a.metrics.leave()
			<-a.slots
		})
	}
}

type gateEntry struct {
	sem  chan struct{}
	refs int
}

// keyedGate serializes writers per tenant/token without a global lock. Each
// entry is reference-counted so idle keys do not accumulate forever.
type keyedGate struct {
	mu      sync.Mutex
	entries map[string]*gateEntry
}

func newKeyedGate() *keyedGate {
	return &keyedGate{entries: make(map[string]*gateEntry)}
}

func (g *keyedGate) acquire(ctx context.Context, key string, deadline time.Time) (release func(), waited time.Duration, err error) {
	start := time.Now()
	g.mu.Lock()
	e := g.entries[key]
	if e == nil {
		e = &gateEntry{sem: make(chan struct{}, 1)}
		g.entries[key] = e
	}
	e.refs++
	g.mu.Unlock()

	select {
	case e.sem <- struct{}{}:
		var once sync.Once
		release = func() {
			once.Do(func() {
				<-e.sem
				g.unref(key, e)
			})
		}
		return release, time.Since(start), nil
	default:
	}

	var timer *time.Timer
	var deadlineC <-chan time.Time
	if !deadline.IsZero() {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			g.unref(key, e)
			return nil, time.Since(start), codeError(CodeQuotaContention, "tenant quota serialization wait threshold exceeded")
		}
		timer = time.NewTimer(remaining)
		deadlineC = timer.C
		defer func() {
			if timer != nil && !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}()
	}

	select {
	case e.sem <- struct{}{}:
		var once sync.Once
		release = func() {
			once.Do(func() {
				<-e.sem
				g.unref(key, e)
			})
		}
		return release, time.Since(start), nil
	case <-deadlineC:
		g.unref(key, e)
		return nil, time.Since(start), codeError(CodeQuotaContention, "tenant quota serialization wait threshold exceeded")
	case <-ctx.Done():
		g.unref(key, e)
		return nil, time.Since(start), domainError(CodeCancelled, "tenant serialization cancelled", ctx.Err())
	}
}

func (g *keyedGate) unref(key string, e *gateEntry) {
	g.mu.Lock()
	e.refs--
	if e.refs == 0 && g.entries[key] == e {
		delete(g.entries, key)
	}
	g.mu.Unlock()
}

type attemptTracker struct {
	mu     sync.Mutex
	active map[string]int
}

func newAttemptTracker() *attemptTracker {
	return &attemptTracker{active: make(map[string]int)}
}

func (a *attemptTracker) begin(tokenID string) (overlapped bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	overlapped = a.active[tokenID] > 0
	a.active[tokenID]++
	return overlapped
}

func (a *attemptTracker) end(tokenID string) {
	a.mu.Lock()
	n := a.active[tokenID] - 1
	if n <= 0 {
		delete(a.active, tokenID)
	} else {
		a.active[tokenID] = n
	}
	a.mu.Unlock()
}
