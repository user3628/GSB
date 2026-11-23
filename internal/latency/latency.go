// Package latency provides deterministic, monotonic-clock-based latency
// sampling for the interactive side. It never reads the wall clock: the
// clock is a logical tick source advanced by a fixed sampling marker derived
// from event content, so repeated runs over the same input produce
// byte-identical aggregates.
package latency

import (
	"sort"
	"sync"

	"flowtrace/internal/model"
)

// MonoClock is a monotonic logical clock. It is never backed by wall time.
type MonoClock struct{ now uint64 }

// Now returns the current logical tick.
func (c *MonoClock) Now() uint64 { return c.now }

// Advance moves the clock forward by delta ticks (monotonic).
func (c *MonoClock) Advance(delta uint64) { c.now += delta }

// SampleCost is the fixed sampling marker: a deterministic per-event cost
// derived only from event content. Identical input always yields identical
// latency samples.
func SampleCost(ev model.Event) uint64 {
	return 3 + uint64(len(ev.Subject)) + uint64(len(ev.SrcField)) + uint64(len(ev.DstField)) + ev.Seq%5
}

// Key identifies an aggregation bucket: window + result code.
type Key struct {
	Window string
	Code   string
}

// Stat aggregates latency samples for one bucket.
type Stat struct {
	Window string `json:"window"`
	Code   string `json:"code"`
	Count  int    `json:"count"`
	Avg    uint64 `json:"avg"`
	P95    uint64 `json:"p95"`
	Max    uint64 `json:"max"`

	samples []uint64
}

// Sampler records per-refresh latencies. Sampling never feeds back into
// tracing decisions.
type Sampler struct {
	mu      sync.Mutex
	clock   *MonoClock
	buckets map[Key]*Stat
	order   []Key
}

func NewSampler() *Sampler {
	return &Sampler{clock: &MonoClock{}, buckets: map[Key]*Stat{}}
}

// Observe records one interactive refresh: the elapsed logical ticks between
// receiving the event and completing the trace update.
func (s *Sampler) Observe(window, code string, ev model.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	start := s.clock.Now()
	cost := SampleCost(ev)
	s.clock.Advance(cost)
	elapsed := s.clock.Now() - start
	k := Key{window, code}
	st, ok := s.buckets[k]
	if !ok {
		st = &Stat{Window: window, Code: code}
		s.buckets[k] = st
		s.order = append(s.order, k)
	}
	st.samples = append(st.samples, elapsed)
}

// Stats returns finalized aggregates in stable (window, code) order.
func (s *Sampler) Stats() []Stat {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := append([]Key{}, s.order...)
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Window != keys[j].Window {
			return keys[i].Window < keys[j].Window
		}
		return keys[i].Code < keys[j].Code
	})
	out := make([]Stat, 0, len(keys))
	for _, k := range keys {
		st := s.buckets[k]
		samples := append([]uint64{}, st.samples...)
		sort.Slice(samples, func(a, b int) bool { return samples[a] < samples[b] })
		var sum uint64
		for _, v := range samples {
			sum += v
		}
		p95 := samples[(len(samples)*95+99)/100-1] // ceil(0.95*n)-1
		out = append(out, Stat{
			Window: st.Window, Code: st.Code,
			Count: len(samples), Avg: sum / uint64(len(samples)),
			P95: p95, Max: samples[len(samples)-1],
		})
	}
	return out
}
