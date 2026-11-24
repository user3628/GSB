// Package metrics collects per-request latency samples off the validation hot
// path and aggregates them deterministically by tenant and result code.
//
// Samples are handed to a dedicated collector goroutine through a buffered
// channel, so recording latency never blocks or serializes ticket validation.
// Every numeric field is an integer (nanoseconds) and outputs are sorted,
// making repeated runs with identical inputs byte-for-byte identical.
package metrics

import (
	"sort"
	"sync"
)

// Sample is one observed validation request.
type Sample struct {
	Seq       int64
	TenantID  string
	Code      string
	LatencyNS int64
}

// Agg is the aggregated statistic for a (tenant, code) pair.
type Agg struct {
	TenantID string `json:"tenantId"`
	Code     string `json:"code"`
	Count    int    `json:"count"`
	AvgNS    int64  `json:"avgNs"`
	P50NS    int64  `json:"p50Ns"`
	P95NS    int64  `json:"p95Ns"`
	MaxNS    int64  `json:"maxNs"`
}

// Collector gathers samples asynchronously.
type Collector struct {
	ch       chan Sample
	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once

	mu      sync.Mutex
	samples []Sample
}

// NewCollector starts a collector with the given channel buffer.
func NewCollector(buffer int) *Collector {
	if buffer <= 0 {
		buffer = 1024
	}
	c := &Collector{
		ch:     make(chan Sample, buffer),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	go c.loop()
	return c
}

func (c *Collector) loop() {
	defer close(c.doneCh)
	for {
		select {
		case s := <-c.ch:
			c.mu.Lock()
			c.samples = append(c.samples, s)
			c.mu.Unlock()
		case <-c.stopCh:
			// Drain any buffered samples before stopping.
			for {
				select {
				case s := <-c.ch:
					c.mu.Lock()
					c.samples = append(c.samples, s)
					c.mu.Unlock()
				default:
					return
				}
			}
		}
	}
}

// Observe enqueues a sample without blocking the validation hot path. Under an
// extreme burst that fills the buffer it falls back to a guarded append so a
// sample is never silently dropped.
func (c *Collector) Observe(s Sample) {
	select {
	case c.ch <- s:
	default:
		c.mu.Lock()
		c.samples = append(c.samples, s)
		c.mu.Unlock()
	}
}

// Stop flushes buffered samples and terminates the collector goroutine.
func (c *Collector) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
	<-c.doneCh
}

// Samples returns a copy of all samples sorted by sequence number.
func (c *Collector) Samples() []Sample {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Sample, len(c.samples))
	copy(out, c.samples)
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// Aggregate produces sorted per (tenant, code) aggregates.
func (c *Collector) Aggregate() []Agg {
	samples := c.Samples()
	type key struct{ tenant, code string }
	buckets := map[key][]int64{}
	var order []key
	for _, s := range samples {
		k := key{s.TenantID, s.Code}
		if _, ok := buckets[k]; !ok {
			order = append(order, k)
		}
		buckets[k] = append(buckets[k], s.LatencyNS)
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].tenant != order[j].tenant {
			return order[i].tenant < order[j].tenant
		}
		return order[i].code < order[j].code
	})
	var out []Agg
	for _, k := range order {
		lat := append([]int64(nil), buckets[k]...)
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		var sum int64
		for _, v := range lat {
			sum += v
		}
		out = append(out, Agg{
			TenantID: k.tenant,
			Code:     k.code,
			Count:    len(lat),
			AvgNS:    sum / int64(len(lat)),
			P50NS:    percentile(lat, 50),
			P95NS:    percentile(lat, 95),
			MaxNS:    lat[len(lat)-1],
		})
	}
	return out
}

// percentile returns the nearest-rank percentile value. Rank is computed as
// ceil(p/100 * N) with a 1-based index, which is fully deterministic.
func percentile(sorted []int64, p int) int64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	rank := (p*n + 99) / 100 // ceil(p*n/100)
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}
