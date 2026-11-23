// Package metrics samples interactive-side adjudication latency using
// monotonic-clock differences and aggregates P50/P95/max statistics.
package metrics

import (
	"sort"
	"time"

	"injectctl/internal/model"
)

// FixedMarker is the only time representation allowed in exported reports.
// Real durations are measured through monotonic clocks for in-process
// assertions, but never serialized, so repeated runs are byte-identical.
const FixedMarker = "MONOTONIC_SAMPLE"

// Stats is the deterministic, serializable per-(landing,code) aggregate.
type Stats struct {
	Landing  string `json:"landing"`
	Code     string `json:"code"`
	Count    int    `json:"count"`
	Average  string `json:"average_marker"`
	P95      string `json:"p95_marker"`
	Maximum  string `json:"maximum_marker"`
	AvgNanos int64  `json:"-"`
	P95Nanos int64  `json:"-"`
	MaxNanos int64  `json:"-"`
}

// Recorder collects latency points.
type Recorder struct {
	now    func() time.Time
	points []model.LatencyPoint
}

// NewRecorder uses time.Now by default; time.Time carries a monotonic reading.
func NewRecorder() *Recorder {
	return &Recorder{now: time.Now}
}

// NewDeterministicRecorder accepts an injected monotonic clock for tests.
func NewDeterministicRecorder(now func() time.Time) *Recorder {
	return &Recorder{now: now}
}

// Measure runs fn and records the monotonic-clock difference.
func (r *Recorder) Measure(seq int, landing, code string, fn func()) {
	start := r.now()
	fn()
	elapsed := r.now().Sub(start) // monotonic difference, wall clock ignored
	r.points = append(r.points, model.LatencyPoint{
		Seq: seq, Landing: landing, Code: code, Nanos: elapsed.Nanoseconds(),
	})
}

// Points returns the raw monotonic samples (never serialized into reports).
func (r *Recorder) Points() []model.LatencyPoint {
	return append([]model.LatencyPoint(nil), r.points...)
}

// Aggregate groups points by landing and code and computes nearest-rank P95.
func (r *Recorder) Aggregate() []Stats {
	type key struct{ landing, code string }
	groups := map[key][]int64{}
	for _, p := range r.points {
		k := key{p.Landing, p.Code}
		groups[k] = append(groups[k], p.Nanos)
	}

	out := make([]Stats, 0, len(groups))
	for k, vals := range groups {
		sorted := append([]int64(nil), vals...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

		var sum int64
		for _, v := range sorted {
			sum += v
		}
		avg := sum / int64(len(sorted))
		// Nearest-rank P95: ceil(0.95 * n), 1-based.
		rank := (95*len(sorted) + 99) / 100
		p95 := sorted[rank-1]
		maxv := sorted[len(sorted)-1]

		out = append(out, Stats{
			Landing: k.landing, Code: k.code, Count: len(sorted),
			Average: FixedMarker, P95: FixedMarker, Maximum: FixedMarker,
			AvgNanos: avg, P95Nanos: p95, MaxNanos: maxv,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Landing != out[j].Landing {
			return out[i].Landing < out[j].Landing
		}
		return out[i].Code < out[j].Code
	})
	return out
}

// SetLastCode tags the most recent latency point with its adjudication code.
func (r *Recorder) SetLastCode(code string) {
	if len(r.points) > 0 {
		r.points[len(r.points)-1].Code = code
	}
}
