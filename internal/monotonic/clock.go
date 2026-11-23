package monotonic

import (
	"sort"
	"sync"
	"time"

	"vpnbench/internal/model"
)

// Clock exposes only monotonic elapsed time to the decision path. Reports never
// contain wall-clock timestamps.
type Clock interface {
	Now() time.Time
	Since(time.Time) time.Duration
	Mode() string
	Marker() string
}

type Real struct{}

func (Real) Now() time.Time                      { return time.Now() }
func (Real) Since(start time.Time) time.Duration { return time.Since(start) }
func (Real) Mode() string                        { return "monotonic-real-v1" }
func (Real) Marker() string                      { return "T_MONO_REAL_V1" }

// Fixed is a virtual monotonic clock. Every observation advances by a fixed
// tick, so a request always observes exactly one tick. This removes scheduler
// and wall-clock noise from acceptance reports while retaining the monotonic
// start/end subtraction contract.
type Fixed struct {
	mu   sync.Mutex
	next int64
	tick time.Duration
}

func NewFixed() *Fixed {
	return &Fixed{next: 1, tick: 250 * time.Microsecond}
}

func (f *Fixed) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	value := time.Unix(0, f.next*int64(f.tick)).UTC()
	f.next++
	return value
}

func (f *Fixed) Since(start time.Time) time.Duration {
	// This is a real subtraction on the virtual monotonic timeline. A dedicated
	// clock per session keeps the observation deterministic even when sessions
	// execute concurrently.
	now := f.Now()
	return now.Sub(start)
}

func (f *Fixed) Mode() string   { return "monotonic-fixed-v1" }
func (f *Fixed) Marker() string { return "T_MONO_FIXED_250US_V1" }

func Default(mode string) Clock {
	if mode == "real" {
		return Real{}
	}
	return NewFixed()
}

func Factory(mode string) func() Clock {
	if mode == "real" {
		return func() Clock { return Real{} }
	}
	return func() Clock { return NewFixed() }
}

// Aggregate groups deterministic samples by tunnel and result code and uses
// nearest-rank P95 without floating point math.
func Aggregate(samples []model.LatencySample) []model.LatencyStat {
	type key struct {
		tunnel string
		code   model.Code
	}
	groups := make(map[key][]int64)
	for _, sample := range samples {
		k := key{tunnel: sample.TunnelID, code: sample.ResultCode}
		groups[k] = append(groups[k], sample.DurationNS)
	}
	stats := make([]model.LatencyStat, 0, len(groups))
	for k, durations := range groups {
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		var sum int64
		for _, value := range durations {
			sum += value
		}
		rank := (95*int64(len(durations)) + 99) / 100
		if rank < 1 {
			rank = 1
		}
		stats = append(stats, model.LatencyStat{
			TunnelID:    k.tunnel,
			ResultCode:  k.code,
			SampleCount: int64(len(durations)),
			AverageNS:   sum / int64(len(durations)),
			P95NS:       durations[rank-1],
			MaxNS:       durations[len(durations)-1],
		})
	}
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].TunnelID != stats[j].TunnelID {
			return stats[i].TunnelID < stats[j].TunnelID
		}
		return stats[i].ResultCode < stats[j].ResultCode
	})
	return stats
}
