package scheduler

import "math"

// EstimateLatencyMs returns a deterministic estimated batch latency:
//
//	latency = base + per_sample * total_samples   (rounded to whole ms)
//
// All inputs come from the active policy snapshot so the estimate is fully
// reproducible for a fixed seed / fixed policy version.
func EstimateLatencyMs(baseMs int64, perSample float64, totalSamples int64) int64 {
	return baseMs + int64(math.Round(perSample*float64(totalSamples)))
}
