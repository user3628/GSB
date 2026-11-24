// Package clock abstracts time acquisition so the engine can run against either
// the real process clock (service mode) or a deterministic virtual clock
// (replay/demo mode).
//
// Latency is always observed as a monotonic difference between two readings,
// never as a wall-clock subtraction. The real clock relies on the monotonic
// reading embedded in time.Time by the Go runtime; the scripted clock uses a
// monotonically increasing virtual nanosecond counter.
package clock

import "time"

// Mark captures an instant together with the request sequence it belongs to.
// The wall field carries a monotonic reading for the real clock, while mono is
// the virtual nanosecond reading for the scripted clock.
type Mark struct {
	wall time.Time
	mono int64
	seq  int64
}

// Seq returns the request sequence associated with the mark.
func (m Mark) Seq() int64 { return m.seq }

// Clock is the time abstraction used by the engine.
type Clock interface {
	// Now returns the current (real or virtual) time.
	Now() time.Time
	// Unix returns the current (real or virtual) time in Unix seconds.
	Unix() int64
	// Mark records a start reading for the request identified by seq.
	Mark(seq int64) Mark
	// Elapsed returns the monotonic duration elapsed since the mark.
	Elapsed(m Mark) time.Duration
	// EventNS returns the timestamp (Unix nanoseconds) of request seq. Real
	// clocks return the current monotonic-backed instant; scripted clocks
	// return a deterministic function of seq so reports stay reproducible.
	EventNS(seq int64) int64
}

// RealClock reads the process clock. Elapsed uses the monotonic reading
// embedded in time.Time and is therefore immune to wall-clock adjustments.
type RealClock struct{}

// NewReal returns a clock backed by the runtime monotonic clock.
func NewReal() RealClock { return RealClock{} }

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) Unix() int64 { return time.Now().Unix() }

func (RealClock) Mark(seq int64) Mark {
	return Mark{wall: time.Now(), seq: seq}
}

func (RealClock) Elapsed(m Mark) time.Duration {
	// time.Since uses the monotonic reading when present, so wall-clock
	// jumps (NTP steps) cannot affect the measurement.
	return time.Since(m.wall)
}

func (RealClock) EventNS(int64) int64 { return time.Now().UnixNano() }
