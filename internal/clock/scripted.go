package clock

import (
	"sync"
	"time"
)

// FixedBase is the constant epoch used by every scripted run. Because it never
// changes, reports produced from identical inputs are byte-for-byte identical.
const FixedBase = "2026-01-01T00:00:00Z"

// baseNS is FixedBase expressed as Unix nanoseconds.
var baseNS = func() int64 {
	base, _ := time.Parse(time.RFC3339, FixedBase)
	return base.UnixNano()
}()

// ScriptedClock is a deterministic virtual clock.
//
// Three quantities are kept independent and scheduling-free:
//   - currentNS: virtual nanoseconds since FixedBase; it only moves via
//     explicit scenario Advance calls,
//   - EventNS(seq): absolute Unix-ns timestamp for durable rows,
//   - LatencyFor(seq): a fixed sub-millisecond latency per request sequence.
type ScriptedClock struct {
	mu        sync.Mutex
	currentNS int64
}

// NewScripted builds a scripted clock anchored at FixedBase.
func NewScripted() *ScriptedClock { return &ScriptedClock{} }

// EventNS returns the deterministic timestamp for request seq: the current
// scenario timeline plus a per-sequence deterministic 1ms offset, expressed as
// absolute Unix nanoseconds anchored at FixedBase.
func (c *ScriptedClock) EventNS(seq int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return baseNS + c.currentNS + seq*1_000_000
}

// Now returns the virtual current time anchored at FixedBase.
func (c *ScriptedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Unix(0, baseNS+c.currentNS)
}

// Unix returns the virtual current time in Unix seconds (FixedBase + offset).
func (c *ScriptedClock) Unix() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return baseNS/int64(time.Second) + c.currentNS/int64(time.Second)
}

func (c *ScriptedClock) Mark(seq int64) Mark {
	return Mark{mono: c.EventNS(seq), seq: seq}
}

// Elapsed returns the fixed deterministic latency of the marked request
// without moving the current timeline.
func (c *ScriptedClock) Elapsed(m Mark) time.Duration {
	return LatencyFor(m.seq)
}

// Advance moves the virtual current time forward, used to force ticket expiry
// deterministically without any real sleeping.
func (c *ScriptedClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.currentNS += int64(d)
	c.mu.Unlock()
}

// LatencyFor returns the deterministic latency of a request sequence.
// 80us + (seq*73) mod 901us -> always within [80us, 980us].
func LatencyFor(seq int64) time.Duration {
	const step = int64(73)
	const span = int64(901) // microseconds
	us := 80 + (seq*step)%span
	return time.Duration(us) * time.Microsecond
}
