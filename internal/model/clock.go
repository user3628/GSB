package model

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

const TimeSample = "mono-local-sample/v1"

// MonoTime is a sample tick on a synthetic monotonic timeline. Comparisons are
// always integer delta operations; wall-clock time is never serialized.
type MonoTime struct {
	Tick int64
}

func T(tick int64) MonoTime { return MonoTime{Tick: tick} }

func (t MonoTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("%s:tick=%d", TimeSample, t.Tick))
}

func (t *MonoTime) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	prefix := TimeSample + ":tick="
	if !strings.HasPrefix(value, prefix) {
		return fmt.Errorf("invalid monotonic time marker %q", value)
	}
	tick, err := strconv.ParseInt(strings.TrimPrefix(value, prefix), 10, 64)
	if err != nil {
		return err
	}
	t.Tick = tick
	return nil
}

func (t MonoTime) Add(d MonoDuration) MonoTime { return MonoTime{Tick: t.Tick + int64(d)} }
func (t MonoTime) Sub(other MonoTime) MonoDuration {
	return MonoDuration(t.Tick - other.Tick)
}

// MonoDuration is a deterministic integer delta on the sample timeline.
type MonoDuration int64

func D(ticks int64) MonoDuration { return MonoDuration(ticks) }

func (d MonoDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("%s:delta=%d", TimeSample, int64(d)))
}

func (d *MonoDuration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	prefix := TimeSample + ":delta="
	if !strings.HasPrefix(value, prefix) {
		return fmt.Errorf("invalid monotonic duration marker %q", value)
	}
	ticks, err := strconv.ParseInt(strings.TrimPrefix(value, prefix), 10, 64)
	if err != nil {
		return err
	}
	*d = MonoDuration(ticks)
	return nil
}

func (d MonoDuration) String() string {
	return fmt.Sprintf("%s:delta=%d", TimeSample, int64(d))
}

// Clock exposes only monotonic sample ticks. The virtual clock is used by all
// deterministic local samples; MonotonicClock is available for live adapters.
type Clock interface {
	Now() MonoTime
	Sample() string
}

type VirtualClock struct {
	mu   sync.Mutex
	tick int64
}

func NewVirtualClock(tick int64) *VirtualClock { return &VirtualClock{tick: tick} }

func (c *VirtualClock) Now() MonoTime {
	c.mu.Lock()
	defer c.mu.Unlock()
	return MonoTime{Tick: c.tick}
}

func (c *VirtualClock) Set(tick int64) {
	c.mu.Lock()
	c.tick = tick
	c.mu.Unlock()
}

func (c *VirtualClock) Sample() string { return TimeSample }

// MonotonicClock intentionally uses time.Since from a private origin rather
// than wall-clock equality. Its tick origin must be persisted if used as a
// long-lived adapter; deterministic CLI workflows use VirtualClock instead.
type MonotonicClock struct {
	origin time.Time
	base   int64
}

func NewMonotonicClock(base int64) *MonotonicClock {
	return &MonotonicClock{origin: time.Now(), base: base}
}

func (c *MonotonicClock) Now() MonoTime {
	return MonoTime{Tick: c.base + int64(time.Since(c.origin))}
}

func (c *MonotonicClock) Sample() string { return TimeSample }
