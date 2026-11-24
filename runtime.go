package authlab

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"
)

// Clock allows experiments to advance time without sleeping.
type Clock interface {
	Now() time.Time
}

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// ManualClock is a concurrency-safe deterministic clock.
type ManualClock struct {
	mu      sync.Mutex
	current time.Time
}

func NewManualClock(at time.Time) *ManualClock {
	return &ManualClock{current: at.UTC()}
}

func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *ManualClock) Advance(delta time.Duration) {
	c.mu.Lock()
	c.current = c.current.Add(delta)
	c.mu.Unlock()
}

func (c *ManualClock) Set(at time.Time) {
	c.mu.Lock()
	c.current = at.UTC()
	c.mu.Unlock()
}

// IDSource supplies deterministic IDs. IDs are never used as bearer secrets.
type IDSource interface {
	Next(kind string) string
}

// DeterministicIDSource emits stable IDs derived from a fixture seed.
type DeterministicIDSource struct {
	seed    []byte
	counter atomic.Uint64
}

func NewDeterministicIDSource(seed []byte) *DeterministicIDSource {
	return &DeterministicIDSource{seed: append([]byte(nil), seed...)}
}

func (s *DeterministicIDSource) Next(kind string) string {
	n := s.counter.Add(1)
	mac := hmac.New(sha256.New, s.seed)
	_, _ = mac.Write([]byte(kind))
	_, _ = mac.Write([]byte{0})
	var encoded [8]byte
	for i := 7; i >= 0; i-- {
		encoded[i] = byte(n)
		n >>= 8
	}
	_, _ = mac.Write(encoded[:])
	return kind + "_" + hex.EncodeToString(mac.Sum(nil)[:12])
}

type randomIDSource struct{}

func (randomIDSource) Next(kind string) string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic("authlab: crypto/rand unavailable: " + err.Error())
	}
	return kind + "_" + hex.EncodeToString(raw[:])
}
