package domain

import (
	"fmt"
	"time"
)

// Config uses milliseconds on the wire for deterministic JSON samples. A zero
// lifetime or idle limit means unlimited.
type Config struct {
	MaxOpen          int   `json:"max_open"`
	MaxIdle          int   `json:"max_idle"`
	MaxLifetimeMS    int64 `json:"max_lifetime_ms"`
	MaxIdleTimeMS    int64 `json:"max_idle_time_ms"`
	AcquireTimeoutMS int64 `json:"acquire_timeout_ms"`
	MaxWaiters       int   `json:"max_waiters"`
	Version          int64 `json:"version"`
}

func (c Config) Validate() error {
	if c.MaxOpen < 1 {
		return fmt.Errorf("max_open must be >= 1")
	}
	if c.MaxIdle < 0 || c.MaxIdle > c.MaxOpen {
		return fmt.Errorf("max_idle must be between 0 and max_open")
	}
	if c.MaxLifetimeMS < 0 || c.MaxIdleTimeMS < 0 {
		return fmt.Errorf("lifetime and idle limits must be >= 0")
	}
	if c.AcquireTimeoutMS < 1 {
		return fmt.Errorf("acquire_timeout_ms must be >= 1")
	}
	if c.MaxWaiters < 0 {
		return fmt.Errorf("max_waiters must be >= 0")
	}
	if c.Version < 1 {
		return fmt.Errorf("version must be >= 1")
	}
	return nil
}

func (c Config) MaxLifetime() time.Duration { return time.Duration(c.MaxLifetimeMS) * time.Millisecond }
func (c Config) MaxIdleTime() time.Duration { return time.Duration(c.MaxIdleTimeMS) * time.Millisecond }
func (c Config) AcquireTimeout() time.Duration {
	return time.Duration(c.AcquireTimeoutMS) * time.Millisecond
}
