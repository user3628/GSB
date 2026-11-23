package cache

import (
	"fmt"
	"time"

	"cdn-edge-consistency/internal/model"
)

type State string

const (
	StateMiss         State = "MISS"
	StateRevalidating State = "REVALIDATING"
	StateFresh        State = "FRESH"
	StateStale        State = "STALE"
	StatePurged       State = "PURGED"
)

var legalTransitions = map[State]map[State]bool{
	StateMiss: {
		StateRevalidating: true,
		StatePurged:       true,
	},
	StateRevalidating: {
		StateFresh:  true,
		StateStale:  true,
		StateMiss:   true,
		StatePurged: true,
	},
	StateFresh: {
		StateStale:        true,
		StateRevalidating: true,
		StatePurged:       true,
	},
	StateStale: {
		StateFresh:        true,
		StateRevalidating: true,
		StatePurged:       true,
	},
	StatePurged: {
		StateMiss: true,
	},
}

func ValidateTransition(from, to State) error {
	if from == to {
		return nil
	}
	if legalTransitions[from][to] {
		return nil
	}
	return model.WithDetails(model.NewBiz(model.CodeInvalidTransition,
		fmt.Sprintf("illegal cache state transition %s -> %s", from, to)),
		map[string]any{"from": from, "to": to})
}

func CanTransition(from, to State) bool {
	return ValidateTransition(from, to) == nil
}

type LookupClass string

const (
	ClassMiss       LookupClass = "MISS"
	ClassFresh      LookupClass = "FRESH"
	ClassStaleSWR   LookupClass = "STALE_SWR"
	ClassStaleWait  LookupClass = "STALE_WAIT"
	ClassWaitFlight LookupClass = "WAIT_FLIGHT"
)

// Classify uses integer millisecond timestamps. The freshness interval is
// [writtenAt, freshUntil). The stale-while-revalidate interval is closed:
// [freshUntil, staleUntil]. Therefore an entry is still allowed to serve stale
// at exactly staleUntil when SWR > 0; one millisecond later it is expired.
type TTLDecision struct {
	Class        LookupClass
	State        State
	ServeStale   bool
	TriggerAsync bool
	Expired      bool
	Reason       string
}

func Classify(etag string, state State, writtenAtMs, freshUntilMs, staleUntilMs, nowMs, swrMs int64) TTLDecision {
	switch state {
	case StatePurged:
		return TTLDecision{Class: ClassMiss, State: StateMiss, Expired: true, Reason: "entry is PURGED"}
	case StateMiss:
		return TTLDecision{Class: ClassMiss, State: StateMiss, Expired: true, Reason: "entry is MISS"}
	}
	hasContent := etag != ""
	if state == StateFresh && hasContent && nowMs < freshUntilMs {
		return TTLDecision{Class: ClassFresh, State: StateFresh, Reason: "now < fresh_until"}
	}
	if hasContent && swrMs > 0 && nowMs >= freshUntilMs && nowMs <= staleUntilMs {
		if state == StateRevalidating {
			return TTLDecision{Class: ClassWaitFlight, State: StateRevalidating, ServeStale: true, Reason: "revalidation in progress; closed SWR window permits stale"}
		}
		return TTLDecision{Class: ClassStaleSWR, State: StateStale, ServeStale: true, TriggerAsync: true, Reason: "fresh_until <= now <= stale_until (closed interval)"}
	}
	if hasContent && nowMs < freshUntilMs && state == StateRevalidating {
		return TTLDecision{Class: ClassWaitFlight, State: StateRevalidating, Reason: "revalidation in progress"}
	}
	if hasContent && nowMs >= freshUntilMs {
		if state == StateRevalidating {
			return TTLDecision{Class: ClassWaitFlight, State: StateRevalidating, Reason: "revalidation in progress; stale window expired"}
		}
		return TTLDecision{Class: ClassStaleWait, State: StateStale, Expired: true, Reason: "now > stale_until"}
	}
	return TTLDecision{Class: ClassMiss, State: StateMiss, Expired: true, Reason: "entry has no reusable content"}
}

func ComputeWindows(writtenAtMs, ttlMs, swrMs int64) (freshUntilMs, staleUntilMs int64) {
	if ttlMs < 0 {
		ttlMs = 0
	}
	if swrMs < 0 {
		swrMs = 0
	}
	freshUntilMs = writtenAtMs + ttlMs
	staleUntilMs = freshUntilMs + swrMs
	return
}

func NowMs(clock model.Clock) int64 {
	if clock == nil {
		return time.Now().UnixMilli()
	}
	return clock.Now().UnixMilli()
}
