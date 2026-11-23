package cache

import "testing"

func TestStateMachineTransitions(t *testing.T) {
	legal := [][2]State{
		{StateMiss, StateRevalidating}, {StateRevalidating, StateFresh}, {StateRevalidating, StateStale},
		{StateFresh, StateStale}, {StateFresh, StateRevalidating}, {StateStale, StateRevalidating},
		{StateFresh, StatePurged}, {StateStale, StatePurged}, {StatePurged, StateMiss},
	}
	for _, pair := range legal {
		if err := ValidateTransition(pair[0], pair[1]); err != nil {
			t.Errorf("expected legal %s->%s: %v", pair[0], pair[1], err)
		}
	}
	illegal := [][2]State{
		{StateFresh, StateMiss}, {StateStale, StateMiss}, {StatePurged, StateFresh}, {StateMiss, StateFresh},
	}
	for _, pair := range illegal {
		if err := ValidateTransition(pair[0], pair[1]); err == nil {
			t.Errorf("expected illegal %s->%s", pair[0], pair[1])
		}
	}
}

func TestTTLBoundariesInclusiveSWR(t *testing.T) {
	written := int64(1000)
	fresh, stale := ComputeWindows(written, 2000, 1000)
	if fresh != 3000 || stale != 4000 {
		t.Fatalf("windows=(%d,%d)", fresh, stale)
	}
	cases := []struct {
		now   int64
		class LookupClass
	}{
		{2999, ClassFresh},
		{3000, ClassStaleSWR},
		{4000, ClassStaleSWR},
		{4001, ClassStaleWait},
	}
	for _, tc := range cases {
		got := Classify(`"etag"`, StateFresh, written, fresh, stale, tc.now, 1000)
		if got.Class != tc.class {
			t.Errorf("now=%d class=%s want %s (%s)", tc.now, got.Class, tc.class, got.Reason)
		}
	}
	noSWR := Classify(`"etag"`, StateFresh, written, fresh, fresh, fresh, 0)
	if noSWR.Class != ClassStaleWait {
		t.Errorf("zero SWR at boundary class=%s", noSWR.Class)
	}
}
