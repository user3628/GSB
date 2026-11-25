package bus

import "testing"

func TestAllowedTransitionMatrix(t *testing.T) {
	states := []EventState{StateAccepted, StateDispatched, StateAcked, StateRedelivered, StateDeadLettered}
	legal := map[EventState]map[EventState]bool{
		StateAccepted:     {StateDispatched: true, StateDeadLettered: true},
		StateDispatched:   {StateAcked: true, StateRedelivered: true, StateDeadLettered: true},
		StateRedelivered:  {StateDispatched: true, StateDeadLettered: true},
		StateAcked:        {StateRedelivered: true},
		StateDeadLettered: {StateRedelivered: true},
	}
	for _, from := range states {
		for _, to := range states {
			want := legal[from][to]
			if got := AllowedTransition(from, to); got != want {
				t.Fatalf("AllowedTransition(%s,%s)=%v, want %v", from, to, got, want)
			}
		}
	}
}
