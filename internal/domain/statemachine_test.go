package domain

import "testing"

func TestStateMachine(t *testing.T) {
	legal := [][2]string{
		{StatePending, StateAdmitted},
		{StatePending, StateRejected},
		{StateAdmitted, StateBatched},
		{StateAdmitted, StatePreempted},
		{StateBatched, StateRunning},
		{StateBatched, StateAdmitted},
		{StateRunning, StateCompleted},
		{StateRunning, StatePreempted},
		{StatePreempted, StatePending},
	}
	for _, c := range legal {
		if !CanTransition(c[0], c[1]) {
			t.Errorf("expected legal transition %s->%s", c[0], c[1])
		}
	}
	illegal := [][2]string{
		{StatePending, StateRunning},
		{StateBatched, StateCompleted},
		{StateCompleted, StatePending},
		{StateRejected, StateAdmitted},
		{StateAdmitted, StateRunning},
	}
	for _, c := range illegal {
		if CanTransition(c[0], c[1]) {
			t.Errorf("expected illegal transition %s->%s", c[0], c[1])
		}
	}
	if !IsTerminal(StateCompleted) || !IsTerminal(StateRejected) {
		t.Error("completed/rejected must be terminal")
	}
}
