package domain

// legalTransitions defines the request state machine.
//
//	PENDING   -> ADMITTED | REJECTED | PREEMPTED
//	ADMITTED  -> BATCHED  | REJECTED | PREEMPTED
//	BATCHED   -> RUNNING  | ADMITTED(downgrade/un-batch) | ABORTED rollback
//	RUNNING   -> COMPLETED | PREEMPTED
//	PREEMPTED -> PENDING(re-queue)
//	REJECTED / COMPLETED are terminal.
var legalTransitions = map[string]map[string]bool{
	StatePending: {
		StateAdmitted:  true,
		StateRejected:  true,
		StatePreempted: true,
	},
	StateAdmitted: {
		StateBatched:   true,
		StateRejected:  true,
		StatePreempted: true,
	},
	StateBatched: {
		StateRunning:  true,
		StateAdmitted: true, // downgrade: batch released, request stays admitted
		StateAborted:  true, // recovery rollback path
	},
	StateRunning: {
		StateCompleted: true,
		StatePreempted: true,
	},
	StatePreempted: {
		StatePending: true,
	},
	StateRejected:  {},
	StateCompleted: {},
}

// StateAborted is an internal transient marker used only during recovery;
// persisted request rows are never left in this state (they return to ADMITTED).
const StateAborted = "ABORTED"

// CanTransition reports whether moving from -> to is legal in the state machine.
func CanTransition(from, to string) bool {
	next, ok := legalTransitions[from]
	if !ok {
		return false
	}
	return next[to]
}

// IsTerminal reports whether a state has no outgoing transitions.
func IsTerminal(state string) bool {
	return len(legalTransitions[state]) == 0
}
