package model

// Legal alert state transitions.
//
//	CANDIDATE  -> FIRING | SUPPRESSED | RESOLVED
//	FIRING     -> SUPPRESSED | RESOLVED
//	SUPPRESSED -> FIRING | RESOLVED
//	RESOLVED   -> (terminal)
var legalTransitions = map[AlertState]map[AlertState]bool{
	StateCandidate: {
		StateFiring:     true,
		StateSuppressed: true,
		StateResolved:   true,
	},
	StateFiring: {
		StateSuppressed: true,
		StateResolved:   true,
	},
	StateSuppressed: {
		StateFiring:   true,
		StateResolved: true,
	},
	StateResolved: {},
}

// CanTransition reports whether from -> to is a legal lifecycle transition.
func CanTransition(from, to AlertState) bool {
	return legalTransitions[from][to]
}
