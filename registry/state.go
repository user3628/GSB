package registry

// CanTransition defines the complete legal transition graph.
func CanTransition(from, to State) bool {
	if from == to {
		return true
	}
	switch from {
	case StateUnknown:
		return to == StateHealthy || to == StateSuspect || to == StateUnhealthy || to == StateDraining
	case StateHealthy:
		return to == StateSuspect || to == StateUnhealthy || to == StateDraining
	case StateSuspect:
		return to == StateHealthy || to == StateUnhealthy || to == StateDraining
	case StateUnhealthy:
		return to == StateHealthy || to == StateSuspect || to == StateDraining
	case StateDraining:
		return to == StateUnknown
	default:
		return false
	}
}

// nextProbeState applies consecutive success/failure thresholds while staying
// on legal edges. DRAINING is terminal with respect to automatic probes.
func nextProbeState(cur State, ok bool, success, failure, okThreshold, failThreshold int) State {
	if cur == StateDraining {
		return cur
	}
	if ok {
		if success >= okThreshold {
			return StateHealthy
		}
		switch cur {
		case StateUnknown, StateUnhealthy:
			return StateSuspect
		default:
			return cur
		}
	}
	if failure >= failThreshold {
		return StateUnhealthy
	}
	switch cur {
	case StateUnknown, StateHealthy:
		return StateSuspect
	default:
		return cur
	}
}
