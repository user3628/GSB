package domain

func IsTerminal(state string) bool {
	return state == StateRolledBack || state == StateAborted
}

func CanTransition(from, to string) bool {
	if from == to {
		return false
	}
	switch from {
	case StatePrepared:
		return to == StateCanary || to == StateAborted
	case StateCanary:
		return to == StateObserving || to == StatePromoting || to == StateRolledBack || to == StateAborted
	case StateObserving:
		return to == StatePromoting || to == StateRolledBack || to == StateAborted
	case StatePromoting:
		return to == StateObserving || to == StateRolledBack || to == StateAborted
	default:
		return false
	}
}

func ValidateTransition(from, to string) error {
	if !CanTransition(from, to) {
		return WithContext(NewError(CodeInvalidTransition, "illegal release state transition"), "from_state", from).ContextValue("to_state", to)
	}
	return nil
}

func (e *BusinessError) ContextValue(key string, value any) *BusinessError {
	if e.Context == nil {
		e.Context = map[string]any{}
	}
	e.Context[key] = value
	return e
}
