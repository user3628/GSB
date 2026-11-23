package cache

import "sort"

var transitions = map[State]map[State]bool{
	StatePending:   {StateWriting: true},
	StateWriting:   {StateAvailable: true},
	StateAvailable: {StateStale: true},
	StateStale:     {StateEvicted: true},
	StateEvicted:   {},
}

func CanTransition(from, to State) bool { return transitions[from][to] }

func ValidateTransition(from, to State) error {
	if CanTransition(from, to) {
		return nil
	}
	return newError(CodeInvalidTransition, "cache entry state transition is not allowed", map[string]any{
		"from": from,
		"to":   to,
	})
}

func AllowedTransitions(from State) []State {
	next := make([]State, 0, len(transitions[from]))
	for state, allowed := range transitions[from] {
		if allowed {
			next = append(next, state)
		}
	}
	sort.Slice(next, func(i, j int) bool { return next[i] < next[j] })
	return next
}
