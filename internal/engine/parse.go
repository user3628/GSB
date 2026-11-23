package engine

import (
	"errors"
	"fmt"
	"sort"
)

// ParseWindows validates every window. Malformed windows are converted into
// rejected conclusions carrying E_WINDOW_MALFORMED and never enter the
// scheduling queue.
func ParseWindows(ws []Window) ([]Window, []Conclusion) {
	idCount := map[string]int{}
	for _, w := range ws {
		for _, e := range w.Events {
			idCount[e.ID]++
		}
	}

	valid := make([]Window, 0, len(ws))
	malformed := make([]Conclusion, 0)
	for _, w := range ws {
		if err := validateWindow(w, idCount); err != nil {
			malformed = append(malformed, Conclusion{
				WindowID: w.ID,
				Verdict:  VerdictRejected,
				Codes:    []string{CodeWindowMalformed},
				Reason:   err.Error(),
			})
			continue
		}
		valid = append(valid, w)
	}
	sort.Slice(valid, func(i, j int) bool { return valid[i].ID < valid[j].ID })
	sort.Slice(malformed, func(i, j int) bool { return malformed[i].WindowID < malformed[j].WindowID })
	return valid, malformed
}

func validateWindow(w Window, idCount map[string]int) error {
	if w.ID == "" {
		return errors.New("empty window id")
	}
	if len(w.Events) == 0 {
		return errors.New("window has no events")
	}
	if w.Claim.Depth < 1 {
		return fmt.Errorf("resource claim depth %d out of domain", w.Claim.Depth)
	}
	if w.Claim.Memory < 1 {
		return fmt.Errorf("resource claim memory %d out of domain", w.Claim.Memory)
	}
	for _, e := range w.Events {
		if e.ID == "" {
			return errors.New("event with empty id")
		}
		if idCount[e.ID] > 1 {
			return fmt.Errorf("duplicate event id %s", e.ID)
		}
		if e.Weight < 1 || e.Weight > 10 {
			return fmt.Errorf("event %s weight %d out of domain [1,10]", e.ID, e.Weight)
		}
		switch e.Op {
		case OpCreate, OpRead, OpUpdate, OpDelete, OpGrant, OpRevoke:
		default:
			return fmt.Errorf("event %s has unknown op %q", e.ID, e.Op)
		}
	}
	return nil
}
