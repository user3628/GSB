// Package adjudicate keeps the interactive side and the batch settlement
// side consistent for one event window.
package adjudicate

import (
	"sort"

	"flowtrace/internal/codes"
	"flowtrace/internal/latency"
	"flowtrace/internal/model"
	"flowtrace/internal/policy"
	"flowtrace/internal/trace"
)

// Window tracks one event window on both sides.
type Window struct {
	ID          string
	policy      *policy.Snapshot
	interactive *trace.Engine
	sampler     *latency.Sampler

	arrival []model.Event // events in arrival order

	// interactive-side judgments in arrival order (as displayed)
	interactiveJudgments []model.Judgment
	// settlement-side final judgments in canonical order
	settlementJudgments []model.Judgment
	corrections         []model.Correction
	version             int // shared judgment version, forward-only
	settled             bool
}

// NewWindow creates a window adjudicator sharing the given latency sampler.
func NewWindow(id string, p *policy.Snapshot, sampler *latency.Sampler) *Window {
	return &Window{ID: id, policy: p, interactive: trace.New(p), sampler: sampler, version: 1}
}

// OnEvent feeds one arrived event to the interactive side: it updates the
// trace, records the judgment with the current version, and samples the
// refresh latency on the monotonic logical clock.
func (w *Window) OnEvent(ev model.Event) model.Judgment {
	w.arrival = append(w.arrival, ev)
	j := w.interactive.Apply(ev)
	j.Version = w.version
	w.interactiveJudgments = append(w.interactiveJudgments, j)
	w.sampler.Observe(w.ID, j.Code, ev)
	return j
}

// InteractiveMarks returns the interactive side's current mark set.
func (w *Window) InteractiveMarks() map[string][]string { return w.interactive.Marks() }

// InteractiveJudgments returns the interactive side's judgments as displayed.
func (w *Window) InteractiveJudgments() []model.Judgment {
	return append([]model.Judgment{}, w.interactiveJudgments...)
}

// Corrections returns the forward-only revision records emitted at settlement.
func (w *Window) Corrections() []model.Correction {
	return append([]model.Correction{}, w.corrections...)
}

// Settle recomputes the window in canonical (Seq, LogicalTime) order and
// reconciles with the interactive side. Divergent judgments are never
// silently rewritten: each fix emits a Correction and advances the shared
// version; the interactive display then refers to the same final version.
func (w *Window) Settle() {
	ordered := trace.CanonicalOrder(w.arrival)
	settlement := trace.New(w.policy)
	final := make([]model.Judgment, 0, len(ordered))
	for _, ev := range ordered {
		final = append(final, settlement.Apply(ev))
	}

	byKey := map[string]model.Judgment{}
	for _, j := range w.interactiveJudgments {
		byKey[model.Key(j.EventSeq, j.DstField)] = j
	}
	for i := range final {
		j := &final[i]
		prev, ok := byKey[model.Key(j.EventSeq, j.DstField)]
		if ok && prev.Code == j.Code {
			j.Version = prev.Version
		} else {
			w.version++
			j.Version = w.version
			from := codes.OK
			if ok {
				from = prev.Code
			}
			w.corrections = append(w.corrections, model.Correction{
				EventSeq: j.EventSeq, DstField: j.DstField,
				FromCode: from, ToCode: j.Code, Version: w.version,
			})
		}
	}
	w.settlementJudgments = final
	w.settled = true
}

// SettlementJudgments returns the final settlement judgments.
func (w *Window) SettlementJudgments() []model.Judgment {
	return append([]model.Judgment{}, w.settlementJudgments...)
}

// Version returns the current shared judgment version.
func (w *Window) Version() int { return w.version }

// Consistency compares the interactive display (after corrections) with the
// settlement result, per (EventSeq, DstField). Both sides must agree exactly.
func (w *Window) Consistency() ConsistencyResult {
	settled := map[string]model.Judgment{}
	for _, j := range w.settlementJudgments {
		settled[model.Key(j.EventSeq, j.DstField)] = j
	}
	// Interactive final view: arrival-order judgments with corrections applied.
	corr := map[string]model.Correction{}
	for _, c := range w.corrections {
		corr[model.Key(c.EventSeq, c.DstField)] = c
	}
	res := ConsistencyResult{Consistent: true, Window: w.ID}
	for _, j := range w.interactiveJudgments {
		k := model.Key(j.EventSeq, j.DstField)
		sj, ok := settled[k]
		if !ok {
			res.Consistent = false
			res.Mismatches = append(res.Mismatches, Mismatch{Key: k, Detail: "missing on settlement side"})
			continue
		}
		shown := j.Code
		if c, fixed := corr[k]; fixed {
			shown = c.ToCode // interactive display advanced to the correction
		}
		if shown != sj.Code {
			res.Consistent = false
			res.Mismatches = append(res.Mismatches, Mismatch{Key: k, Detail: "interactive=" + shown + " settlement=" + sj.Code})
		}
	}
	if len(w.interactiveJudgments) != len(w.settlementJudgments) {
		res.Consistent = false
		res.Mismatches = append(res.Mismatches, Mismatch{Key: "*", Detail: "judgment count differs"})
	}
	sort.Slice(res.Mismatches, func(a, b int) bool { return res.Mismatches[a].Key < res.Mismatches[b].Key })
	return res
}

// ConsistencyResult is the per-window two-sided comparison verdict.
type ConsistencyResult struct {
	Window     string     `json:"window"`
	Consistent bool       `json:"consistent"`
	Mismatches []Mismatch `json:"mismatches,omitempty"`
}

// Mismatch describes one divergent judgment.
type Mismatch struct {
	Key    string `json:"key"`
	Detail string `json:"detail"`
}

// Arrival returns the events in arrival order (as received interactively).
func (w *Window) Arrival() []model.Event {
	return append([]model.Event{}, w.arrival...)
}
