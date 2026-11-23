// Package trace implements the sensitive-mark propagation engine.
package trace

import (
	"sort"

	"flowtrace/internal/codes"
	"flowtrace/internal/model"
	"flowtrace/internal/policy"
)

// Engine propagates sensitive marks along the event chain. Marks are seeded
// from the policy snapshot's sensitive field declarations and advanced in
// (Seq, LogicalTime) order. The engine is deterministic and single-threaded;
// each window/session owns its own engine instance.
type Engine struct {
	policy *policy.Snapshot

	// field -> mark -> minimal source chain (field path ending at field)
	lineage map[string]map[string][]string

	Judgments []model.Judgment
	version   int
}

// New creates an engine seeded from the given policy snapshot.
func New(p *policy.Snapshot) *Engine {
	e := &Engine{policy: p, lineage: map[string]map[string][]string{}, version: 1}
	fields := make([]string, 0, len(p.SensitiveFields))
	for f := range p.SensitiveFields {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	for _, f := range fields {
		marks := append([]string{}, p.SensitiveFields[f]...)
		sort.Strings(marks)
		lm := map[string][]string{}
		for _, m := range marks {
			lm[m] = []string{f}
		}
		e.lineage[f] = lm
	}
	return e
}

// Marks returns a deterministic copy of the current mark set:
// field -> sorted marks.
func (e *Engine) Marks() map[string][]string {
	out := make(map[string][]string, len(e.lineage))
	for f, lm := range e.lineage {
		marks := make([]string, 0, len(lm))
		for m := range lm {
			marks = append(marks, m)
		}
		sort.Strings(marks)
		out[f] = marks
	}
	return out
}

// Version returns the current judgment version (monotonic, forward-only).
func (e *Engine) Version() int { return e.version }

// propagate applies the per-op propagation semantics.
func (e *Engine) propagate(op model.Op, srcMarks []string) []string {
	switch op {
	case model.OpCrossTableCopy:
		// Cross-table copy carries the full mark set.
		return srcMarks
	case model.OpCrossServiceExport:
		// Cross-service export carries the full mark set and is always
		// boundary-checked against the export destination.
		return srcMarks
	case model.OpAggregateWrite:
		// Aggregation de-identifies marks declared aggregate-safe.
		out := make([]string, 0, len(srcMarks))
		for _, m := range srcMarks {
			if !e.policy.AggregateSafe[m] {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

// Apply advances the engine by one event and returns its judgment.
// Cycle safety: marks only ever grow per field and the (field, mark) space
// is finite, so propagation always terminates; events that would add no new
// mark to the destination (including cycles back to an already-marked field)
// are no-ops and never re-diffuse.
func (e *Engine) Apply(ev model.Event) model.Judgment {
	srcLineage := e.lineage[ev.SrcField]
	srcMarks := make([]string, 0, len(srcLineage))
	for m := range srcLineage {
		srcMarks = append(srcMarks, m)
	}
	sort.Strings(srcMarks)
	propagated := e.propagate(ev.Op, srcMarks)

	j := model.Judgment{EventSeq: ev.Seq, DstField: ev.DstField, Code: codes.OK, Version: e.version}

	if len(propagated) > 0 && ev.SrcField != ev.DstField {
		dstLineage := e.lineage[ev.DstField]
		if dstLineage == nil {
			dstLineage = map[string][]string{}
			e.lineage[ev.DstField] = dstLineage
		}
		for _, m := range propagated {
			if _, exists := dstLineage[m]; exists {
				continue // already marked: cycle / re-arrival, no diffusion
			}
			path := append(append([]string{}, srcLineage[m]...), ev.DstField)
			dstLineage[m] = path
		}
	}

	if len(propagated) > 0 && !e.policy.Authorized(ev.DstField) {
		j.Code = codes.EFlowEscaped
		j.Marks = propagated
		j.Path = minimalPath(e.lineage[ev.DstField], propagated)
	}
	e.Judgments = append(e.Judgments, j)
	return j
}

// minimalPath returns the shortest source chain among the escaped marks.
func minimalPath(dstLineage map[string][]string, marks []string) []string {
	var best []string
	for _, m := range marks {
		p := dstLineage[m]
		if best == nil || (len(p) > 0 && len(p) < len(best)) {
			best = p
		}
		if best == nil && len(p) > 0 {
			best = p
		}
	}
	return append([]string{}, best...)
}

// CanonicalOrder sorts events by (Seq, LogicalTime) — the dual ordering used
// by the settlement side.
func CanonicalOrder(events []model.Event) []model.Event {
	out := append([]model.Event{}, events...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Seq != out[j].Seq {
			return out[i].Seq < out[j].Seq
		}
		return out[i].LogicalTime < out[j].LogicalTime
	})
	return out
}
