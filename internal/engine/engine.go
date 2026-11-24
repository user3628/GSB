// Package engine implements the traffic verdict core, multi-snapshot
// divergence localization and error-propagation classification.
package engine

import (
	"sort"

	"fwadjudge/internal/codes"
	"fwadjudge/internal/fixed"
	"fwadjudge/internal/ledger"
	"fwadjudge/internal/model"
)

// Verdict is the adjudication result of one probe in one snapshot.
type Verdict struct {
	SnapshotID string
	ProbeID    string
	Action     model.Action
	HitRule    int   // -1 when no rule matched (default deny)
	Implicit   bool  // same-priority conflicting actions
	Conflicts  []int // conflicting rule indices, sorted
}

// Session holds the isolated per-snapshot compare state: rule set,
// error ledger and accumulated fixed-point traffic statistics.
type Session struct {
	SnapshotID string
	Rules      []model.Rule
	Ledger     *ledger.Ledger
	Stats      map[int]fixed.Fixed // hit rule index -> accumulated weight
}

func NewSession(snapshotID string, rules []model.Rule) *Session {
	return &Session{
		SnapshotID: snapshotID,
		Rules:      rules,
		Ledger:     ledger.New(),
		Stats:      map[int]fixed.Fixed{},
	}
}

func matchRule(r *model.Rule, p *model.Probe, src, dst fixed.U128) bool {
	if r.Direction != model.DirAny && r.Direction != p.Direction {
		return false
	}
	if r.Protocol != model.ProtoAny && r.Protocol != p.Protocol {
		return false
	}
	if src.Cmp(r.SrcLo) < 0 || src.Cmp(r.SrcHi) > 0 {
		return false
	}
	if dst.Cmp(r.DstLo) < 0 || dst.Cmp(r.DstHi) > 0 {
		return false
	}
	return p.DstPort >= r.PortLo && p.DstPort <= r.PortHi
}

// VerdictFor adjudicates one probe inside a single snapshot, picking
// the highest-priority matching rule. Equal-priority rules with
// conflicting actions are an implicit divergence.
func VerdictFor(rules []model.Rule, p *model.Probe) (Verdict, *codes.Error) {
	v := Verdict{ProbeID: p.ID, Action: model.ActionDeny, HitRule: -1}
	src, err := fixed.ParseIP(p.SrcIP)
	if err != nil {
		return v, codes.At(codes.ERuleInvalid, err.Error(), "", -1, -1)
	}
	dst, err := fixed.ParseIP(p.DstIP)
	if err != nil {
		return v, codes.At(codes.ERuleInvalid, err.Error(), "", -1, -1)
	}
	best := 0
	matched := -1
	var hits []int
	for i := range rules {
		if !matchRule(&rules[i], p, src, dst) {
			continue
		}
		if matched == -1 || rules[i].Priority > best {
			best = rules[i].Priority
			matched = i
			hits = hits[:0]
			hits = append(hits, i)
		} else if rules[i].Priority == best {
			hits = append(hits, i)
		}
	}
	if matched == -1 {
		return v, nil
	}
	actions := map[model.Action]bool{}
	for _, i := range hits {
		actions[rules[i].Action] = true
	}
	v.HitRule = rules[matched].Index
	v.Action = rules[matched].Action
	if len(actions) > 1 {
		v.Implicit = true
		for _, i := range hits {
			v.Conflicts = append(v.Conflicts, rules[i].Index)
		}
		sort.Ints(v.Conflicts)
		// Deterministic pick: lowest rule index wins the verdict.
		low := hits[0]
		for _, i := range hits {
			if rules[i].Index < rules[low].Index {
				low = i
			}
		}
		v.HitRule = rules[low].Index
		v.Action = rules[low].Action
	}
	return v, nil
}

// Observe adjudicates one probe and accumulates fixed-point traffic
// statistics (weight * count) with per-step truncation accounting.
// On E_FIXED_OVERFLOW nothing is staged into the ledger, so no partial
// writes remain.
func (s *Session) Observe(p model.Probe) (Verdict, *codes.Error) {
	v, err := VerdictFor(s.Rules, &p)
	if err != nil {
		return v, err
	}
	v.SnapshotID = s.SnapshotID
	var staged []ledger.Entry
	weight, werr, berr := fixed.ParseDecimal(p.Weight)
	if berr != nil {
		return v, propagate(berr, s.SnapshotID, v.HitRule)
	}
	if werr > 0 {
		staged = append(staged, ledger.Entry{SnapshotID: s.SnapshotID, RuleIndex: v.HitRule, Field: "weight", Amount: werr})
	}
	count, cerr, berr := fixed.ParseDecimal(p.Count)
	if berr != nil {
		return v, propagate(berr, s.SnapshotID, v.HitRule)
	}
	if cerr > 0 {
		staged = append(staged, ledger.Entry{SnapshotID: s.SnapshotID, RuleIndex: v.HitRule, Field: "count", Amount: cerr})
	}
	acc, merr, berr := fixed.Mul(weight, count)
	if berr != nil {
		return v, propagate(berr, s.SnapshotID, v.HitRule)
	}
	if merr > 0 {
		staged = append(staged, ledger.Entry{SnapshotID: s.SnapshotID, RuleIndex: v.HitRule, Field: "weight*count", Amount: merr})
	}
	// Commit point: only now do staged entries become visible.
	s.Ledger.Apply(staged)
	s.Stats[v.HitRule] += acc
	return v, nil
}

// propagate re-anchors a conversion error to the current snapshot and
// rule while preserving its business code.
func propagate(err error, snapshotID string, ruleIndex int) *codes.Error {
	if ce, ok := err.(*codes.Error); ok {
		return codes.At(ce.Code, ce.Message, snapshotID, ruleIndex, -1)
	}
	return codes.At(codes.ERuleInvalid, err.Error(), snapshotID, ruleIndex, -1)
}

// Divergence describes a verdict divergence between two snapshots.
type Divergence struct {
	SnapA, SnapB     string
	ProbeID          string
	RuleA, RuleB     int
	Field            string
	ActionA, ActionB model.Action
	ImplicitA        bool
	ImplicitB        bool
	Class            string // "deterministic" or "uncertain"
	LoA, HiA         fixed.Fixed
	LoB, HiB         fixed.Fixed
}

// fieldOrder is the fixed comparison order for divergence localization.
var fieldOrder = []string{"index", "priority", "action", "direction", "protocol", "src", "dst", "port_range"}

func ruleField(r *model.Rule, name string) string {
	switch name {
	case "index":
		return itoa(r.Index)
	case "priority":
		return itoa(r.Priority)
	case "action":
		return string(r.Action)
	case "direction":
		return string(r.Direction)
	case "protocol":
		return string(r.Protocol)
	case "src":
		return r.SrcCIDR
	case "dst":
		return r.DstCIDR
	case "port_range":
		return itoa(r.PortLo) + "-" + itoa(r.PortHi)
	}
	return ""
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

// FirstDivergentField finds the first differing field (in fieldOrder)
// between the two hit rules.
func FirstDivergentField(a, b *model.Rule) string {
	for _, f := range fieldOrder {
		if ruleField(a, f) != ruleField(b, f) {
			return f
		}
	}
	return "implicit_conflict"
}

// ComparePair compares the verdicts of one probe in two sessions and,
// when they diverge, localizes the first diverging rule and field and
// classifies the divergence under error propagation.
func ComparePair(a, b *Session, vA, vB Verdict) *Divergence {
	if vA.Action == vB.Action && !vA.Implicit && !vB.Implicit {
		return nil
	}
	d := &Divergence{
		SnapA: a.SnapshotID, SnapB: b.SnapshotID,
		ProbeID: vA.ProbeID,
		RuleA:   vA.HitRule, RuleB: vB.HitRule,
		ActionA: vA.Action, ActionB: vB.Action,
		ImplicitA: vA.Implicit, ImplicitB: vB.Implicit,
	}
	switch {
	case vA.HitRule == -1 || vB.HitRule == -1:
		d.Field = "hit_presence"
	case vA.HitRule != vB.HitRule:
		d.Field = "index"
	default:
		ra := findRule(a.Rules, vA.HitRule)
		rb := findRule(b.Rules, vB.HitRule)
		if ra != nil && rb != nil {
			d.Field = FirstDivergentField(ra, rb)
		} else {
			d.Field = "hit_presence"
		}
	}
	d.Class, d.LoA, d.HiA, d.LoB, d.HiB = classify(
		a.Stats[vA.HitRule], a.Ledger.BoundUnits(),
		b.Stats[vB.HitRule], b.Ledger.BoundUnits())
	return d
}

func findRule(rules []model.Rule, index int) *model.Rule {
	for i := range rules {
		if rules[i].Index == index {
			return &rules[i]
		}
	}
	return nil
}

// classify builds the error intervals [v-bound, v+bound] for both
// snapshots and reports "deterministic" when they do not overlap.
func classify(vA fixed.Fixed, boundA int64, vB fixed.Fixed, boundB int64) (string, fixed.Fixed, fixed.Fixed, fixed.Fixed, fixed.Fixed) {
	bA := fixed.Fixed(ledger.BoundFixed(boundA))
	bB := fixed.Fixed(ledger.BoundFixed(boundB))
	loA, hiA := vA-bA, vA+bA
	loB, hiB := vB-bB, vB+bB
	if loA <= hiB && loB <= hiA {
		return "uncertain", loA, hiA, loB, hiB
	}
	return "deterministic", loA, hiA, loB, hiB
}
