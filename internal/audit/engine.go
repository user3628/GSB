package audit

import (
	"fmt"
	"math/big"
	"sort"

	"configaudit/internal/fixedpoint"
	"configaudit/internal/model"
	"configaudit/internal/parser"
	"configaudit/internal/report"
	"configaudit/internal/units"
)

// Re-export shared result types so engine signatures stay stable while the
// persistence layer depends on the standalone report package (no import cycle).
type (
	Contribution      = report.Contribution
	CombinationResult = report.CombinationResult
	KeyResult         = report.KeyResult
	Report            = report.Report
)

// Engine runs deterministic audits over a parsed dataset.
type Engine struct {
	ds *parser.Dataset
}

// NewEngine builds an engine over a dataset.
func NewEngine(ds *parser.Dataset) *Engine { return &Engine{ds: ds} }

// Run executes the full traversal without sampling.
func (e *Engine) Run() (*Report, error) {
	result := &Report{NodeCount: len(e.ds.Nodes), KeyCount: len(e.ds.Specs)}
	// group source lines by node|key
	groups := map[string][]model.SourceLine{}
	for _, sl := range e.ds.Sources {
		k := sl.NodeID + "|" + sl.KeyPath
		groups[k] = append(groups[k], sl)
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, gk := range keys {
		kr, err := e.auditKey(groups[gk])
		if err != nil {
			return nil, err
		}
		result.Keys = append(result.Keys, kr)
		result.CombinationCnt += len(kr.Combinations)
	}
	if err := e.checkCompleteness(result); err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) auditKey(lines []model.SourceLine) (*KeyResult, error) {
	if len(lines) == 0 {
		return nil, fixedpoint.NewError(fixedpoint.ESpecInvalid, "empty source group")
	}
	nodeID, keyPath := lines[0].NodeID, lines[0].KeyPath
	spec := e.ds.SpecByKey[keyPath]
	node := e.ds.NodeByID[nodeID]

	// 1. normalize every source description (both generations share the path).
	parsedLines := make([]parsedEntry, 0, len(lines))
	for _, sl := range lines {
		rec, err := e.normalize(sl, spec.Scale)
		if err != nil {
			return nil, err
		}
		ch := findChannel(node, sl.Channel)
		parsedLines = append(parsedLines, parsedEntry{sl: sl, rec: rec, ch: ch})
	}

	// 2. equal-priority conflict detection across distinct values.
	if err := detectPriorityConflict(parsedLines); err != nil {
		return nil, err
	}

	// 3. deterministic ordering: priority desc, then declared channel order.
	sort.SliceStable(parsedLines, func(i, j int) bool {
		if parsedLines[i].ch.Priority != parsedLines[j].ch.Priority {
			return parsedLines[i].ch.Priority > parsedLines[j].ch.Priority
		}
		return parsedLines[i].ch.Order < parsedLines[j].ch.Order
	})

	kr := &KeyResult{NodeID: nodeID, KeyPath: keyPath}
	overlayErr := int64(0)
	exclusiveHit := false
	for i, p := range parsedLines {
		chain, err := fixedpoint.EvalChain(p.rec.Normalized, spec.Chain)
		if err != nil {
			return nil, err
		}
		c := Contribution{
			Channel: p.sl.Channel, Generation: p.rec.Generation, RawText: p.sl.Text,
			Record: p.rec, Chain: chain, OrderIndex: i,
		}
		if exclusiveHit {
			c.Shadowed = true
			c.ShadowReason = "shadowed_by_exclusive_higher_priority_channel"
		}
		// Overlay order variation can change rounding: sum at canonical scale.
		if !exclusiveHit {
			if i > 0 {
				sum, err := fixedpoint.Add(kr.Effective, chain.Final)
				if err != nil {
					return nil, err
				}
				kr.Effective = sum
			} else {
				kr.Effective = chain.Final
			}
			overlayErr += p.rec.CumErrorULP
			kr.OverlayOrder = append(kr.OverlayOrder, p.sl.Channel)
			if p.ch.Exclusive {
				exclusiveHit = true
				kr.WinnerChannel = p.sl.Channel
			}
		}
		kr.Contributions = append(kr.Contributions, c)
	}
	if kr.WinnerChannel == "" && len(parsedLines) > 0 {
		kr.WinnerChannel = parsedLines[0].sl.Channel
	}
	kr.OverlayErrorULP = overlayErr

	// 4. per channel combinations + high precision reference comparison.
	for _, p := range parsedLines {
		chain, err := fixedpoint.EvalChain(p.rec.Normalized, spec.Chain)
		if err != nil {
			return nil, err
		}
		ref, ok := e.ds.References[refKey(nodeID, keyPath, p.sl.Channel)]
		if !ok {
			return nil, fixedpoint.NewError(fixedpoint.ESpecInvalid,
				"missing reference for %s/%s/%s", nodeID, keyPath, p.sl.Channel)
		}
		comb, err := compareCombination(nodeID, keyPath, p.sl.Channel, spec, chain, ref)
		if err != nil {
			return nil, err
		}
		kr.Combinations = append(kr.Combinations, comb)
	}
	sort.Slice(kr.Combinations, func(i, j int) bool {
		return kr.Combinations[i].Channel < kr.Combinations[j].Channel
	})
	// Effective value bound: overlay error plus each chain bound.
	effBound := big.NewRat(0, 1)
	ulp := ratULP(kr.Effective.Scale)
	for _, c := range kr.Contributions {
		if c.Shadowed {
			continue
		}
		effBound.Add(effBound, c.Chain.BoundRat)
	}
	effBound.Add(effBound, new(big.Rat).Mul(big.NewRat(overlayErr, 1), ulp))
	kr.EffectiveBound = effBound.RatString()
	return kr, nil
}

func (e *Engine) normalize(sl model.SourceLine, scale int) (units.Record, error) {
	if sl.Format == "v2" {
		return units.ParseStructured(sl.Text, units.Structured{
			Value: sl.Value, Unit: sl.Unit, Priority: sl.Priority,
			Channel: sl.Channel, Order: sl.Order,
		}, scale)
	}
	return units.ParseLegacy(sl.Text, scale)
}

// parsedEntry is one normalized source line with its resolved channel.
type parsedEntry struct {
	sl  model.SourceLine
	rec units.Record
	ch  model.Channel
}

func detectPriorityConflict(ps []parsedEntry) error {
	type sig struct {
		priority int
		mant     int64
		scale    int
		channel  string
	}
	seen := map[int]sig{}
	for _, p := range ps {
		pr := p.ch.Priority
		if prev, ok := seen[pr]; ok {
			if prev.mant != p.rec.Normalized.Mantissa || prev.scale != p.rec.Normalized.Scale {
				return fixedpoint.NewError(fixedpoint.EPriorityConflict,
					"equal priority %d conflict on %s/%s between channels %s and %s",
					pr, p.sl.NodeID, p.sl.KeyPath, prev.channel, p.sl.Channel)
			}
		} else {
			seen[pr] = sig{priority: pr, mant: p.rec.Normalized.Mantissa, scale: p.rec.Normalized.Scale, channel: p.sl.Channel}
		}
	}
	return nil
}

func compareCombination(nodeID, keyPath, channel string, spec model.Spec, chain *fixedpoint.ChainResult, ref model.Reference) (CombinationResult, error) {
	refRat, ok := new(big.Rat).SetString(ref.Exact)
	if !ok {
		return CombinationResult{}, fixedpoint.NewError(fixedpoint.ESpecInvalid, "bad reference %q", ref.Exact)
	}
	finalRat := chain.Final.Rat()
	absDev := new(big.Rat).Abs(new(big.Rat).Sub(finalRat, refRat))
	relDev := big.NewRat(0, 1)
	if refRat.Sign() != 0 {
		relDev = new(big.Rat).Quo(absDev, new(big.Rat).Abs(refRat))
	}
	declared, ok := new(big.Rat).SetString(spec.DeclaredBound)
	if !ok {
		return CombinationResult{}, fixedpoint.NewError(fixedpoint.ESpecInvalid, "bad declared bound %q", spec.DeclaredBound)
	}
	// Both constraints must hold, so the operative bound is the tighter one.
	// When the chain is exact (bound 0) the declared bound is the only envelope.
	bound := chain.BoundRat
	if bound.Sign() == 0 || declared.Cmp(bound) < 0 {
		bound = declared
	}
	within := absDev.Cmp(bound) <= 0
	itemID := fmt.Sprintf("%s|%s|%s", nodeID, keyPath, channel)
	if !within {
		return CombinationResult{}, fixedpoint.NewError(fixedpoint.EErrorBoundExceeded,
			"item %s abs deviation %s exceeds bound %s", itemID, absDev.RatString(), bound.RatString()).
			WithItem(itemID)
	}
	return CombinationResult{
		ItemID: itemID, NodeID: nodeID, KeyPath: keyPath, Channel: channel,
		Normalized:   chain.Steps[0].Input,
		ChainFinal:   chain.Final,
		ErrorBound:   bound.RatString(),
		ActualError:  chain.ActualAbsError().RatString(),
		Reference:    ref.Exact,
		AbsDeviation: absDev.RatString(),
		RelDeviation: relDev.RatString(),
		WithinBound:  true,
		Steps:        chain.Steps,
	}, nil
}

func (e *Engine) checkCompleteness(r *Report) error {
	expected := 0
	for _, n := range e.ds.Nodes {
		for _, spec := range e.ds.Specs {
			for _, ch := range n.Channels {
				if e.hasSource(n.ID, spec.KeyPath, ch.Name) {
					expected++
				}
			}
		}
	}
	if expected != r.CombinationCnt {
		return fixedpoint.NewError(fixedpoint.ESpecInvalid,
			"incomplete traversal: expected %d combinations, got %d", expected, r.CombinationCnt)
	}
	return nil
}

func (e *Engine) hasSource(nodeID, keyPath, channel string) bool {
	for _, sl := range e.ds.Sources {
		if sl.NodeID == nodeID && sl.KeyPath == keyPath && sl.Channel == channel {
			return true
		}
	}
	return false
}

func findChannel(n model.Node, name string) model.Channel {
	for _, c := range n.Channels {
		if c.Name == name {
			return c
		}
	}
	return model.Channel{Name: name}
}

func refKey(node, key, channel string) string { return node + "|" + key + "|" + channel }

func ratULP(scale int) *big.Rat {
	pow, ok := fixedpoint.Pow10(scale)
	if !ok {
		return big.NewRat(1, 1)
	}
	return big.NewRat(1, pow)
}
