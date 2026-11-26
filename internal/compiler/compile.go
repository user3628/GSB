package compiler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"netpolcompiler/internal/fixedpoint"
	"netpolcompiler/internal/model"
)

type SelectorEntry struct {
	EntryID           string              `json:"entryId"`
	RuleID            string              `json:"ruleId"`
	Namespace         string              `json:"namespace"`
	Kind              string              `json:"kind"`
	Selector          model.LabelSelector `json:"selector"`
	CanonicalSelector string              `json:"canonicalSelector"`
	ConstraintCount   int                 `json:"constraintCount"`
}

type ContainmentRelation struct {
	Kind            string `json:"kind"`
	Relation        string `json:"relation"`
	AEntryID        string `json:"aEntryId"`
	BEntryID        string `json:"bEntryId"`
	SpecificEntryID string `json:"specificEntryId,omitempty"`
	GeneralEntryID  string `json:"generalEntryId,omitempty"`
	SpecificRuleID  string `json:"specificRuleId,omitempty"`
	GeneralRuleID   string `json:"generalRuleId,omitempty"`
}

type SelectorIndex struct {
	Namespace    string                `json:"namespace"`
	Entries      []SelectorEntry       `json:"entries"`
	Containments []ContainmentRelation `json:"containments"`
}

type ConflictPair struct {
	Namespace         string          `json:"namespace"`
	RuleIDs           [2]string       `json:"ruleIds"`
	Actions           [2]model.Action `json:"actions"`
	Priorities        [2]int          `json:"priorities"`
	DeclarationOrders [2]string       `json:"declarationOrders"`
	Protocol          string          `json:"protocol"`
	PortOverlaps      []PortInterval  `json:"portOverlaps"`
	WinnerRuleID      string          `json:"winnerRuleId"`
	WinningAction     model.Action    `json:"winningAction"`
	Resolution        string          `json:"resolution"`
}

type AdjudicationView struct {
	Namespace     string         `json:"namespace"`
	OrderRule     string         `json:"orderRule"`
	ConflictPairs []ConflictPair `json:"conflictPairs"`
}

type CompiledNamespace struct {
	Namespace      string            `json:"namespace"`
	Version        int64             `json:"version"`
	RuleCount      int               `json:"ruleCount"`
	Rules          []model.Rule      `json:"rules"`
	SelectorIndex  SelectorIndex     `json:"selectorIndex"`
	PortView       PortView          `json:"portView"`
	Adjudication   AdjudicationView  `json:"adjudication"`
	Consistency    fixedpoint.Result `json:"consistency"`
	CompiledDigest string            `json:"compiledDigest"`
}

type GlobalReport struct {
	Version               int64                        `json:"version"`
	Scale                 int64                        `json:"scale"`
	Namespaces            []fixedpoint.Result          `json:"namespaces"`
	Ranking               []fixedpoint.RankedNamespace `json:"ranking"`
	AggregateScoreQ       int64                        `json:"aggregateScoreQ"`
	AggregateScoreDecimal string                       `json:"aggregateScoreDecimal"`
	DefaultThresholdQ     int64                        `json:"defaultThresholdQ"`
	DefaultTolerancePpm   int64                        `json:"defaultRelativeTolerancePpm"`
	Formula               string                       `json:"formula"`
}

func CompileNamespace(version int64, namespace string, rules []model.Rule) *CompiledNamespace {
	namespaceRules := make([]model.Rule, 0, len(rules))
	for _, rule := range rules {
		if rule.Namespace == namespace {
			namespaceRules = append(namespaceRules, rule)
		}
	}
	orderedRules := model.CanonicalRules(namespaceRules)

	selectorIndex := buildSelectorIndex(namespace, orderedRules)
	portView := CompilePorts(namespace, orderedRules)
	conflicts := buildConflictPairs(namespace, orderedRules)

	totalPairs := len(orderedRules) * (len(orderedRules) - 1) / 2
	rawIntervals := portView.TotalRawCount
	mergedIntervals := portView.TotalMergedCount
	consistency := fixedpoint.Compute(fixedpoint.Input{
		Namespace: namespace, RuleCount: len(orderedRules), TotalPairs: totalPairs,
		ConflictPairs: len(conflicts), RawIntervals: rawIntervals, MergedIntervals: mergedIntervals,
		RuleRefCount: 10, SnapshotVersion: version,
	})

	compiled := &CompiledNamespace{
		Namespace: namespace, Version: version, RuleCount: len(orderedRules), Rules: orderedRules,
		SelectorIndex: selectorIndex, PortView: portView,
		Adjudication: AdjudicationView{
			Namespace:     namespace,
			OrderRule:     "priority integer descending; ties retain canonical declaration order (explicit declarationOrder, otherwise rule id lexicographic)",
			ConflictPairs: conflicts,
		},
		Consistency: consistency,
	}
	compiled.CompiledDigest = DigestCompiled(compiled)
	return compiled
}

func buildSelectorIndex(namespace string, rules []model.Rule) SelectorIndex {
	index := SelectorIndex{Namespace: namespace, Entries: []SelectorEntry{}, Containments: []ContainmentRelation{}}
	for _, rule := range rules {
		source := NormalizeSelector(rule.Source)
		destination := NormalizeSelector(rule.Destination)
		index.Entries = append(index.Entries,
			SelectorEntry{
				EntryID: rule.ID + ":source", RuleID: rule.ID, Namespace: namespace, Kind: "source",
				Selector: source, CanonicalSelector: CanonicalSelector(source), ConstraintCount: selectorConstraintCount(source),
			},
			SelectorEntry{
				EntryID: rule.ID + ":destination", RuleID: rule.ID, Namespace: namespace, Kind: "destination",
				Selector: destination, CanonicalSelector: CanonicalSelector(destination), ConstraintCount: selectorConstraintCount(destination),
			},
		)
	}

	for i := 0; i < len(index.Entries); i++ {
		for j := i + 1; j < len(index.Entries); j++ {
			a := index.Entries[i]
			b := index.Entries[j]
			if a.Kind != b.Kind {
				continue
			}
			aSubsetB := SelectorSubsetOf(a.Selector, b.Selector)
			bSubsetA := SelectorSubsetOf(b.Selector, a.Selector)
			if !aSubsetB && !bSubsetA {
				continue
			}
			relation := ContainmentRelation{Kind: a.Kind, AEntryID: a.EntryID, BEntryID: b.EntryID}
			if aSubsetB && bSubsetA {
				relation.Relation = "equal"
				relation.SpecificEntryID = a.EntryID
				relation.GeneralEntryID = b.EntryID
				relation.SpecificRuleID = a.RuleID
				relation.GeneralRuleID = b.RuleID
			} else if aSubsetB {
				relation.Relation = "strict_subset"
				relation.SpecificEntryID = a.EntryID
				relation.GeneralEntryID = b.EntryID
				relation.SpecificRuleID = a.RuleID
				relation.GeneralRuleID = b.RuleID
			} else {
				relation.Relation = "strict_subset"
				relation.SpecificEntryID = b.EntryID
				relation.GeneralEntryID = a.EntryID
				relation.SpecificRuleID = b.RuleID
				relation.GeneralRuleID = a.RuleID
			}
			index.Containments = append(index.Containments, relation)
		}
	}
	return index
}

func buildConflictPairs(namespace string, rules []model.Rule) []ConflictPair {
	pairs := []ConflictPair{}
	for i := 0; i < len(rules); i++ {
		for j := i + 1; j < len(rules); j++ {
			left, right := rules[i], rules[j]
			if left.Action == right.Action || left.Protocol != right.Protocol {
				continue
			}
			if !SelectorsCanOverlap(left.Source, right.Source) || !SelectorsCanOverlap(left.Destination, right.Destination) {
				continue
			}
			overlaps := IntersectPortRanges(left.Ports, right.Ports)
			if len(overlaps) == 0 {
				continue
			}
			winnerIndex := i
			resolution := "declaration_order"
			if left.Priority != right.Priority {
				if right.Priority > left.Priority {
					winnerIndex = j
				}
				resolution = "priority_descending"
			}
			winner := rules[winnerIndex]
			pairs = append(pairs, ConflictPair{
				Namespace:  namespace,
				RuleIDs:    [2]string{left.ID, right.ID},
				Actions:    [2]model.Action{left.Action, right.Action},
				Priorities: [2]int{left.Priority, right.Priority},
				DeclarationOrders: [2]string{
					declarationOrderLabel(left), declarationOrderLabel(right),
				},
				Protocol:      string(left.Protocol),
				PortOverlaps:  overlaps,
				WinnerRuleID:  winner.ID,
				WinningAction: winner.Action,
				Resolution:    resolution,
			})
		}
	}
	return pairs
}

func declarationOrderLabel(rule model.Rule) string {
	if rule.DeclarationOrder != nil {
		return fmt.Sprintf("explicit:%d", *rule.DeclarationOrder)
	}
	return "canonical-rule-id"
}

func BuildGlobalReport(version int64, namespaces map[string]*CompiledNamespace, tolerancePpm int64) *GlobalReport {
	results := make([]fixedpoint.Result, 0, len(namespaces))
	for _, compiled := range namespaces {
		if compiled == nil {
			continue
		}
		results = append(results, compiled.Consistency)
	}
	sortResultsByNamespace(results)
	var aggregateScoreQ int64
	if len(results) > 0 {
		var scoreSum int64
		for _, result := range results {
			scoreSum += result.ScoreQ
		}
		aggregateScoreQ = (scoreSum + int64(len(results))/2) / int64(len(results))
	}
	return &GlobalReport{
		Version: version, Scale: fixedpoint.Scale, Namespaces: results,
		Ranking:               fixedpoint.Rank(results, tolerancePpm),
		AggregateScoreQ:       aggregateScoreQ,
		AggregateScoreDecimal: fmt.Sprintf("%d.%06d", aggregateScoreQ/fixedpoint.Scale, aggregateScoreQ%fixedpoint.Scale),
		DefaultThresholdQ:     750_000, DefaultTolerancePpm: 50_000,
		Formula: "scoreQ6 = 0.20*min(ruleCount,10)/10 + 0.50*(1-conflictPairs/totalPairs) + 0.30*(rawIntervals-mergedIntervals)/rawIntervals; intermediate divisions use round-half-up to Q6",
	}
}

func sortResultsByNamespace(results []fixedpoint.Result) {
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Namespace < results[i].Namespace {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
}

func DigestCompiled(compiled *CompiledNamespace) string {
	copyValue := *compiled
	copyValue.CompiledDigest = ""
	data, err := json.Marshal(copyValue)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func StableJSON(value any) []byte {
	data, _ := json.Marshal(value)
	return data
}

func stableMarshal(value any) ([]byte, error) {
	return json.Marshal(value)
}
