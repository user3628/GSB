package policy

import "sort"

// TraceNode is one deterministic step in a rule dependency chain.
type TraceNode struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Detail  string `json:"detail,omitempty"`
	Missing bool   `json:"missing,omitempty"`
}

// Trace is the dependency chain for a single rule.
type Trace struct {
	RuleID string      `json:"rule_id"`
	Found  bool        `json:"found"`
	Chain  []TraceNode `json:"chain"`
}

// TraceRule follows rule dependencies first, then the referenced entity's
// parent hierarchy. Output order is deterministic.
func TraceRule(p *Policy, ruleID string) Trace {
	trace := Trace{RuleID: ruleID, Chain: []TraceNode{}}
	rule, ok := p.ruleByID(ruleID)
	if !ok {
		return trace
	}
	trace.Found = true
	visited := make(map[string]bool)
	var walkRule func(Rule)
	walkRule = func(current Rule) {
		if visited["rule/"+current.ID] {
			return
		}
		visited["rule/"+current.ID] = true
		trace.Chain = append(trace.Chain, TraceNode{ID: current.ID, Kind: KindRule, Detail: current.Effect + " " + current.Action})
		deps := append([]string(nil), current.DependsOn...)
		sort.Strings(deps)
		for _, dep := range deps {
			if next, exists := p.ruleByID(dep); exists {
				walkRule(next)
			} else {
				trace.Chain = append(trace.Chain, TraceNode{ID: dep, Kind: KindRule, Missing: true})
			}
		}
	}
	walkRule(rule)

	kind, entity, exists := p.entityByID(rule.Ref)
	if !exists {
		trace.Chain = append(trace.Chain, TraceNode{ID: rule.Ref, Kind: KindRule, Missing: true})
		return trace
	}
	currentKind, currentID := kind, rule.Ref
	for currentID != "" {
		if visited["entity/"+currentID] {
			break
		}
		visited["entity/"+currentID] = true
		trace.Chain = append(trace.Chain, TraceNode{ID: currentID, Kind: currentKind, Detail: entityDetail(entity)})
		parent := p.parentID(currentKind, currentID)
		if parent == "" {
			break
		}
		nextKind, nextEntity, ok := p.entityByID(parent)
		if !ok {
			trace.Chain = append(trace.Chain, TraceNode{ID: parent, Kind: currentKind, Missing: true})
			break
		}
		currentKind, currentID, entity = nextKind, parent, nextEntity
	}
	return trace
}

func entityDetail(value any) string {
	switch v := value.(type) {
	case Directory:
		return v.Path
	case ProcessGroup:
		return "executables=" + joinStrings(v.Executables)
	case RegistryPath:
		return v.Path
	default:
		return ""
	}
}

func joinStrings(values []string) string {
	out := ""
	for i, value := range values {
		if i > 0 {
			out += ","
		}
		out += value
	}
	return out
}
