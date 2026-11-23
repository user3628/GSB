package policy

import (
	"fmt"
	"sort"

	"sandboxpolicy/business"
)

// StateIssues scans the complete snapshot for unresolved references and
// hierarchy links. Unlike Closure, it is not tied to one change; it is useful
// when diagnosing a later snapshot that inherited old invalidations.
func StateIssues(p *Policy) []AffectedRule {
	if p == nil {
		return []AffectedRule{}
	}
	entityIndex := make(map[string]string, len(p.Directories)+len(p.ProcessGroups)+len(p.RegistryPaths))
	for _, v := range p.Directories {
		entityIndex[v.ID] = KindDirectory
	}
	for _, v := range p.ProcessGroups {
		entityIndex[v.ID] = KindProcessGroup
	}
	for _, v := range p.RegistryPaths {
		entityIndex[v.ID] = KindRegistryPath
	}
	parents := make(map[string]string)
	for _, v := range p.Directories {
		parents[KindDirectory+"/"+v.ID] = v.ParentID
	}
	for _, v := range p.ProcessGroups {
		parents[KindProcessGroup+"/"+v.ID] = v.ParentID
	}
	for _, v := range p.RegistryPaths {
		parents[KindRegistryPath+"/"+v.ID] = v.ParentID
	}
	direct := make(map[string]AffectedRule)
	for _, rule := range p.Rules {
		kind, ok := entityIndex[rule.Ref]
		if !ok {
			direct[rule.ID] = AffectedRule{RuleID: rule.ID, Code: business.CodeDepInvalidated, Effect: rule.Effect, Action: rule.Action, Ref: rule.Ref, Direct: true, Via: []string{rule.ID, rule.Ref}}
			continue
		}
		missingParent := ""
		for current := rule.Ref; current != ""; current = parents[kind+"/"+current] {
			parent := parents[kind+"/"+current]
			if parent == "" {
				break
			}
			if _, exists := entityIndex[parent]; !exists {
				missingParent = parent
				break
			}
		}
		if missingParent != "" {
			direct[rule.ID] = AffectedRule{RuleID: rule.ID, Code: business.CodeDepInvalidated, Effect: rule.Effect, Action: rule.Action, Ref: rule.Ref, Direct: false, Via: []string{rule.ID, rule.Ref, missingParent}}
			continue
		}
		for _, dep := range rule.DependsOn {
			if _, exists := p.ruleByID(dep); !exists {
				direct[rule.ID] = AffectedRule{RuleID: rule.ID, Code: business.CodeDepInvalidated, Effect: rule.Effect, Action: rule.Action, Ref: rule.Ref, Direct: false, Via: []string{rule.ID, dep}}
				break
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for _, rule := range p.Rules {
			if _, exists := direct[rule.ID]; exists {
				continue
			}
			for _, dep := range rule.DependsOn {
				parent, invalid := direct[dep]
				if !invalid {
					continue
				}
				via := append([]string{rule.ID}, parent.Via...)
				direct[rule.ID] = AffectedRule{RuleID: rule.ID, Code: business.CodeDepInvalidated, Effect: rule.Effect, Action: rule.Action, Ref: rule.Ref, Direct: false, Via: via}
				changed = true
				break
			}
		}
	}
	out := make([]AffectedRule, 0, len(direct))
	for _, issue := range direct {
		out = append(out, issue)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RuleID != out[j].RuleID {
			return out[i].RuleID < out[j].RuleID
		}
		return out[i].Code < out[j].Code
	})
	return out
}

// ValidateState verifies structural invariants of a persisted snapshot while
// explicitly permitting dangling references recorded by dependency diagnosis.
func ValidateState(p *Policy) error {
	if err := NormalizeSnapshot(p); err != nil {
		return err
	}
	for _, rule := range p.Rules {
		if _, _, ok := p.entityByID(rule.Ref); !ok {
			continue
		}
		if want := actionKind[rule.Action]; want == "" {
			return fmt.Errorf("rule %s has unsupported action %s", rule.ID, rule.Action)
		}
	}
	return nil
}
