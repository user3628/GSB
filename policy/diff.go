package policy

import (
	"fmt"
	"sort"
	"strings"

	"sandboxpolicy/business"
)

// Trigger identifies the upstream operation that invalidated dependent rules.
type Trigger struct {
	Operation string `json:"operation"`
	Kind      string `json:"kind"`
	ID        string `json:"id"`
	Change    string `json:"change"`
	Detail    string `json:"detail"`
}

// AffectedRule is a directly or transitively invalidated rule.
type AffectedRule struct {
	RuleID string        `json:"rule_id"`
	Code   business.Code `json:"code"`
	Effect string        `json:"effect,omitempty"`
	Action string        `json:"action,omitempty"`
	Ref    string        `json:"ref,omitempty"`
	Direct bool          `json:"direct"`
	Via    []string      `json:"via"`
}

// Closure is persisted with the snapshot version that introduced it.
type Closure struct {
	Namespace     string          `json:"namespace"`
	Version       int             `json:"version"`
	ParentVersion int             `json:"parent_version"`
	Triggers      []Trigger       `json:"triggers"`
	Direct        []AffectedRule  `json:"direct"`
	Transitive    []AffectedRule  `json:"transitive"`
	BusinessCodes []business.Code `json:"business_codes"`
}

type pendingRule struct {
	code   business.Code
	direct bool
	via    []string
}

// ComputeClosure calculates dependency invalidation for a single incremental
// transition. Entity descendants and reverse rule dependencies are traversed.
func ComputeClosure(old, next *Policy, cs *ChangeSet, version int) Closure {
	closure := Closure{
		Namespace:     next.Namespace,
		Version:       version,
		ParentVersion: version - 1,
		Triggers:      []Trigger{},
		Direct:        []AffectedRule{},
		Transitive:    []AffectedRule{},
		BusinessCodes: []business.Code{},
	}
	if cs == nil || old == nil {
		return closure
	}

	affected := make(map[string]pendingRule)
	for i := range cs.Operations {
		op := &cs.Operations[i]
		switch op.Op {
		case OpDelete:
			kind, id := op.Kind, op.ID
			trigger := Trigger{Operation: OpDelete, Kind: kind, ID: id, Change: "deleted", Detail: fmt.Sprintf("%s %q deleted", kind, id)}
			closure.Triggers = append(closure.Triggers, trigger)
			entityIDs := entitySubtree(old, kind, id)
			for _, rule := range next.Rules {
				if _, ok := entityIDs[rule.Ref]; !ok {
					continue
				}
				direct := rule.Ref == id
				via := []string{rule.ID, id}
				if !direct {
					via = []string{rule.ID, rule.Ref, id}
				}
				addAffected(affected, rule.ID, business.CodeDepInvalidated, direct, via)
			}
			if kind == KindRule {
				propagateDeletedRule(affected, next, id)
			}
		case OpModify:
			if tightened, detail := tightening(old, next, op.Kind, op.ID); tightened {
				trigger := Trigger{Operation: OpModify, Kind: op.Kind, ID: op.ID, Change: "tightened", Detail: detail}
				closure.Triggers = append(closure.Triggers, trigger)
				ids := entitySubtree(old, op.Kind, op.ID)
				for _, rule := range next.Rules {
					if _, ok := ids[rule.Ref]; !ok {
						continue
					}
					direct := rule.Ref == op.ID
					via := []string{rule.ID, op.ID}
					if !direct {
						via = []string{rule.ID, rule.Ref, op.ID}
					}
					addAffected(affected, rule.ID, business.CodeDepRestricted, direct, via)
				}
			}
		}
	}

	propagateRuleDependencies(affected, next)
	closure.BusinessCodes = []business.Code{}
	for _, rule := range next.Rules {
		pending, ok := affected[rule.ID]
		if !ok {
			continue
		}
		item := AffectedRule{
			RuleID: rule.ID,
			Code:   pending.code,
			Effect: rule.Effect,
			Action: rule.Action,
			Ref:    rule.Ref,
			Direct: pending.direct,
			Via:    append([]string(nil), pending.via...),
		}
		if pending.direct {
			closure.Direct = append(closure.Direct, item)
		} else {
			closure.Transitive = append(closure.Transitive, item)
		}
		closure.BusinessCodes = append(closure.BusinessCodes, pending.code)
	}
	sortTriggers(closure.Triggers)
	sortRules(closure.Direct)
	sortRules(closure.Transitive)
	closure.BusinessCodes = business.Codes(closure.BusinessCodes...)
	return closure
}

func addAffected(affected map[string]pendingRule, ruleID string, code business.Code, direct bool, via []string) {
	existing, ok := affected[ruleID]
	if !ok {
		affected[ruleID] = pendingRule{code: code, direct: direct, via: via}
		return
	}
	if direct && !existing.direct {
		existing.direct = true
		existing.via = via
	}
	if betterCode(code, existing.code) {
		existing.code = code
	}
	affected[ruleID] = existing
}

func betterCode(candidate, current business.Code) bool {
	if current == "" {
		return true
	}
	if candidate == current {
		return false
	}
	return candidate < current
}

func propagateDeletedRule(affected map[string]pendingRule, next *Policy, deletedID string) {
	for _, rule := range next.Rules {
		for _, dep := range rule.DependsOn {
			if dep == deletedID {
				addAffected(affected, rule.ID, business.CodeDepInvalidated, false, []string{rule.ID, deletedID})
			}
		}
	}
}

func propagateRuleDependencies(affected map[string]pendingRule, p *Policy) {
	for changed := true; changed; {
		changed = false
		for _, rule := range p.Rules {
			existing, ok := affected[rule.ID]
			if !ok {
				for _, dep := range rule.DependsOn {
					parent, found := affected[dep]
					if !found {
						continue
					}
					via := append([]string{rule.ID}, parent.via...)
					addAffected(affected, rule.ID, parent.code, false, via)
					changed = true
					break
				}
				continue
			}
			for _, dep := range rule.DependsOn {
				parent, found := affected[dep]
				if !found {
					continue
				}
				if betterCode(parent.code, existing.code) {
					existing.code = parent.code
					affected[rule.ID] = existing
					changed = true
				}
			}
		}
	}
}

func entitySubtree(p *Policy, kind, root string) map[string]struct{} {
	out := map[string]struct{}{root: {}}
	changed := true
	for changed {
		changed = false
		switch kind {
		case KindDirectory:
			for _, v := range p.Directories {
				if _, ok := out[v.ID]; ok {
					continue
				}
				if _, ok := out[v.ParentID]; ok {
					out[v.ID] = struct{}{}
					changed = true
				}
			}
		case KindProcessGroup:
			for _, v := range p.ProcessGroups {
				if _, ok := out[v.ID]; ok {
					continue
				}
				if _, ok := out[v.ParentID]; ok {
					out[v.ID] = struct{}{}
					changed = true
				}
			}
		case KindRegistryPath:
			for _, v := range p.RegistryPaths {
				if _, ok := out[v.ID]; ok {
					continue
				}
				if _, ok := out[v.ParentID]; ok {
					out[v.ID] = struct{}{}
					changed = true
				}
			}
		}
	}
	return out
}

func tightening(old, next *Policy, kind, id string) (bool, string) {
	switch kind {
	case KindDirectory:
		before, ok1 := directoryByID(old, id)
		after, ok2 := directoryByID(next, id)
		if !ok1 || !ok2 {
			return false, ""
		}
		if removed := removedStrings(before.Access, after.Access); len(removed) > 0 {
			return true, fmt.Sprintf("directory %q access tightened by removing %s", id, strings.Join(removed, ","))
		}
		if !equalFold(before.Path, after.Path) && pathContains(before.Path, after.Path) {
			return true, fmt.Sprintf("directory %q scope narrowed from %s to %s", id, before.Path, after.Path)
		}
	case KindProcessGroup:
		before, ok1 := processGroupByID(old, id)
		after, ok2 := processGroupByID(next, id)
		if !ok1 || !ok2 {
			return false, ""
		}
		if removed := removedStrings(before.Executables, after.Executables); len(removed) > 0 {
			return true, fmt.Sprintf("process group %q tightened by removing %s", id, strings.Join(removed, ","))
		}
	case KindRegistryPath:
		before, ok1 := registryByID(old, id)
		after, ok2 := registryByID(next, id)
		if !ok1 || !ok2 {
			return false, ""
		}
		if removed := removedStrings(before.Access, after.Access); len(removed) > 0 {
			return true, fmt.Sprintf("registry path %q access tightened by removing %s", id, strings.Join(removed, ","))
		}
		if !equalFold(before.Path, after.Path) && pathContains(before.Path, after.Path) {
			return true, fmt.Sprintf("registry path %q scope narrowed from %s to %s", id, before.Path, after.Path)
		}
	}
	return false, ""
}

func removedStrings(before, after []string) []string {
	set := make(map[string]struct{}, len(after))
	for _, value := range after {
		set[value] = struct{}{}
	}
	var removed []string
	for _, value := range before {
		if _, ok := set[value]; !ok {
			removed = append(removed, value)
		}
	}
	sort.Strings(removed)
	return removed
}

func directoryByID(p *Policy, id string) (Directory, bool) {
	for _, v := range p.Directories {
		if v.ID == id {
			return v, true
		}
	}
	return Directory{}, false
}

func processGroupByID(p *Policy, id string) (ProcessGroup, bool) {
	for _, v := range p.ProcessGroups {
		if v.ID == id {
			return v, true
		}
	}
	return ProcessGroup{}, false
}

func registryByID(p *Policy, id string) (RegistryPath, bool) {
	for _, v := range p.RegistryPaths {
		if v.ID == id {
			return v, true
		}
	}
	return RegistryPath{}, false
}

func sortTriggers(values []Trigger) {
	sort.Slice(values, func(i, j int) bool {
		if values[i].Kind != values[j].Kind {
			return values[i].Kind < values[j].Kind
		}
		if values[i].ID != values[j].ID {
			return values[i].ID < values[j].ID
		}
		return values[i].Change < values[j].Change
	})
}

func sortRules(values []AffectedRule) {
	sort.Slice(values, func(i, j int) bool { return values[i].RuleID < values[j].RuleID })
}
