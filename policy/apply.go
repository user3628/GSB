package policy

import (
	"encoding/json"
	"fmt"
	"strings"

	"sandboxpolicy/business"
)

// ApplyResult contains the immutable result of applying a change set. The
// caller decides whether to commit it as a new snapshot.
type ApplyResult struct {
	Policy  *Policy
	Closure Closure
}

// Apply applies a normalized change set to a cloned baseline. Existing rules
// may be left dangling by an upstream delete; those dependencies are reported
// by the closure. New or modified references must resolve in the final state.
func Apply(baseline *Policy, cs *ChangeSet, newVersion int) (*ApplyResult, error) {
	if baseline == nil {
		return nil, business.New(business.CodeBaselineMissing, "baseline snapshot is unavailable")
	}
	if cs == nil {
		return nil, business.At(business.CodePolicyMalformed, "changeset", business.ReasonInvalidField, "changeset is null")
	}
	if cs.Namespace != baseline.Namespace {
		return nil, &business.Error{Code: business.CodeNamespaceScope, Entry: "changeset.namespace", Message: fmt.Sprintf("changeset namespace %q does not match snapshot namespace %q", cs.Namespace, baseline.Namespace)}
	}
	if err := NormalizeChangeSet(cs); err != nil {
		return nil, err
	}
	if cs.BaseVersion > 0 && cs.BaseVersion != newVersion-1 {
		return nil, &business.Error{Code: business.CodeChangeConflict, Entry: "changeset.base_version", Version: cs.BaseVersion, Message: fmt.Sprintf("base version %d does not match committed version %d", cs.BaseVersion, newVersion-1)}
	}

	old := baseline.Clone()
	next := baseline.Clone()
	touched := make(map[string]string)
	for i := range cs.Operations {
		op := &cs.Operations[i]
		entry := fmt.Sprintf("changeset.operations[%d]", i)
		exists, currentID := entityExists(next, op.Kind, op.ID)
		switch op.Op {
		case OpAdd:
			if exists {
				return nil, business.At(business.CodeChangeConflict, entry, "add_existing", fmt.Sprintf("%s %q already exists", op.Kind, op.ID))
			}
			value, id, err := decodeEntity(op.Kind, op.Entity)
			if err != nil {
				return nil, business.At(business.CodePolicyMalformed, entry+".entity", business.ReasonInvalidField, err.Error())
			}
			if id != op.ID {
				return nil, business.At(business.CodePolicyMalformed, entry+".entity.id", business.ReasonInvalidField, fmt.Sprintf("entity id %q does not match operation id %q", id, op.ID))
			}
			appendEntity(next, op.Kind, value)
			touched[op.Kind+"/"+op.ID] = OpAdd
		case OpModify:
			if !exists || currentID != op.ID {
				return nil, business.At(business.CodeChangeConflict, entry, "modify_missing", fmt.Sprintf("%s %q does not exist", op.Kind, op.ID))
			}
			value, id, err := decodeEntity(op.Kind, op.Entity)
			if err != nil {
				return nil, business.At(business.CodePolicyMalformed, entry+".entity", business.ReasonInvalidField, err.Error())
			}
			if id != op.ID {
				return nil, business.At(business.CodePolicyMalformed, entry+".entity.id", business.ReasonInvalidField, fmt.Sprintf("entity id %q does not match operation id %q", id, op.ID))
			}
			replaceEntity(next, op.Kind, op.ID, value)
			touched[op.Kind+"/"+op.ID] = OpModify
		case OpDelete:
			if !exists {
				return nil, business.At(business.CodeChangeConflict, entry, "delete_missing", fmt.Sprintf("%s %q does not exist", op.Kind, op.ID))
			}
			deleteEntity(next, op.Kind, op.ID)
			touched[op.Kind+"/"+op.ID] = OpDelete
		default:
			return nil, business.At(business.CodePolicyMalformed, entry+".op", business.ReasonInvalidField, "unknown operation")
		}
	}

	if err := normalizeCore(next); err != nil {
		return nil, err
	}
	if err := validateReferences(next, false); err != nil {
		return nil, err
	}
	if err := validateTouchedReferences(next, touched); err != nil {
		return nil, err
	}

	closure := ComputeClosure(old, next, cs, newVersion)
	return &ApplyResult{Policy: next, Closure: closure}, nil
}

func validateTouchedReferences(p *Policy, touched map[string]string) error {
	for key, op := range touched {
		if op == OpDelete {
			continue
		}
		parts := strings.SplitN(key, "/", 2)
		kind, id := parts[0], parts[1]
		switch kind {
		case KindRule:
			rule, ok := p.ruleByID(id)
			if !ok {
				continue
			}
			if _, _, ok := p.entityByID(rule.Ref); !ok {
				return business.At(business.CodePolicyMalformed, "rules."+id+".ref", business.ReasonReferenceMissing, fmt.Sprintf("reference %q does not resolve", rule.Ref))
			}
			for _, dep := range rule.DependsOn {
				if _, ok := p.ruleByID(dep); !ok {
					return business.At(business.CodePolicyMalformed, "rules."+id+".depends_on", business.ReasonReferenceMissing, fmt.Sprintf("rule dependency %q does not resolve", dep))
				}
			}
		case KindDirectory, KindProcessGroup, KindRegistryPath:
			parent := p.parentID(kind, id)
			if parent == "" {
				continue
			}
			parentKind, _, ok := p.entityByID(parent)
			if !ok {
				return business.At(business.CodePolicyMalformed, kind+"."+id+".parent_id", business.ReasonReferenceMissing, fmt.Sprintf("parent %q does not resolve", parent))
			}
			if parentKind != kind {
				return business.At(business.CodePolicyMalformed, kind+"."+id+".parent_id", business.ReasonLayerMismatch, "parent belongs to a different layer")
			}
		}
	}
	return nil
}

func entityExists(p *Policy, kind, id string) (bool, string) {
	switch kind {
	case KindDirectory:
		for _, v := range p.Directories {
			if v.ID == id {
				return true, v.ID
			}
		}
	case KindProcessGroup:
		for _, v := range p.ProcessGroups {
			if v.ID == id {
				return true, v.ID
			}
		}
	case KindRegistryPath:
		for _, v := range p.RegistryPaths {
			if v.ID == id {
				return true, v.ID
			}
		}
	case KindRule:
		for _, v := range p.Rules {
			if v.ID == id {
				return true, v.ID
			}
		}
	}
	return false, ""
}

func decodeEntity(kind string, raw json.RawMessage) (any, string, error) {
	switch kind {
	case KindDirectory:
		var v Directory
		if err := decodeStrict(raw, &v); err != nil {
			return nil, "", err
		}
		return v, strings.TrimSpace(v.ID), nil
	case KindProcessGroup:
		var v ProcessGroup
		if err := decodeStrict(raw, &v); err != nil {
			return nil, "", err
		}
		return v, strings.TrimSpace(v.ID), nil
	case KindRegistryPath:
		var v RegistryPath
		if err := decodeStrict(raw, &v); err != nil {
			return nil, "", err
		}
		return v, strings.TrimSpace(v.ID), nil
	case KindRule:
		var v Rule
		if err := decodeStrict(raw, &v); err != nil {
			return nil, "", err
		}
		return v, strings.TrimSpace(v.ID), nil
	default:
		return nil, "", fmt.Errorf("unknown entity kind %q", kind)
	}
}

func appendEntity(p *Policy, kind string, value any) {
	switch kind {
	case KindDirectory:
		p.Directories = append(p.Directories, value.(Directory))
	case KindProcessGroup:
		p.ProcessGroups = append(p.ProcessGroups, value.(ProcessGroup))
	case KindRegistryPath:
		p.RegistryPaths = append(p.RegistryPaths, value.(RegistryPath))
	case KindRule:
		p.Rules = append(p.Rules, value.(Rule))
	}
}

func replaceEntity(p *Policy, kind, id string, value any) {
	switch kind {
	case KindDirectory:
		for i := range p.Directories {
			if p.Directories[i].ID == id {
				p.Directories[i] = value.(Directory)
				return
			}
		}
	case KindProcessGroup:
		for i := range p.ProcessGroups {
			if p.ProcessGroups[i].ID == id {
				p.ProcessGroups[i] = value.(ProcessGroup)
				return
			}
		}
	case KindRegistryPath:
		for i := range p.RegistryPaths {
			if p.RegistryPaths[i].ID == id {
				p.RegistryPaths[i] = value.(RegistryPath)
				return
			}
		}
	case KindRule:
		for i := range p.Rules {
			if p.Rules[i].ID == id {
				p.Rules[i] = value.(Rule)
				return
			}
		}
	}
}

func deleteEntity(p *Policy, kind, id string) {
	switch kind {
	case KindDirectory:
		out := p.Directories[:0]
		for _, v := range p.Directories {
			if v.ID != id {
				out = append(out, v)
			}
		}
		p.Directories = out
	case KindProcessGroup:
		out := p.ProcessGroups[:0]
		for _, v := range p.ProcessGroups {
			if v.ID != id {
				out = append(out, v)
			}
		}
		p.ProcessGroups = out
	case KindRegistryPath:
		out := p.RegistryPaths[:0]
		for _, v := range p.RegistryPaths {
			if v.ID != id {
				out = append(out, v)
			}
		}
		p.RegistryPaths = out
	case KindRule:
		out := p.Rules[:0]
		for _, v := range p.Rules {
			if v.ID != id {
				out = append(out, v)
			}
		}
		p.Rules = out
	}
}
