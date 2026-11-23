package policy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"unicode"

	"sandboxpolicy/business"
)

// NormalizeStrict normalizes a complete input policy and requires every
// reference and hierarchy link to resolve. It is used for baselines and for
// standalone policy files.
func NormalizeStrict(p *Policy) error {
	if err := normalizeCore(p); err != nil {
		return err
	}
	return validateReferences(p, true)
}

// NormalizeSnapshot normalizes a persisted snapshot. Deliberately dangling
// references are permitted after an upstream delete; dependency diagnostics
// identify them as E_DEP_INVALIDATED rather than rejecting the whole snapshot.
func NormalizeSnapshot(p *Policy) error {
	if err := normalizeCore(p); err != nil {
		return err
	}
	return validateReferences(p, false)
}

// NormalizeChangeSet validates operation shape and normalizes its namespace.
// State-dependent checks are performed by Apply.
func NormalizeChangeSet(cs *ChangeSet) error {
	if cs == nil {
		return business.At(business.CodePolicyMalformed, "changeset", business.ReasonInvalidField, "changeset is null")
	}
	if cs.SchemaVersion == 0 {
		cs.SchemaVersion = SchemaVersion
	}
	if cs.SchemaVersion != SchemaVersion {
		return business.At(business.CodePolicyMalformed, "changeset.schema_version", business.ReasonInvalidField, fmt.Sprintf("unsupported schema version %d", cs.SchemaVersion))
	}
	cs.Namespace = strings.TrimSpace(cs.Namespace)
	if cs.Namespace == "" {
		return business.At(business.CodePolicyMalformed, "changeset.namespace", business.ReasonInvalidField, "namespace is required")
	}
	if strings.Contains(cs.Namespace, "::") || hasSpace(cs.Namespace) {
		return business.At(business.CodePolicyMalformed, "changeset.namespace", business.ReasonInvalidField, "namespace contains invalid characters")
	}
	if cs.BaseVersion < 0 {
		return business.At(business.CodePolicyMalformed, "changeset.base_version", business.ReasonInvalidField, "base_version cannot be negative")
	}
	if len(cs.Operations) == 0 {
		return business.At(business.CodePolicyMalformed, "changeset.operations", business.ReasonInvalidField, "at least one operation is required")
	}
	seen := make(map[string]string, len(cs.Operations))
	for i := range cs.Operations {
		op := &cs.Operations[i]
		op.Op = strings.ToLower(strings.TrimSpace(op.Op))
		op.Kind = normalizeKind(op.Kind)
		op.ID = strings.TrimSpace(op.ID)
		entry := fmt.Sprintf("changeset.operations[%d]", i)
		switch op.Op {
		case OpAdd, OpModify, OpDelete:
		default:
			return business.At(business.CodePolicyMalformed, entry+".op", business.ReasonInvalidField, "op must be add, modify or delete")
		}
		switch op.Kind {
		case KindDirectory, KindProcessGroup, KindRegistryPath, KindRule:
		default:
			return business.At(business.CodePolicyMalformed, entry+".kind", business.ReasonInvalidField, "unknown entity kind")
		}
		if len(op.Entity) == 0 && op.ID == "" {
			return business.At(business.CodePolicyMalformed, entry, business.ReasonInvalidField, "entity or id is required")
		}
		if op.Op != OpDelete && len(op.Entity) == 0 {
			return business.At(business.CodePolicyMalformed, entry+".entity", business.ReasonInvalidField, "entity is required for add and modify")
		}
		key := op.Kind + "/" + op.ID
		if op.ID == "" {
			id, err := rawEntityID(op.Kind, op.Entity)
			if err != nil {
				return business.At(business.CodePolicyMalformed, entry+".entity", business.ReasonInvalidField, err.Error())
			}
			if id == "" {
				return business.At(business.CodePolicyMalformed, entry+".entity.id", business.ReasonInvalidField, "identifier is required")
			}
			op.ID = id
			key = op.Kind + "/" + op.ID
		}
		if err := validateID(op.ID, entry+".id"); err != nil {
			return err
		}
		if prev, exists := seen[key]; exists {
			reason := "duplicate_operation_target"
			if prev == OpDelete && op.Op == OpAdd {
				reason = "delete_then_add_conflict"
			}
			return business.At(business.CodeChangeConflict, entry, reason, fmt.Sprintf("%s is touched more than once (%s then %s)", key, prev, op.Op))
		}
		seen[key] = op.Op
	}
	return nil
}

func normalizeCore(p *Policy) error {
	if p == nil {
		return business.At(business.CodePolicyMalformed, "policy", business.ReasonInvalidField, "policy is null")
	}
	if p.SchemaVersion == 0 {
		p.SchemaVersion = SchemaVersion
	}
	if p.SchemaVersion != SchemaVersion {
		return business.At(business.CodePolicyMalformed, "policy.schema_version", business.ReasonInvalidField, fmt.Sprintf("unsupported schema version %d", p.SchemaVersion))
	}
	p.Namespace = strings.TrimSpace(p.Namespace)
	if p.Namespace == "" {
		return business.At(business.CodePolicyMalformed, "policy.namespace", business.ReasonInvalidField, "namespace is required")
	}
	if strings.Contains(p.Namespace, "::") || hasSpace(p.Namespace) {
		return business.At(business.CodePolicyMalformed, "policy.namespace", business.ReasonInvalidField, "namespace contains invalid characters")
	}
	p.normalizeSlices()

	ids := make(map[string]string)
	register := func(id, entry string) error {
		if err := validateID(id, entry); err != nil {
			return err
		}
		if previous, ok := ids[id]; ok {
			return business.At(business.CodePolicyMalformed, entry, business.ReasonDuplicateIdentifier, fmt.Sprintf("identifier %q already used by %s", id, previous))
		}
		ids[id] = entry
		return nil
	}

	for i := range p.Directories {
		d := &p.Directories[i]
		entry := fmt.Sprintf("directories[%d]", i)
		d.ID = strings.TrimSpace(d.ID)
		if err := register(d.ID, entry+".id"); err != nil {
			return err
		}
		entry = "directories." + d.ID
		normalizedPath, err := normalizePath(d.Path)
		if err != nil {
			return business.At(business.CodePolicyMalformed, entry+".path", business.ReasonInvalidField, err.Error())
		}
		d.Path = normalizedPath
		parent, err := normalizeReference(p.Namespace, d.ParentID)
		if err != nil {
			return wrapScope(err, entry+".parent_id")
		}
		d.ParentID = parent
		access, err := normalizeAccess(d.Access)
		if err != nil {
			return business.At(business.CodePolicyMalformed, entry+".access", business.ReasonInvalidField, err.Error())
		}
		d.Access = access
	}

	for i := range p.ProcessGroups {
		g := &p.ProcessGroups[i]
		entry := fmt.Sprintf("process_groups[%d]", i)
		g.ID = strings.TrimSpace(g.ID)
		if err := register(g.ID, entry+".id"); err != nil {
			return err
		}
		entry = "process_groups." + g.ID
		parent, err := normalizeReference(p.Namespace, g.ParentID)
		if err != nil {
			return wrapScope(err, entry+".parent_id")
		}
		g.ParentID = parent
		exes, err := normalizeStringSet(g.Executables, false)
		if err != nil {
			return business.At(business.CodePolicyMalformed, entry+".executables", business.ReasonInvalidField, err.Error())
		}
		if len(exes) == 0 {
			return business.At(business.CodePolicyMalformed, entry+".executables", business.ReasonInvalidField, "at least one executable is required")
		}
		g.Executables = exes
	}

	for i := range p.RegistryPaths {
		r := &p.RegistryPaths[i]
		entry := fmt.Sprintf("registry_paths[%d]", i)
		r.ID = strings.TrimSpace(r.ID)
		if err := register(r.ID, entry+".id"); err != nil {
			return err
		}
		entry = "registry_paths." + r.ID
		normalizedPath, err := normalizePath(r.Path)
		if err != nil {
			return business.At(business.CodePolicyMalformed, entry+".path", business.ReasonInvalidField, err.Error())
		}
		r.Path = normalizedPath
		parent, err := normalizeReference(p.Namespace, r.ParentID)
		if err != nil {
			return wrapScope(err, entry+".parent_id")
		}
		r.ParentID = parent
		access, err := normalizeAccess(r.Access)
		if err != nil {
			return business.At(business.CodePolicyMalformed, entry+".access", business.ReasonInvalidField, err.Error())
		}
		r.Access = access
	}

	for i := range p.Rules {
		r := &p.Rules[i]
		entry := fmt.Sprintf("rules[%d]", i)
		r.ID = strings.TrimSpace(r.ID)
		if err := register(r.ID, entry+".id"); err != nil {
			return err
		}
		entry = "rules." + r.ID
		r.Effect = strings.ToLower(strings.TrimSpace(r.Effect))
		if r.Effect != EffectAllow && r.Effect != EffectDeny {
			return business.At(business.CodePolicyMalformed, entry+".effect", business.ReasonInvalidField, "effect must be allow or deny")
		}
		r.Action = strings.ToLower(strings.TrimSpace(r.Action))
		if _, ok := actionKind[r.Action]; !ok {
			return business.At(business.CodePolicyMalformed, entry+".action", business.ReasonInvalidField, "unsupported action")
		}
		ref, err := normalizeReference(p.Namespace, r.Ref)
		if err != nil {
			return wrapScope(err, entry+".ref")
		}
		if ref == "" {
			return business.At(business.CodePolicyMalformed, entry+".ref", business.ReasonInvalidField, "ref is required")
		}
		r.Ref = ref
		deps := make([]string, 0, len(r.DependsOn))
		for j, dep := range r.DependsOn {
			normalized, err := normalizeReference(p.Namespace, dep)
			if err != nil {
				return wrapScope(err, fmt.Sprintf("%s.depends_on[%d]", entry, j))
			}
			if normalized == "" {
				return business.At(business.CodePolicyMalformed, fmt.Sprintf("%s.depends_on[%d]", entry, j), business.ReasonInvalidField, "empty dependency")
			}
			deps = append(deps, normalized)
		}
		sort.Strings(deps)
		deps = dedupeStrings(deps)
		r.DependsOn = deps
	}

	sort.Slice(p.Directories, func(i, j int) bool { return p.Directories[i].ID < p.Directories[j].ID })
	sort.Slice(p.ProcessGroups, func(i, j int) bool { return p.ProcessGroups[i].ID < p.ProcessGroups[j].ID })
	sort.Slice(p.RegistryPaths, func(i, j int) bool { return p.RegistryPaths[i].ID < p.RegistryPaths[j].ID })
	sort.Slice(p.Rules, func(i, j int) bool { return p.Rules[i].ID < p.Rules[j].ID })
	return nil
}

func validateReferences(p *Policy, strict bool) error {
	index := make(map[string]string, len(p.Directories)+len(p.ProcessGroups)+len(p.RegistryPaths))
	for _, d := range p.Directories {
		index[d.ID] = KindDirectory
	}
	for _, g := range p.ProcessGroups {
		index[g.ID] = KindProcessGroup
	}
	for _, r := range p.RegistryPaths {
		index[r.ID] = KindRegistryPath
	}

	for i := range p.Directories {
		if err := validateParent(p, index, KindDirectory, p.Directories[i].ID, p.Directories[i].ParentID, strict); err != nil {
			return err
		}
		if err := validatePathHierarchy(p, KindDirectory, p.Directories[i].ID, p.Directories[i].ParentID); err != nil {
			return err
		}
	}
	for i := range p.ProcessGroups {
		if err := validateParent(p, index, KindProcessGroup, p.ProcessGroups[i].ID, p.ProcessGroups[i].ParentID, strict); err != nil {
			return err
		}
	}
	for i := range p.RegistryPaths {
		if err := validateParent(p, index, KindRegistryPath, p.RegistryPaths[i].ID, p.RegistryPaths[i].ParentID, strict); err != nil {
			return err
		}
		if err := validatePathHierarchy(p, KindRegistryPath, p.RegistryPaths[i].ID, p.RegistryPaths[i].ParentID); err != nil {
			return err
		}
	}

	for i := range p.Rules {
		r := p.Rules[i]
		entry := "rules." + r.ID
		kind, ok := index[r.Ref]
		if !ok {
			if !strict {
				continue
			}
			return business.At(business.CodePolicyMalformed, entry+".ref", business.ReasonReferenceMissing, fmt.Sprintf("reference %q does not resolve", r.Ref))
		}
		if want := actionKind[r.Action]; kind != want {
			return business.At(business.CodePolicyMalformed, entry+".ref", business.ReasonLayerMismatch, fmt.Sprintf("action %s requires a %s, got %s", r.Action, want, kind))
		}
		for _, dep := range r.DependsOn {
			if _, ok := p.ruleByID(dep); !ok && strict {
				return business.At(business.CodePolicyMalformed, entry+".depends_on", business.ReasonReferenceMissing, fmt.Sprintf("rule dependency %q does not resolve", dep))
			}
		}
	}

	if err := validateEntityCycles(p, strict); err != nil {
		return err
	}
	if err := validateRuleCycles(p, strict); err != nil {
		return err
	}
	return nil
}

func validateParent(p *Policy, index map[string]string, kind, id, parent string, strict bool) error {
	if parent == "" {
		return nil
	}
	parentKind, ok := index[parent]
	if !ok {
		if !strict {
			return nil
		}
		return business.At(business.CodePolicyMalformed, kind+"."+id+".parent_id", business.ReasonReferenceMissing, fmt.Sprintf("parent %q does not resolve", parent))
	}
	if parentKind != kind {
		return business.At(business.CodePolicyMalformed, kind+"."+id+".parent_id", business.ReasonLayerMismatch, fmt.Sprintf("parent %q is %s, expected %s", parent, parentKind, kind))
	}
	return nil
}

func validatePathHierarchy(p *Policy, kind, id, parent string) error {
	if parent == "" {
		return nil
	}
	var childPath, parentPath string
	switch kind {
	case KindDirectory:
		childPath = p.directoryPath(id)
		parentPath = p.directoryPath(parent)
	case KindRegistryPath:
		parentPath = p.registryPath(parent)
		childPath = p.registryPath(id)
	}
	if childPath == "" || parentPath == "" {
		return nil
	}
	if !pathContains(parentPath, childPath) || equalFold(childPath, parentPath) {
		return business.At(business.CodePolicyMalformed, kind+"."+id+".parent_id", business.ReasonHierarchyInvalid, fmt.Sprintf("path %q is not beneath parent %q", childPath, parentPath))
	}
	return nil
}

func (p *Policy) directoryPath(id string) string {
	for i := range p.Directories {
		if p.Directories[i].ID == id {
			return p.Directories[i].Path
		}
	}
	return ""
}

func (p *Policy) registryPath(id string) string {
	for i := range p.RegistryPaths {
		if p.RegistryPaths[i].ID == id {
			return p.RegistryPaths[i].Path
		}
	}
	return ""
}

func validateEntityCycles(p *Policy, strict bool) error {
	parents := make(map[string]string)
	for _, d := range p.Directories {
		parents[KindDirectory+"/"+d.ID] = prefixKey(KindDirectory, d.ParentID)
	}
	for _, g := range p.ProcessGroups {
		parents[KindProcessGroup+"/"+g.ID] = prefixKey(KindProcessGroup, g.ParentID)
	}
	for _, r := range p.RegistryPaths {
		parents[KindRegistryPath+"/"+r.ID] = prefixKey(KindRegistryPath, r.ParentID)
	}
	for start, parent := range parents {
		seen := map[string]bool{start: true}
		depth := 0
		for parent != "" {
			if seen[parent] {
				return business.At(business.CodePolicyMalformed, "hierarchy", business.ReasonDependencyCycle, fmt.Sprintf("entity hierarchy cycle includes %s", start))
			}
			seen[parent] = true
			depth++
			if depth > 32 {
				return business.At(business.CodePolicyMalformed, "hierarchy", business.ReasonHierarchyInvalid, "entity hierarchy exceeds depth 32")
			}
			next, ok := parents[parent]
			if !ok {
				break
			}
			parent = next
		}
	}
	_ = strict
	return nil
}

func validateRuleCycles(p *Policy, strict bool) error {
	state := make(map[string]uint8)
	var visit func(string) error
	visit = func(id string) error {
		switch state[id] {
		case 1:
			return business.At(business.CodePolicyMalformed, "rules."+id+".depends_on", business.ReasonDependencyCycle, fmt.Sprintf("rule dependency cycle includes %q", id))
		case 2:
			return nil
		}
		state[id] = 1
		rule, ok := p.ruleByID(id)
		if ok {
			for _, dep := range rule.DependsOn {
				if _, exists := p.ruleByID(dep); !exists && !strict {
					continue
				}
				if err := visit(dep); err != nil {
					return err
				}
			}
		}
		state[id] = 2
		return nil
	}
	for i := range p.Rules {
		if err := visit(p.Rules[i].ID); err != nil {
			return err
		}
	}
	return nil
}

func prefixKey(kind, id string) string {
	if id == "" {
		return ""
	}
	return kind + "/" + id
}

// CanonicalJSON returns stable bytes for an already-normalized policy.
func CanonicalJSON(p *Policy) ([]byte, error) {
	return json.Marshal(p)
}

// Digest computes the stable SHA-256 digest of canonical policy bytes.
func Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// DigestPolicy computes the digest a normalized policy will have in storage.
func DigestPolicy(p *Policy) (string, error) {
	data, err := CanonicalJSON(p)
	if err != nil {
		return "", err
	}
	return Digest(data), nil
}

func normalizeKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "directory", "dir":
		return KindDirectory
	case "process_group", "process-group", "processgroup", "process":
		return KindProcessGroup
	case "registry_path", "registry-path", "registrypath", "registry":
		return KindRegistryPath
	case "rule":
		return KindRule
	default:
		return strings.ToLower(strings.TrimSpace(kind))
	}
}

func normalizeReference(namespace, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	parts := strings.Split(value, "::")
	if len(parts) > 2 {
		return "", business.At(business.CodePolicyMalformed, "reference", business.ReasonInvalidField, "reference contains more than one namespace separator")
	}
	if len(parts) == 2 {
		refNS := strings.TrimSpace(parts[0])
		local := strings.TrimSpace(parts[1])
		if refNS != namespace {
			return "", &business.Error{Code: business.CodeNamespaceScope, Entry: "reference", Message: fmt.Sprintf("reference %q points outside namespace %q", value, namespace)}
		}
		return local, nil
	}
	return strings.TrimSpace(parts[0]), nil
}

func wrapScope(err error, entry string) error {
	var be *business.Error
	if errors.As(err, &be) {
		copy := *be
		copy.Entry = entry
		return &copy
	}
	return err
}

func validateID(id, entry string) error {
	if id == "" {
		return business.At(business.CodePolicyMalformed, entry, business.ReasonInvalidField, "identifier is required")
	}
	if strings.Contains(id, "::") {
		return &business.Error{Code: business.CodeNamespaceScope, Entry: entry, Message: "qualified identifiers are only valid in reference fields"}
	}
	if hasSpace(id) {
		return business.At(business.CodePolicyMalformed, entry, business.ReasonInvalidField, "identifier contains whitespace")
	}
	return nil
}

func hasSpace(value string) bool {
	for _, r := range value {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return true
		}
	}
	return false
}

func normalizeAccess(values []string) ([]string, error) {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if _, ok := validAccess[value]; !ok {
			return nil, fmt.Errorf("unsupported access %q", value)
		}
		out = append(out, value)
	}
	sort.Strings(out)
	return dedupeStrings(out), nil
}

func normalizeStringSet(values []string, fold bool) ([]string, error) {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("empty value is not allowed")
		}
		if hasSpace(value) {
			return nil, fmt.Errorf("value %q contains whitespace", value)
		}
		if fold {
			value = strings.ToLower(value)
		}
		out = append(out, value)
	}
	sort.Strings(out)
	return dedupeStrings(out), nil
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

func normalizePath(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("path is required")
	}
	value = strings.ReplaceAll(value, "/", "\\")
	unc := strings.HasPrefix(value, "\\\\")
	cleaned := path.Clean(strings.ReplaceAll(value, "\\", "/"))
	if unc {
		cleaned = "/" + strings.TrimPrefix(cleaned, "//")
		cleaned = "//" + strings.TrimPrefix(cleaned, "/")
	}
	if len(cleaned) == 2 && cleaned[1] == ':' && isASCIIAlpha(cleaned[0]) {
		cleaned += "/"
	}
	if cleaned == "." {
		return "", fmt.Errorf("path cleans to empty value")
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("path escapes its root")
	}
	return strings.ReplaceAll(cleaned, "/", "\\"), nil
}

func isASCIIAlpha(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func pathContains(parent, child string) bool {
	parent = strings.TrimRight(parent, "\\/")
	child = strings.TrimRight(child, "\\/")
	if parent == "" {
		return true
	}
	parent = strings.ToLower(strings.ReplaceAll(parent, "/", "\\"))
	child = strings.ToLower(strings.ReplaceAll(child, "/", "\\"))
	return child == parent || strings.HasPrefix(child, parent+"\\")
}

func equalFold(a, b string) bool {
	return strings.EqualFold(strings.TrimRight(a, "\\/"), strings.TrimRight(b, "\\/"))
}

func rawEntityID(kind string, raw json.RawMessage) (string, error) {
	switch kind {
	case KindDirectory:
		var v Directory
		if err := json.Unmarshal(raw, &v); err != nil {
			return "", err
		}
		return strings.TrimSpace(v.ID), nil
	case KindProcessGroup:
		var v ProcessGroup
		if err := json.Unmarshal(raw, &v); err != nil {
			return "", err
		}
		return strings.TrimSpace(v.ID), nil
	case KindRegistryPath:
		var v RegistryPath
		if err := json.Unmarshal(raw, &v); err != nil {
			return "", err
		}
		return strings.TrimSpace(v.ID), nil
	case KindRule:
		var v Rule
		if err := json.Unmarshal(raw, &v); err != nil {
			return "", err
		}
		return strings.TrimSpace(v.ID), nil
	default:
		return "", fmt.Errorf("unknown kind %q", kind)
	}
}
