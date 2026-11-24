package model

import (
	"fmt"
	"path"
	"sort"
	"strings"

	"hostacl-workbench/internal/apperr"
)

type issue struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func invalid(issues []issue) error {
	return apperr.With(apperr.SpecInvalid, "policy specification is invalid", map[string]any{
		"issues": issues,
	})
}

func ValidateSnapshot(in Policy) (*Policy, error) {
	p := ClonePolicy(&in)
	var issues []issue
	at, err := normalizeTime(p.GeneratedAt)
	if err != nil {
		issues = append(issues, issue{"generated_at", err.Error()})
	} else {
		p.GeneratedAt = at
	}
	if p.Version < 1 {
		issues = append(issues, issue{"version", "must be >= 1"})
	}
	if strings.TrimSpace(p.Digest) == "" {
		issues = append(issues, issue{"digest", "must be present"})
	}
	canonicalPolicy(p)
	issues = append(issues, validatePolicyBody(p)...)
	if len(issues) > 0 {
		return nil, invalid(issues)
	}
	computed := SnapshotDigest(p)
	if p.Digest != computed {
		return nil, invalid([]issue{{"digest", fmt.Sprintf("mismatch: got %s, want %s", p.Digest, computed)}})
	}
	return p, nil
}

func validatePolicyBody(p *Policy) []issue {
	var issues []issue
	subjects := map[string]Subject{}
	for i, s := range p.Subjects {
		field := fmt.Sprintf("subjects[%d]", i)
		if strings.TrimSpace(s.ID) == "" {
			issues = append(issues, issue{field + ".id", "must not be empty"})
			continue
		}
		if _, exists := subjects[s.ID]; exists {
			issues = append(issues, issue{field + ".id", "duplicate subject id " + s.ID})
		}
		subjects[s.ID] = s
		switch s.Type {
		case SubjectUser, SubjectService, SubjectGroup:
		default:
			issues = append(issues, issue{field + ".type", "must be user, service, or group"})
		}
		if s.Priority < 0 {
			issues = append(issues, issue{field + ".priority", "must be >= 0"})
		}
		if s.Type != SubjectGroup && len(s.Members) > 0 {
			issues = append(issues, issue{field + ".members", "only groups may declare members"})
		}
		seen := map[string]bool{}
		for j, member := range s.Members {
			mfield := fmt.Sprintf("%s.members[%d]", field, j)
			if member == s.ID {
				issues = append(issues, issue{mfield, "group cannot contain itself"})
			}
			if seen[member] {
				issues = append(issues, issue{mfield, "duplicate member " + member})
			}
			seen[member] = true
		}
	}
	// Subject references are checked after all subjects have been indexed, so a
	// forward reference is valid.
	for i, s := range p.Subjects {
		if s.Type != SubjectGroup {
			continue
		}
		for j, member := range s.Members {
			if _, ok := subjects[member]; !ok {
				issues = append(issues, issue{fmt.Sprintf("subjects[%d].members[%d]", i, j), "member does not exist: " + member})
			}
		}
	}
	if cycle := groupCycle(p.Subjects); len(cycle) > 0 {
		issues = append(issues, issue{"subjects", "group membership cycle: " + strings.Join(cycle, " -> ")})
	}

	paths := map[string]ManagedPath{}
	for i, managed := range p.Paths {
		field := fmt.Sprintf("paths[%d]", i)
		if msg := validateRelativePath(managed.Path); msg != "" {
			issues = append(issues, issue{field + ".path", msg})
		}
		if _, exists := paths[managed.Path]; exists {
			issues = append(issues, issue{field + ".path", "duplicate managed path " + managed.Path})
		}
		paths[managed.Path] = managed
		if managed.Kind != "directory" && managed.Kind != "file" {
			issues = append(issues, issue{field + ".kind", "must be directory or file"})
		}
		for j, ace := range managed.ACL {
			afield := fmt.Sprintf("%s.acl[%d]", field, j)
			if _, ok := subjects[ace.SubjectID]; !ok {
				issues = append(issues, issue{afield + ".subject_id", "subject does not exist: " + ace.SubjectID})
			}
			if ace.Effect != "allow" && ace.Effect != "deny" {
				issues = append(issues, issue{afield + ".effect", "must be allow or deny"})
			}
			if len(ace.Rights) == 0 {
				issues = append(issues, issue{afield + ".rights", "must contain read and/or write"})
			}
			for k, right := range ace.Rights {
				if right != "read" && right != "write" {
					issues = append(issues, issue{fmt.Sprintf("%s.rights[%d]", afield, k), "unknown right " + right})
				}
			}
		}
	}
	// A managed file cannot be the parent of another managed path.
	for i, managed := range p.Paths {
		parent := parentPath(managed.Path)
		for parent != "" {
			if pth, ok := paths[parent]; ok && pth.Kind != "directory" {
				issues = append(issues, issue{fmt.Sprintf("paths[%d].path", i), "parent is a managed file: " + parent})
				break
			}
			parent = parentPath(parent)
		}
	}
	return issues
}

func groupCycle(subjects []Subject) []string {
	groups := map[string]Subject{}
	for _, s := range subjects {
		if s.Type == SubjectGroup {
			groups[s.ID] = s
		}
	}
	state := map[string]int{}
	var stack []string
	var visit func(string) []string
	visit = func(id string) []string {
		if state[id] == 2 {
			return nil
		}
		if state[id] == 1 {
			for i, s := range stack {
				if s == id {
					out := append([]string(nil), stack[i:]...)
					out = append(out, id)
					return out
				}
			}
			return []string{id, id}
		}
		state[id] = 1
		stack = append(stack, id)
		g := groups[id]
		members := append([]string(nil), g.Members...)
		sort.Strings(members)
		for _, member := range members {
			if _, ok := groups[member]; !ok {
				continue
			}
			if cycle := visit(member); len(cycle) > 0 {
				return cycle
			}
		}
		stack = stack[:len(stack)-1]
		state[id] = 2
		return nil
	}
	ids := make([]string, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if cycle := visit(id); len(cycle) > 0 {
			return cycle
		}
	}
	return nil
}

func validateRelativePath(p string) string {
	if p == "" {
		return "must not be empty"
	}
	if strings.HasPrefix(p, "/") {
		return "must be relative and must not start with /"
	}
	if len(p) >= 2 && p[1] == ':' {
		return "must not contain a drive letter"
	}
	if path.Clean(p) != p {
		return "must be a clean relative path"
	}
	for _, segment := range strings.Split(p, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "must not contain empty, . or .. segments"
		}
	}
	return ""
}

func parentPath(p string) string {
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return ""
	}
	return p[:i]
}

func ValidateIncrementDigest(inc *Increment) (string, error) {
	var issues []issue
	for i := range inc.PathChanges {
		inc.PathChanges[i].PathID = normalizeManagedPath(inc.PathChanges[i].PathID)
		if inc.PathChanges[i].Path != nil {
			inc.PathChanges[i].Path.Path = normalizeManagedPath(inc.PathChanges[i].Path.Path)
		}
	}
	at, err := normalizeTime(inc.GeneratedAt)
	if err != nil {
		issues = append(issues, issue{"generated_at", err.Error()})
	} else {
		inc.GeneratedAt = at
	}
	if inc.BaseVersion < 0 {
		issues = append(issues, issue{"base_version", "must be >= 0"})
	}
	if inc.Version != inc.BaseVersion+1 {
		issues = append(issues, issue{"version", "must equal base_version + 1"})
	}
	if strings.TrimSpace(inc.Digest) == "" {
		issues = append(issues, issue{"digest", "must be present"})
	}
	seenSubjects := map[string]bool{}
	for i, c := range inc.SubjectChanges {
		field := fmt.Sprintf("subject_changes[%d]", i)
		id := changeSubjectID(c)
		if id == "" {
			issues = append(issues, issue{field, "missing subject id"})
		}
		if seenSubjects[id] {
			issues = append(issues, issue{field, "duplicate change for subject " + id})
		}
		seenSubjects[id] = true
		switch c.Op {
		case "upsert":
			if c.Subject == nil {
				issues = append(issues, issue{field + ".subject", "required for upsert"})
			}
		case "delete":
			if c.SubjectID == "" {
				issues = append(issues, issue{field + ".subject_id", "required for delete"})
			}
		default:
			issues = append(issues, issue{field + ".op", "must be upsert or delete"})
		}
	}
	seenPaths := map[string]bool{}
	for i, c := range inc.PathChanges {
		field := fmt.Sprintf("path_changes[%d]", i)
		id := changePathID(c)
		if id == "" {
			issues = append(issues, issue{field, "missing path id"})
		}
		if seenPaths[id] {
			issues = append(issues, issue{field, "duplicate change for path " + id})
		}
		seenPaths[id] = true
		switch c.Op {
		case "upsert":
			if c.Path == nil {
				issues = append(issues, issue{field + ".path", "required for upsert"})
			}
		case "delete":
			if c.PathID == "" {
				issues = append(issues, issue{field + ".path_id", "required for delete"})
			}
		default:
			issues = append(issues, issue{field + ".op", "must be upsert or delete"})
		}
	}
	if len(issues) > 0 {
		return "", invalid(issues)
	}
	computed := IncrementDigest(inc)
	if inc.Digest != computed {
		return "", invalid([]issue{{"digest", fmt.Sprintf("mismatch: got %s, want %s", inc.Digest, computed)}})
	}
	return computed, nil
}

func ApplyIncrement(current *Policy, inc *Increment) (*Policy, error) {
	next := ClonePolicy(current)
	for i := range inc.PathChanges {
		inc.PathChanges[i].PathID = normalizeManagedPath(inc.PathChanges[i].PathID)
		if inc.PathChanges[i].Path != nil {
			inc.PathChanges[i].Path.Path = normalizeManagedPath(inc.PathChanges[i].Path.Path)
		}
	}
	next.Version = inc.Version
	next.GeneratedAt = inc.GeneratedAt
	next.Digest = ""
	subjects := SubjectMap(next)
	paths := PathMap(next)
	var issues []issue
	for _, c := range inc.SubjectChanges {
		switch c.Op {
		case "upsert":
			if c.Subject == nil {
				continue
			}
			subject := *c.Subject
			subject.Members = append([]string(nil), c.Subject.Members...)
			subjects[subject.ID] = subject
		case "delete":
			if _, ok := subjects[c.SubjectID]; !ok {
				issues = append(issues, issue{"subject_changes", "cannot delete missing subject " + c.SubjectID})
				continue
			}
			delete(subjects, c.SubjectID)
		}
	}
	for _, c := range inc.PathChanges {
		switch c.Op {
		case "upsert":
			if c.Path == nil {
				continue
			}
			managed := *c.Path
			managed.ACL = append([]ACE(nil), c.Path.ACL...)
			for i := range managed.ACL {
				managed.ACL[i].Rights = append([]string(nil), c.Path.ACL[i].Rights...)
			}
			paths[managed.Path] = managed
		case "delete":
			if _, ok := paths[c.PathID]; !ok {
				issues = append(issues, issue{"path_changes", "cannot delete missing path " + c.PathID})
				continue
			}
			delete(paths, c.PathID)
		}
	}
	if len(issues) > 0 {
		return nil, invalid(issues)
	}
	next.Subjects = next.Subjects[:0]
	for _, s := range subjects {
		next.Subjects = append(next.Subjects, s)
	}
	next.Paths = next.Paths[:0]
	for _, p := range paths {
		next.Paths = append(next.Paths, p)
	}
	canonicalPolicy(next)
	if issues := validatePolicyBody(next); len(issues) > 0 {
		return nil, invalid(issues)
	}
	next.Digest = SnapshotDigest(next)
	return next, nil
}
