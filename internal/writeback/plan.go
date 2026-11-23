package writeback

import (
	"fmt"
	"sort"
	"strings"

	"example.com/ntfsacl/internal/acl"
	"example.com/ntfsacl/internal/audit"
	"example.com/ntfsacl/internal/domain"
	"example.com/ntfsacl/internal/fixture"
)

const (
	ActionRemoveACE    = "REMOVE_ACE"
	ActionAdjustFlags  = "ADJUST_FLAGS"
	ActionAddInherited = "ADD_MISSING_INHERITED_ACE"

	CodeWritebackRollback  = "E_WRITEBACK_ROLLBACK"
	CodeRecoveryIncomplete = "E_RECOVERY_INCOMPLETE"
	CodeStalePlan          = "E_STALE_PLAN"
	CodeStaleAudit         = "E_STALE_AUDIT"
)

type Action struct {
	NodePath     string      `json:"node_path"`
	ConflictCode string      `json:"conflict_code"`
	Operation    string      `json:"operation"`
	ACEIndex     int         `json:"ace_index"`
	BeforeACE    *domain.ACE `json:"before_ace,omitempty"`
	AfterACE     *domain.ACE `json:"after_ace,omitempty"`
	Reason       string      `json:"reason"`
}

type NodePlan struct {
	NodePath       string   `json:"node_path"`
	Depth          int      `json:"depth"`
	Actions        []Action `json:"actions"`
	Before         []byte   `json:"-"`
	After          []byte   `json:"-"`
	BeforeSHA      string   `json:"before_sha256"`
	AfterSHA       string   `json:"after_sha256"`
	Changed        bool     `json:"changed"`
	BeforeRevision int64    `json:"before_revision,omitempty"`
	AfterRevision  int64    `json:"after_revision,omitempty"`
}

type Plan struct {
	Nodes    []NodePlan `json:"nodes"`
	PlanHash string     `json:"plan_hash"`
}

func BuildPlan(fs *fixture.Store, result audit.Result) (Plan, error) {
	byNode := make(map[string][]domain.Conflict)
	for _, c := range result.Conflicts {
		byNode[c.NodePath] = append(byNode[c.NodePath], c)
	}
	paths := make([]string, 0, len(byNode))
	for p := range byNode {
		paths = append(paths, p)
	}
	sort.Slice(paths, func(i, j int) bool {
		di, dj := depthOf(paths[i]), depthOf(paths[j])
		if di != dj {
			return di < dj
		}
		return paths[i] < paths[j]
	})
	plan := Plan{Nodes: []NodePlan{}}
	for _, nodePath := range paths {
		np, err := buildNodePlan(fs, nodePath, byNode[nodePath])
		if err != nil {
			return Plan{}, err
		}
		plan.Nodes = append(plan.Nodes, np)
	}
	hashPayload := make([]struct {
		NodePath  string   `json:"node_path"`
		BeforeSHA string   `json:"before_sha256"`
		AfterSHA  string   `json:"after_sha256"`
		Actions   []Action `json:"actions"`
	}, 0, len(plan.Nodes))
	for _, n := range plan.Nodes {
		hashPayload = append(hashPayload, struct {
			NodePath  string   `json:"node_path"`
			BeforeSHA string   `json:"before_sha256"`
			AfterSHA  string   `json:"after_sha256"`
			Actions   []Action `json:"actions"`
		}{n.NodePath, n.BeforeSHA, n.AfterSHA, n.Actions})
	}
	b, err := domain.CanonicalJSON(hashPayload)
	if err != nil {
		return Plan{}, err
	}
	plan.PlanHash = domain.HashBytes(b)
	return plan, nil
}

func buildNodePlan(fs *fixture.Store, nodePath string, conflicts []domain.Conflict) (NodePlan, error) {
	before, err := fs.LoadDescriptorBytes(nodePath)
	if err != nil {
		return NodePlan{}, err
	}
	var desc domain.Descriptor
	if err := domain.DecodeJSON(strings.NewReader(string(before)), &desc); err != nil {
		return NodePlan{}, err
	}
	remove := make(map[int]bool)
	flags := make(map[int]domain.InheritFlags)
	var actions []Action
	var additions []domain.ACE
	for _, c := range conflicts {
		switch c.Code {
		case acl.CodeDenyOverride, acl.CodeOrphanSID:
			idx := c.ACEIndex
			if idx < 0 || idx >= len(desc.ACEs) {
				return NodePlan{}, fmt.Errorf("conflict %s at %s references invalid ACE %d", c.Code, nodePath, idx)
			}
			if remove[idx] {
				continue
			}
			ace := desc.ACEs[idx]
			remove[idx] = true
			actions = append(actions, Action{
				NodePath: nodePath, ConflictCode: c.Code, Operation: ActionRemoveACE,
				ACEIndex: idx, BeforeACE: &ace, Reason: c.Detail,
			})
		case acl.CodeFlagIncompatible:
			idx := c.ACEIndex
			if idx < 0 || idx >= len(desc.ACEs) {
				return NodePlan{}, fmt.Errorf("conflict %s at %s references invalid ACE %d", c.Code, nodePath, idx)
			}
			ace := desc.ACEs[idx]
			after := ace
			after.Flags = domain.InheritFlags{}
			flags[idx] = after.Flags
			actions = append(actions, Action{
				NodePath: nodePath, ConflictCode: c.Code, Operation: ActionAdjustFlags,
				ACEIndex: idx, BeforeACE: &ace, AfterACE: &after, Reason: c.Detail,
			})
		case acl.CodeInheritBroken:
			ace, err := sourceACE(fs, c)
			if err != nil {
				return NodePlan{}, err
			}
			ace.Index = len(desc.ACEs) + len(additions)
			ace.Flags = domain.InheritFlags{}
			additions = append(additions, ace)
			actions = append(actions, Action{
				NodePath: nodePath, ConflictCode: c.Code, Operation: ActionAddInherited,
				ACEIndex: ace.Index, AfterACE: &ace, Reason: c.Detail,
			})
		}
	}
	out := domain.Descriptor{
		Schema: desc.Schema, DACLPresent: desc.DACLPresent, Protected: desc.Protected,
		Intents: append([]domain.Intent(nil), desc.Intents...),
		ACEs:    make([]domain.ACE, 0, len(desc.ACEs)+len(additions)),
	}
	for i, ace := range desc.ACEs {
		if remove[i] {
			continue
		}
		if f, ok := flags[i]; ok {
			ace.Flags = f
		}
		ace.Index = len(out.ACEs)
		out.ACEs = append(out.ACEs, ace)
	}
	for _, ace := range additions {
		ace.Index = len(out.ACEs)
		out.ACEs = append(out.ACEs, ace)
	}
	after, err := domain.CanonicalJSON(out)
	if err != nil {
		return NodePlan{}, err
	}
	if err := acl.ValidateDescriptor(nodePath, kindOf(desc, nodePath), out); err != nil {
		return NodePlan{}, fmt.Errorf("repair plan would produce malformed ACL at %s: %w", nodePath, err)
	}
	changes := len(actions) > 0
	sort.SliceStable(actions, func(i, j int) bool {
		if actions[i].ACEIndex != actions[j].ACEIndex {
			return actions[i].ACEIndex < actions[j].ACEIndex
		}
		return actions[i].Operation < actions[j].Operation
	})
	return NodePlan{
		NodePath: nodePath, Depth: depthOf(nodePath), Actions: actions,
		Before: before, After: after, BeforeSHA: domain.HashBytes(before), AfterSHA: domain.HashBytes(after), Changed: changes,
	}, nil
}

func sourceACE(fs *fixture.Store, c domain.Conflict) (domain.ACE, error) {
	if c.FirstDeviation.ACENodePath != "" && c.FirstDeviation.ACEIndex >= 0 {
		desc, _, err := fs.LoadDescriptor(c.FirstDeviation.ACENodePath)
		if err == nil && c.FirstDeviation.ACEIndex < len(desc.ACEs) {
			return desc.ACEs[c.FirstDeviation.ACEIndex], nil
		}
	}
	mask, err := domain.ParseAccess(c.Access)
	if err != nil {
		return domain.ACE{}, err
	}
	return domain.ACE{Type: domain.ACETypeAllow, SID: c.SubjectSID, AccessMask: mask}, nil
}

func kindOf(d domain.Descriptor, nodePath string) string {
	// CI is only rejected for file objects. The schema does not carry kind, so
	// infer conservatively from intents/ACE shape only when generating a plan.
	// The audit engine already validated the original descriptor.
	for _, a := range d.ACEs {
		if a.Flags.CI {
			return "DIRECTORY"
		}
	}
	return "FILE"
}

func depthOf(p string) int {
	if p == "." || p == "" {
		return 0
	}
	return strings.Count(p, "/") + 1
}
