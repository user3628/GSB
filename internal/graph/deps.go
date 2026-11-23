package graph

import (
	"sort"

	"protocolfuzzlab/internal/codes"
	"protocolfuzzlab/internal/model"
)

type node struct {
	kind string
	id   string
}

func (n node) key() string { return n.kind + ":" + n.id }

type Analysis struct {
	InvalidOperators  map[string]model.Diagnostic
	InvalidAssertions map[string]model.Diagnostic
	InvalidCases      map[string]model.Diagnostic
	Diagnostics       []model.Diagnostic
}

func (a Analysis) CaseInvalid(id string) (model.Diagnostic, bool) {
	d, ok := a.InvalidCases[id]
	return d, ok
}

func (a Analysis) InvalidCaseIDs() []string {
	out := make([]string, 0, len(a.InvalidCases))
	for id := range a.InvalidCases {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Analyze computes a deterministic invalidation closure. Structural dangling
// references are always invalid. In addition, every modify/delete operation is
// treated as invalidating reverse dependents; this models a stale coverage
// assertion or a seed template whose semantics no longer match the derived case.
func Analyze(cs model.CaseSet, version int, changes []model.Change) Analysis {
	a := Analysis{
		InvalidOperators:  map[string]model.Diagnostic{},
		InvalidAssertions: map[string]model.Diagnostic{},
		InvalidCases:      map[string]model.Diagnostic{},
	}
	queue := make([]model.Diagnostic, 0)

	var enqueue func(kind, id string, diag model.Diagnostic)
	enqueue = func(kind, id string, diag model.Diagnostic) {
		d := diag
		d.Entity = kind
		d.EntityID = id
		d.Version = version
		if d.Code == "" {
			d.Code = codes.DepInvalidated
		}
		existing, ok := a.diagnosticFor(kind, id)
		if ok {
			// A cycle is the stronger structural diagnosis and should win over a
			// generic stale-reference diagnosis.
			if existing.Code == codes.DepCycle || d.Code != codes.DepCycle {
				return
			}
		}
		a.setDiagnostic(kind, id, d)
		queue = append(queue, d)
	}

	// Operator cycles are discovered before missing-reference checks so the
	// resulting case diagnostics preserve E_DEP_CYCLE.
	for _, opID := range cycles(cs) {
		chain := cycleChain(cs, opID)
		enqueue(model.EntityOperator, opID, model.Diagnostic{
			Code:      codes.DepCycle,
			RootChain: chain,
			Version:   version,
		})
	}

	// Dangling references.
	for _, op := range cs.Operators {
		if _, ok := cs.Seed(op.SeedID); !ok {
			enqueue(model.EntityOperator, op.ID, model.Diagnostic{
				Code:      codes.DepInvalidated,
				RootChain: []string{"operator:" + op.ID, "seed:" + op.SeedID},
			})
		}
		for _, dep := range op.DependsOn {
			if _, ok := cs.Operator(dep); !ok {
				enqueue(model.EntityOperator, op.ID, model.Diagnostic{
					Code:      codes.DepInvalidated,
					RootChain: []string{"operator:" + op.ID, "operator:" + dep},
				})
			}
		}
	}
	for _, as := range cs.Assertions {
		if _, ok := cs.Case(as.CaseID); !ok {
			enqueue(model.EntityAssertion, as.ID, model.Diagnostic{
				Code:      codes.DepInvalidated,
				RootChain: []string{"assertion:" + as.ID, "case:" + as.CaseID},
			})
		}
		if _, ok := cs.Seed(as.TemplateID); !ok {
			enqueue(model.EntityAssertion, as.ID, model.Diagnostic{
				Code:      codes.DepInvalidated,
				RootChain: []string{"assertion:" + as.ID, "seed:" + as.TemplateID},
			})
		}
	}
	for _, tc := range cs.Cases {
		for _, step := range tc.Steps {
			if _, ok := cs.Seed(step.SeedID); !ok {
				enqueue(model.EntityCase, tc.ID, model.Diagnostic{
					Code:      codes.DepInvalidated,
					RootChain: []string{"case:" + tc.ID, "seed:" + step.SeedID},
				})
			}
			for _, opID := range step.OperatorIDs {
				if _, ok := cs.Operator(opID); !ok {
					enqueue(model.EntityCase, tc.ID, model.Diagnostic{
						Code:      codes.DepInvalidated,
						RootChain: []string{"case:" + tc.ID, "operator:" + opID},
					})
				}
			}
		}
	}

	// Change-induced invalidation propagates only to reverse dependents. The
	// changed entity itself remains part of the new case set.
	for _, change := range changes {
		if change.Action != model.ActionModify && change.Action != model.ActionDelete {
			continue
		}
		root := node{kind: change.Entity, id: change.ID}
		chain := []string{root.key()}
		for _, dep := range dependents(cs, root) {
			enqueue(dep.kind, dep.id, model.Diagnostic{
				Code:      codes.DepInvalidated,
				RootChain: append(append([]string(nil), chain...), dep.key()),
			})
		}
	}

	// Propagate invalid nodes to reverse dependents until a fixpoint. Nodes are
	// enqueued at most once per code, so cycles in the dependency graph terminate.
	for len(queue) > 0 {
		d := queue[0]
		queue = queue[1:]
		n := node{kind: d.Entity, id: d.EntityID}
		for _, dep := range dependents(cs, n) {
			chain := append(append([]string(nil), d.RootChain...), dep.key())
			enqueue(dep.kind, dep.id, model.Diagnostic{
				Code:      d.Code,
				RootChain: chain,
			})
		}
	}

	sort.Slice(a.Diagnostics, func(i, j int) bool {
		if a.Diagnostics[i].Entity != a.Diagnostics[j].Entity {
			return entityRank(a.Diagnostics[i].Entity) < entityRank(a.Diagnostics[j].Entity)
		}
		if a.Diagnostics[i].EntityID != a.Diagnostics[j].EntityID {
			return a.Diagnostics[i].EntityID < a.Diagnostics[j].EntityID
		}
		if a.Diagnostics[i].Code != a.Diagnostics[j].Code {
			return a.Diagnostics[i].Code < a.Diagnostics[j].Code
		}
		return chainString(a.Diagnostics[i].RootChain) < chainString(a.Diagnostics[j].RootChain)
	})
	return a
}

func (a *Analysis) diagnosticFor(kind, id string) (model.Diagnostic, bool) {
	switch kind {
	case model.EntityOperator:
		d, ok := a.InvalidOperators[id]
		return d, ok
	case model.EntityAssertion:
		d, ok := a.InvalidAssertions[id]
		return d, ok
	case model.EntityCase:
		d, ok := a.InvalidCases[id]
		return d, ok
	default:
		return model.Diagnostic{}, false
	}
}

func (a *Analysis) setDiagnostic(kind, id string, d model.Diagnostic) {
	switch kind {
	case model.EntityOperator:
		a.InvalidOperators[id] = d
	case model.EntityAssertion:
		a.InvalidAssertions[id] = d
	case model.EntityCase:
		a.InvalidCases[id] = d
	}
	a.Diagnostics = append(a.Diagnostics, d)
}

func dependents(cs model.CaseSet, n node) []node {
	var out []node
	switch n.kind {
	case model.EntitySeed:
		for _, op := range cs.Operators {
			if op.SeedID == n.id {
				out = append(out, node{model.EntityOperator, op.ID})
			}
		}
		for _, as := range cs.Assertions {
			if as.TemplateID == n.id {
				out = append(out, node{model.EntityAssertion, as.ID})
			}
		}
		for _, tc := range cs.Cases {
			for _, step := range tc.Steps {
				if step.SeedID == n.id {
					out = append(out, node{model.EntityCase, tc.ID})
					break
				}
			}
		}
	case model.EntityOperator:
		for _, tc := range cs.Cases {
			uses := false
			for _, step := range tc.Steps {
				for _, opID := range step.OperatorIDs {
					if opID == n.id {
						uses = true
						break
					}
				}
			}
			if uses {
				out = append(out, node{model.EntityCase, tc.ID})
			}
		}
		for _, op := range cs.Operators {
			for _, dep := range op.DependsOn {
				if dep == n.id {
					out = append(out, node{model.EntityOperator, op.ID})
					break
				}
			}
		}
	case model.EntityAssertion:
		// A coverage assertion is downstream of its case for execution. Editing
		// or invalidating an assertion does not make the case unexecutable.
	case model.EntityCase:
		for _, as := range cs.Assertions {
			if as.CaseID == n.id {
				out = append(out, node{model.EntityAssertion, as.ID})
			}
		}
	}
	return dedupeNodes(out)
}

func dedupeNodes(in []node) []node {
	seen := make(map[string]bool, len(in))
	out := make([]node, 0, len(in))
	for _, n := range in {
		if !seen[n.key()] {
			seen[n.key()] = true
			out = append(out, n)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].kind != out[j].kind {
			return entityRank(out[i].kind) < entityRank(out[j].kind)
		}
		return out[i].id < out[j].id
	})
	return out
}

func cycles(cs model.CaseSet) []string {
	deps := make(map[string][]string, len(cs.Operators))
	ids := make([]string, 0, len(cs.Operators))
	for _, op := range cs.Operators {
		ids = append(ids, op.ID)
		deps[op.ID] = append([]string(nil), op.DependsOn...)
		sort.Strings(deps[op.ID])
	}
	sort.Strings(ids)
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(ids))
	var stack []string
	found := map[string]bool{}
	var visit func(string)
	visit = func(id string) {
		color[id] = gray
		stack = append(stack, id)
		for _, dep := range deps[id] {
			if _, exists := deps[dep]; !exists {
				continue
			}
			switch color[dep] {
			case white:
				visit(dep)
			case gray:
				start := 0
				for i, candidate := range stack {
					if candidate == dep {
						start = i
						break
					}
				}
				for _, cycleID := range stack[start:] {
					found[cycleID] = true
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = black
	}
	for _, id := range ids {
		if color[id] == white {
			visit(id)
		}
	}
	out := make([]string, 0, len(found))
	for id := range found {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func cycleChain(cs model.CaseSet, start string) []string {
	deps := map[string][]string{}
	for _, op := range cs.Operators {
		deps[op.ID] = append([]string(nil), op.DependsOn...)
		sort.Strings(deps[op.ID])
	}
	// Produce one stable cycle containing start by walking the first edge that
	// can return to start; the full cycle is useful as a root-cause chain.
	path := []string{start}
	seen := map[string]bool{start: true}
	var walk func(string) bool
	walk = func(cur string) bool {
		for _, dep := range deps[cur] {
			if dep == start {
				path = append(path, dep)
				return true
			}
			if seen[dep] {
				continue
			}
			seen[dep] = true
			path = append(path, dep)
			if walk(dep) {
				return true
			}
			path = path[:len(path)-1]
		}
		return false
	}
	if !walk(start) {
		return []string{"operator:" + start}
	}
	out := make([]string, len(path))
	for i, id := range path {
		out[i] = "operator:" + id
	}
	return out
}

func entityRank(entity string) int {
	switch entity {
	case model.EntitySeed:
		return 0
	case model.EntityOperator:
		return 1
	case model.EntityAssertion:
		return 2
	case model.EntityCase:
		return 3
	default:
		return 4
	}
}

func chainString(chain []string) string {
	out := ""
	for _, part := range chain {
		out += part + "\x00"
	}
	return out
}

// Affected collects changed entities plus all reverse dependents for the diff
// summary. It is intentionally broader than the invalid set.
func Affected(before, after model.CaseSet, changes []model.Change) ([]string, []string) {
	caseSet := map[string]bool{}
	opSet := map[string]bool{}
	for _, tc := range before.Cases {
		caseSet[tc.ID] = false
	}
	for _, tc := range after.Cases {
		if _, ok := caseSet[tc.ID]; !ok {
			caseSet[tc.ID] = false
		}
	}
	for _, op := range after.Operators {
		opSet[op.ID] = false
	}
	for _, change := range changes {
		if change.Entity == model.EntityCase {
			caseSet[change.ID] = true
		}
		if change.Entity == model.EntityOperator {
			opSet[change.ID] = true
		}
		if change.Entity == model.EntityAssertion {
			if as, ok := before.Assertion(change.ID); ok {
				caseSet[as.CaseID] = true
			}
			if as, ok := after.Assertion(change.ID); ok {
				caseSet[as.CaseID] = true
			}
		}
		root := node{kind: change.Entity, id: change.ID}
		visited := map[string]bool{root.key(): true}
		queue := []node{root}
		for len(queue) > 0 {
			n := queue[0]
			queue = queue[1:]
			if n.kind == model.EntityCase {
				caseSet[n.id] = true
			}
			if n.kind == model.EntityOperator {
				opSet[n.id] = true
			}
			for _, dep := range dependents(after, n) {
				if !visited[dep.key()] {
					visited[dep.key()] = true
					queue = append(queue, dep)
				}
			}
		}
	}
	return sortedTrue(caseSet), sortedTrue(opSet)
}

func sortedTrue(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for id, include := range m {
		if include {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

type TraceLink struct {
	FromType string `json:"from_type"`
	FromID   string `json:"from_id"`
	ToType   string `json:"to_type"`
	ToID     string `json:"to_id"`
	Relation string `json:"relation"`
}

// TraceCase returns the case's dependency spine, including the coverage
// assertions attached to it and transitive operator/seed references.
func TraceCase(cs model.CaseSet, caseID string) []TraceLink {
	var out []TraceLink
	seen := map[string]bool{}
	var visitCase func(string)
	visitOperator := func(string) {}
	visitCase = func(id string) {
		key := "case:" + id
		if seen[key] {
			return
		}
		seen[key] = true
		tc, ok := cs.Case(id)
		if !ok {
			return
		}
		for _, step := range tc.Steps {
			out = append(out, TraceLink{model.EntityCase, id, model.EntitySeed, step.SeedID, "step_template"})
			for _, opID := range step.OperatorIDs {
				out = append(out, TraceLink{model.EntityCase, id, model.EntityOperator, opID, "step_operator"})
				visitOperator(opID)
			}
		}
		for _, as := range cs.AssertionsForCase(id) {
			out = append(out, TraceLink{model.EntityCase, id, model.EntityAssertion, as.ID, "coverage_assertion"})
			out = append(out, TraceLink{model.EntityAssertion, as.ID, model.EntitySeed, as.TemplateID, "assertion_template"})
		}
	}
	var visitOp func(string)
	visitOp = func(id string) {
		key := "operator:" + id
		if seen[key] {
			return
		}
		seen[key] = true
		op, ok := cs.Operator(id)
		if !ok {
			return
		}
		out = append(out, TraceLink{model.EntityOperator, id, model.EntitySeed, op.SeedID, "operator_template"})
		for _, dep := range op.DependsOn {
			out = append(out, TraceLink{model.EntityOperator, id, model.EntityOperator, dep, "operator_dependency"})
			visitOp(dep)
		}
	}
	visitOperator = visitOp
	visitCase(caseID)
	sort.Slice(out, func(i, j int) bool {
		a := out[i]
		b := out[j]
		if a.FromType != b.FromType {
			return entityRank(a.FromType) < entityRank(b.FromType)
		}
		if a.FromID != b.FromID {
			return a.FromID < b.FromID
		}
		if a.Relation != b.Relation {
			return a.Relation < b.Relation
		}
		if a.ToType != b.ToType {
			return entityRank(a.ToType) < entityRank(b.ToType)
		}
		return a.ToID < b.ToID
	})
	// Deduplicate links that can be reached from both case and assertion walks.
	deduped := out[:0]
	last := ""
	for _, link := range out {
		key := link.FromType + "\x00" + link.FromID + "\x00" + link.Relation + "\x00" + link.ToType + "\x00" + link.ToID
		if key != last {
			deduped = append(deduped, link)
			last = key
		}
	}
	return deduped
}
