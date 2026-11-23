package audit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"example.com/ntfsacl/internal/acl"
	"example.com/ntfsacl/internal/domain"
	"example.com/ntfsacl/internal/fixture"
	"example.com/ntfsacl/internal/report"
	"example.com/ntfsacl/internal/safepath"
	"example.com/ntfsacl/internal/store"
)

const CodeNodeBudget = "E_NODE_BUDGET"

type Config struct {
	Concurrency int
	NodeBudget  int
}

type Request struct {
	ID      string
	Subtree string
}

type SessionResult struct {
	Request Request
	Nodes   []domain.Node
	Rebuild acl.Rebuild
	Code    string
	Partial bool
	Err     error
}

type Result struct {
	FixtureID string
	Sessions  []report.SessionReport
	Nodes     []domain.NodeReport
	Conflicts []domain.Conflict
	Summary   []domain.ConflictSummary
	TreeHash  string
	Code      string
}

func Open(workdir string) (*fixture.Store, domain.Policy, error) {
	fs, err := fixture.Open(workdir)
	if err != nil {
		return nil, domain.Policy{}, err
	}
	policy, err := fs.Policy()
	if err != nil {
		return nil, domain.Policy{}, err
	}
	return fs, policy, nil
}

// Run audits independent sessions through a bounded worker pool. Requests over
// the concurrency limit wait for a slot; they are never rejected.
func Run(ctx context.Context, workdir string, requests []Request, cfg Config) (Result, error) {
	fs, policy, err := Open(workdir)
	if err != nil {
		return Result{}, err
	}
	if len(requests) == 0 {
		requests = []Request{{ID: "audit-1", Subtree: "."}}
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	results := make([]SessionResult, len(requests))
	slots := make(chan struct{}, cfg.Concurrency)
	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				results[idx] = SessionResult{Request: requests[idx], Err: ctx.Err()}
				return
			}
			results[idx] = runSession(ctx, fs, policy, requests[idx], cfg.NodeBudget)
		}(i)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	sessionReports := make([]report.SessionReport, 0, len(results))
	seenNodes := make(map[string]bool)
	var allNodes []domain.Node
	var allNodeReports []domain.NodeReport
	var allConflicts []domain.Conflict
	overallCode := ""
	for _, sr := range results {
		if sr.Err != nil {
			return Result{}, fmt.Errorf("session %s (%s): %w", sr.Request.ID, sr.Request.Subtree, sr.Err)
		}
		status := "COMPLETED"
		if sr.Partial {
			status = "PARTIAL"
		}
		sessionReports = append(sessionReports, report.SessionReport{
			ID: sr.Request.ID, Subtree: sr.Request.Subtree, Status: status, Code: sr.Code,
			NodesAudited: len(sr.Nodes), NodeBudget: cfg.NodeBudget, Partial: sr.Partial,
		})
		if sr.Code != "" {
			overallCode = sr.Code
		}
		for _, n := range sr.Nodes {
			if !seenNodes[n.RelPath] {
				seenNodes[n.RelPath] = true
				allNodes = append(allNodes, n)
			}
		}
		for _, nr := range sr.Rebuild.Nodes {
			if !containsNodeReport(allNodeReports, nr.RelPath) {
				allNodeReports = append(allNodeReports, nr)
			}
		}
		for _, c := range sr.Rebuild.Conflicts {
			if !containsConflict(allConflicts, c) {
				allConflicts = append(allConflicts, c)
			}
		}
	}
	sort.Slice(allNodeReports, func(i, j int) bool { return allNodeReports[i].RelPath < allNodeReports[j].RelPath })
	sort.Slice(allConflicts, func(i, j int) bool {
		if allConflicts[i].Code != allConflicts[j].Code {
			return conflictRank(allConflicts[i].Code) < conflictRank(allConflicts[j].Code)
		}
		if allConflicts[i].NodePath != allConflicts[j].NodePath {
			return allConflicts[i].NodePath < allConflicts[j].NodePath
		}
		if allConflicts[i].ACEIndex != allConflicts[j].ACEIndex {
			return allConflicts[i].ACEIndex < allConflicts[j].ACEIndex
		}
		return allConflicts[i].SubjectSID < allConflicts[j].SubjectSID
	})
	sort.Slice(allNodes, func(i, j int) bool { return allNodes[i].RelPath < allNodes[j].RelPath })
	treeHash := computeTreeHash(policy.FixtureID, allNodes)
	return Result{
		FixtureID: policy.FixtureID,
		Sessions:  sessionReports,
		Nodes:     allNodeReports,
		Conflicts: allConflicts,
		Summary:   summarize(allConflicts),
		TreeHash:  treeHash,
		Code:      overallCode,
	}, nil
}

func (r Result) Rebuild() acl.Rebuild {
	return acl.Rebuild{Nodes: r.Nodes, Conflicts: r.Conflicts, Summary: r.Summary}
}

func (r Result) Report(version, parentVersion int64, parentHash string) (report.Report, error) {
	return r.ReportAs(store.KindAudit, version, parentVersion, parentHash)
}

func (r Result) ReportAs(kind string, version, parentVersion int64, parentHash string) (report.Report, error) {
	rep := report.New(report.SnapshotRef{Version: version, Kind: kind, ParentVersion: parentVersion, ParentChainHash: parentHash}, r.FixtureID, r.TreeHash, r.Code, r.Sessions, r.Rebuild())
	return rep, nil
}

func runSession(ctx context.Context, fs *fixture.Store, policy domain.Policy, req Request, budget int) SessionResult {
	out := SessionResult{Request: req}
	if err := ctx.Err(); err != nil {
		out.Err = err
		return out
	}
	nodes, partial, err := loadNodes(fs, req.Subtree, budget)
	if err != nil {
		out.Err = err
		return out
	}
	if partial {
		out.Partial = true
		out.Code = CodeNodeBudget
	}
	rebuild, err := acl.RebuildTree(nodes, policy)
	if err != nil {
		out.Err = err
		return out
	}
	out.Nodes = nodes
	out.Rebuild = rebuild
	return out
}

func loadNodes(fs *fixture.Store, subtree string, budget int) ([]domain.Node, bool, error) {
	subtree, err := safepath.CleanRel(subtree)
	if err != nil {
		return nil, false, err
	}
	start, err := safepath.Join(fs.TreeRoot, subtree)
	if err != nil {
		return nil, false, err
	}
	if err := safepath.EnsureNoSymlink(fs.TreeRoot, start); err != nil {
		return nil, false, err
	}
	info, err := os.Lstat(start)
	if err != nil {
		return nil, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, false, fmt.Errorf("symlink subtree is not supported")
	}
	var rels []string
	queue := []string{subtree}
	for len(queue) > 0 {
		rel := queue[0]
		queue = queue[1:]
		target, err := safepath.Join(fs.TreeRoot, rel)
		if err != nil {
			return nil, false, err
		}
		info, err := os.Lstat(target)
		if err != nil {
			return nil, false, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, false, fmt.Errorf("symlink node rejected: %s", rel)
		}
		rels = append(rels, rel)
		if !info.IsDir() {
			continue
		}
		entries, err := os.ReadDir(target)
		if err != nil {
			return nil, false, err
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
		for _, e := range entries {
			childRel := e.Name()
			if rel != "." {
				childRel = rel + "/" + e.Name()
			}
			if e.Type()&os.ModeSymlink != 0 {
				return nil, false, fmt.Errorf("symlink node rejected: %s", childRel)
			}
			queue = append(queue, childRel)
		}
	}
	partial := false
	if budget > 0 && len(rels) > budget {
		rels = rels[:budget]
		partial = true
	}
	nodes := make([]domain.Node, 0, len(rels))
	for _, rel := range rels {
		desc, sha, err := fs.LoadDescriptor(rel)
		if err != nil {
			return nil, false, &acl.MalformedError{Code: acl.CodeACLMalformed, NodePath: rel, ACEIndex: -1, Detail: "descriptor metadata unreadable: " + err.Error()}
		}
		target, _ := safepath.Join(fs.TreeRoot, rel)
		info, err := os.Lstat(target)
		if err != nil {
			return nil, false, err
		}
		kind := "FILE"
		if info.IsDir() {
			kind = "DIRECTORY"
		}
		depth := 0
		if rel != "." {
			depth = strings.Count(rel, "/") + 1
		}
		nodes = append(nodes, domain.Node{RelPath: rel, Kind: kind, Depth: depth, Descriptor: desc, DescriptorSHA: sha})
	}
	return nodes, partial, nil
}

func computeTreeHash(fixtureID string, nodes []domain.Node) string {
	sorted := append([]domain.Node(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].RelPath < sorted[j].RelPath })
	var b strings.Builder
	b.WriteString(fixtureID)
	b.WriteByte(0)
	for _, n := range sorted {
		b.WriteString(n.RelPath)
		b.WriteByte(0)
		b.WriteString(n.Kind)
		b.WriteByte(0)
		b.WriteString(n.DescriptorSHA)
		b.WriteByte(0)
	}
	return domain.HashBytes([]byte(b.String()))
}

func summarize(conflicts []domain.Conflict) []domain.ConflictSummary {
	codes := []string{acl.CodeDenyOverride, acl.CodeInheritBroken, acl.CodeFlagIncompatible, acl.CodeOrphanSID}
	var out []domain.ConflictSummary
	for _, code := range codes {
		var first *domain.Conflict
		count := 0
		for i := range conflicts {
			if conflicts[i].Code != code {
				continue
			}
			count++
			if first == nil || lessConflict(&conflicts[i], first) {
				first = &conflicts[i]
			}
		}
		if first != nil {
			out = append(out, domain.ConflictSummary{Code: code, Count: count, FirstNodePath: first.NodePath, FirstACENodePath: first.FirstDeviation.ACENodePath, FirstACEIndex: first.ACEIndex, FirstSubjectSID: first.SubjectSID})
		}
	}
	return out
}

func lessConflict(a, b *domain.Conflict) bool {
	if a.NodePath != b.NodePath {
		return a.NodePath < b.NodePath
	}
	if a.ACEIndex != b.ACEIndex {
		return a.ACEIndex < b.ACEIndex
	}
	return a.SubjectSID < b.SubjectSID
}

func conflictRank(code string) int {
	switch code {
	case acl.CodeDenyOverride:
		return 0
	case acl.CodeInheritBroken:
		return 1
	case acl.CodeFlagIncompatible:
		return 2
	default:
		return 3
	}
}

func containsNodeReport(list []domain.NodeReport, path string) bool {
	for _, n := range list {
		if n.RelPath == path {
			return true
		}
	}
	return false
}

func containsConflict(list []domain.Conflict, c domain.Conflict) bool {
	for _, x := range list {
		if x.Code == c.Code && x.NodePath == c.NodePath && x.ACEIndex == c.ACEIndex && x.SubjectSID == c.SubjectSID {
			return true
		}
	}
	return false
}

// IsSessionError exposes the underlying malformed error to the CLI while
// preserving the session label.
func IsSessionError(err error) (*acl.MalformedError, bool) {
	var me *acl.MalformedError
	if errors.As(err, &me) {
		return me, true
	}
	return nil, false
}

// RelativeFilepath is a helper used by tests and diagnostics.
func RelativeFilepath(root, p string) string {
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return p
	}
	return filepath.ToSlash(rel)
}
