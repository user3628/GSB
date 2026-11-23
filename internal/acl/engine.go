package acl

import (
	"fmt"
	"sort"
	"strings"

	"example.com/ntfsacl/internal/domain"
)

const (
	CodeDenyOverride     = "E_CONFLICT_DENY_OVERRIDE"
	CodeInheritBroken    = "E_INHERIT_BROKEN"
	CodeFlagIncompatible = "E_FLAG_INCOMPATIBLE"
	CodeOrphanSID        = "E_ORPHAN_SID"
)

type BuildOptions struct {
	// AllowedSIDs is the principal directory. Nil means no orphan checking.
	AllowedSIDs map[string]bool
}

type Rebuild struct {
	Nodes     []domain.NodeReport
	Conflicts []domain.Conflict
	Summary   []domain.ConflictSummary
}

func RebuildTree(nodes []domain.Node, policy domain.Policy) (Rebuild, error) {
	for _, n := range nodes {
		if err := ValidateDescriptor(n.RelPath, n.Kind, n.Descriptor); err != nil {
			return Rebuild{}, err
		}
	}
	byPath := make(map[string]domain.Node, len(nodes))
	for _, n := range nodes {
		byPath[n.RelPath] = n
	}
	ordered := append([]domain.Node(nil), nodes...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Depth != ordered[j].Depth {
			return ordered[i].Depth < ordered[j].Depth
		}
		return ordered[i].RelPath < ordered[j].RelPath
	})

	effectiveByPath := make(map[string][]domain.EffectiveACE, len(nodes))
	out := Rebuild{Nodes: make([]domain.NodeReport, 0, len(nodes))}
	known := make(map[string]bool, len(policy.KnownSIDs))
	for _, sid := range policy.KnownSIDs {
		known[strings.ToUpper(sid)] = true
	}
	if len(known) == 0 {
		for _, n := range append([]domain.Node(nil), nodes...) {
			for _, a := range n.Descriptor.ACEs {
				known[strings.ToUpper(a.SID)] = true
			}
		}
	}

	for _, n := range ordered {
		var inherited []domain.EffectiveACE
		if !n.Descriptor.Protected {
			parent := ParentPath(n.RelPath)
			if parent != "" {
				inherited = propagate(effectiveByPath[parent], n.RelPath, n.Kind)
			}
		}
		combined := append([]domain.EffectiveACE(nil), inherited...)
		for i, ace := range n.Descriptor.ACEs {
			ace.Type = strings.ToUpper(ace.Type)
			combined = append(combined, domain.EffectiveACE{
				OriginalIndex:   i,
				Type:            ace.Type,
				SID:             strings.ToUpper(ace.SID),
				AccessMask:      ace.AccessMask,
				AccessMaskHex:   fmt.Sprintf("0x%08X", ace.AccessMask),
				Flags:           ace.Flags,
				Origin:          domain.OriginExplicit,
				Effective:       !ace.Flags.IO,
				SourcePath:      n.RelPath,
				InheritanceStep: 0,
				SourceChain: []domain.SourceRef{{
					NodePath:        n.RelPath,
					ACENodePath:     n.RelPath,
					ACEIndex:        i,
					Origin:          domain.OriginExplicit,
					InheritanceStep: 0,
				}},
			})
		}
		canonicalize(combined)
		effectiveByPath[n.RelPath] = combined

		nr := domain.NodeReport{
			RelPath:   n.RelPath,
			Kind:      n.Kind,
			ACLHash:   n.DescriptorSHA,
			Protected: n.Descriptor.Protected,
			ACEs:      combined,
			Decisions: []domain.Decision{},
		}
		for _, intent := range n.Descriptor.Intents {
			nr.Decisions = append(nr.Decisions, decide(combined, intent))
		}
		out.Nodes = append(out.Nodes, nr)
	}
	sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].RelPath < out.Nodes[j].RelPath })

	conflicts := detectConflicts(nodes, effectiveByPath, known, byPath)
	sort.Slice(conflicts, func(i, j int) bool {
		if conflicts[i].Code != conflicts[j].Code {
			return conflictRank(conflicts[i].Code) < conflictRank(conflicts[j].Code)
		}
		if conflicts[i].NodePath != conflicts[j].NodePath {
			return conflicts[i].NodePath < conflicts[j].NodePath
		}
		if conflicts[i].ACEIndex != conflicts[j].ACEIndex {
			return conflicts[i].ACEIndex < conflicts[j].ACEIndex
		}
		return conflicts[i].SubjectSID < conflicts[j].SubjectSID
	})
	out.Conflicts = conflicts
	out.Summary = summarize(conflicts)
	return out, nil
}

func ParentPath(p string) string {
	if p == "" || p == "." {
		return ""
	}
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[:i]
	}
	return "."
}

func BaseName(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[i+1:]
	}
	return p
}

func propagate(parent []domain.EffectiveACE, childPath, childKind string) []domain.EffectiveACE {
	var out []domain.EffectiveACE
	for _, ace := range parent {
		if !ace.Flags.AnyInheritance() {
			continue
		}
		applies := (childKind == "DIRECTORY" && ace.Flags.CI) || (childKind == "FILE" && ace.Flags.OI)
		if !applies {
			continue
		}
		clone := ace
		clone.Flags.IO = false
		if clone.Flags.NP {
			clone.Flags = domain.InheritFlags{}
		}
		clone.Origin = domain.OriginInherited
		clone.Effective = true
		clone.InheritedFrom = ParentPath(childPath)
		clone.InheritanceStep = ace.InheritanceStep + 1
		chain := append([]domain.SourceRef(nil), ace.SourceChain...)
		chain = append(chain, domain.SourceRef{
			NodePath:        childPath,
			ACENodePath:     ace.SourcePath,
			ACEIndex:        ace.OriginalIndex,
			Origin:          domain.OriginInherited,
			InheritedFrom:   ParentPath(childPath),
			InheritanceStep: clone.InheritanceStep,
		})
		clone.SourceChain = chain
		out = append(out, clone)
	}
	return out
}

func canonicalize(aces []domain.EffectiveACE) {
	sort.SliceStable(aces, func(i, j int) bool {
		a, b := aces[i], aces[j]
		ao, bo := originRank(a.Origin), originRank(b.Origin)
		if ao != bo {
			return ao < bo
		}
		if at, bt := aceTypeRank(a.Type), aceTypeRank(b.Type); at != bt {
			return at < bt
		}
		if a.InheritanceStep != b.InheritanceStep {
			return a.InheritanceStep < b.InheritanceStep
		}
		if a.SourcePath != b.SourcePath {
			return a.SourcePath < b.SourcePath
		}
		return a.OriginalIndex < b.OriginalIndex
	})
	for i := range aces {
		aces[i].Sequence = i
	}
}

func originRank(s string) int {
	if s == domain.OriginExplicit {
		return 0
	}
	return 1
}

func aceTypeRank(s string) int {
	switch s {
	case domain.ACETypeDeny:
		return 0
	case domain.ACETypeAllow:
		return 1
	default:
		return 2
	}
}

func decide(aces []domain.EffectiveACE, intent domain.Intent) domain.Decision {
	mask, _ := domain.ParseAccess(intent.Access)
	sid := strings.ToUpper(intent.SID)
	d := domain.Decision{
		SID:           sid,
		Access:        strings.ToUpper(intent.Access),
		AccessMask:    mask,
		AccessMaskHex: fmt.Sprintf("0x%08X", mask),
		Expected:      intent.Expected,
		Effective:     domain.DecisionNotGranted,
		Sources:       []domain.SourceRef{},
		Contributors:  []int{},
	}
	for _, ace := range aces {
		if !ace.Effective || ace.SID != sid || ace.Type == domain.ACETypeAudit {
			continue
		}
		if ace.AccessMask&mask == 0 {
			continue
		}
		switch ace.Type {
		case domain.ACETypeDeny:
			d.DeniedMask |= ace.AccessMask
		case domain.ACETypeAllow:
			d.AllowedMask |= ace.AccessMask
		}
		d.Contributors = append(d.Contributors, ace.Sequence)
		for _, ref := range ace.SourceChain {
			d.Sources = append(d.Sources, ref)
		}
	}
	if d.DeniedMask&mask != 0 {
		d.Effective = domain.DecisionDeny
	} else if d.AllowedMask&mask == mask {
		d.Effective = domain.DecisionAllow
	}
	d.Matched = d.Effective == d.Expected
	return d
}

func detectConflicts(nodes []domain.Node, rebuilt map[string][]domain.EffectiveACE, known map[string]bool, byPath map[string]domain.Node) []domain.Conflict {
	seen := make(map[string]bool)
	var out []domain.Conflict
	add := func(c domain.Conflict) {
		key := fmt.Sprintf("%s|%s|%d|%s", c.Code, c.NodePath, c.ACEIndex, c.SubjectSID)
		if seen[key] {
			return
		}
		seen[key] = true
		if len(c.Closure) == 0 && c.FirstDeviation.NodePath != "" {
			c.Closure = []domain.SourceRef{c.FirstDeviation}
		}
		out = append(out, c)
	}
	sortedNodes := append([]domain.Node(nil), nodes...)
	sort.Slice(sortedNodes, func(i, j int) bool { return sortedNodes[i].RelPath < sortedNodes[j].RelPath })

	for _, n := range sortedNodes {
		aces := rebuilt[n.RelPath]
		// Orphan references are detected independently of declared intent.
		for _, ace := range aces {
			if ace.Origin != domain.OriginExplicit || known[ace.SID] {
				continue
			}
			add(domain.Conflict{
				Code:           CodeOrphanSID,
				NodePath:       n.RelPath,
				ACEIndex:       ace.OriginalIndex,
				SubjectSID:     ace.SID,
				Detail:         "explicit ACE references a SID outside the fixture principal registry",
				FirstDeviation: sourceRef(n.RelPath, ace, 0),
				Closure:        ace.SourceChain,
			})
		}

		// A copied explicit ACE whose inheritance flags disagree with the
		// inherited ACE of the same lineage is a flag incompatibility.
		for _, explicit := range aces {
			if explicit.Origin != domain.OriginExplicit || !explicit.Flags.AnyInheritance() {
				continue
			}
			for _, inherited := range aces {
				if inherited.Origin != domain.OriginInherited || inherited.Type != explicit.Type ||
					inherited.SID != explicit.SID || inherited.AccessMask != explicit.AccessMask {
					continue
				}
				if explicit.Flags != inherited.Flags {
					closure := append([]domain.SourceRef(nil), inherited.SourceChain...)
					closure = append(closure, sourceRef(n.RelPath, explicit, 0))
					add(domain.Conflict{
						Code:       CodeFlagIncompatible,
						NodePath:   n.RelPath,
						ACEIndex:   explicit.OriginalIndex,
						SubjectSID: explicit.SID,
						Detail: fmt.Sprintf("explicit flags %s disagree with inherited flags %s from %s",
							explicit.Flags.String(), inherited.Flags.String(), inherited.SourcePath),
						FirstDeviation: sourceRef(n.RelPath, explicit, 0),
						Closure:        closure,
					})
					break
				}
			}
		}

		// Detect an explicit allow placed before an explicit deny in the raw
		// DACL. Canonical ordering moves deny first; the inversion itself is a
		// deny-override conflict even when the final deny is intentional.
		rawAllows := make(map[string]domain.ACE)
		for _, raw := range n.Descriptor.ACEs {
			if raw.Type == domain.ACETypeAllow {
				rawAllows[raw.SID] = raw
				continue
			}
			if raw.Type != domain.ACETypeDeny {
				continue
			}
			allow, ok := rawAllows[raw.SID]
			if !ok || allow.AccessMask&raw.AccessMask == 0 {
				continue
			}
			denyEff := findByOriginal(aces, domain.OriginExplicit, raw.Index)
			allowEff := findByOriginal(aces, domain.OriginExplicit, allow.Index)
			closure := []domain.SourceRef{}
			if allowEff != nil {
				closure = append(closure, allowEff.SourceChain...)
			}
			if denyEff != nil {
				closure = append(closure, denyEff.SourceChain...)
			}
			add(domain.Conflict{
				Code:           CodeDenyOverride,
				NodePath:       n.RelPath,
				ACEIndex:       raw.Index,
				SubjectSID:     raw.SID,
				Detail:         fmt.Sprintf("explicit allow at raw ACE %d precedes explicit deny at raw ACE %d", allow.Index, raw.Index),
				FirstDeviation: domain.SourceRef{NodePath: n.RelPath, ACENodePath: n.RelPath, ACEIndex: raw.Index, Origin: domain.OriginExplicit},
				Closure:        closure,
			})
		}

		nodeReport := findNodeReport(n.RelPath, rebuilt)
		for _, intent := range n.Descriptor.Intents {
			d := decideForIntent(nodeReport, intent)
			if d.Matched {
				continue
			}
			if d.Expected == domain.DecisionAllow && d.Effective == domain.DecisionDeny {
				deny, inheritedAllow := denyAndInheritedAllow(aces, strings.ToUpper(intent.SID), d.AccessMask)
				if deny != nil && inheritedAllow != nil {
					closure := append([]domain.SourceRef(nil), deny.SourceChain...)
					closure = append(closure, inheritedAllow.SourceChain...)
					add(domain.Conflict{
						Code:           CodeDenyOverride,
						NodePath:       n.RelPath,
						ACEIndex:       deny.OriginalIndex,
						SubjectSID:     d.SID,
						Access:         d.Access,
						Detail:         "explicit deny overrides an inherited allow for the declared intent",
						FirstDeviation: sourceRef(n.RelPath, *deny, 0),
						Closure:        closure,
					})
					continue
				}
			}
			// A declared source missing from the reconstructed source chain is
			// the stable signature of a broken inheritance chain.
			if intent.ExpectedSource != "" {
				sourcePath, sourceIndex := parseSource(intent.ExpectedSource)
				if !hasSource(d.Sources, sourcePath, sourceIndex) {
					ref := domain.SourceRef{NodePath: n.RelPath, ACENodePath: sourcePath, ACEIndex: sourceIndex, Origin: domain.OriginInherited}
					add(domain.Conflict{
						Code:           CodeInheritBroken,
						NodePath:       n.RelPath,
						ACEIndex:       sourceIndex,
						SubjectSID:     d.SID,
						Access:         d.Access,
						Detail:         fmt.Sprintf("declared inheritance source %s:%d is absent; effective=%s expected=%s", sourcePath, sourceIndex, d.Effective, d.Expected),
						FirstDeviation: ref,
						Closure:        []domain.SourceRef{ref},
					})
					continue
				}
			}
			if d.Effective == domain.DecisionNotGranted && d.Expected != domain.DecisionNotGranted {
				add(domain.Conflict{
					Code:           CodeInheritBroken,
					NodePath:       n.RelPath,
					ACEIndex:       -1,
					SubjectSID:     d.SID,
					Access:         d.Access,
					Detail:         fmt.Sprintf("declared %s is not reachable (effective=%s); no applicable source ACE", d.Expected, d.Effective),
					FirstDeviation: domain.SourceRef{NodePath: n.RelPath, ACENodePath: n.RelPath, ACEIndex: -1, Origin: domain.OriginInherited},
					Closure:        []domain.SourceRef{},
				})
			}
		}
	}
	return out
}

func findByOriginal(aces []domain.EffectiveACE, origin string, index int) *domain.EffectiveACE {
	for i := range aces {
		if aces[i].Origin == origin && aces[i].OriginalIndex == index {
			return &aces[i]
		}
	}
	return nil
}

func findNodeReport(path string, rebuilt map[string][]domain.EffectiveACE) domain.NodeReport {
	return domain.NodeReport{RelPath: path, ACEs: rebuilt[path]}
}

func decideForIntent(nr domain.NodeReport, intent domain.Intent) domain.Decision {
	return decide(nr.ACEs, intent)
}

func denyAndInheritedAllow(aces []domain.EffectiveACE, sid string, mask uint32) (*domain.EffectiveACE, *domain.EffectiveACE) {
	var deny, allow *domain.EffectiveACE
	for i := range aces {
		a := &aces[i]
		if !a.Effective || a.SID != sid || a.AccessMask&mask == 0 {
			continue
		}
		if a.Type == domain.ACETypeDeny && a.Origin == domain.OriginExplicit && deny == nil {
			deny = a
		}
		if a.Type == domain.ACETypeAllow && a.Origin == domain.OriginInherited && allow == nil {
			allow = a
		}
	}
	return deny, allow
}

func sourceRef(nodePath string, ace domain.EffectiveACE, step int) domain.SourceRef {
	return domain.SourceRef{
		NodePath:        nodePath,
		ACENodePath:     ace.SourcePath,
		ACEIndex:        ace.OriginalIndex,
		Origin:          ace.Origin,
		InheritedFrom:   ace.InheritedFrom,
		InheritanceStep: step,
	}
}

func parseSource(s string) (string, int) {
	i := strings.LastIndexByte(s, ':')
	if i <= 0 {
		return s, -1
	}
	idx := -1
	fmt.Sscanf(s[i+1:], "%d", &idx)
	return s[:i], idx
}

func hasSource(refs []domain.SourceRef, nodePath string, index int) bool {
	for _, r := range refs {
		if r.ACENodePath == nodePath && r.ACEIndex == index {
			return true
		}
	}
	return false
}

func summarize(conflicts []domain.Conflict) []domain.ConflictSummary {
	codes := []string{CodeDenyOverride, CodeInheritBroken, CodeFlagIncompatible, CodeOrphanSID}
	var out []domain.ConflictSummary
	for _, code := range codes {
		var first *domain.Conflict
		count := 0
		for i := range conflicts {
			if conflicts[i].Code != code {
				continue
			}
			count++
			if first == nil || conflictLess(&conflicts[i], first) {
				first = &conflicts[i]
			}
		}
		if first == nil {
			continue
		}
		out = append(out, domain.ConflictSummary{
			Code:             code,
			Count:            count,
			FirstNodePath:    first.NodePath,
			FirstACENodePath: first.FirstDeviation.ACENodePath,
			FirstACEIndex:    first.ACEIndex,
			FirstSubjectSID:  first.SubjectSID,
		})
	}
	return out
}

func conflictLess(a, b *domain.Conflict) bool {
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
	case CodeDenyOverride:
		return 0
	case CodeInheritBroken:
		return 1
	case CodeFlagIncompatible:
		return 2
	default:
		return 3
	}
}
