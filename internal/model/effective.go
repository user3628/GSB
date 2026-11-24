package model

import (
	"sort"
	"strings"
)

type viewDigestInput struct {
	SubjectID  string     `json:"subject_id"`
	Path       string     `json:"path"`
	Version    int64      `json:"version"`
	Read       bool       `json:"read"`
	Write      bool       `json:"write"`
	Principals []string   `json:"principals"`
	Chain      []string   `json:"chain"`
	Matches    []ACEMatch `json:"matches"`
}

func ComputeViews(p *Policy) []EffectiveView {
	paths := PathMap(p)
	containers := groupContainers(p.Subjects)
	subjectIDs := sortedSubjectIDs(p.Subjects)
	pathIDs := sortedPathIDs(p.Paths)
	views := make([]EffectiveView, 0, len(subjectIDs)*len(pathIDs))
	for _, subjectID := range subjectIDs {
		principals := principalClosure(subjectID, containers)
		for _, pathID := range pathIDs {
			target := paths[pathID]
			chain := managedChain(pathID, paths)
			var matches []ACEMatch
			for index, chainPath := range chain {
				direct := index == len(chain)-1
				if !direct && !inheritedTo(target, chain[index+1:], paths) {
					continue
				}
				for _, ace := range paths[chainPath].ACL {
					if !containsString(principals, ace.SubjectID) {
						continue
					}
					matches = append(matches, ACEMatch{
						SourcePath: chainPath,
						SubjectID:  ace.SubjectID,
						Effect:     ace.Effect,
						Inherited:  !direct,
						Rights:     append([]string(nil), ace.Rights...),
					})
				}
			}
			rights := decideRights(matches)
			view := EffectiveView{
				SubjectID: subjectID, Path: pathID, Version: p.Version, Rights: rights,
				Principals: principals, Chain: chain, Matches: matches,
			}
			view.Digest = hashJSON(viewDigestInput{
				SubjectID: subjectID, Path: pathID, Version: p.Version,
				Read: rights.Read, Write: rights.Write,
				Principals: principals, Chain: chain, Matches: matches,
			})
			views = append(views, view)
		}
	}
	return views
}

func groupContainers(subjects []Subject) map[string][]string {
	out := map[string][]string{}
	for _, s := range subjects {
		if s.Type != SubjectGroup {
			continue
		}
		for _, member := range s.Members {
			out[member] = append(out[member], s.ID)
		}
	}
	for key := range out {
		sort.Strings(out[key])
		out[key] = uniqueStrings(out[key])
	}
	return out
}

func principalClosure(id string, containers map[string][]string) []string {
	seen := map[string]bool{id: true}
	queue := []string{id}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, parent := range containers[cur] {
			if !seen[parent] {
				seen[parent] = true
				queue = append(queue, parent)
			}
		}
	}
	out := make([]string, 0, len(seen))
	for principal := range seen {
		out = append(out, principal)
	}
	sort.Strings(out)
	return out
}

func managedChain(pathID string, paths map[string]ManagedPath) []string {
	var reversed []string
	cur := pathID
	for cur != "" {
		if _, ok := paths[cur]; ok {
			reversed = append(reversed, cur)
		}
		cur = parentPath(cur)
	}
	chain := make([]string, len(reversed))
	for i := range reversed {
		chain[len(reversed)-1-i] = reversed[i]
	}
	return chain
}

func inheritedTo(target ManagedPath, descendants []string, paths map[string]ManagedPath) bool {
	if !target.Inherit {
		return false
	}
	for _, id := range descendants {
		p, ok := paths[id]
		if !ok {
			continue
		}
		if !p.Inherit {
			return false
		}
	}
	return true
}

func decideRights(matches []ACEMatch) Rights {
	var allowed, denied Rights
	for _, match := range matches {
		grantRead := containsString(match.Rights, "read")
		grantWrite := containsString(match.Rights, "write")
		if match.Effect == "deny" {
			denied.Read = denied.Read || grantRead
			denied.Write = denied.Write || grantWrite
		} else if match.Effect == "allow" {
			allowed.Read = allowed.Read || grantRead
			allowed.Write = allowed.Write || grantWrite
		}
	}
	return Rights{Read: allowed.Read && !denied.Read, Write: allowed.Write && !denied.Write}
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func sortedSubjectIDs(subjects []Subject) []string {
	out := make([]string, 0, len(subjects))
	for _, s := range subjects {
		out = append(out, s.ID)
	}
	sort.Strings(out)
	return out
}

func sortedPathIDs(paths []ManagedPath) []string {
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		out = append(out, p.Path)
	}
	sort.Strings(out)
	return out
}

func ViewDigest(views []EffectiveView) string {
	copied := append([]EffectiveView(nil), views...)
	sort.Slice(copied, func(i, j int) bool {
		if copied[i].SubjectID != copied[j].SubjectID {
			return copied[i].SubjectID < copied[j].SubjectID
		}
		return copied[i].Path < copied[j].Path
	})
	digests := make([]string, len(copied))
	for i := range copied {
		digests[i] = copied[i].Digest
	}
	return hashJSON(digests)
}

func ExpectedAccess(p *Policy, views []EffectiveView) map[string]map[string]AccessBits {
	out := make(map[string]map[string]AccessBits, len(p.Paths))
	for _, pth := range p.Paths {
		out[pth.Path] = map[string]AccessBits{}
	}
	for _, view := range views {
		out[view.Path][view.SubjectID] = AccessBits{Read: view.Rights.Read, Write: view.Rights.Write}
	}
	return out
}

func CascadeDiff(oldPolicy, newPolicy *Policy, oldViews, newViews []EffectiveView) CascadeReport {
	report := CascadeReport{ToVersion: newPolicy.Version, Transactional: true, ViewDigest: ViewDigest(newViews)}
	if oldPolicy != nil {
		report.FromVersion = oldPolicy.Version
	}
	oldByKey := viewMap(oldViews)
	newByKey := viewMap(newViews)
	subjects := map[string]bool{}
	paths := map[string]bool{}
	for key, oldView := range oldByKey {
		newView, ok := newByKey[key]
		if !ok || semanticViewDiff(oldView, newView) {
			parts := strings.SplitN(key, "\x00", 2)
			if len(parts) == 2 {
				subjects[parts[0]] = true
				paths[parts[1]] = true
			}
		}
	}
	for key := range newByKey {
		if _, ok := oldByKey[key]; !ok {
			parts := strings.SplitN(key, "\x00", 2)
			if len(parts) == 2 {
				subjects[parts[0]] = true
				paths[parts[1]] = true
			}
		}
	}
	report.AffectedSubjects = sortedKeys(subjects)
	report.AffectedPaths = sortedKeys(paths)
	report.ViewsInvalidated = len(newViews)
	report.ViewsRebuilt = len(newViews)
	return report
}

func viewMap(views []EffectiveView) map[string]EffectiveView {
	out := make(map[string]EffectiveView, len(views))
	for _, view := range views {
		out[view.SubjectID+"\x00"+view.Path] = view
	}
	return out
}

func semanticViewDiff(a, b EffectiveView) bool {
	if !a.Rights.Equal(b.Rights) || strings.Join(a.Principals, "\x00") != strings.Join(b.Principals, "\x00") || strings.Join(a.Chain, "\x00") != strings.Join(b.Chain, "\x00") || len(a.Matches) != len(b.Matches) {
		return true
	}
	for i := range a.Matches {
		x, y := a.Matches[i], b.Matches[i]
		if x.SourcePath != y.SourcePath || x.SubjectID != y.SubjectID || x.Effect != y.Effect || x.Inherited != y.Inherited || strings.Join(x.Rights, "\x00") != strings.Join(y.Rights, "\x00") {
			return true
		}
	}
	return false
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
