package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type SubjectType string

const (
	SubjectUser    SubjectType = "user"
	SubjectService SubjectType = "service"
	SubjectGroup   SubjectType = "group"
)

type Subject struct {
	ID       string      `json:"id"`
	Type     SubjectType `json:"type"`
	Members  []string    `json:"members,omitempty"`
	Priority int         `json:"priority"`
}

type ACE struct {
	SubjectID string   `json:"subject_id"`
	Effect    string   `json:"effect"`
	Rights    []string `json:"rights"`
}

type ManagedPath struct {
	Path    string `json:"path"`
	Kind    string `json:"kind"`
	Inherit bool   `json:"inherit"`
	ACL     []ACE  `json:"acl"`
}

type Policy struct {
	Version     int64         `json:"version"`
	GeneratedAt string        `json:"generated_at"`
	Digest      string        `json:"digest"`
	Subjects    []Subject     `json:"subjects"`
	Paths       []ManagedPath `json:"paths"`
}

type SubjectChange struct {
	Op        string   `json:"op"`
	Subject   *Subject `json:"subject,omitempty"`
	SubjectID string   `json:"subject_id,omitempty"`
}

type PathChange struct {
	Op     string       `json:"op"`
	Path   *ManagedPath `json:"path,omitempty"`
	PathID string       `json:"path_id,omitempty"`
}

type Increment struct {
	BaseVersion    int64           `json:"base_version"`
	Version        int64           `json:"version"`
	GeneratedAt    string          `json:"generated_at"`
	Digest         string          `json:"digest"`
	SubjectChanges []SubjectChange `json:"subject_changes,omitempty"`
	PathChanges    []PathChange    `json:"path_changes,omitempty"`
}

type Rights struct {
	Read  bool
	Write bool
}

func (r Rights) Equal(o Rights) bool { return r.Read == o.Read && r.Write == o.Write }
func (r Rights) Strings() []string {
	out := make([]string, 0, 2)
	if r.Read {
		out = append(out, "read")
	}
	if r.Write {
		out = append(out, "write")
	}
	return out
}
func (r Rights) IsZero() bool { return !r.Read && !r.Write }

type ACEMatch struct {
	SourcePath string   `json:"source_path"`
	SubjectID  string   `json:"subject_id"`
	Effect     string   `json:"effect"`
	Inherited  bool     `json:"inherited"`
	Rights     []string `json:"rights"`
}

type EffectiveView struct {
	SubjectID  string     `json:"subject_id"`
	Path       string     `json:"path"`
	Version    int64      `json:"version"`
	Rights     Rights     `json:"rights"`
	Principals []string   `json:"principals"`
	Chain      []string   `json:"chain"`
	Matches    []ACEMatch `json:"matches"`
	Digest     string     `json:"digest"`
}

type AccessBits struct {
	Read  bool
	Write bool
}

type PathMaterialization struct {
	Path           string                `json:"path"`
	Kind           string                `json:"kind"`
	Expected       map[string]AccessBits `json:"expected"`
	ReadbackDigest string                `json:"readback_digest"`
}

type ApplyReport struct {
	Version        int64                 `json:"version"`
	TreePath       string                `json:"tree_path"`
	Paths          []PathMaterialization `json:"paths"`
	ReadbackDigest string                `json:"readback_digest"`
	Platform       string                `json:"platform"`
}

type OpenResult struct {
	SubjectID    string `json:"subject_id"`
	Path         string `json:"path"`
	OpenMode     string `json:"open_mode"`
	Requested    string `json:"requested"`
	Actual       string `json:"actual"`
	KernelDenied bool   `json:"kernel_denied"`
	Expected     string `json:"expected"`
	Match        bool   `json:"match"`
	Identity     string `json:"identity"`
	Engine       string `json:"engine"`
	Error        string `json:"error,omitempty"`
}

type SkippedTask struct {
	SubjectID string `json:"subject_id"`
	Path      string `json:"path"`
	Priority  int    `json:"priority"`
	Reason    string `json:"reason"`
}

type VerificationReport struct {
	RunID          string        `json:"run_id"`
	Version        int64         `json:"version"`
	BudgetMS       int64         `json:"budget_ms"`
	MaxConcurrency int           `json:"max_concurrency"`
	QueueSize      int           `json:"queue_size"`
	Results        []OpenResult  `json:"results"`
	Skipped        []SkippedTask `json:"skipped"`
	Degraded       bool          `json:"degraded"`
	Mismatches     int           `json:"mismatches"`
	Status         string        `json:"status"`
}

type CascadeReport struct {
	FromVersion      int64    `json:"from_version"`
	ToVersion        int64    `json:"to_version"`
	AffectedSubjects []string `json:"affected_subjects"`
	AffectedPaths    []string `json:"affected_paths"`
	ViewsInvalidated int      `json:"views_invalidated"`
	ViewsRebuilt     int      `json:"views_rebuilt"`
	Transactional    bool     `json:"transactional"`
	ViewDigest       string   `json:"view_digest"`
}

type VerifyTask struct {
	SubjectID string
	Path      string
	Mode      string
	Priority  int
}

type VerificationOptions struct {
	MaxConcurrency int
	QueueSize      int
	Budget         time.Duration
	SubjectIDs     []string
	Paths          []string
}

func SubjectMap(p *Policy) map[string]Subject {
	m := make(map[string]Subject, len(p.Subjects))
	for _, s := range p.Subjects {
		m[s.ID] = s
	}
	return m
}

func PathMap(p *Policy) map[string]ManagedPath {
	m := make(map[string]ManagedPath, len(p.Paths))
	for _, x := range p.Paths {
		m[x.Path] = x
	}
	return m
}

func ClonePolicy(p *Policy) *Policy {
	if p == nil {
		return nil
	}
	b, _ := json.Marshal(p)
	var out Policy
	_ = json.Unmarshal(b, &out)
	return &out
}

func CloneIncrement(inc *Increment) *Increment {
	if inc == nil {
		return nil
	}
	b, _ := json.Marshal(inc)
	var out Increment
	_ = json.Unmarshal(b, &out)
	return &out
}

func normalizeTime(s string) (string, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return "", fmt.Errorf("generated_at must be RFC3339: %w", err)
	}
	return t.UTC().Format(time.RFC3339Nano), nil
}

func hashJSON(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func canonicalRights(rights []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, 2)
	for _, r := range rights {
		if !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	sort.Strings(out)
	return out
}

func canonicalPolicy(p *Policy) {
	for i := range p.Paths {
		p.Paths[i].Path = normalizeManagedPath(p.Paths[i].Path)
	}
	sort.Slice(p.Subjects, func(i, j int) bool { return p.Subjects[i].ID < p.Subjects[j].ID })
	for i := range p.Subjects {
		sort.Strings(p.Subjects[i].Members)
		p.Subjects[i].Members = uniqueStrings(p.Subjects[i].Members)
	}
	sort.Slice(p.Paths, func(i, j int) bool { return p.Paths[i].Path < p.Paths[j].Path })
	for i := range p.Paths {
		sort.Slice(p.Paths[i].ACL, func(a, b int) bool {
			x, y := p.Paths[i].ACL[a], p.Paths[i].ACL[b]
			if x.SubjectID != y.SubjectID {
				return x.SubjectID < y.SubjectID
			}
			if x.Effect != y.Effect {
				return x.Effect < y.Effect
			}
			return strings.Join(canonicalRights(x.Rights), ",") < strings.Join(canonicalRights(y.Rights), ",")
		})
		for j := range p.Paths[i].ACL {
			p.Paths[i].ACL[j].Rights = canonicalRights(p.Paths[i].ACL[j].Rights)
		}
	}
}

func uniqueStrings(in []string) []string {
	if len(in) == 0 {
		return in
	}
	out := in[:1]
	for _, s := range in[1:] {
		if s != out[len(out)-1] {
			out = append(out, s)
		}
	}
	return out
}

type snapshotDigestInput struct {
	Version     int64         `json:"version"`
	GeneratedAt string        `json:"generated_at"`
	Subjects    []Subject     `json:"subjects"`
	Paths       []ManagedPath `json:"paths"`
}

func SnapshotDigest(p *Policy) string {
	q := ClonePolicy(p)
	q.Digest = ""
	canonicalPolicy(q)
	return hashJSON(snapshotDigestInput{Version: q.Version, GeneratedAt: q.GeneratedAt, Subjects: q.Subjects, Paths: q.Paths})
}

type incrementDigestInput struct {
	BaseVersion    int64           `json:"base_version"`
	Version        int64           `json:"version"`
	GeneratedAt    string          `json:"generated_at"`
	SubjectChanges []SubjectChange `json:"subject_changes,omitempty"`
	PathChanges    []PathChange    `json:"path_changes,omitempty"`
}

func IncrementDigest(inc *Increment) string {
	q := *inc
	q.Digest = ""
	q.SubjectChanges = append([]SubjectChange(nil), inc.SubjectChanges...)
	q.PathChanges = append([]PathChange(nil), inc.PathChanges...)
	for i := range q.PathChanges {
		q.PathChanges[i].PathID = normalizeManagedPath(q.PathChanges[i].PathID)
		if q.PathChanges[i].Path != nil {
			p := *q.PathChanges[i].Path
			p.Path = normalizeManagedPath(p.Path)
			q.PathChanges[i].Path = &p
		}
	}
	sort.Slice(q.SubjectChanges, func(i, j int) bool {
		a, b := changeSubjectID(q.SubjectChanges[i]), changeSubjectID(q.SubjectChanges[j])
		if a != b {
			return a < b
		}
		return q.SubjectChanges[i].Op < q.SubjectChanges[j].Op
	})
	sort.Slice(q.PathChanges, func(i, j int) bool {
		a, b := changePathID(q.PathChanges[i]), changePathID(q.PathChanges[j])
		if a != b {
			return a < b
		}
		return q.PathChanges[i].Op < q.PathChanges[j].Op
	})
	for i := range q.SubjectChanges {
		if q.SubjectChanges[i].Subject != nil {
			s := *q.SubjectChanges[i].Subject
			s.Members = append([]string(nil), s.Members...)
			sort.Strings(s.Members)
			q.SubjectChanges[i].Subject = &s
		}
	}
	for i := range q.PathChanges {
		if q.PathChanges[i].Path != nil {
			p := *q.PathChanges[i].Path
			p.ACL = append([]ACE(nil), p.ACL...)
			for j := range p.ACL {
				p.ACL[j].Rights = canonicalRights(p.ACL[j].Rights)
			}
			sort.Slice(p.ACL, func(a, b int) bool {
				if p.ACL[a].SubjectID != p.ACL[b].SubjectID {
					return p.ACL[a].SubjectID < p.ACL[b].SubjectID
				}
				if p.ACL[a].Effect != p.ACL[b].Effect {
					return p.ACL[a].Effect < p.ACL[b].Effect
				}
				return strings.Join(p.ACL[a].Rights, ",") < strings.Join(p.ACL[b].Rights, ",")
			})
			q.PathChanges[i].Path = &p
		}
	}
	return hashJSON(incrementDigestInput{
		BaseVersion: q.BaseVersion, Version: q.Version, GeneratedAt: q.GeneratedAt,
		SubjectChanges: q.SubjectChanges, PathChanges: q.PathChanges,
	})
}

func changeSubjectID(c SubjectChange) string {
	if c.Subject != nil {
		return c.Subject.ID
	}
	return c.SubjectID
}

func changePathID(c PathChange) string {
	if c.Path != nil {
		return normalizeManagedPath(c.Path.Path)
	}
	return normalizeManagedPath(c.PathID)
}

func normalizeManagedPath(value string) string {
	return strings.ReplaceAll(value, `\`, "/")
}
