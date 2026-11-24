// Package report renders deterministic JSON adjudication reports. Absolute
// workspace paths are replaced by a fixed alias so repeated runs with
// identical inputs produce byte-identical output even when the temporary
// directory name changes. Sorting is stable by manifest id and path index;
// the only time-like value is snapshot.FixedSampleMarker.
package report

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"

	"pathcourt/internal/reparse"
	"pathcourt/internal/resolver"
	"pathcourt/internal/snapshot"
)

// DefaultWorkspaceAlias replaces the generated temporary working directory.
const DefaultWorkspaceAlias = "${WORKSPACE}"

// PathReport is one input path's auditable verdict.
type PathReport struct {
	ManifestID   string             `json:"manifest_id"`
	PathIndex    int                `json:"path_index"`
	Input        string             `json:"input"`
	Normalized   string             `json:"normalized"`
	FinalPath    string             `json:"final_path"`
	Code         string             `json:"code"`
	FailIndex    int                `json:"fail_index,omitempty"`
	FailPath     string             `json:"fail_path,omitempty"`
	FirstEscape  *resolver.EscapeInfo `json:"first_escape,omitempty"`
	Cycle        *resolver.CycleInfo  `json:"cycle,omitempty"`
	DepthCutoff  int                `json:"depth_cutoff,omitempty"`
	Steps        []resolver.Step    `json:"steps"`
	Hops         []HopReport        `json:"hops"`
}

// HopReport is one reparse hop with both raw and normalized target.
type HopReport struct {
	Seq            int          `json:"seq"`
	LinkPath       string       `json:"link_path"`
	Kind           reparse.Kind `json:"kind"`
	RawTarget      string       `json:"raw_target"`
	ResolvedTarget string       `json:"resolved_target"`
	Relative       bool         `json:"relative"`
	Emulated       bool         `json:"emulated"`
}

// VersionEntry summarizes one committed snapshot.
type VersionEntry struct {
	Version     int64  `json:"version"`
	RootID      string `json:"root_id"`
	ManifestID  string `json:"manifest_id"`
	ContentHash string `json:"content_hash"`
	SampleMark  string `json:"sample_mark"`
}

// DiffEntry is the whole-replacement-aware comparison of two versions.
type DiffEntry struct {
	From       int64  `json:"from"`
	To         int64  `json:"to"`
	Kind       string `json:"kind"`
	Changed    int    `json:"changed"`
	SharedKeys int    `json:"shared_keys"`
	FromKeys   int    `json:"from_keys"`
	ToKeys     int    `json:"to_keys"`
}

// Report is the top-level deterministic export.
type Report struct {
	SampleMark      string         `json:"sample_mark"`
	WorkspaceAlias  string         `json:"workspace_alias"`
	RestrictedRoot  string         `json:"restricted_root"`
	RootID          string         `json:"root_id"`
	Paths           []PathReport   `json:"paths"`
	VersionChain    []VersionEntry `json:"version_chain"`
	VersionDiffs    []DiffEntry    `json:"version_diffs"`
	LoadStatus      string         `json:"load_status"`
	LoadMessage     string         `json:"load_message,omitempty"`
}

// Builder aliases absolute paths before serialization.
type Builder struct {
	workspace string
	alias     string
}

// NewBuilder creates a report builder for one working directory.
func NewBuilder(workspace, alias string) *Builder {
	if alias == "" {
		alias = DefaultWorkspaceAlias
	}
	return &Builder{workspace: strings.TrimRight(workspace, `/\`), alias: alias}
}

func (b *Builder) mask(p string) string {
	if p == "" || b.workspace == "" {
		return p
	}
	// Replace both separator spellings of the workspace prefix.
	for _, variant := range []string{b.workspace, strings.ReplaceAll(b.workspace, `\`, "/"), strings.ReplaceAll(b.workspace, "/", `\`)} {
		if strings.HasPrefix(p, variant) {
			return b.alias + p[len(variant):]
		}
	}
	return p
}

func (b *Builder) toPath(row snapshot.Row) PathReport {
	r := row.Result
	hops := make([]HopReport, 0, len(r.Hops))
	for _, h := range r.Hops {
		hops = append(hops, HopReport{
			Seq:            h.Seq,
			LinkPath:       b.mask(h.LinkPath),
			Kind:           h.Kind,
			RawTarget:      b.mask(h.RawTarget),
			ResolvedTarget: b.mask(h.ResolvedTarget),
			Relative:       h.Relative,
			Emulated:       h.Emulated,
		})
	}
	steps := make([]resolver.Step, len(r.Steps))
	for i, st := range r.Steps {
		st.Path = b.mask(st.Path)
		steps[i] = st
	}
	pr := PathReport{
		ManifestID:  row.ManifestID,
		PathIndex:   row.PathIndex,
		Input:       r.Input,
		Normalized:  b.mask(r.Normalized),
		FinalPath:   b.mask(r.FinalPath),
		Code:        string(r.Code),
		FailIndex:   r.FailIndex,
		FailPath:    b.mask(r.FailPath),
		FirstEscape: r.FirstEscape,
		Cycle:       r.Cycle,
		DepthCutoff: r.DepthCutoff,
		Steps:       steps,
		Hops:        hops,
	}
	if r.Cycle != nil {
		cy := *r.Cycle
		cy.Target = b.mask(cy.Target)
		pr.Cycle = &cy
	}
	if pr.FirstEscape != nil {
		cp := *pr.FirstEscape
		cp.Path = b.mask(cp.Path)
		pr.FirstEscape = &cp
	}
	return pr
}

// Build assembles and serializes a deterministic report from loaded
// snapshot versions.
func (b *Builder) Build(versions []snapshot.Version, loadErr error) ([]byte, error) {
	rep := Report{
		SampleMark:     snapshot.FixedSampleMarker,
		WorkspaceAlias: b.alias,
		LoadStatus:     "contiguous",
		Paths:          []PathReport{},
		VersionChain:   []VersionEntry{},
		VersionDiffs:   []DiffEntry{},
	}
	if loadErr != nil {
		rep.LoadStatus = "rolled-back"
		rep.LoadMessage = loadErr.Error()
	}

	var rows []snapshot.Row
	for i, v := range versions {
		if i == 0 {
			rep.RestrictedRoot = b.mask(v.RootPath)
			rep.RootID = v.RootID
		}
		rep.VersionChain = append(rep.VersionChain, VersionEntry{
			Version:     v.Version,
			RootID:      v.RootID,
			ManifestID:  v.ManifestID,
			ContentHash: v.ContentHash,
			SampleMark:  v.SampleMark,
		})
		rows = append(rows, v.Rows...)
		if i > 0 {
			d := snapshot.Compare(versions[i-1], v)
			rep.VersionDiffs = append(rep.VersionDiffs, DiffEntry{
				From:       d.From,
				To:         d.To,
				Kind:       string(d.Kind),
				Changed:    d.Changed,
				SharedKeys: d.SharedKeys,
				FromKeys:   d.FromKeys,
				ToKeys:     d.ToKeys,
			})
		}
	}

	for _, row := range rows {
		rep.Paths = append(rep.Paths, b.toPath(row))
	}
	sort.SliceStable(rep.Paths, func(i, j int) bool {
		if rep.Paths[i].ManifestID != rep.Paths[j].ManifestID {
			return rep.Paths[i].ManifestID < rep.Paths[j].ManifestID
		}
		return rep.Paths[i].PathIndex < rep.Paths[j].PathIndex
	})

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
