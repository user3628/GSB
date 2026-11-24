// Package resolver implements component-by-component path resolution on the
// real file system. Every component is inspected through the reparse
// inspector, reparse jumps are recorded as an auditable hop chain, and the
// final target is adjudicated against a restricted root using deterministic
// Windows-style normalization.
package resolver

import (
	"strings"

	"pathcourt/internal/bizcode"
	"pathcourt/internal/pathnorm"
	"pathcourt/internal/reparse"
)

// DefaultMaxJumps is the cap on reparse point jumps per resolved path.
const DefaultMaxJumps = 16

// Hop records one reparse point traversal.
type Hop struct {
	Seq            int          `json:"seq"`
	LinkPath       string       `json:"link_path"`
	Kind           reparse.Kind `json:"kind"`
	RawTarget      string       `json:"raw_target"`
	ResolvedTarget string       `json:"resolved_target"`
	Relative       bool         `json:"relative"`
	Emulated       bool         `json:"emulated"`
}

// Step records one component traversal of the input path.
type Step struct {
	Index     int          `json:"index"`
	Component string       `json:"component"`
	Path      string       `json:"path"`
	Kind      reparse.Kind `json:"kind"`
	Exists    bool         `json:"exists"`
	IsDir     bool         `json:"is_dir"`
}

// EscapeInfo records the first component that carried resolution outside
// the restricted root.
type EscapeInfo struct {
	Component int    `json:"component"`
	Path      string `json:"path"`
	ViaHop    int    `json:"via_hop"`
}

// CycleInfo identifies the repeated target that proves a link cycle.
type CycleInfo struct {
	Target       string `json:"target"`
	FirstHopSeq  int    `json:"first_hop_seq"`
	RepeatHopSeq int    `json:"repeat_hop_seq"`
}

// Result is the deterministic verdict for one input path.
type Result struct {
	ManifestID string       `json:"manifest_id"`
	PathIndex  int          `json:"path_index"`
	Input      string       `json:"input"`
	Normalized string       `json:"normalized"`
	Root       string       `json:"root"`
	FinalPath  string       `json:"final_path"`
	Code       bizcode.Code `json:"code"`
	Steps      []Step       `json:"steps"`
	Hops       []Hop        `json:"hops"`
	FirstEscape *EscapeInfo `json:"first_escape,omitempty"`
	Cycle      *CycleInfo   `json:"cycle,omitempty"`
	DepthCutoff int         `json:"depth_cutoff,omitempty"`
	FailIndex  int          `json:"fail_index,omitempty"`
	FailPath   string       `json:"fail_path,omitempty"`
}

// Config configures a Resolver.
type Config struct {
	Root     string
	MaxJumps int
}

// Resolver is safe for concurrent use: it holds no per-resolution state.
type Resolver struct {
	root     string
	maxJumps int
	inspect  reparse.Inspector
}

// New builds a Resolver rooted at the restricted root.
func New(cfg Config, inspector reparse.Inspector) *Resolver {
	max := cfg.MaxJumps
	if max <= 0 {
		max = DefaultMaxJumps
	}
	return &Resolver{
		root:     pathnorm.Normalize("", cfg.Root),
		maxJumps: max,
		inspect:  inspector,
	}
}

// Root returns the normalized restricted root.
func (r *Resolver) Root() string { return r.root }

// Resolve walks the input path one component at a time and returns a fully
// traced verdict. The result is a pure function of the file-system state.
func (r *Resolver) Resolve(manifestID string, pathIndex int, input string) (*Result, error) {
	res := &Result{
		ManifestID: manifestID,
		PathIndex:  pathIndex,
		Input:      input,
		Root:       r.root,
		Steps:      []Step{},
		Hops:       []Hop{},
	}

	anchor, comps := r.anchorAndComponents(input)
	cur := anchor
	seen := map[string]int{}

	// A reparse point located directly at the restricted root is resolved
	// before any input component is consumed.
	verdict, done, err := r.followReparse(res, &cur, seen, 0)
	if err != nil {
		return nil, err
	}
	if done {
		return verdict, nil
	}

	for len(comps) > 0 {
		raw := comps[0]
		comps = comps[1:]
		idx := len(res.Steps) + 1

		switch raw {
		case ".":
			res.Steps = append(res.Steps, Step{Index: idx, Component: raw, Path: cur, Kind: reparse.KindNone, Exists: true, IsDir: true})
			continue
		case "..":
			cur = parentPath(cur)
			res.Steps = append(res.Steps, Step{Index: idx, Component: raw, Path: cur, Kind: reparse.KindNone, Exists: true, IsDir: true})
			r.markEscape(res, idx, cur, 0)
			continue
		}

		next := pathnorm.Normalize(cur, raw)
		info, err := r.inspect.Inspect(reparse.FSPath(next))
		if err != nil {
			return nil, err
		}
		step := Step{Index: idx, Component: raw, Path: next, Exists: info.Exists, IsDir: info.IsDir, Kind: info.Kind}
		res.Steps = append(res.Steps, step)

		if !info.Exists {
			r.finishMissing(res, idx, next)
			return res, nil
		}
		if info.Kind == reparse.KindNone && !info.IsDir {
			if len(comps) > 0 {
				r.finish(res, bizcode.E_NOT_DIRECTORY, idx, next)
				return res, nil
			}
			cur = next
			break
		}

		cur = next
		r.markEscape(res, idx, cur, 0)

		// Resolve the reparse point (and any chained targets) before
		// consuming the next input component.
		verdict, done, err := r.followReparse(res, &cur, seen, idx)
		if err != nil {
			return nil, err
		}
		if done {
			return verdict, nil
		}
	}

	finalInfo, err := r.inspect.Inspect(reparse.FSPath(cur))
	if err != nil {
		return nil, err
	}
	res.FinalPath = cur
	res.Normalized = cur
	switch {
	case !finalInfo.Exists:
		if pathnorm.Within(r.root, cur) {
			r.finish(res, bizcode.E_TARGET_MISSING, len(res.Steps), cur)
		} else {
			r.finish(res, bizcode.E_ESCAPE_DANGLING, len(res.Steps), cur)
		}
	case !pathnorm.Within(r.root, cur):
		r.finish(res, bizcode.E_PATH_ESCAPE, len(res.Steps), cur)
	default:
		res.Code = bizcode.OK
	}
	return res, nil
}

// followReparse keeps resolving reparse points at cur until a plain entry,
// a missing target, a cycle or the depth cap is reached. On done==true the
// returned verdict is terminal; otherwise cur has been rewritten in place.
func (r *Resolver) followReparse(res *Result, cur *string, seen map[string]int, componentIdx int) (*Result, bool, error) {
	for {
		info, err := r.inspect.Inspect(reparse.FSPath(*cur))
		if err != nil {
			return nil, false, err
		}
		if !info.Exists || info.Kind == reparse.KindNone {
			return nil, false, nil
		}

		targetNorm := r.resolveTarget(*cur, info.Target)
		hop := Hop{
			Seq:            len(res.Hops) + 1,
			LinkPath:       *cur,
			Kind:           info.Kind,
			RawTarget:      info.Target,
			ResolvedTarget: targetNorm,
			Relative:       !pathnorm.IsAbsolute(info.Target),
			Emulated:       info.Emulated,
		}
		res.Hops = append(res.Hops, hop)

		key := pathnorm.Key(targetNorm)
		if first, ok := seen[key]; ok {
			res.Code = bizcode.E_LINK_CYCLE
			res.Cycle = &CycleInfo{Target: targetNorm, FirstHopSeq: first, RepeatHopSeq: hop.Seq}
			r.finish(res, bizcode.E_LINK_CYCLE, componentIdx, targetNorm)
			return res, true, nil
		}
		seen[key] = hop.Seq

		if hop.Seq > r.maxJumps {
			r.finish(res, bizcode.E_LINK_DEPTH, componentIdx, targetNorm)
			res.DepthCutoff = r.maxJumps
			return res, true, nil
		}

		*cur = targetNorm
		r.markEscape(res, componentIdx, *cur, hop.Seq)
	}
}

func (r *Resolver) markEscape(res *Result, component int, path string, viaHop int) {
	if res.FirstEscape == nil && !pathnorm.Within(r.root, path) {
		res.FirstEscape = &EscapeInfo{Component: component, Path: path, ViaHop: viaHop}
	}
}

func (r *Resolver) finishMissing(res *Result, idx int, path string) {
	if pathnorm.Within(r.root, path) {
		r.finish(res, bizcode.E_TARGET_MISSING, idx, path)
	} else {
		r.finish(res, bizcode.E_ESCAPE_DANGLING, idx, path)
	}
}

func (r *Resolver) finish(res *Result, code bizcode.Code, idx int, path string) {
	res.Code = code
	res.FailIndex = idx
	res.FailPath = path
	res.FinalPath = path
	res.Normalized = path
}

// anchorAndComponents splits an input into its absolute anchor and the
// traversable components. Relative inputs anchor at the restricted root.
func (r *Resolver) anchorAndComponents(input string) (string, []string) {
	cleaned := strings.ReplaceAll(input, "/", `\`)
	if !pathnorm.IsAbsolute(cleaned) {
		return r.root, pathnorm.Split(cleaned)
	}
	comps := pathnorm.Split(cleaned)
	switch {
	case len(comps) > 0 && comps[0] == `\\` && pathnorm.LeadingUNC(cleaned):
		n := 3
		if len(comps) < n {
			n = len(comps)
		}
		anchor := `\\` + strings.Join(comps[1:n], `\`)
		return anchor, comps[n:]
	case len(comps) > 1 && len(comps[0]) == 2 && comps[0][1] == ':':
		return comps[0] + `\`, comps[1:]
	default:
		return `\`, comps
	}
}

// resolveTarget normalizes a link target. Relative targets are interpreted
// against the directory containing the link.
func (r *Resolver) resolveTarget(linkPath, rawTarget string) string {
	return pathnorm.Normalize(dirOf(linkPath), rawTarget)
}

// dirOf returns the parent directory of a Windows-normalized path.
func dirOf(p string) string {
	idx := strings.LastIndex(p, `\`)
	if idx <= 0 {
		return p
	}
	parent := p[:idx]
	if len(parent) == 2 && parent[1] == ':' {
		return parent + `\`
	}
	return parent
}

func parentPath(p string) string {
	normalized := pathnorm.Normalize("", p)
	idx := strings.LastIndex(normalized, `\`)
	if idx <= 0 {
		return normalized
	}
	parent := normalized[:idx]
	if len(parent) == 2 && parent[1] == ':' {
		return parent + `\`
	}
	return parent
}
