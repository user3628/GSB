package pipeline

import (
	"bytes"
	"os"
	"path/filepath"

	"matbench/internal/control"
	"matbench/internal/errorsx"
	"matbench/internal/fp"
	"matbench/internal/noise"
	"matbench/internal/palette"
	"matbench/internal/recipe"
)

// StopChecker is polled between indivisible material units.
type StopChecker interface {
	StopRequested() bool
}

// RunOptions controls an execution.
type RunOptions struct {
	Workers  int
	OnlyIDs  []string
	Stop     StopChecker
	ForceAll bool
	// BaselineInputs carries the pre-change stage input fingerprints per
	// material during an incremental run. A stage whose current input differs
	// from its baseline must be rebuilt even if the checkpoint was written by a
	// previous (already patched) execution.
	BaselineInputs map[string]map[string]string
	// ForceFromStage forces a stage and every downstream stage to rebuild,
	// modeling invalidation that propagates through upstream artifacts even
	// when a stage's own declared input fingerprint is unchanged.
	ForceFromStage map[string]string
}

// RunSummary is the deterministic digest of a complete run.
type RunSummary struct {
	MaterialCount int               `json:"material_count"`
	ReportDigest  string            `json:"report_digest"`
	TextureDigest map[string]string `json:"texture_digest"`
	Stopped       bool              `json:"stopped"`
}

func (p *Pipeline) wanted(book *recipe.Book, opts RunOptions) []recipe.Recipe {
	if len(opts.OnlyIDs) == 0 {
		return book.Recipes
	}
	allow := map[string]bool{}
	for _, id := range opts.OnlyIDs {
		allow[id] = true
	}
	var out []recipe.Recipe
	for _, r := range book.Recipes {
		if allow[r.MaterialID] {
			out = append(out, r)
		}
	}
	return out
}

// RunFull executes all four stages per material. Completed checkpoints are
// reused on recovery so a resumed run is byte identical to one continuous run.
func (p *Pipeline) RunFull(book *recipe.Book, opts RunOptions) (*RunSummary, error) {
	if err := p.Store.Lock(); err != nil {
		return nil, err
	}
	defer p.Store.Unlock()

	states := map[string]*MaterialState{}
	stopped := false
	for _, r := range p.wanted(book, opts) {
		if opts.Stop != nil && opts.Stop.StopRequested() {
			stopped = true
			break
		}
		st := &MaterialState{Recipe: r}
		inputFPs, err := StageInputs(book, r)
		if err != nil {
			return nil, err
		}
		for _, stage := range Stages {
			if !opts.ForceAll {
				baseline := ""
				if opts.BaselineInputs != nil {
					if m, ok := opts.BaselineInputs[r.MaterialID]; ok {
						baseline = m[stage]
					}
				}
				changedByPatch := baseline != "" && baseline != inputFPs[stage]
				if from, ok := opts.ForceFromStage[r.MaterialID]; ok &&
					stageIndex(stage) >= stageIndex(from) {
					changedByPatch = true
				}
				if reused, err := p.tryReuse(st, stage, inputFPs[stage], changedByPatch); err != nil {
					return nil, err
				} else if reused {
					continue
				}
			}
			if err := p.RunStage(book, st, stage, opts.Workers); err != nil {
				return nil, err
			}
			if err := p.Checkpoint(st, stage, inputFPs[stage]); err != nil {
				return nil, err
			}
		}
		states[r.MaterialID] = st
	}
	sum, err := p.summarize(book, states, stopped)
	if err != nil {
		return nil, err
	}
	return sum, nil
}

// tryReuse restores an upstream stage artifact when its input fingerprint and
// the stored record match. Returns false when rebuild is required.
func (p *Pipeline) tryReuse(st *MaterialState, stage, inputFP string, forceRebuild bool) (bool, error) {
	cur, err := p.Store.Active(st.Recipe.MaterialID, stage)
	if err != nil {
		return false, err
	}
	if forceRebuild || cur == nil || cur.RecipeFP != inputFP {
		return false, nil
	}
	switch stage {
	case StageControl:
		var v control.Result
		if err := decodeCheckpoint(cur.Payload, &v); err != nil {
			return false, err
		}
		st.Control = &v
	case StageNoise:
		var v noise.Result
		if err := decodeCheckpoint(cur.Payload, &v); err != nil {
			return false, err
		}
		st.Noise = &v
	case StagePalette:
		var v palette.Result
		if err := decodeCheckpoint(cur.Payload, &v); err != nil {
			return false, err
		}
		st.Palette = &v
	case StageReport:
		var v Report
		if err := decodeCheckpoint(cur.Payload, &v); err != nil {
			return false, err
		}
		st.Report = &v
	}
	return true, nil
}

// RunIncremental applies patches, invalidates only the earliest affected stage
// and downstream stages, then verifies byte equality with a from-scratch run.
func (p *Pipeline) RunIncremental(base *recipe.Book, patches []recipe.Patch, opts RunOptions) (*RunSummary, error) {
	nextBook := base.Clone()
	earliest := map[string]string{}
	for _, patch := range patches {
		for field := range patchFields(patch) {
			stage := EarliestStage(field)
			if cur, ok := earliest[patch.MaterialID]; !ok || stageIndex(stage) < stageIndex(cur) {
				earliest[patch.MaterialID] = stage
			}
		}
		if _, err := nextBook.ApplyPatch(patch); err != nil {
			return nil, errorsx.New(errorsx.EInvalidInput, patch.MaterialID, err.Error())
		}
	}

	ids := make([]string, 0, len(earliest))
	for id := range earliest {
		ids = append(ids, id)
	}
	opts.OnlyIDs = ids
	opts.ForceAll = false
	baselines := map[string]map[string]string{}

	// Invalidation trail + precise stage rebuilding.
	for _, id := range ids {
		from := earliest[id]
		oldR := base.Find(id)
		newR := nextBook.Find(id)
		oldFPs, err := StageInputs(base, *oldR)
		if err != nil {
			return nil, err
		}
		newFPs, err := StageInputs(nextBook, *newR)
		if err != nil {
			return nil, err
		}
		baselines[id] = oldFPs
		for _, stage := range Stages {
			if stageIndex(stage) < stageIndex(from) {
				continue
			}
			reason := "invalidation from " + from
			if oldFPs[stage] == newFPs[stage] {
				reason += " (downstream propagation)"
			}
			if err := p.Store.LogInvalidation(id, stage, reason, oldFPs[stage], newFPs[stage]); err != nil {
				return nil, err
			}
		}
	}
	opts.BaselineInputs = baselines
	opts.ForceFromStage = earliest

	incSummary, err := p.RunFull(nextBook, opts)
	if err != nil {
		return nil, err
	}

	// Reference from-scratch execution in an isolated workspace, then compare
	// every stage payload and texture byte-for-byte.
	if mismatch, err := p.verifyAgainstScratch(nextBook, ids, opts); err != nil {
		return nil, err
	} else if mismatch != nil {
		return nil, errorsx.New(errorsx.EIncrementalMismatch, mismatch.materialID,
			"first difference at stage "+mismatch.stage)
	}
	return incSummary, nil
}

type mismatch struct {
	materialID string
	stage      string
}

func (p *Pipeline) verifyAgainstScratch(book *recipe.Book, ids []string, opts RunOptions) (*mismatch, error) {
	scratch, err := os.MkdirTemp(p.Dir, "scratch-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(scratch)
	if err := copyCards(p.Dir, scratch); err != nil {
		return nil, err
	}
	s, err := openScratch(scratch)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	ref := &Pipeline{Dir: scratch, Store: s}
	refOpts := RunOptions{Workers: opts.Workers, OnlyIDs: ids, ForceAll: true}
	if _, err := ref.RunFull(book, refOpts); err != nil {
		return nil, err
	}
	for _, id := range ids {
		for _, stage := range Stages {
			a, err := p.Store.Active(id, stage)
			if err != nil {
				return nil, err
			}
			b, err := s.Active(id, stage)
			if err != nil {
				return nil, err
			}
			if a == nil || b == nil || !bytes.Equal(a.Payload, b.Payload) ||
				a.ArtifactFP != b.ArtifactFP {
				return &mismatch{materialID: id, stage: stage}, nil
			}
		}
		if mm, err := compareTextures(p.Dir, scratch, id); err != nil {
			return nil, err
		} else if mm {
			return &mismatch{materialID: id, stage: StagePalette}, nil
		}
	}
	return nil, nil
}

func compareTextures(aDir, bDir, id string) (bool, error) {
	a, err := os.ReadFile(filepath.Join(aDir, "artifacts", "textures", id+"_texture.png"))
	if err != nil {
		return false, err
	}
	b, err := os.ReadFile(filepath.Join(bDir, "artifacts", "textures", id+"_texture.png"))
	if err != nil {
		return false, err
	}
	return !bytes.Equal(a, b), nil
}

func copyCards(srcDir, dstDir string) error {
	return copyDir(filepath.Join(srcDir, "cards"), filepath.Join(dstDir, "cards"))
}

func (p *Pipeline) summarize(book *recipe.Book, states map[string]*MaterialState, stopped bool) (*RunSummary, error) {
	h := fp.New("run:summary")
	textures := map[string]string{}
	for _, r := range book.Recipes {
		st, ok := states[r.MaterialID]
		if !ok || st.Report == nil {
			continue
		}
		rb, err := fp.CanonicalJSON(st.Report)
		if err != nil {
			return nil, err
		}
		h.AddString(r.MaterialID).AddBytes(rb)
		tex, err := os.ReadFile(filepath.Join(p.Dir, st.Palette.TexturePath))
		if err != nil {
			return nil, err
		}
		texFP := fp.New("texture:png").AddBytes(tex).Sum()
		textures[r.MaterialID] = texFP
		h.AddString(r.MaterialID + ":tex").AddString(texFP)
	}
	return &RunSummary{
		MaterialCount: len(states),
		ReportDigest:  h.Sum(),
		TextureDigest: textures,
		Stopped:       stopped,
	}, nil
}

func stageIndex(stage string) int {
	for i, s := range Stages {
		if s == stage {
			return i
		}
	}
	return len(Stages)
}

func patchFields(p recipe.Patch) map[string]bool {
	out := map[string]bool{}
	if p.Description != nil {
		out["description"] = true
	}
	if p.NoiseOctaves != nil {
		out["noise_octaves"] = true
	}
	if p.NoiseFrequency != nil {
		out["noise_frequency"] = true
	}
	if p.NoiseAmplitudes != nil {
		out["noise_amplitudes"] = true
	}
	if p.CardName != nil {
		out["card_name"] = true
	}
	if p.TargetResolution != nil {
		out["target_resolution"] = true
	}
	if p.BaseColor != nil {
		out["base_color"] = true
	}
	if p.Roughness != nil {
		out["roughness"] = true
	}
	if p.Metallic != nil {
		out["metallic"] = true
	}
	return out
}
