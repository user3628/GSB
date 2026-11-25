// Package pipeline orchestrates the four deterministic stages, dependency
// fingerprinting, precise invalidation and incremental rebuilds.
package pipeline

import (
	"encoding/json"

	"matbench/internal/control"
	"matbench/internal/errorsx"
	"matbench/internal/fp"
	"matbench/internal/noise"
	"matbench/internal/palette"
	"matbench/internal/recipe"
	"matbench/internal/store"
)

// Stage names and dependency order.
const (
	StageControl = "control"
	StageNoise   = "noise"
	StagePalette = "palette"
	StageReport  = "report"
)

// Stages in execution order.
var Stages = []string{StageControl, StageNoise, StagePalette, StageReport}

// Pipeline holds workspace paths and the checkpoint store.
type Pipeline struct {
	Dir   string
	Store *store.Store
}

// MaterialState caches one material's in-memory stage outputs.
type MaterialState struct {
	Recipe  recipe.Recipe
	Control *control.Result
	Noise   *noise.Result
	Palette *palette.Result
	Report  *Report
}

// StageInputs returns the content fingerprint of the exact inputs of each
// stage. A changed fingerprint at stage k means k is the earliest invalidated
// stage; later stages inherit invalidation through their own input hashes.
func StageInputs(b *recipe.Book, r recipe.Recipe) (map[string]string, error) {
	controlFP, err := fp.Of("input:control", map[string]any{
		"material_id": r.MaterialID,
		"description": r.Description,
	})
	if err != nil {
		return nil, err
	}
	noiseFP, err := fp.Of("input:noise", map[string]any{
		"material_id": r.MaterialID,
		"noise":       r.Noise,
	})
	if err != nil {
		return nil, err
	}
	paletteFP, err := fp.Of("input:palette", map[string]any{
		"material_id":       r.MaterialID,
		"card_name":         r.CardName,
		"target_resolution": r.TargetResolution,
		"base_color":        r.BaseColor,
		"roughness":         r.Roughness,
		"metallic":          r.Metallic,
	})
	if err != nil {
		return nil, err
	}
	reportFP, err := fp.Of("input:report", map[string]any{
		"material_id": r.MaterialID,
		"schema":      b.SchemaVersion,
	})
	if err != nil {
		return nil, err
	}
	return map[string]string{
		StageControl: controlFP,
		StageNoise:   noiseFP,
		StagePalette: paletteFP,
		StageReport:  reportFP,
	}, nil
}

// StageArtifactFP returns the output fingerprint produced for a stage.
func StageArtifactFP(st *MaterialState, stage string) string {
	switch stage {
	case StageControl:
		if st.Control != nil {
			return st.Control.Fingerprint
		}
	case StageNoise:
		if st.Noise != nil {
			return st.Noise.Fingerprint
		}
	case StagePalette:
		if st.Palette != nil {
			return st.Palette.Fingerprint
		}
	case StageReport:
		if st.Report != nil {
			return st.Report.Fingerprint
		}
	}
	return ""
}

// EarliestStage maps a changed field to its earliest affected stage.
func EarliestStage(field string) string {
	switch field {
	case "description":
		return StageControl
	case "noise_octaves", "noise_frequency", "noise_amplitudes", "noise_seed":
		return StageNoise
	case "card_name", "target_resolution", "base_color", "roughness", "metallic":
		return StagePalette
	default:
		return StageControl
	}
}

func marshalResult(v any) ([]byte, error) { return json.Marshal(v) }

// RunStage executes one stage for a material and updates the state.
func (p *Pipeline) RunStage(b *recipe.Book, st *MaterialState, stage string, workers int) error {
	r := st.Recipe
	switch stage {
	case StageControl:
		res, err := control.Generate(r.MaterialID, r.Description)
		if err != nil {
			return err
		}
		st.Control = res
	case StageNoise:
		res, err := noise.Synthesize(r, workers)
		if err != nil {
			return err
		}
		st.Noise = res
	case StagePalette:
		cardSpec := b.FindCard(r.CardName)
		if cardSpec == nil {
			return errorsx.New(errorsx.ECardDimMismatch, r.MaterialID,
				"stage palette: card declaration not found: "+r.CardName)
		}
		card, err := palette.LoadCard(p.Dir+"/cards", *cardSpec)
		if err != nil {
			return errorsx.New(errorsx.ECardDimMismatch, r.MaterialID,
				"stage palette: cannot load card: "+err.Error()).WithStage(StagePalette)
		}
		if card == nil {
			return errorsx.New(errorsx.ECardDimMismatch, r.MaterialID,
				"stage palette: card pixel size differs from declared size").WithStage(StagePalette)
		}
		res, err := palette.Quantize(r, card, *cardSpec, st.Noise, st.Control,
			p.Dir)
		if err != nil {
			return err
		}
		st.Palette = res
	case StageReport:
		rep := BuildReport(st)
		st.Report = rep
	}
	return nil
}

// Checkpoint writes one stage result transactionally with monotonic sequence.
func (p *Pipeline) Checkpoint(st *MaterialState, stage string, inputFP string) error {
	var artifactFP string
	var body any
	switch stage {
	case StageControl:
		artifactFP, body = st.Control.Fingerprint, st.Control
	case StageNoise:
		artifactFP, body = st.Noise.Fingerprint, st.Noise
	case StagePalette:
		artifactFP, body = st.Palette.Fingerprint, st.Palette
	case StageReport:
		artifactFP, body = st.Report.Fingerprint, st.Report
	}
	raw, err := marshalResult(body)
	if err != nil {
		return err
	}
	cur, err := p.Store.Active(st.Recipe.MaterialID, stage)
	if err != nil {
		return err
	}
	seq := 1
	if cur != nil {
		seq = cur.Seq + 1
	}
	outcome, err := p.Store.Publish(store.Checkpoint{
		MaterialID: st.Recipe.MaterialID,
		Stage:      stage,
		Seq:        seq,
		ConfigVer:  1,
		RecipeFP:   inputFP,
		ArtifactFP: artifactFP,
		Payload:    raw,
	})
	if err != nil {
		return err
	}
	if outcome != store.PublishAccepted {
		return errorsx.New(errorsx.EStaleCheckpoint, st.Recipe.MaterialID,
			"checkpoint publish rejected: "+string(outcome)).WithStage(stage)
	}
	return nil
}
