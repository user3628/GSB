package pipeline

import (
	"matbench/internal/fp"
)

// Report is the stage-4 diagnostic summary for one material.
type Report struct {
	MaterialID        string            `json:"material_id"`
	NormalizedPrompt  string            `json:"normalized_prompt"`
	Tokens            int               `json:"token_count"`
	DegenerateNoise   bool              `json:"degenerate_noise"`
	DegenerateNote    string            `json:"degenerate_note,omitempty"`
	ReviewOctaves     []int             `json:"review_octaves"`
	OctaveEvidence    []OctaveEvidence  `json:"octave_evidence"`
	ParamErrors       []ParamEvidence   `json:"param_errors"`
	TexturePath       string            `json:"texture_path"`
	StageFingerprints map[string]string `json:"stage_fingerprints"`
	Fingerprint       string            `json:"fingerprint"`
}

// OctaveEvidence preserves both attribution bases for review.
type OctaveEvidence struct {
	Index           int     `json:"index"`
	LogContribution float64 `json:"log_contribution"`
	PerturbL1       float64 `json:"perturb_l1"`
	ErrorBound      float64 `json:"error_bound"`
	CumulativeBound float64 `json:"cumulative_bound"`
	ContribRank     int     `json:"contrib_rank"`
	PerturbRank     int     `json:"perturb_rank"`
	ReviewFlag      bool    `json:"review_flag"`
}

// ParamEvidence is the per-parameter precision/error evidence.
type ParamEvidence struct {
	Name       string  `json:"name"`
	Value      float64 `json:"value"`
	ErrorBound float64 `json:"error_bound"`
	Digits     int     `json:"effective_digits"`
}

// BuildReport aggregates evidence from earlier stages.
func BuildReport(st *MaterialState) *Report {
	rep := &Report{
		MaterialID:        st.Recipe.MaterialID,
		StageFingerprints: map[string]string{},
	}
	if st.Control != nil {
		rep.NormalizedPrompt = st.Control.Normalized
		rep.Tokens = len(st.Control.Tokens)
		rep.StageFingerprints[StageControl] = st.Control.Fingerprint
	}
	if st.Noise != nil {
		rep.DegenerateNoise = st.Noise.Degenerate
		rep.DegenerateNote = st.Noise.DegenerateNote
		rep.ReviewOctaves = st.Noise.ReviewOctaves
		for _, o := range st.Noise.Octaves {
			rep.OctaveEvidence = append(rep.OctaveEvidence, OctaveEvidence{
				Index:           o.Index,
				LogContribution: o.LogContribution,
				PerturbL1:       o.PerturbL1,
				ErrorBound:      o.ErrorBound,
				CumulativeBound: o.CumulativeBound,
				ContribRank:     o.ContribRank,
				PerturbRank:     o.PerturbRank,
				ReviewFlag:      o.ReviewFlag,
			})
		}
		rep.StageFingerprints[StageNoise] = st.Noise.Fingerprint
	}
	if st.Palette != nil {
		rep.TexturePath = st.Palette.TexturePath
		for _, p := range st.Palette.Params {
			rep.ParamErrors = append(rep.ParamErrors, ParamEvidence{
				Name:       p.Name,
				Value:      p.Value,
				ErrorBound: p.ErrorBound,
				Digits:     p.Digits,
			})
		}
		rep.StageFingerprints[StagePalette] = st.Palette.Fingerprint
	}
	rep.Fingerprint, _ = fp.Of("stage:report", rep)
	rep.StageFingerprints[StageReport] = rep.Fingerprint
	return rep
}
