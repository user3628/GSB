package engine

import (
	"ticketpipeline/internal/determ"
	"ticketpipeline/internal/model"
)

// stageConfig is the policy subset that can change a stage's output. Each
// stage fingerprints only what it actually consumes, so a threshold tweak
// leaves stages 1-2 reusable while a hash-dimension change rebuilds all four.
// Only the concrete fields a stage consumes participate in its fingerprint.
// The monotonic Version and the Injection switch block are deliberately
// excluded: injections take effect through the actual numeric fields (e.g.
// zero smoothing sets smoothing=0), so a mere version bump must not force a
// full rebuild. Version is still recorded on every checkpoint row.
type stageConfig struct {
	Stage          string             `json:"stage"`
	HashDim        int                `json:"hash_dim,omitempty"`
	SampleRatio    float64            `json:"sample_ratio,omitempty"`
	Smoothing      float64            `json:"smoothing,omitempty"`
	IntentWeights  map[string]float64 `json:"intent_weights,omitempty"`
	PriorityThresh []float64          `json:"priority_thresholds,omitempty"`
}

// StageConfigFingerprint returns a stable fingerprint of the policy subset
// relevant to a stage.
func StageConfigFingerprint(stage string, p model.Policy) (string, error) {
	cfg := stageConfig{Stage: stage}
	switch stage {
	case model.StageNormalize:
		cfg.HashDim = p.HashDim
		cfg.SampleRatio = p.SampleRatio
	case model.StageIntent:
		cfg.HashDim = p.HashDim
		cfg.SampleRatio = p.SampleRatio
		cfg.Smoothing = p.Smoothing
		cfg.IntentWeights = p.IntentWeights
	case model.StagePriority:
		cfg.PriorityThresh = p.PriorityThresh
		cfg.Smoothing = p.Smoothing
		cfg.HashDim = p.HashDim
	case model.StageValidate:
		// validation only aggregates, but still version-gated so a report
		// always carries the exact policy it was built under.
	}
	return determ.Fingerprint(cfg)
}

// EarliestChangedStage compares per-stage config fingerprints of old and new
// policies and returns the earliest stage whose subset changed. An empty
// string means no stage-relevant change was detected.
func EarliestChangedStage(old, new model.Policy, oldFP, newFP map[string]string) string {
	for _, stage := range model.Stages {
		if oldFP[stage] != newFP[stage] {
			return stage
		}
	}
	return ""
}
