package experiment

import (
	"regworkbench/internal/config"
	"regworkbench/internal/cv"
	"regworkbench/internal/model"
	"regworkbench/internal/workerr"
)

type FoldPointArtifact struct {
	model.FitResult
	Metrics           model.Metrics `json:"metrics"`
	SelectedMetric    float64       `json:"selected_metric"`
	BundleFingerprint string        `json:"bundle_fingerprint"`
	CacheKey          string        `json:"cache_key"`
}

type FoldArtifact struct {
	Fold              int                 `json:"fold"`
	FoldFingerprint   string              `json:"fold_fingerprint"`
	BundleFingerprint string              `json:"bundle_fingerprint"`
	SolverFingerprint string              `json:"solver_fingerprint"`
	Points            []FoldPointArtifact `json:"points"`
	Digest            string              `json:"digest"`
}

type LiveProgress struct {
	Fold       int               `json:"fold"`
	Lambda     float64           `json:"lambda"`
	L1Ratio    float64           `json:"l1_ratio"`
	Iteration  int               `json:"iteration"`
	Trajectory []model.Iteration `json:"trajectory"`
}

type CandidateSummary struct {
	Lambda        float64   `json:"lambda"`
	L1Ratio       float64   `json:"l1_ratio"`
	Unregularized bool      `json:"unregularized"`
	FoldValues    []float64 `json:"fold_values"`
	MeanMetric    float64   `json:"mean_metric"`
	StdMetric     float64   `json:"std_metric"`
	Selected      bool      `json:"selected"`
}

type Rejection struct {
	Lambda     float64 `json:"lambda"`
	L1Ratio    float64 `json:"l1_ratio"`
	Code       string  `json:"code"`
	Detail     string  `json:"detail"`
	MeanMetric float64 `json:"mean_metric"`
}

type SelectionBasis struct {
	Metric          string  `json:"metric"`
	Direction       string  `json:"direction"`
	Rule            string  `json:"rule"`
	TieBreak        string  `json:"tie_break"`
	SelectedLambda  float64 `json:"selected_lambda"`
	SelectedL1Ratio float64 `json:"selected_l1_ratio"`
	SelectedMean    float64 `json:"selected_mean"`
	SelectedStd     float64 `json:"selected_std"`
}

type ValidationSummary struct {
	Metric     string             `json:"metric"`
	KFolds     int                `json:"k_folds"`
	Candidates []CandidateSummary `json:"candidates"`
	Selected   CandidateSummary   `json:"selected"`
	Basis      SelectionBasis     `json:"basis"`
	Rejections []Rejection        `json:"rejections"`
}

type CoefficientsArtifact struct {
	Scope             string    `json:"scope"`
	Fold              *int      `json:"fold,omitempty"`
	Lambda            float64   `json:"lambda"`
	L1Ratio           float64   `json:"l1_ratio"`
	Unregularized     bool      `json:"unregularized"`
	FeatureNames      []string  `json:"feature_names"`
	Coefficients      []float64 `json:"coefficients"`
	Intercept         float64   `json:"intercept"`
	Digest            string    `json:"digest"`
	SampleFingerprint string    `json:"sample_fingerprint"`
	ConfigVersion     string    `json:"config_version"`
}

type ModelArtifact struct {
	CoefficientsArtifact
	TrainingRows int               `json:"training_rows"`
	Trajectory   []model.Iteration `json:"trajectory,omitempty"`
}

type Snapshot struct {
	SessionID         string                `json:"session_id"`
	State             string                `json:"state"`
	Config            config.Config         `json:"config"`
	ConfigVersion     string                `json:"config_version"`
	SampleFingerprint string                `json:"sample_fingerprint"`
	Stage             string                `json:"stage"`
	CompletedStages   []string              `json:"completed_stages"`
	CompletedFolds    []int                 `json:"completed_folds"`
	CurrentFold       int                   `json:"current_fold"`
	CurrentLambda     float64               `json:"current_lambda"`
	CurrentL1Ratio    float64               `json:"current_l1_ratio"`
	CurrentIteration  int                   `json:"current_iteration"`
	Live              *LiveProgress         `json:"live,omitempty"`
	Split             *cv.Split             `json:"split,omitempty"`
	Folds             map[int]*FoldArtifact `json:"folds,omitempty"`
	CurrentBest       *CoefficientsArtifact `json:"current_best,omitempty"`
	Validation        *ValidationSummary    `json:"validation,omitempty"`
	FinalModel        *ModelArtifact        `json:"final_model,omitempty"`
	Fingerprints      map[string]string     `json:"fingerprints"`
	MonotonicSequence int64                 `json:"monotonic_sequence"`
	LastError         *workerr.Error        `json:"last_error,omitempty"`
}

type ChangePlan struct {
	EarliestStage    string   `json:"earliest_stage"`
	ConfigVersionOld string   `json:"config_version_old"`
	ConfigVersionNew string   `json:"config_version_new"`
	ChangedFields    []string `json:"changed_fields"`
	AffectedFolds    []int    `json:"affected_folds"`
	ReusableFolds    []int    `json:"reusable_folds"`
	Reason           string   `json:"reason"`
}
