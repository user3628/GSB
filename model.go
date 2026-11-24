package abeval

const (
	DecisionRollout  = "rollout"
	DecisionObserve  = "observe"
	DecisionRollback = "rollback"

	StatusImported        = "imported"
	StatusRunning         = "running"
	StatusGracefulStopped = "graceful_stopped"
	StatusCompleted       = "completed"

	ScenarioRollout      = "rollout"
	ScenarioInsufficient = "insufficient"
	ScenarioRollback     = "rollback"
	ScenarioRecovery     = "recovery"
)

type Event struct {
	EventID     string `json:"event_id"`
	TimestampMS int64  `json:"timestamp_ms"`
	UserID      string `json:"user_id"`
	Group       string `json:"group"`
	ItemID      string `json:"item_id"`
	Clicked     int    `json:"clicked"`
	LatencyUS   int64  `json:"latency_us"`
}

type GroupWeight struct {
	Group  string  `json:"group"`
	Weight float64 `json:"weight"`
}

type StageConfig struct {
	Index                int    `json:"index"`
	Name                 string `json:"name"`
	StartMS              int64  `json:"start_ms"`
	EndMS                int64  `json:"end_ms"`
	MinExposuresPerGroup int    `json:"min_exposures_per_group"`
}

type CTRThreshold struct {
	MinLiftAbs      float64 `json:"min_lift_abs"`
	RollbackDropAbs float64 `json:"rollback_drop_abs"`
	ZScore          float64 `json:"z_score"`
}

type LatencyThreshold struct {
	Quantile        float64 `json:"quantile"`
	MaxQuantileUS   int64   `json:"max_quantile_us"`
	MaxRegressionUS int64   `json:"max_regression_us"`
}

type Config struct {
	ExperimentID         string           `json:"experiment_id"`
	ControlGroup         string           `json:"control_group"`
	TreatmentGroup       string           `json:"treatment_group"`
	Groups               []string         `json:"groups"`
	Traffic              []GroupWeight    `json:"traffic"`
	Stages               []StageConfig    `json:"stages"`
	MinExposuresPerGroup int              `json:"min_exposures_per_group,omitempty"`
	CTR                  CTRThreshold     `json:"ctr"`
	Latency              LatencyThreshold `json:"latency"`
	StopAfterStage       int              `json:"stop_after_stage,omitempty"`
	ResumeRound          int              `json:"resume_round,omitempty"`
}

type Dataset struct {
	Version int64   `json:"version"`
	BatchID string  `json:"batch_id,omitempty"`
	Config  Config  `json:"config"`
	Events  []Event `json:"events"`
}

type Quantile struct {
	P         float64 `json:"p"`
	ValueUS   int64   `json:"value_us"`
	Available bool    `json:"available"`
}

type GroupStats struct {
	Group     string     `json:"group"`
	Exposures int        `json:"exposures"`
	Clicks    int        `json:"clicks"`
	CTR       float64    `json:"ctr"`
	Quantiles []Quantile `json:"quantiles"`
}

type CTRComparison struct {
	ControlGroup     string  `json:"control_group"`
	TreatmentGroup   string  `json:"treatment_group"`
	ControlCTR       float64 `json:"control_ctr"`
	TreatmentCTR     float64 `json:"treatment_ctr"`
	AbsoluteLift     float64 `json:"absolute_lift"`
	RelativeLift     float64 `json:"relative_lift"`
	StandardError    float64 `json:"standard_error"`
	CILevel          float64 `json:"ci_level"`
	CILower          float64 `json:"ci_lower"`
	CIUpper          float64 `json:"ci_upper"`
	RequiredPerGroup int     `json:"required_per_group"`
	MeetsMinSample   bool    `json:"meets_min_sample"`
}

type LatencyComparison struct {
	QuantileP           float64 `json:"quantile_p"`
	ControlUS           int64   `json:"control_us"`
	TreatmentUS         int64   `json:"treatment_us"`
	DeltaUS             int64   `json:"delta_us"`
	MaxQuantileUS       int64   `json:"max_quantile_us"`
	MaxRegressionUS     int64   `json:"max_regression_us"`
	AbsoluteSatisfied   bool    `json:"absolute_satisfied"`
	RegressionSatisfied bool    `json:"regression_satisfied"`
	Available           bool    `json:"available"`
}

type Criterion struct {
	Code      string `json:"code"`
	Scope     string `json:"scope"`
	Passed    bool   `json:"passed"`
	Actual    string `json:"actual"`
	Threshold string `json:"threshold,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

type Verdict struct {
	Decision   string      `json:"decision"`
	ReasonCode string      `json:"reason_code"`
	Summary    string      `json:"summary"`
	Criteria   []Criterion `json:"criteria"`
	Negated    []string    `json:"negated_reasons"`
}

type StageMetrics struct {
	StageIndex           int               `json:"stage_index"`
	StageEventCount      int               `json:"stage_event_count"`
	StageGroups          []GroupStats      `json:"stage_groups"`
	CumulativeEventCount int               `json:"cumulative_event_count"`
	CumulativeGroups     []GroupStats      `json:"cumulative_groups"`
	CTR                  CTRComparison     `json:"ctr_comparison"`
	Latency              LatencyComparison `json:"latency_comparison"`
}

type StageCheckpoint struct {
	BatchID     string       `json:"batch_id"`
	Version     int64        `json:"version"`
	ContentHash string       `json:"content_hash"`
	StageIndex  int          `json:"stage_index"`
	Stats       StageMetrics `json:"stats"`
	Verdict     Verdict      `json:"verdict"`
	StatsHash   string       `json:"stats_hash"`
	WriterID    string       `json:"writer_id,omitempty"`
}

type StageReport struct {
	Stage   StageConfig  `json:"stage"`
	Metrics StageMetrics `json:"metrics"`
	Verdict Verdict      `json:"verdict"`
}

type RevisionInfo struct {
	Version     int64  `json:"version"`
	ConfigHash  string `json:"config_hash"`
	SampleHash  string `json:"sample_hash"`
	ContentHash string `json:"content_hash"`
}

type Report struct {
	SchemaVersion string        `json:"schema_version"`
	BatchID       string        `json:"batch_id"`
	ExperimentID  string        `json:"experiment_id"`
	Revision      RevisionInfo  `json:"revision"`
	Status        string        `json:"status"`
	Decision      string        `json:"decision,omitempty"`
	DecisionCode  string        `json:"decision_code,omitempty"`
	Summary       string        `json:"summary,omitempty"`
	Stages        []StageReport `json:"stages"`
	FinalVerdict  *Verdict      `json:"final_verdict,omitempty"`
}

type BatchStatus struct {
	BatchID       string `json:"batch_id"`
	ExperimentID  string `json:"experiment_id"`
	Version       int64  `json:"version"`
	ConfigHash    string `json:"config_hash"`
	SampleHash    string `json:"sample_hash"`
	ContentHash   string `json:"content_hash"`
	TotalStages   int    `json:"total_stages"`
	CurrentStage  int    `json:"current_stage"`
	Status        string `json:"status"`
	StopRequested bool   `json:"stop_requested"`
	Decision      string `json:"decision,omitempty"`
	DecisionCode  string `json:"decision_code,omitempty"`
}

type ImportResult struct {
	BatchID         string `json:"batch_id"`
	Created         bool   `json:"created"`
	Replaced        bool   `json:"replaced"`
	Idempotent      bool   `json:"idempotent"`
	PreviousVersion int64  `json:"previous_version,omitempty"`
	CurrentVersion  int64  `json:"current_version"`
	ContentHash     string `json:"content_hash"`
	EventCount      int    `json:"event_count"`
}
