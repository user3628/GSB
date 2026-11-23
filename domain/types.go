package domain

import (
	"encoding/json"

	"releasegate/numeric"
)

const (
	GroupCanary = "canary"
	GroupStable = "stable"

	StatePrepared   = "PREPARED"
	StateCanary     = "CANARY"
	StateObserving  = "OBSERVING"
	StatePromoting  = "PROMOTING"
	StateRolledBack = "ROLLED_BACK"
	StateAborted    = "ABORTED"

	OutcomePromote     = "PROMOTE"
	OutcomeObserveMore = "OBSERVE_MORE"
	OutcomeGateFailed  = "GATE_FAILED"
)

type GateThresholds struct {
	MaxAbsSuccessDropPPM      int64 `json:"max_abs_success_drop_ppm"`
	MaxRelativeSuccessDropPPM int64 `json:"max_relative_success_drop_ppm"`
	MaxLatencyDeltaUS         int64 `json:"max_latency_delta_us"`
	MaxCIHalfWidthPPM         int64 `json:"max_ci_half_width_ppm"`
	RelativeTolerancePPM      int64 `json:"relative_tolerance_ppm"`
}

type StageDefinition struct {
	Number                  int            `json:"number"`
	TargetTrafficPerMille   int            `json:"target_traffic_per_mille"`
	MinObservationSamples   int64          `json:"min_observation_samples"`
	LatencyQuantilePerMille int64          `json:"latency_quantile_per_mille"`
	Gate                    GateThresholds `json:"gate"`
}

type ReleaseDefinition struct {
	ReleaseID          string            `json:"release_id"`
	QuantileMethod     string            `json:"quantile_method"`
	ConfidenceLevelPPM int64             `json:"confidence_level_ppm"`
	Stages             []StageDefinition `json:"stages"`
}

type Release struct {
	ReleaseID           string            `json:"release_id"`
	Definition          ReleaseDefinition `json:"definition"`
	State               string            `json:"state"`
	CurrentStage        int               `json:"current_stage"`
	TrafficPerMille     int               `json:"traffic_per_mille"`
	CurrentStatsVersion int64             `json:"current_stats_version"`
	CreatedAtMS         int64             `json:"created_at_ms"`
	UpdatedAtMS         int64             `json:"updated_at_ms"`
}

// ObservationInput is the wire shape. Pointers make missing required values
// distinguishable from legitimate zero values.
type ObservationInput struct {
	TimestampMS int64  `json:"timestamp_ms"`
	InstanceID  string `json:"instance_id"`
	Group       string `json:"group"`
	Success     *bool  `json:"success"`
	LatencyUS   *int64 `json:"latency_us"`
	Stage       int    `json:"stage,omitempty"`

	TimestampPresent bool `json:"-"`
	SuccessPresent   bool `json:"-"`
	LatencyPresent   bool `json:"-"`
}

func (o *ObservationInput) UnmarshalJSON(data []byte) error {
	type wire struct {
		TimestampMS *int64 `json:"timestamp_ms"`
		InstanceID  string `json:"instance_id"`
		Group       string `json:"group"`
		Success     *bool  `json:"success"`
		LatencyUS   *int64 `json:"latency_us"`
		Stage       int    `json:"stage,omitempty"`
	}
	var w wire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	o.InstanceID = w.InstanceID
	o.Group = w.Group
	o.Success = w.Success
	o.LatencyUS = w.LatencyUS
	o.Stage = w.Stage
	if w.TimestampMS != nil {
		o.TimestampMS = *w.TimestampMS
		o.TimestampPresent = true
	}
	o.SuccessPresent = w.Success != nil
	o.LatencyPresent = w.LatencyUS != nil
	return nil
}

type Observation struct {
	TimestampMS int64
	InstanceID  string
	Group       string
	Success     bool
	LatencyUS   int64
	Stage       int
	BatchID     string
	Seq         int
}

type BatchInput struct {
	ReleaseID    string             `json:"release_id"`
	BatchID      string             `json:"batch_id"`
	Stage        int                `json:"stage"`
	Observations []ObservationInput `json:"observations"`
}

type Batch struct {
	ReleaseID    string        `json:"release_id"`
	BatchID      string        `json:"batch_id"`
	Stage        int           `json:"stage"`
	Observations []Observation `json:"observations"`
}

type GroupMetrics struct {
	Samples                 int64                `json:"samples"`
	Successes               int64                `json:"successes"`
	Failures                int64                `json:"failures"`
	LatencySamples          int64                `json:"latency_samples"`
	SuccessRatePPM          int64                `json:"success_rate_ppm"`
	SuccessRate             string               `json:"success_rate"`
	SuccessRateRoundingStep numeric.RoundingStep `json:"success_rate_rounding_step"`
	SuccessRateAudit        numeric.Audit        `json:"success_rate_audit"`
	LatencyQuantileMethod   string               `json:"latency_quantile_method"`
	LatencyQuantileUS       int64                `json:"latency_quantile_us"`
	QuantileRoundingStep    numeric.RoundingStep `json:"quantile_rounding_step"`
	QuantileAudit           numeric.Audit        `json:"quantile_audit"`
	CountCompensation       int64                `json:"count_compensation"`
}

type StageMetrics struct {
	Scope                   string       `json:"scope"`
	Stage                   int          `json:"stage"`
	StagesIncluded          []int        `json:"stages_included"`
	TargetTrafficPerMille   int          `json:"target_traffic_per_mille"`
	MinObservationSamples   int64        `json:"min_observation_samples"`
	LatencyQuantilePerMille int64        `json:"latency_quantile_per_mille"`
	Canary                  GroupMetrics `json:"canary"`
	Stable                  GroupMetrics `json:"stable"`
	CountCompensation       int64        `json:"count_compensation"`
	CountMethod             string       `json:"count_method"`
	MergedLatencySamples    int64        `json:"merged_latency_samples"`
	QuantileMergeOrder      string       `json:"quantile_merge_order"`
}

type DifferenceEstimate struct {
	Scope                       string                 `json:"scope"`
	Stage                       int                    `json:"stage"`
	CanarySamples               int64                  `json:"canary_samples"`
	StableSamples               int64                  `json:"stable_samples"`
	CanarySuccesses             int64                  `json:"canary_successes"`
	StableSuccesses             int64                  `json:"stable_successes"`
	CanaryRatePPM               int64                  `json:"canary_rate_ppm"`
	StableRatePPM               int64                  `json:"stable_rate_ppm"`
	AbsoluteDifferencePPM       int64                  `json:"absolute_difference_ppm"`
	RelativeDifferencePPM       int64                  `json:"relative_difference_ppm"`
	AbsSuccessDropPPM           int64                  `json:"abs_success_drop_ppm"`
	RelativeSuccessDropPPM      int64                  `json:"relative_success_drop_ppm"`
	CIHalfWidthPPM              int64                  `json:"ci_half_width_ppm"`
	AbsDropUpperPPM             int64                  `json:"abs_drop_upper_ppm"`
	RelativeDropUpperPPM        int64                  `json:"relative_drop_upper_ppm"`
	LatencyDropUS               int64                  `json:"latency_drop_us"`
	CanaryLatencyQuantileUS     int64                  `json:"canary_latency_quantile_us"`
	StableLatencyQuantileUS     int64                  `json:"stable_latency_quantile_us"`
	ErrorBoundPPM               int64                  `json:"error_bound_ppm"`
	RelativeTolerancePPM        int64                  `json:"relative_tolerance_ppm"`
	RoundingSteps               []numeric.RoundingStep `json:"rounding_steps"`
	AccumulatedRoundingErrorPPM numeric.RationalJSON   `json:"accumulated_rounding_error_ppm"`
}

type ErrorContract struct {
	ReleaseID             string            `json:"release_id"`
	StatsVersion          int64             `json:"stats_version"`
	Scope                 string            `json:"scope"`
	Stage                 int               `json:"stage"`
	QuantileMethod        string            `json:"quantile_method"`
	ConfidenceLevelPPM    int64             `json:"confidence_level_ppm"`
	ConfidenceZPPM        int64             `json:"confidence_z_ppm"`
	Scale                 int64             `json:"scale"`
	DecimalPlaces         int               `json:"decimal_places"`
	RoundingMode          string            `json:"rounding_mode"`
	CanonicalPath         string            `json:"canonical_path"`
	SampleSizes           map[string]int64  `json:"sample_sizes"`
	RuntimeErrorBoundPPM  int64             `json:"runtime_error_bound_ppm"`
	RuntimeCIHalfWidthPPM int64             `json:"runtime_ci_half_width_ppm"`
	Formulas              map[string]string `json:"formulas"`
	GateThresholds        GateThresholds    `json:"gate_thresholds"`
	Audit                 numeric.Audit     `json:"audit"`
}

type StatsSnapshot struct {
	ReleaseID               string             `json:"release_id"`
	Version                 int64              `json:"version"`
	Stage                   int                `json:"stage"`
	CreatedAtMS             int64              `json:"created_at_ms"`
	QuantileMethod          string             `json:"quantile_method"`
	StageMetrics            StageMetrics       `json:"stage_metrics"`
	CumulativeMetrics       StageMetrics       `json:"cumulative_metrics"`
	StageEstimate           DifferenceEstimate `json:"stage_estimate"`
	CumulativeEstimate      DifferenceEstimate `json:"cumulative_estimate"`
	StageErrorContract      ErrorContract      `json:"stage_error_contract"`
	CumulativeErrorContract ErrorContract      `json:"cumulative_error_contract"`
}

type GateMeasured struct {
	AbsDropUpperPPM      int64 `json:"abs_drop_upper_ppm"`
	AbsSuccessDropPPM    int64 `json:"abs_success_drop_ppm"`
	CanarySamples        int64 `json:"canary_samples"`
	CIHalfWidthPPM       int64 `json:"ci_half_width_ppm"`
	ErrorBoundPPM        int64 `json:"error_bound_ppm"`
	LatencyDropUS        int64 `json:"latency_drop_us"`
	MeasuredValue        int64 `json:"measured_value"`
	RelativeDropPPM      int64 `json:"relative_drop_ppm"`
	RelativeDropUpperPPM int64 `json:"relative_drop_upper_ppm"`
	RelativeTolerancePPM int64 `json:"relative_tolerance_ppm"`
	StableSamples        int64 `json:"stable_samples"`
	ThresholdValue       int64 `json:"threshold_value"`
	ToleranceApplied     int64 `json:"tolerance_applied"`
}

type GateDecision struct {
	ID                  int64        `json:"id"`
	ReleaseID           string       `json:"release_id"`
	StatsVersion        int64        `json:"stats_version"`
	Stage               int          `json:"stage"`
	Scope               string       `json:"scope"`
	Outcome             string       `json:"outcome"`
	Code                string       `json:"code"`
	TriggeredThreshold  string       `json:"triggered_threshold,omitempty"`
	Measured            GateMeasured `json:"measured"`
	FromState           string       `json:"from_state"`
	ToState             string       `json:"to_state"`
	FromStage           int          `json:"from_stage"`
	ToStage             int          `json:"to_stage"`
	FromTrafficPerMille int          `json:"from_traffic_per_mille"`
	ToTrafficPerMille   int          `json:"to_traffic_per_mille"`
	CreatedAtMS         int64        `json:"created_at_ms"`
}

type GateResult struct {
	Decision      GateDecision       `json:"decision"`
	StageMetrics  StageMetrics       `json:"stage_metrics"`
	Estimate      DifferenceEstimate `json:"estimate"`
	ErrorContract ErrorContract      `json:"error_contract"`
}

type BatchResult struct {
	ReleaseID        string `json:"release_id"`
	BatchID          string `json:"batch_id"`
	Status           string `json:"status"`
	IdempotentReuse  bool   `json:"idempotent_reuse"`
	StatsVersion     int64  `json:"stats_version"`
	ObservationCount int    `json:"observation_count"`
	ContentHash      string `json:"content_hash"`
}

type TransitionRequest struct {
	ToState string `json:"to_state"`
	To      string `json:"to"`
	Reason  string `json:"reason"`
}

func (r TransitionRequest) Destination() string {
	if r.ToState != "" {
		return r.ToState
	}
	return r.To
}
