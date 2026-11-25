// Package model contains the domain types, deterministic normalization,
// fixed-point arithmetic and state-machine rules of the engine.
package model

// Sensitivity is an alert sensitivity tier. Lower numeric value means higher
// sensitivity, which is used as the deterministic patrol-degradation order.
type Sensitivity int

const (
	SensHigh   Sensitivity = 1
	SensMedium Sensitivity = 2
	SensLow    Sensitivity = 3
)

func (s Sensitivity) Valid() bool { return s >= SensHigh && s <= SensLow }

func (s Sensitivity) String() string {
	switch s {
	case SensHigh:
		return "HIGH"
	case SensMedium:
		return "MEDIUM"
	case SensLow:
		return "LOW"
	default:
		return "UNKNOWN"
	}
}

// ParseSensitivity parses the canonical tier names.
func ParseSensitivity(v string) (Sensitivity, bool) {
	switch v {
	case "HIGH":
		return SensHigh, true
	case "MEDIUM":
		return SensMedium, true
	case "LOW":
		return SensLow, true
	default:
		return 0, false
	}
}

// AlertState is the lifecycle state of an alert.
type AlertState string

const (
	StateCandidate  AlertState = "CANDIDATE"
	StateFiring     AlertState = "FIRING"
	StateSuppressed AlertState = "SUPPRESSED"
	StateResolved   AlertState = "RESOLVED"
)

// Service is a normalized service declaration.
type Service struct {
	ID             string      `json:"id"`
	Zone           string      `json:"zone"`
	WeightPerMille int64       `json:"weight_per_mille"`
	Upstreams      []string    `json:"upstreams"`
	Sensitivity    Sensitivity `json:"sensitivity"`
	SilenceWindow  int64       `json:"silence_window_ms"`
}

// Observation is a single metric sample.
type Observation struct {
	TimestampMS int64             `json:"timestamp_ms"`
	ServiceID   string            `json:"service_id"`
	Metric      string            `json:"metric"`
	Value       int64             `json:"value"`
	Labels      map[string]string `json:"labels"`
}

// SuppressionRule is a declared suppression rule keyed by service, zone and
// dependency hop.
type SuppressionRule struct {
	ID      string `json:"id"`
	Service string `json:"service"`
	Zone    string `json:"zone"`
	Hop     int    `json:"hop"`
}

// PolicySnapshot is a versioned, fully self-contained policy snapshot.
type PolicySnapshot struct {
	Version             int64             `json:"version"`
	Thresholds          map[string]int64  `json:"thresholds"`
	FingerprintFields   []string          `json:"fingerprint_fields"`
	SuppressionWindowMS int64             `json:"suppression_window_ms"`
	RootCauseTolerance  int64             `json:"root_cause_tolerance_per_mille"`
	ConcurrencyLimit    int               `json:"concurrency_limit"`
	RoundBudgetMS       int64             `json:"round_budget_ms"`
	SuppressionRules    []SuppressionRule `json:"suppression_rules"`
	Checksum            string            `json:"checksum"`
}

// Alert is a (possibly merged) alert produced from observations.
type Alert struct {
	ID                 string            `json:"id"`
	Version            int64             `json:"version"`
	Fingerprint        string            `json:"fingerprint"`
	ServiceID          string            `json:"service_id"`
	Zone               string            `json:"zone"`
	Metric             string            `json:"metric"`
	Labels             map[string]string `json:"labels"`
	FirstSeenMS        int64             `json:"first_seen_ms"`
	LastSeenMS         int64             `json:"last_seen_ms"`
	WindowStartMS      int64             `json:"window_start_ms"`
	WindowEndMS        int64             `json:"window_end_ms"`
	State              AlertState        `json:"state"`
	MergedCount        int64             `json:"merged_count"`
	SuppressedBy       []string          `json:"suppressed_by"`
	PrimaryReason      string            `json:"primary_reason"`
	ReleaseAtMS        int64             `json:"release_at_ms"`
	SourceObservations []int64           `json:"source_observations"`
	LastObservationSeq int64             `json:"last_observation_seq"`
}

// SuppressionHit records one rule hit for an alert.
type SuppressionHit struct {
	AlertID     string `json:"alert_id"`
	RuleID      string `json:"rule_id"`
	Service     string `json:"rule_service"`
	Zone        string `json:"rule_zone"`
	Hop         int    `json:"hop"`
	OrderIndex  int    `json:"order_index"`
	MatchedAtMS int64  `json:"matched_at_ms"`
	ReleaseAtMS int64  `json:"release_at_ms"`
}

// RootCauseScore is one scored root-cause candidate for a trigger alert.
type RootCauseScore struct {
	TriggerAlertID    string `json:"trigger_alert_id"`
	CandidateService  string `json:"candidate_service"`
	Hops              int    `json:"hops"`
	TimeDeltaMS       int64  `json:"time_delta_ms"`
	ScoreRawMilli     int64  `json:"score_raw_milli"`
	ScoreFinalMilli   int64  `json:"score_final_milli"`
	RoundingErrorNano int64  `json:"rounding_error_nano"`
	Rank              int    `json:"rank"`
	Version           int64  `json:"version"`
}

// SamplingReport describes how the sampling path selected observations.
type SamplingReport struct {
	Method        string `json:"method"`
	Limit         int    `json:"limit"`
	TotalInput    int    `json:"total_input"`
	Processed     int    `json:"processed"`
	SampleRate    string `json:"sample_rate"`
	Confidence    string `json:"confidence"`
	SelectedIndex []int  `json:"selected_indices"`
}

// PatrolTask is one unit of patrol work (one service).
type PatrolTask struct {
	ServiceID   string
	Zone        string
	Sensitivity Sensitivity
	Weight      int64
}

// PatrolTrace is the deterministic trace of a patrol round.
type PatrolTrace struct {
	RoundID          string   `json:"round_id"`
	ConcurrencyLimit int      `json:"concurrency_limit"`
	QueueCapacity    int      `json:"queue_capacity"`
	BudgetMS         int64    `json:"budget_ms"`
	Enqueued         []string `json:"enqueued"`
	Degraded         []string `json:"degraded"`
	Completed        []string `json:"completed"`
	RemainingBudget  int64    `json:"remaining_budget_ms"`
}
