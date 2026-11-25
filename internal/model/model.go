package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const Scale int64 = 1_000_000

const (
	CodeSnapshotInvalid     = "E_SNAPSHOT_INVALID"
	CodeStaleBase           = "E_STALE_BASE"
	CodeInsufficientSamples = "E_INSUFFICIENT_SAMPLES"
	CodeStaleRead           = "E_STALE_READ"
	CodeConflictRequest     = "E_CONFLICTING_REQUEST"
	CodeInvalidRequest      = "E_INVALID_REQUEST"
	CodeNotFound            = "E_NOT_FOUND"
	CodeModelInvalid        = "E_MODEL_INVALID"
	CodeInternal            = "E_INTERNAL"
)

type BusinessError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
	Err     error          `json:"-"`
}

func (e *BusinessError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *BusinessError) Unwrap() error { return e.Err }

func NewError(code, message string, context map[string]any) *BusinessError {
	return &BusinessError{Code: code, Message: message, Context: context}
}

func AsBusinessError(err error) (*BusinessError, bool) {
	var be *BusinessError
	if errors.As(err, &be) {
		return be, true
	}
	return nil, false
}

type Subscription struct {
	Metric string `json:"metric"`
	Weight int64  `json:"weight"`
}

type Policy struct {
	WindowMS                  int64 `json:"window_ms"`
	TargetConcurrency         int64 `json:"target_concurrency"`
	LatencyBudgetMS           int64 `json:"latency_budget_ms"`
	MinReplicas               int   `json:"min_replicas"`
	MaxReplicas               int   `json:"max_replicas"`
	MinLatencySamples         int   `json:"min_latency_samples"`
	QuantilePercent           int   `json:"quantile_percent"`
	SaturationWeightMicro     int64 `json:"saturation_weight_micro"`
	LatencyRiskWeightMicro    int64 `json:"latency_risk_weight_micro"`
	IncrementalThresholdMicro int64 `json:"incremental_threshold_micro"`
	PropagationThresholdMicro int64 `json:"propagation_threshold_micro"`
}

func (p Policy) Normalize() Policy {
	if p.WindowMS <= 0 {
		p.WindowMS = 60_000
	}
	if p.TargetConcurrency <= 0 {
		p.TargetConcurrency = 20
	}
	if p.LatencyBudgetMS <= 0 {
		p.LatencyBudgetMS = 200
	}
	if p.MinReplicas <= 0 {
		p.MinReplicas = 1
	}
	if p.MaxReplicas < p.MinReplicas {
		p.MaxReplicas = p.MinReplicas + 4
	}
	if p.MinLatencySamples <= 0 {
		p.MinLatencySamples = 8
	}
	if p.QuantilePercent <= 0 || p.QuantilePercent >= 100 {
		p.QuantilePercent = 90
	}
	if p.SaturationWeightMicro <= 0 {
		p.SaturationWeightMicro = 600_000
	}
	if p.LatencyRiskWeightMicro <= 0 {
		p.LatencyRiskWeightMicro = Scale - p.SaturationWeightMicro
	}
	if p.IncrementalThresholdMicro <= 0 {
		p.IncrementalThresholdMicro = 250_000
	}
	if p.PropagationThresholdMicro <= 0 {
		p.PropagationThresholdMicro = 500_000
	}
	return p
}

type Service struct {
	ID            string         `json:"id"`
	Component     string         `json:"component"`
	ScalingGroup  string         `json:"scaling_group"`
	Dependencies  []string       `json:"dependencies,omitempty"`
	Subscriptions []Subscription `json:"subscriptions,omitempty"`
	Policy        Policy         `json:"policy"`
}

func (s *Service) Normalize() {
	s.ID = strings.TrimSpace(s.ID)
	s.Component = strings.TrimSpace(s.Component)
	s.ScalingGroup = strings.TrimSpace(s.ScalingGroup)
	s.Dependencies = compactStrings(s.Dependencies)
	for i := range s.Subscriptions {
		s.Subscriptions[i].Metric = strings.TrimSpace(s.Subscriptions[i].Metric)
		if s.Subscriptions[i].Weight <= 0 {
			s.Subscriptions[i].Weight = 1
		}
	}
	s.Policy = s.Policy.Normalize()
}

func compactStrings(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

type Catalog struct {
	Services []Service `json:"services"`
}

type Sample struct {
	ServiceID        string `json:"service_id"`
	InstanceID       string `json:"instance_id"`
	Metric           string `json:"metric"`
	SampledAtMS      int64  `json:"sampled_at_ms"`
	Value            int64  `json:"value"`
	CollectionWeight int64  `json:"collection_weight"`
}

func (s Sample) NormalizeWeight() Sample {
	if s.CollectionWeight <= 0 {
		s.CollectionWeight = 1
	}
	return s
}

type SampleKey struct {
	ServiceID   string `json:"service_id"`
	InstanceID  string `json:"instance_id"`
	Metric      string `json:"metric"`
	SampledAtMS int64  `json:"sampled_at_ms"`
}

func (k SampleKey) String() string {
	return fmt.Sprintf("%s|%s|%s|%d", k.ServiceID, k.InstanceID, k.Metric, k.SampledAtMS)
}

func (s Sample) Key() SampleKey {
	return SampleKey{
		ServiceID: s.ServiceID, InstanceID: s.InstanceID, Metric: s.Metric, SampledAtMS: s.SampledAtMS,
	}
}

type Snapshot struct {
	Version       int64    `json:"version"`
	GeneratedAtMS int64    `json:"generated_at_ms"`
	Digest        string   `json:"digest,omitempty"`
	Samples       []Sample `json:"samples"`
}

type digestSample struct {
	ServiceID        string `json:"service_id"`
	InstanceID       string `json:"instance_id"`
	Metric           string `json:"metric"`
	SampledAtMS      int64  `json:"sampled_at_ms"`
	Value            int64  `json:"value"`
	CollectionWeight int64  `json:"collection_weight"`
}

func NormalizeSamples(in []Sample) []Sample {
	out := make([]Sample, len(in))
	copy(out, in)
	for i := range out {
		out[i] = out[i].NormalizeWeight()
		out[i].ServiceID = strings.TrimSpace(out[i].ServiceID)
		out[i].InstanceID = strings.TrimSpace(out[i].InstanceID)
		out[i].Metric = strings.TrimSpace(out[i].Metric)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].Key().String(), out[j].Key().String()
		if a != b {
			return a < b
		}
		return out[i].Value < out[j].Value
	})
	return out
}

func DigestSamples(samples []Sample) (string, error) {
	normalized := NormalizeSamples(samples)
	payload := make([]digestSample, 0, len(normalized))
	for _, s := range normalized {
		payload = append(payload, digestSample(s))
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func DigestSnapshot(snapshot Snapshot) (string, error) {
	payload := struct {
		Version       int64          `json:"version"`
		GeneratedAtMS int64          `json:"generated_at_ms"`
		Samples       []digestSample `json:"samples"`
	}{
		Version:       snapshot.Version,
		GeneratedAtMS: snapshot.GeneratedAtMS,
		Samples:       make([]digestSample, 0, len(snapshot.Samples)),
	}
	for _, sample := range NormalizeSamples(snapshot.Samples) {
		payload.Samples = append(payload.Samples, digestSample(sample))
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func (s Snapshot) ValidateDigest() error {
	want, err := DigestSnapshot(s)
	if err != nil {
		return err
	}
	if s.Digest == "" {
		return NewError(CodeSnapshotInvalid, "snapshot digest is empty", map[string]any{"version": s.Version})
	}
	if !strings.EqualFold(s.Digest, want) {
		return NewError(CodeSnapshotInvalid, "snapshot digest mismatch", map[string]any{
			"version": s.Version, "expected_digest": want, "actual_digest": s.Digest,
		})
	}
	return nil
}

type SnapshotRequest struct {
	CommitID string   `json:"commit_id"`
	Snapshot Snapshot `json:"snapshot"`
}

type SampleChange struct {
	Op     string     `json:"op,omitempty"`
	Sample *Sample    `json:"sample,omitempty"`
	Key    *SampleKey `json:"key,omitempty"`
}

type SubscriptionChange struct {
	Op        string `json:"op,omitempty"`
	ServiceID string `json:"service_id"`
	Metric    string `json:"metric"`
	Weight    int64  `json:"weight,omitempty"`
}

type DependencyChange struct {
	Op        string `json:"op,omitempty"`
	ServiceID string `json:"service_id"`
	DependsOn string `json:"depends_on"`
}

type IncrementRequest struct {
	CommitID            string               `json:"commit_id"`
	BaseVersion         int64                `json:"base_version"`
	SampleChanges       []SampleChange       `json:"sample_changes,omitempty"`
	SubscriptionChanges []SubscriptionChange `json:"subscription_changes,omitempty"`
	DependencyChanges   []DependencyChange   `json:"dependency_changes,omitempty"`
	Mode                string               `json:"mode,omitempty"`
	CrashAfterIntent    bool                 `json:"crash_after_intent,omitempty"`
}

type RebuildRequest struct {
	CommitID string   `json:"commit_id"`
	Mode     string   `json:"mode,omitempty"`
	Services []string `json:"services,omitempty"`
}

type ReadMetricsResponse struct {
	ServiceID           string        `json:"service_id"`
	ObservedVersion     *int64        `json:"observed_version,omitempty"`
	CurrentVersion      int64         `json:"current_version"`
	MaterializedVersion int64         `json:"materialized_version"`
	Fresh               bool          `json:"fresh"`
	Metrics             []MetricValue `json:"metrics"`
	Rebuild             *RebuildRun   `json:"rebuild,omitempty"`
}

type MetricValue struct {
	Metric          string         `json:"metric"`
	ValueMicro      int64          `json:"value_micro"`
	Scale           int64          `json:"scale"`
	SampleCount     int            `json:"sample_count"`
	SourceBreakdown []MetricSource `json:"source_breakdown,omitempty"`
	Checksum        string         `json:"checksum"`
}

type MetricSource struct {
	ServiceID   string `json:"service_id"`
	WeightMicro int64  `json:"weight_micro"`
	ValueMicro  int64  `json:"value_micro"`
}

type InstanceBasis struct {
	InstanceID           string `json:"instance_id"`
	SampleCount          int    `json:"sample_count"`
	QuantileRank         int    `json:"quantile_rank"`
	P90LatencyMS         int64  `json:"p90_latency_ms"`
	RequestCount         int64  `json:"request_count"`
	CollectionWeight     int64  `json:"collection_weight"`
	EstConcurrencyMicro  int64  `json:"estimated_concurrency_micro"`
	SaturationRatioMicro int64  `json:"saturation_ratio_micro"`
	LatencyRatioMicro    int64  `json:"latency_ratio_micro"`
	RiskScoreMicro       int64  `json:"risk_score_micro"`
}

type DecisionResponse struct {
	ServiceID                  string          `json:"service_id"`
	ObservedVersion            *int64          `json:"observed_version,omitempty"`
	CurrentVersion             int64           `json:"current_version"`
	MaterializedVersion        int64           `json:"materialized_version"`
	DesiredReplicas            int             `json:"desired_replicas"`
	MinReplicas                int             `json:"min_replicas"`
	MaxReplicas                int             `json:"max_replicas"`
	RiskScoreMicro             int64           `json:"risk_score_micro"`
	SaturationScoreMicro       int64           `json:"saturation_score_micro"`
	LatencyScoreMicro          int64           `json:"latency_score_micro"`
	ConcurrencyDesiredReplicas int             `json:"concurrency_desired_replicas"`
	LatencyDesiredReplicas     int             `json:"latency_desired_replicas"`
	Scale                      int64           `json:"scale"`
	SampleSize                 int             `json:"sample_size"`
	Formula                    string          `json:"formula"`
	Basis                      []InstanceBasis `json:"basis"`
	Clamped                    bool            `json:"clamped"`
	RawReplicas                int             `json:"raw_replicas"`
	Checksum                   string          `json:"checksum"`
}

type ImpactNode struct {
	ServiceID string   `json:"service_id"`
	Hops      int      `json:"hops"`
	Path      []string `json:"path"`
	Metrics   []string `json:"propagating_metrics,omitempty"`
}

type ImpactResponse struct {
	TriggerServiceID     string       `json:"trigger_service_id"`
	CurrentVersion       int64        `json:"current_version"`
	ImpactSet            []ImpactNode `json:"impact_set"`
	UpstreamDependencies []ImpactNode `json:"upstream_dependencies"`
	Checksum             string       `json:"checksum"`
}

type RebuildRun struct {
	ID                        int64    `json:"id"`
	Version                   int64    `json:"version"`
	RequestedMode             string   `json:"requested_mode"`
	SelectedPath              string   `json:"selected_path"`
	ThresholdMicro            int64    `json:"threshold_micro"`
	PropagationThresholdMicro int64    `json:"propagation_threshold_micro"`
	TouchedSubscriptionItems  int      `json:"touched_subscription_items"`
	TotalSubscriptionItems    int      `json:"total_subscription_items"`
	TouchedSampleRecords      int      `json:"touched_sample_records"`
	TotalSampleRecords        int      `json:"total_sample_records"`
	TouchedRatioMicro         int64    `json:"touched_ratio_micro"`
	PropagationCardinality    int      `json:"propagation_cardinality"`
	ServiceCount              int      `json:"service_count"`
	RecomputeScope            []string `json:"recompute_scope"`
	Reason                    string   `json:"reason"`
	MetricsChecksum           string   `json:"metrics_checksum"`
	DecisionsChecksum         string   `json:"decisions_checksum"`
	CreatedAtMS               int64    `json:"created_at_ms"`
}

type RecoveryReport struct {
	ID                  int64    `json:"id"`
	StartedAtMS         int64    `json:"started_at_ms"`
	FinishedAtMS        int64    `json:"finished_at_ms"`
	FoundPendingIntents int      `json:"found_pending_intents"`
	AbortedCommitIDs    []string `json:"aborted_commit_ids"`
	RecoveredToVersion  int64    `json:"recovered_to_version"`
	MaterializedVersion int64    `json:"materialized_version"`
	Action              string   `json:"action"`
	Notes               []string `json:"notes,omitempty"`
}

type CommitResponse struct {
	CommitID         string      `json:"commit_id"`
	Kind             string      `json:"kind"`
	Version          int64       `json:"version"`
	BaseVersion      int64       `json:"base_version,omitempty"`
	IdempotentReplay bool        `json:"idempotent_replay,omitempty"`
	CrashArmed       bool        `json:"crash_armed,omitempty"`
	Rebuild          *RebuildRun `json:"rebuild,omitempty"`
}

type HealthResponse struct {
	Status              string `json:"status"`
	TimeMS              int64  `json:"time_ms"`
	CurrentVersion      int64  `json:"current_version"`
	MaterializedVersion int64  `json:"materialized_version"`
	DBWritable          bool   `json:"db_writable"`
}
