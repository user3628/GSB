package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	CodeObservationInvalid = "E_OBSERVATION_INVALID"
	CodeReportMismatch     = "E_REPORT_MISMATCH"
	CodeHealTimeout        = "E_HEAL_TIMEOUT"
	CodeHealRejected       = "E_HEAL_REJECTED"
	CodeHealFailed         = "E_HEAL_FAILED"
	CodeQueueFull          = "E_QUEUE_FULL"
	CodeStaleSnapshot      = "E_STALE_SNAPSHOT"
	CodeSnapshotInvalid    = "E_SNAPSHOT_INVALID"
	CodeStaleRead          = "E_STALE_READ"
	CodeStoreFailed        = "E_STORE_FAILED"
)

const (
	ObservationHeartbeat ObservationType = "HEARTBEAT"
	ObservationRenewal   ObservationType = "RENEWAL"
)

const (
	HolderActive  HolderStatus = "ACTIVE"
	HolderDrifted HolderStatus = "DRIFTED"
	HolderRevoked HolderStatus = "REVOKED"
)

const (
	HealAuto              HealStrategy = "AUTO"
	HealShortenLease      HealStrategy = "SHORTEN_LEASE"
	HealRequireEarlyRenew HealStrategy = "REQUIRE_EARLY_RENEWAL"
	HealRevokeLease       HealStrategy = "REVOKE_LEASE"
)

const (
	TimingEarly       = "EARLY"
	TimingOnTime      = "ON_TIME"
	TimingLate        = "LATE"
	TimingNotRenewal  = "NOT_RENEWAL"
	TimingUnavailable = "NOT_APPLICABLE"
)

// BizError is the stable error envelope consumed by an upper scheduling system.
type BizError struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	HolderID        string `json:"holder_id,omitempty"`
	ReportSeq       int64  `json:"report_seq,omitempty"`
	CurrentVersion  int64  `json:"current_version,omitempty"`
	ObservedVersion int64  `json:"observed_version,omitempty"`
}

func (e *BizError) Error() string {
	if e == nil {
		return ""
	}
	return e.Code + ": " + e.Message
}

func Biz(code, message string) *BizError {
	return &BizError{Code: code, Message: message}
}

func IsCode(err error, code string) bool {
	var target *BizError
	return errors.As(err, &target) && target.Code == code
}

type ObservationType string

func (t ObservationType) Valid() bool {
	return t == ObservationHeartbeat || t == ObservationRenewal
}

type HolderStatus string

func (s HolderStatus) Valid() bool {
	return s == HolderActive || s == HolderDrifted || s == HolderRevoked
}

type HealStrategy string

func (s HealStrategy) Valid() bool {
	return s == HealAuto || s == HealShortenLease || s == HealRequireEarlyRenew || s == HealRevokeLease
}

// Policy is the declared timing policy for a snapshot or a batch.
type Policy struct {
	SafetyRatio              float64 `json:"safety_ratio"`
	LateToleranceMS          int64   `json:"late_tolerance_ms"`
	DriftIntervalThresholdMS int64   `json:"drift_interval_threshold_ms"`
	ConsecutiveLateThreshold int     `json:"consecutive_late_threshold"`
	HealTimeoutMS            int64   `json:"heal_timeout_ms"`
}

func DefaultPolicy() Policy {
	return Policy{
		SafetyRatio:              0.70,
		LateToleranceMS:          50,
		DriftIntervalThresholdMS: 1200,
		ConsecutiveLateThreshold: 2,
		HealTimeoutMS:            300,
	}
}

// Holder is the observed lease owner. LeaseDurationMS is a positive integer.
type Holder struct {
	ID              string       `json:"holder_id"`
	LeaseDurationMS int64        `json:"lease_duration_ms"`
	MaxRenewals     int          `json:"max_renewals"`
	Status          HolderStatus `json:"status"`
	HealStrategy    HealStrategy `json:"heal_strategy"`
}

// Observation is one delivery at the local endpoint. ReportSeq is the client's
// stable renewal identity; ServerArrivalMS is deliberately coarse and may be
// out of order relative to ClientTimestampMS.
type Observation struct {
	ReportSeq         int64           `json:"report_seq"`
	ClientTimestampMS int64           `json:"client_timestamp_ms"`
	ServerArrivalMS   int64           `json:"server_arrival_ms"`
	Type              ObservationType `json:"type"`
	Digest            string          `json:"digest"`
}

type ObservationRecord struct {
	HolderID string `json:"holder_id"`
	Observation
}

type StoredObservation struct {
	ObservationRecord
	DuplicateCount int `json:"duplicate_count"`
}

// Dataset is a complete observation snapshot body.
type Dataset struct {
	Policy       Policy              `json:"policy"`
	Holders      []Holder            `json:"holders"`
	Observations []ObservationRecord `json:"observations"`
}

// Snapshot is an atomically replaceable observation snapshot.
type Snapshot struct {
	Version       int64   `json:"version"`
	GeneratedAtMS int64   `json:"generated_at_ms"`
	Digest        string  `json:"digest"`
	Dataset       Dataset `json:"dataset"`
}

type RebuiltObservation struct {
	HolderID                  string          `json:"holder_id"`
	ReportSeq                 int64           `json:"report_seq"`
	RebuiltSeq                int             `json:"rebuilt_seq"`
	ClientTimestampMS         int64           `json:"client_timestamp_ms"`
	ServerArrivalMS           int64           `json:"server_arrival_ms"`
	Type                      ObservationType `json:"type"`
	Digest                    string          `json:"digest"`
	DuplicateCount            int             `json:"duplicate_count"`
	PreviousClientTimestampMS *int64          `json:"previous_client_timestamp_ms,omitempty"`
	IntervalMS                int64           `json:"interval_ms"`
	ElapsedSinceRenewalMS     int64           `json:"elapsed_since_renewal_ms"`
	SafeRenewalPointMS        int64           `json:"safe_renewal_point_ms"`
	RelativeToSafePointMS     int64           `json:"relative_to_safe_point_ms"`
	Timing                    string          `json:"timing"`
	WithinLease               bool            `json:"within_lease"`
	LeaseRemainingMS          int64           `json:"lease_remaining_ms"`
	RenewalApplied            bool            `json:"renewal_applied"`
	LateOutsideTolerance      bool            `json:"late_outside_tolerance"`
	DriftContributionMS       int64           `json:"drift_contribution_ms"`
	DelayOverDriftThresholdMS int64           `json:"delay_over_drift_threshold_ms"`
}

type DriftBasis struct {
	ReportSeq      int64  `json:"report_seq"`
	RebuiltSeq     int    `json:"rebuilt_seq"`
	Kind           string `json:"kind"`
	Message        string `json:"message"`
	ContributionMS int64  `json:"contribution_ms"`
}

type DriftDecision struct {
	Drifted             bool         `json:"drifted"`
	MagnitudeMS         int64        `json:"drift_magnitude_ms"`
	MaxConsecutiveLate  int          `json:"max_consecutive_late"`
	LateCount           int          `json:"late_count"`
	IntervalThresholdMS int64        `json:"interval_threshold_ms"`
	ToleranceMS         int64        `json:"tolerance_ms"`
	Basis               []DriftBasis `json:"basis"`
}

type RebuildResult struct {
	HolderID               string               `json:"holder_id"`
	LeaseDurationMS        int64                `json:"lease_duration_ms"`
	SafeRenewalPointMS     int64                `json:"safe_renewal_point_ms"`
	ObservationCount       int                  `json:"observation_count"`
	DuplicateDeliveryCount int                  `json:"duplicate_delivery_count"`
	UniqueReportCount      int                  `json:"unique_report_count"`
	RenewalCount           int                  `json:"renewal_count"`
	RenewalsExhausted      bool                 `json:"renewals_exhausted"`
	LeaseValidAtLast       bool                 `json:"lease_valid_at_last"`
	LastObservedClientMS   int64                `json:"last_observed_client_ms"`
	Rebuilt                []RebuiltObservation `json:"rebuilt"`
	Drift                  DriftDecision        `json:"drift"`
}

type HealAction string

const (
	HealActionShortenLease      HealAction = "SHORTEN_LEASE"
	HealActionRequireEarlyRenew HealAction = "REQUIRE_EARLY_RENEWAL"
	HealActionRevokeLease       HealAction = "REVOKE_LEASE"
)

const (
	HealStatusSent     = "SENT"
	HealStatusTimeout  = "TIMEOUT"
	HealStatusRejected = "REJECTED"
	HealStatusFailed   = "FAILED"
)

func (a HealAction) Valid() bool {
	return a == HealActionShortenLease || a == HealActionRequireEarlyRenew || a == HealActionRevokeLease
}

type HealDecision struct {
	HolderID             string     `json:"holder_id"`
	Action               HealAction `json:"action"`
	Trigger              string     `json:"trigger"`
	Reason               string     `json:"reason"`
	DriftMagnitudeMS     int64      `json:"drift_magnitude_ms"`
	PreviousLeaseMS      int64      `json:"previous_lease_ms"`
	NewLeaseDurationMS   int64      `json:"new_lease_duration_ms,omitempty"`
	EarlyRenewPointMS    int64      `json:"early_renew_point_ms,omitempty"`
	DecidedAtMS          int64      `json:"decided_at_ms"`
	ExpectedRecoveryAtMS int64      `json:"expected_recovery_at_ms"`
}

type HealExecution struct {
	Decision      HealDecision `json:"decision"`
	Status        string       `json:"status"`
	HTTPStatus    int          `json:"http_status,omitempty"`
	ErrorCode     string       `json:"error_code,omitempty"`
	ErrorMessage  string       `json:"error_message,omitempty"`
	SentAtMS      int64        `json:"sent_at_ms"`
	CompletedAtMS int64        `json:"completed_at_ms"`
	RequestBodyID string       `json:"request_body_id"`
}

type DeferredEntry struct {
	HolderID          string `json:"holder_id"`
	DriftMagnitudeMS  int64  `json:"drift_magnitude_ms"`
	Order             int    `json:"order"`
	Reason            string `json:"reason"`
	RemainingBudgetMS int64  `json:"remaining_budget_ms"`
}

// State is the complete committed state at one integer version. Every state row
// is immutable, so a reader can pin one complete version while writers advance.
type State struct {
	Version               int64                    `json:"version"`
	SnapshotVersion       int64                    `json:"snapshot_version"`
	SnapshotGeneratedAtMS int64                    `json:"snapshot_generated_at_ms"`
	SnapshotDigest        string                   `json:"snapshot_digest"`
	Policy                Policy                   `json:"policy"`
	Holders               []Holder                 `json:"holders"`
	Observations          []StoredObservation      `json:"observations"`
	Results               map[string]RebuildResult `json:"results"`
	Heals                 map[string]HealExecution `json:"heals"`
	Deferred              []DeferredEntry          `json:"deferred"`
}

func NewState() State {
	return State{
		Results: make(map[string]RebuildResult),
		Heals:   make(map[string]HealExecution),
	}
}

func (s State) Clone() State {
	out := s
	out.Holders = append([]Holder(nil), s.Holders...)
	out.Observations = append([]StoredObservation(nil), s.Observations...)
	out.Deferred = append([]DeferredEntry(nil), s.Deferred...)
	out.Results = make(map[string]RebuildResult, len(s.Results))
	for k, v := range s.Results {
		cp := v
		cp.Rebuilt = append([]RebuiltObservation(nil), v.Rebuilt...)
		cp.Drift.Basis = append([]DriftBasis(nil), v.Drift.Basis...)
		out.Results[k] = cp
	}
	out.Heals = make(map[string]HealExecution, len(s.Heals))
	for k, v := range s.Heals {
		out.Heals[k] = v
	}
	return out
}

type ReadResult struct {
	StateVersion          int64           `json:"state_version"`
	SnapshotVersion       int64           `json:"snapshot_version"`
	SnapshotGeneratedAtMS int64           `json:"snapshot_generated_at_ms"`
	SnapshotDigest        string          `json:"snapshot_digest"`
	Policy                Policy          `json:"policy"`
	Holders               []Holder        `json:"holders"`
	Results               []RebuildResult `json:"results"`
	Heals                 []HealExecution `json:"heals"`
	Deferred              []DeferredEntry `json:"deferred"`
}

type SnapshotSubmission struct {
	StateVersion    int64  `json:"state_version"`
	SnapshotVersion int64  `json:"snapshot_version"`
	Digest          string `json:"digest"`
}

type SnapshotDigestPayload struct {
	Version       int64   `json:"version"`
	GeneratedAtMS int64   `json:"generated_at_ms"`
	Dataset       Dataset `json:"dataset"`
}

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func SHA256Digest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ComputeSnapshotDigest(snapshot Snapshot) (string, error) {
	payload := SnapshotDigestPayload{
		Version:       snapshot.Version,
		GeneratedAtMS: snapshot.GeneratedAtMS,
		Dataset:       snapshot.Dataset,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return SHA256Digest(data), nil
}

func DigestValid(digest string) bool {
	return digestPattern.MatchString(digest)
}

func ValidatePolicy(policy Policy) *BizError {
	if policy.SafetyRatio <= 0 || policy.SafetyRatio >= 1 {
		return Biz(CodeObservationInvalid, "safety_ratio must be greater than 0 and less than 1")
	}
	if policy.LateToleranceMS < 0 {
		return Biz(CodeObservationInvalid, "late_tolerance_ms must be non-negative")
	}
	if policy.DriftIntervalThresholdMS <= 0 {
		return Biz(CodeObservationInvalid, "drift_interval_threshold_ms must be positive")
	}
	if policy.ConsecutiveLateThreshold <= 0 {
		return Biz(CodeObservationInvalid, "consecutive_late_threshold must be positive")
	}
	if policy.HealTimeoutMS <= 0 {
		return Biz(CodeObservationInvalid, "heal_timeout_ms must be positive")
	}
	return nil
}

// ValidateDataset performs the all-or-nothing intake checks. Sequence numbers
// are checked in capture order; duplicate sequence numbers are permitted and
// resolved by the idempotent merge step. Out-of-order delivery is represented
// by ServerArrivalMS and does not make the sequence decrease.
func ValidateDataset(dataset Dataset) *BizError {
	if err := ValidatePolicy(dataset.Policy); err != nil {
		return err
	}
	holders := make(map[string]Holder, len(dataset.Holders))
	for _, holder := range dataset.Holders {
		if strings.TrimSpace(holder.ID) == "" {
			return Biz(CodeObservationInvalid, "holder_id must not be empty")
		}
		if _, exists := holders[holder.ID]; exists {
			return &BizError{Code: CodeObservationInvalid, Message: "duplicate holder_id", HolderID: holder.ID}
		}
		if holder.LeaseDurationMS <= 0 {
			return &BizError{Code: CodeObservationInvalid, Message: "lease_duration_ms must be a positive integer", HolderID: holder.ID}
		}
		if holder.MaxRenewals <= 0 {
			return &BizError{Code: CodeObservationInvalid, Message: "max_renewals must be positive", HolderID: holder.ID}
		}
		if !holder.Status.Valid() {
			return &BizError{Code: CodeObservationInvalid, Message: "invalid holder status", HolderID: holder.ID}
		}
		if !holder.HealStrategy.Valid() {
			return &BizError{Code: CodeObservationInvalid, Message: "invalid heal_strategy", HolderID: holder.ID}
		}
		holders[holder.ID] = holder
	}
	lastSeq := make(map[string]int64, len(dataset.Holders))
	seenHolder := make(map[string]bool, len(dataset.Holders))
	for _, record := range dataset.Observations {
		holder, ok := holders[record.HolderID]
		if !ok {
			return &BizError{Code: CodeObservationInvalid, Message: "observation references unknown holder", HolderID: record.HolderID, ReportSeq: record.ReportSeq}
		}
		_ = holder
		if record.ReportSeq <= 0 {
			return &BizError{Code: CodeObservationInvalid, Message: "report_seq must be positive", HolderID: record.HolderID, ReportSeq: record.ReportSeq}
		}
		if previous, exists := lastSeq[record.HolderID]; exists && record.ReportSeq < previous {
			return &BizError{Code: CodeObservationInvalid, Message: fmt.Sprintf("report_seq decreased from %d to %d in capture order", previous, record.ReportSeq), HolderID: record.HolderID, ReportSeq: record.ReportSeq}
		}
		lastSeq[record.HolderID] = record.ReportSeq
		seenHolder[record.HolderID] = true
		if record.ClientTimestampMS <= 0 {
			return &BizError{Code: CodeObservationInvalid, Message: "client_timestamp_ms must be positive", HolderID: record.HolderID, ReportSeq: record.ReportSeq}
		}
		if record.ServerArrivalMS <= 0 {
			return &BizError{Code: CodeObservationInvalid, Message: "server_arrival_ms must be positive", HolderID: record.HolderID, ReportSeq: record.ReportSeq}
		}
		if !record.Type.Valid() {
			return &BizError{Code: CodeObservationInvalid, Message: "type must be HEARTBEAT or RENEWAL", HolderID: record.HolderID, ReportSeq: record.ReportSeq}
		}
		if !DigestValid(record.Digest) {
			return &BizError{Code: CodeObservationInvalid, Message: "digest must match sha256:<64 lowercase hex>", HolderID: record.HolderID, ReportSeq: record.ReportSeq}
		}
	}
	_ = seenHolder
	return nil
}

func ValidateSnapshot(snapshot Snapshot) *BizError {
	if snapshot.Version <= 0 {
		return Biz(CodeSnapshotInvalid, "snapshot version must be positive")
	}
	if snapshot.GeneratedAtMS <= 0 {
		return Biz(CodeSnapshotInvalid, "snapshot generated_at_ms must be positive")
	}
	if !DigestValid(snapshot.Digest) {
		return Biz(CodeSnapshotInvalid, "snapshot digest must match sha256:<64 lowercase hex>")
	}
	computed, err := ComputeSnapshotDigest(snapshot)
	if err != nil {
		return Biz(CodeSnapshotInvalid, "snapshot digest could not be computed")
	}
	if computed != snapshot.Digest {
		return Biz(CodeSnapshotInvalid, "snapshot digest mismatch")
	}
	if validationErr := ValidateDataset(snapshot.Dataset); validationErr != nil {
		return validationErr
	}
	return nil
}
