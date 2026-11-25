package registry

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

// State is the persisted health state of a service instance.
type State string

const (
	StateUnknown   State = "UNKNOWN"
	StateHealthy   State = "HEALTHY"
	StateSuspect   State = "SUSPECT"
	StateUnhealthy State = "UNHEALTHY"
	StateDraining  State = "DRAINING"
)

// ProbeKind identifies one of the three supported probes.
type ProbeKind string

const (
	ProbeHTTP  ProbeKind = "HTTP"
	ProbeTCP   ProbeKind = "TCP"
	ProbeLease ProbeKind = "LEASE"
)

// ProbeSpec carries the per-instance probe contract. TimeoutMS is independent
// for each probe class and is always enforced with a real deadline.
type ProbeSpec struct {
	Kind           ProbeKind `json:"kind"`
	Path           string    `json:"path,omitempty"`
	ExpectedStatus int       `json:"expectedStatus,omitempty"`
	ExpectedMarker string    `json:"expectedMarker,omitempty"`
	TimeoutMS      int       `json:"timeoutMs"`
}

// Instance is the registration declaration submitted by an operator.
type Instance struct {
	ServiceName      string    `json:"serviceName"`
	InstanceID       string    `json:"instanceId"`
	Endpoint         string    `json:"endpoint"`
	Weight           int       `json:"weight"`
	LeaseTTLMS       int64     `json:"leaseTtlMs"`
	Probe            ProbeSpec `json:"probe"`
	Priority         int       `json:"priority"`
	SuccessThreshold int       `json:"successThreshold"`
	FailureThreshold int       `json:"failureThreshold"`
}

// SnapshotRequest is a complete, versioned registration snapshot.
type SnapshotRequest struct {
	RequestID   string     `json:"requestId,omitempty"`
	Version     int64      `json:"version"`
	GeneratedAt string     `json:"generatedAt"`
	Digest      string     `json:"digest"`
	Instances   []Instance `json:"instances"`
}

// SnapshotResult is returned after a successful full-snapshot commit.
type SnapshotResult struct {
	Version          int64  `json:"version"`
	GeneratedAt      string `json:"generatedAt"`
	Digest           string `json:"digest"`
	InstanceCount    int    `json:"instanceCount"`
	DerivedRevision  int64  `json:"derivedRevision"`
	IdempotentReplay bool   `json:"idempotentReplay,omitempty"`
}

// InstanceRef identifies an instance in a delta or query.
type InstanceRef struct {
	ServiceName string `json:"serviceName"`
	InstanceID  string `json:"instanceId"`
}

// DeltaRequest is a base-version-bound patch. Upserts replace a complete
// instance declaration; removes delete an instance by identity.
type DeltaRequest struct {
	RequestID   string        `json:"requestId,omitempty"`
	BaseVersion int64         `json:"baseVersion"`
	Version     int64         `json:"version,omitempty"`
	Upserts     []Instance    `json:"upserts,omitempty"`
	Removes     []InstanceRef `json:"removes,omitempty"`
}

// DeltaResult is returned after a successful incremental commit.
type DeltaResult struct {
	BaseVersion      int64  `json:"baseVersion"`
	Version          int64  `json:"version"`
	Digest           string `json:"digest"`
	InstanceCount    int    `json:"instanceCount"`
	DerivedRevision  int64  `json:"derivedRevision"`
	IdempotentReplay bool   `json:"idempotentReplay,omitempty"`
}

// RuntimeInstance is the current configuration plus persisted health/lease
// state for one instance.
type RuntimeInstance struct {
	Instance
	ConfigFingerprint  string `json:"configFingerprint"`
	State              State  `json:"state"`
	ConsecutiveSuccess int    `json:"consecutiveSuccess"`
	ConsecutiveFailure int    `json:"consecutiveFailure"`
	LastHeartbeatMS    int64  `json:"lastHeartbeatMs"`
	LastProbeAt        string `json:"lastProbeAt,omitempty"`
	LastProbeOK        *bool  `json:"lastProbeOk,omitempty"`
	LastProbeCode      string `json:"lastProbeCode,omitempty"`
	UpdatedRevision    int64  `json:"updatedRevision"`
}

// CurrentSnapshot is the complete materialized view at one committed version.
type CurrentSnapshot struct {
	Version         int64             `json:"version"`
	GeneratedAt     string            `json:"generatedAt"`
	Digest          string            `json:"digest"`
	DerivedRevision int64             `json:"derivedRevision"`
	Instances       []RuntimeInstance `json:"instances"`
}

// HealthSummary is a per-service counter projection.
type HealthSummary struct {
	Revision        int64  `json:"revision"`
	ServiceName     string `json:"serviceName"`
	Total           int    `json:"total"`
	Healthy         int    `json:"healthy"`
	Suspect         int    `json:"suspect"`
	Unhealthy       int    `json:"unhealthy"`
	Unknown         int    `json:"unknown"`
	Draining        int    `json:"draining"`
	Available       int    `json:"available"`
	AvailableWeight int64  `json:"availableWeight"`
	TotalWeight     int64  `json:"totalWeight"`
}

// WeightAllocation contains every configured instance, including excluded
// instances. SharePPM is normalized only over available (HEALTHY) instances.
type WeightAllocation struct {
	Revision    int64  `json:"revision"`
	ServiceName string `json:"serviceName"`
	InstanceID  string `json:"instanceId"`
	Weight      int    `json:"weight"`
	Available   bool   `json:"available"`
	SharePPM    int64  `json:"sharePpm"`
}

// ProbePlan is a derived reference to instance fields and lease state.
type ProbePlan struct {
	Revision          int64     `json:"revision"`
	ServiceName       string    `json:"serviceName"`
	InstanceID        string    `json:"instanceId"`
	Endpoint          string    `json:"endpoint"`
	Probe             ProbeSpec `json:"probe"`
	Priority          int       `json:"priority"`
	LeaseTTLMS        int64     `json:"leaseTtlMs"`
	LeaseExpiresAtMS  int64     `json:"leaseExpiresAtMs"`
	LeaseRemainingMS  int64     `json:"leaseRemainingMs"`
	ConfigFingerprint string    `json:"configFingerprint"`
	HealthState       State     `json:"healthState"`
}

// DerivedView is the atomically rebuilt projection set for a revision.
type DerivedView struct {
	Revision        int64              `json:"revision"`
	SnapshotVersion int64              `json:"snapshotVersion"`
	Status          string             `json:"status"`
	HealthSummary   []HealthSummary    `json:"healthSummary"`
	Weights         []WeightAllocation `json:"weights"`
	ProbePlans      []ProbePlan        `json:"probePlans"`
}

// HeartbeatRequest reports a real timestamp for a lease-backed instance.
type HeartbeatRequest struct {
	RequestID  string `json:"requestId,omitempty"`
	ObservedAt string `json:"observedAt,omitempty"`
}

// HeartbeatResult describes the committed lease projection.
type HeartbeatResult struct {
	ServiceName     string `json:"serviceName"`
	InstanceID      string `json:"instanceId"`
	LastHeartbeatMS int64  `json:"lastHeartbeatMs"`
	ExpiresAtMS     int64  `json:"expiresAtMs"`
	DerivedRevision int64  `json:"derivedRevision"`
}

// SetStateRequest drives the explicit state machine. Threshold-driven probe
// transitions use the same validator internally.
type SetStateRequest struct {
	RequestID string `json:"requestId,omitempty"`
	State     State  `json:"state"`
	Reason    string `json:"reason,omitempty"`
}

// StateResult reports a committed explicit transition.
type StateResult struct {
	ServiceName     string `json:"serviceName"`
	InstanceID      string `json:"instanceId"`
	From            State  `json:"from"`
	To              State  `json:"to"`
	DerivedRevision int64  `json:"derivedRevision"`
}

// RoundOptions controls all three scheduler budgets.
type RoundOptions struct {
	RequestID             string `json:"requestId,omitempty"`
	GlobalConcurrency     int    `json:"globalConcurrency"`
	PerServiceConcurrency int    `json:"perServiceConcurrency"`
	BudgetMS              int64  `json:"budgetMs"`
	WaitQueueCapacity     int    `json:"waitQueueCapacity"`
}

// ProbeOutcome is the real result of one probe execution.
type ProbeOutcome struct {
	ServiceName string    `json:"serviceName"`
	InstanceID  string    `json:"instanceId"`
	Kind        ProbeKind `json:"kind"`
	OK          bool      `json:"ok"`
	Code        string    `json:"code"`
	LatencyMS   int64     `json:"latencyMs"`
	ObservedAt  string    `json:"observedAt"`
	Detail      string    `json:"detail,omitempty"`
}

// DeferredTask explains a deterministic postponement.
type DeferredTask struct {
	Rank              int       `json:"rank"`
	ServiceName       string    `json:"serviceName"`
	InstanceID        string    `json:"instanceId"`
	Kind              ProbeKind `json:"kind"`
	Priority          int       `json:"priority"`
	AdmissionIndex    int       `json:"admissionIndex"`
	Reason            string    `json:"reason"`
	ConsumedBudgetMS  int64     `json:"consumedBudgetMs"`
	RemainingBudgetMS int64     `json:"remainingBudgetMs"`
}

// RoundResult is both the API response and the durable round verdict.
type RoundResult struct {
	RoundID          string         `json:"roundId"`
	SnapshotVersion  int64          `json:"snapshotVersion"`
	DerivedRevision  int64          `json:"derivedRevision"`
	Status           string         `json:"status"`
	Started          int            `json:"started"`
	Completed        int            `json:"completed"`
	Deferred         []DeferredTask `json:"deferred"`
	Outcomes         []ProbeOutcome `json:"outcomes"`
	BudgetMS         int64          `json:"budgetMs"`
	ElapsedMS        int64          `json:"elapsedMs"`
	IdempotentReplay bool           `json:"idempotentReplay,omitempty"`
}

// IntentView exposes the write-ahead intent journal for diagnostics.
type IntentView struct {
	ID              string `json:"id"`
	Kind            string `json:"kind"`
	RequestID       string `json:"requestId,omitempty"`
	Status          string `json:"status"`
	TargetVersion   int64  `json:"targetVersion,omitempty"`
	DerivedRevision int64  `json:"derivedRevision,omitempty"`
	RoundID         string `json:"roundId,omitempty"`
	CreatedAt       string `json:"createdAt"`
	UpdatedAt       string `json:"updatedAt"`
}

// RecoveryReport is persisted after scanning unfinished intents. It contains
// only facts that were present on disk when recovery ran.
type RecoveryReport struct {
	RecoveredAt               string   `json:"recoveredAt"`
	CurrentVersion            int64    `json:"currentVersion"`
	CurrentDerivedRevision    int64    `json:"currentDerivedRevision"`
	ScannedIntents            int      `json:"scannedIntents"`
	AbortedIntents            int      `json:"abortedIntents"`
	ResetProbeResults         int      `json:"resetProbeResults"`
	DiscardedDerivedRevisions int      `json:"discardedDerivedRevisions"`
	DiscardedStagedSnapshots  int      `json:"discardedStagedSnapshots"`
	Details                   []string `json:"details"`
}

var identityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// Validate checks a complete instance declaration without mutating state.
func (in Instance) Validate() error {
	if !identityPattern.MatchString(in.ServiceName) {
		return fmt.Errorf("serviceName must match %s", identityPattern.String())
	}
	if !identityPattern.MatchString(in.InstanceID) {
		return fmt.Errorf("instanceId must match %s", identityPattern.String())
	}
	if in.Weight < 1 || in.Weight > 1000 {
		return fmt.Errorf("weight must be in [1,1000]")
	}
	if in.LeaseTTLMS < 1 || in.LeaseTTLMS > 86_400_000 {
		return fmt.Errorf("leaseTtlMs must be in [1,86400000]")
	}
	if in.Priority < 0 || in.Priority > 1000 {
		return fmt.Errorf("priority must be in [0,1000]")
	}
	if in.SuccessThreshold < 1 || in.SuccessThreshold > 100 {
		return fmt.Errorf("successThreshold must be in [1,100]")
	}
	if in.FailureThreshold < 1 || in.FailureThreshold > 100 {
		return fmt.Errorf("failureThreshold must be in [1,100]")
	}
	if in.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	switch in.Probe.Kind {
	case ProbeHTTP:
		u, err := url.Parse(in.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("HTTP endpoint must be an absolute http(s) URL")
		}
		if in.Probe.ExpectedStatus < 100 || in.Probe.ExpectedStatus > 599 {
			return fmt.Errorf("HTTP expectedStatus must be in [100,599]")
		}
		if strings.TrimSpace(in.Probe.ExpectedMarker) == "" {
			return fmt.Errorf("HTTP expectedMarker is required")
		}
	case ProbeTCP:
		if _, _, err := net.SplitHostPort(in.Endpoint); err != nil {
			return fmt.Errorf("TCP endpoint must be host:port: %w", err)
		}
	case ProbeLease:
		if strings.TrimSpace(in.Endpoint) == "" {
			return fmt.Errorf("LEASE endpoint is required")
		}
	default:
		return fmt.Errorf("probe.kind must be HTTP, TCP, or LEASE")
	}
	if in.Probe.TimeoutMS < 1 || in.Probe.TimeoutMS > 300_000 {
		return fmt.Errorf("probe.timeoutMs must be in [1,300000]")
	}
	return nil
}

// ValidateSnapshot validates every declaration and identity uniqueness.
func (s SnapshotRequest) Validate() error {
	if s.Version < 1 {
		return fmt.Errorf("version must be positive")
	}
	if _, err := time.Parse(time.RFC3339Nano, s.GeneratedAt); err != nil {
		return fmt.Errorf("generatedAt must be RFC3339: %w", err)
	}
	seen := make(map[string]struct{}, len(s.Instances))
	for i, in := range s.Instances {
		if err := in.Validate(); err != nil {
			return fmt.Errorf("instances[%d]: %w", i, err)
		}
		key := in.ServiceName + "\x00" + in.InstanceID
		if _, exists := seen[key]; exists {
			return fmt.Errorf("instances[%d]: duplicate %s/%s", i, in.ServiceName, in.InstanceID)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// SortedInstances returns a canonical service-name/instance-id ordering.
func SortedInstances(in []Instance) []Instance {
	out := append([]Instance(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].ServiceName != out[j].ServiceName {
			return out[i].ServiceName < out[j].ServiceName
		}
		return out[i].InstanceID < out[j].InstanceID
	})
	return out
}

// ConfigFingerprint hashes only registration fields whose changes invalidate
// derived structures. Health counters and heartbeats are intentionally absent.
func ConfigFingerprint(in Instance) string {
	b, _ := json.Marshal(struct {
		ServiceName      string    `json:"serviceName"`
		InstanceID       string    `json:"instanceId"`
		Endpoint         string    `json:"endpoint"`
		Weight           int       `json:"weight"`
		LeaseTTLMS       int64     `json:"leaseTtlMs"`
		Probe            ProbeSpec `json:"probe"`
		Priority         int       `json:"priority"`
		SuccessThreshold int       `json:"successThreshold"`
		FailureThreshold int       `json:"failureThreshold"`
	}{in.ServiceName, in.InstanceID, in.Endpoint, in.Weight, in.LeaseTTLMS, in.Probe, in.Priority, in.SuccessThreshold, in.FailureThreshold})
	return sha256Hex(b)
}
