package model

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
)

const (
	CodeSpecInvalid            = "E_SPEC_INVALID"
	CodeDependencyCycle        = "E_DEPENDENCY_CYCLE"
	CodeStaleBase              = "E_STALE_BASE"
	CodeStaleVersion           = "E_STALE_VERSION"
	CodeConflictingRequest     = "E_CONFLICTING_REQUEST"
	CodeVersionMismatch        = "E_VERSION_MISMATCH"
	CodeProbeTimeout           = "E_PROBE_TIMEOUT"
	CodeRecoverTimeout         = "E_RECOVER_TIMEOUT"
	CodeProbeConnectionRefused = "E_PROBE_CONNECTION_REFUSED"
	CodeRecoverConnectionRef   = "E_RECOVER_CONNECTION_REFUSED"
	CodeProbeRejected          = "E_PROBE_REJECTED"
	CodeRecoverRejected        = "E_RECOVER_REJECTED"
	CodeProbeNetwork           = "E_PROBE_NETWORK"
	CodeRecoverNetwork         = "E_RECOVER_NETWORK"
	CodeNotFound               = "E_NOT_FOUND"
	CodeInvalidRequest         = "E_INVALID_REQUEST"
	CodeProcessInterrupted     = "E_PROCESS_INTERRUPTED"
	CodeInternal               = "E_INTERNAL"
)

type BusinessError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
	Status  int            `json:"-"`
	Partial any            `json:"-"`
}

func (e *BusinessError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewError(code, message string, status int) *BusinessError {
	return &BusinessError{Code: code, Message: message, Status: status}
}

func (e *BusinessError) WithContext(key string, value any) *BusinessError {
	if e.Context == nil {
		e.Context = map[string]any{}
	}
	e.Context[key] = value
	return e
}

func (e *BusinessError) WithPartial(partial any) *BusinessError {
	e.Partial = partial
	return e
}

type Resource struct {
	CPUMillicores int64 `json:"cpu_millicores"`
	MemoryBytes   int64 `json:"memory_bytes"`
}

type HealthProbe struct {
	Type           string `json:"type"`
	URL            string `json:"url"`
	Method         string `json:"method,omitempty"`
	TimeoutMs      int64  `json:"timeout_ms"`
	ExpectedStatus int    `json:"expected_status,omitempty"`
}

type RecoveryRequest struct {
	URL       string `json:"url"`
	Method    string `json:"method,omitempty"`
	TimeoutMs int64  `json:"timeout_ms"`
	Body      string `json:"body,omitempty"`
}

type Component struct {
	ID                        string          `json:"id"`
	Resources                 Resource        `json:"resources"`
	Capacity                  Resource        `json:"capacity"`
	HealthProbe               HealthProbe     `json:"health_probe"`
	RecoveryRequest           RecoveryRequest `json:"recovery_request"`
	HealthWeightPermille      int64           `json:"health_weight_permille"`
	ResourceWeightPermille    int64           `json:"resource_weight_permille"`
	ConsistencyWeightPermille int64           `json:"consistency_weight_permille"`
	DependsOn                 []string        `json:"depends_on"`
}

type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (e Edge) Key() string {
	return e.From + "->" + e.To
}

type Observation struct {
	State      string `json:"state"`
	ObservedAt string `json:"observed_at,omitempty"`
}

type ConsistencyConfig struct {
	ThresholdPermille    int64 `json:"threshold_permille"`
	RelativeTolerancePPM int64 `json:"relative_tolerance_ppm"`
}

type Snapshot struct {
	Version           int64                  `json:"version"`
	GeneratedAt       string                 `json:"generated_at"`
	ChecksumSHA256    string                 `json:"checksum_sha256"`
	Components        []Component            `json:"components"`
	Edges             []Edge                 `json:"edges"`
	Observations      map[string]Observation `json:"observations"`
	ConsistencyConfig ConsistencyConfig      `json:"consistency_config"`
}

func (s Snapshot) Clone() Snapshot {
	out := s
	out.Components = make([]Component, len(s.Components))
	for i := range s.Components {
		out.Components[i] = s.Components[i].Clone()
	}
	out.Edges = append([]Edge(nil), s.Edges...)
	out.Observations = make(map[string]Observation, len(s.Observations))
	for k, v := range s.Observations {
		out.Observations[k] = v
	}
	return out
}

func (c Component) Clone() Component {
	out := c
	out.DependsOn = append([]string(nil), c.DependsOn...)
	return out
}

func (s Snapshot) ComponentMap() map[string]Component {
	out := make(map[string]Component, len(s.Components))
	for _, c := range s.Components {
		out[c.ID] = c
	}
	return out
}

func (s Snapshot) SortedComponents() []Component {
	out := make([]Component, len(s.Components))
	copy(out, s.Components)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s Snapshot) SortedEdges() []Edge {
	out := make([]Edge, len(s.Edges))
	copy(out, s.Edges)
	sort.Slice(out, func(i, j int) bool {
		if out[i].From == out[j].From {
			return out[i].To < out[j].To
		}
		return out[i].From < out[j].From
	})
	return out
}

type DuplicateComponent struct {
	ID             string `json:"id"`
	FirstIndex     int    `json:"first_index"`
	DuplicateIndex int    `json:"duplicate_index"`
}

type NormalizedSnapshot struct {
	Snapshot   Snapshot
	Duplicates []DuplicateComponent
}

type IncrementOperation struct {
	Type            string           `json:"type"`
	ComponentID     string           `json:"component_id,omitempty"`
	Component       *Component       `json:"component,omitempty"`
	Resources       *Resource        `json:"resources,omitempty"`
	Capacity        *Resource        `json:"capacity,omitempty"`
	HealthProbe     *HealthProbe     `json:"health_probe,omitempty"`
	RecoveryRequest *RecoveryRequest `json:"recovery_request,omitempty"`
	Observation     *Observation     `json:"observation,omitempty"`
	From            string           `json:"from,omitempty"`
	To              string           `json:"to,omitempty"`
}

type IncrementRequest struct {
	IdempotencyKey string               `json:"idempotency_key"`
	BaseVersion    int64                `json:"base_version"`
	GeneratedAt    string               `json:"generated_at"`
	Note           string               `json:"note,omitempty"`
	Operations     []IncrementOperation `json:"operations"`
}

type FullSnapshotRequest struct {
	IdempotencyKey string   `json:"idempotency_key"`
	Snapshot       Snapshot `json:"snapshot"`
}

type DrillRequest struct {
	IdempotencyKey   string   `json:"idempotency_key"`
	Version          int64    `json:"version"`
	FailedComponents []string `json:"failed_components"`
	MaxConcurrency   int      `json:"max_concurrency,omitempty"`
	Note             string   `json:"note,omitempty"`
}

func IsLoopbackURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if host == "localhost" || host == "::1" {
		return true
	}
	return strings.HasPrefix(host, "127.")
}

func IsHTTPURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}
