package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
)

type Phase string

const (
	PhaseGenerated Phase = "generated"
	PhaseActive    Phase = "active"
	PhaseRotated   Phase = "rotated"
	PhaseArchived  Phase = "archived"
	PhaseDestroyed Phase = "destroyed"
)

var phaseOrder = map[Phase]int{
	PhaseGenerated: 0,
	PhaseActive:    1,
	PhaseRotated:   2,
	PhaseArchived:  3,
	PhaseDestroyed: 4,
}

func PhaseRank(p Phase) int {
	rank, ok := phaseOrder[p]
	if !ok {
		return -1
	}
	return rank
}

func NextPhase(p Phase) (Phase, bool) {
	switch p {
	case PhaseGenerated:
		return PhaseActive, true
	case PhaseActive:
		return PhaseRotated, true
	case PhaseRotated:
		return PhaseArchived, true
	case PhaseArchived:
		return PhaseDestroyed, true
	default:
		return "", false
	}
}

func ExpectedPhase(operation string) (Phase, bool) {
	switch strings.ToLower(operation) {
	case "activate":
		return PhaseGenerated, true
	case "rotate", "use":
		return PhaseActive, true
	case "archive":
		return PhaseRotated, true
	case "destroy":
		return PhaseArchived, true
	default:
		return "", false
	}
}

type AlgorithmSpec struct {
	Name     string `json:"name"`
	Family   string `json:"family"`
	Strength int    `json:"strength_bits"`
}

var algorithms = map[string]AlgorithmSpec{
	"aes-128-gcm":          {Name: "aes-128-gcm", Family: "AES", Strength: 128},
	"aes-256-gcm":          {Name: "aes-256-gcm", Family: "AES", Strength: 256},
	"chacha20-poly1305":    {Name: "chacha20-poly1305", Family: "CHACHA20", Strength: 256},
	"ecdsa-p256-sha256":    {Name: "ecdsa-p256-sha256", Family: "ECDSA", Strength: 256},
	"rsa-2048-oaep-sha256": {Name: "rsa-2048-oaep-sha256", Family: "RSA", Strength: 112},
}

func LookupAlgorithm(name string) (AlgorithmSpec, bool) {
	spec, ok := algorithms[strings.ToLower(strings.TrimSpace(name))]
	return spec, ok
}

// ResolveStrength validates a caller-provided strength. A lower requested
// strength is preserved so policy checks can return E_WEAK_ALGO, while an
// impossible upgrade is a configuration error.
func ResolveStrength(spec AlgorithmSpec, requested int) (int, error) {
	if requested <= 0 {
		return spec.Strength, nil
	}
	if requested > spec.Strength {
		return 0, NewError(CodeConfig, "requested strength %d exceeds %s maximum %d", requested, spec.Name, spec.Strength)
	}
	return requested, nil
}

type Key struct {
	ID               string       `json:"id"`
	Subject          string       `json:"subject"`
	Algorithm        string       `json:"algorithm"`
	Family           string       `json:"family"`
	StrengthBits     int          `json:"strength_bits"`
	CreatedAt        MonoTime     `json:"created_at"`
	ExpiresAt        MonoTime     `json:"expires_at"`
	RotationInterval MonoDuration `json:"rotation_interval"`
	Phase            Phase        `json:"phase"`
	ParentID         string       `json:"parent_id,omitempty"`
	SuccessorID      string       `json:"successor_id,omitempty"`
}

type SubjectPolicy struct {
	Subject            string       `json:"subject"`
	MinStrengthBits    int          `json:"min_strength_bits"`
	RotationInterval   MonoDuration `json:"rotation_interval"`
	DefaultLifetime    MonoDuration `json:"default_lifetime"`
	MaxPendingRequests int          `json:"max_pending_requests"`
}

type Audit struct {
	Seq             int64    `json:"seq"`
	EventID         string   `json:"event_id"`
	Subject         string   `json:"subject"`
	KeyID           string   `json:"key_id"`
	Action          string   `json:"action"`
	FromPhase       Phase    `json:"from_phase"`
	ToPhase         Phase    `json:"to_phase"`
	Code            Code     `json:"code"`
	At              MonoTime `json:"at"`
	SnapshotVersion int64    `json:"snapshot_version"`
	Detail          string   `json:"detail,omitempty"`
}

type StateSnapshot struct {
	Schema     string          `json:"schema"`
	Version    int64           `json:"version"`
	TimeSample string          `json:"time_sample"`
	Policies   []SubjectPolicy `json:"policies"`
	Keys       []Key           `json:"keys"`
	Audits     []Audit         `json:"audits"`
}

func NewStateSnapshot(version int64, policies []SubjectPolicy, keys []Key, audits []Audit) StateSnapshot {
	sort.Slice(policies, func(i, j int) bool { return policies[i].Subject < policies[j].Subject })
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Subject == keys[j].Subject {
			return keys[i].ID < keys[j].ID
		}
		return keys[i].Subject < keys[j].Subject
	})
	sort.Slice(audits, func(i, j int) bool {
		if audits[i].Seq == audits[j].Seq {
			return audits[i].EventID < audits[j].EventID
		}
		return audits[i].Seq < audits[j].Seq
	})
	return StateSnapshot{
		Schema:     "keylife.snapshot/v1",
		Version:    version,
		TimeSample: TimeSample,
		Policies:   policies,
		Keys:       keys,
		Audits:     audits,
	}
}

func CanonicalJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}

func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type KeyMetrics struct {
	Age               MonoDuration `json:"age"`
	LifetimeRemaining MonoDuration `json:"lifetime_remaining"`
	RotationDueAt     MonoTime     `json:"rotation_due_at"`
	RotationRemaining MonoDuration `json:"rotation_remaining"`
	RotationOverdue   bool         `json:"rotation_overdue"`
	Expired           bool         `json:"expired"`
}

func Metrics(key Key, now MonoTime) KeyMetrics {
	ageTicks := now.Tick - key.CreatedAt.Tick
	if ageTicks < 0 {
		ageTicks = 0
	}
	lifeTicks := key.ExpiresAt.Tick - now.Tick
	if lifeTicks < 0 {
		lifeTicks = 0
	}
	due := key.CreatedAt.Add(key.RotationInterval)
	rotationTicks := due.Tick - now.Tick
	overdue := rotationTicks < 0
	if rotationTicks < 0 {
		rotationTicks = 0
	}
	return KeyMetrics{
		Age:               MonoDuration(ageTicks),
		LifetimeRemaining: MonoDuration(lifeTicks),
		RotationDueAt:     due,
		RotationRemaining: MonoDuration(rotationTicks),
		RotationOverdue:   overdue,
		Expired:           now.Tick >= key.ExpiresAt.Tick,
	}
}

type DecisionRequest struct {
	RequestID    string `json:"request_id"`
	Subject      string `json:"subject"`
	KeyID        string `json:"key_id"`
	Actor        string `json:"actor"`
	Operation    string `json:"operation"`
	Algorithm    string `json:"algorithm,omitempty"`
	StrengthBits int    `json:"strength_bits,omitempty"`
}

type Check struct {
	Name   string `json:"name"`
	Code   Code   `json:"code"`
	Detail string `json:"detail,omitempty"`
}

type Decision struct {
	Sequence      int             `json:"sequence"`
	Request       DecisionRequest `json:"request"`
	At            MonoTime        `json:"at"`
	Code          Code            `json:"code"`
	Message       string          `json:"message"`
	KeyPhase      Phase           `json:"key_phase,omitempty"`
	PolicyMinBits int             `json:"policy_min_bits,omitempty"`
	EffectiveBits int             `json:"effective_bits,omitempty"`
	Metrics       *KeyMetrics     `json:"metrics,omitempty"`
	Checks        []Check         `json:"checks"`
}

type DecisionAudit struct {
	Sequence  int      `json:"sequence"`
	RequestID string   `json:"request_id"`
	Subject   string   `json:"subject"`
	KeyID     string   `json:"key_id"`
	Actor     string   `json:"actor"`
	Operation string   `json:"operation"`
	Code      Code     `json:"code"`
	At        MonoTime `json:"at"`
	Checks    []Check  `json:"checks"`
}
