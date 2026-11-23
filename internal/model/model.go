package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

const (
	FormatBinary = "binary"
	FormatJSONL  = "jsonl"
	FormatAuto   = "auto"

	AlignmentNone = "none"
	AlignmentUp   = "up"
	AlignmentDown = "down"

	TruncationFloor   = "floor"
	TruncationCeil    = "ceil"
	TruncationNearest = "nearest"
)

// Presence records whether an optional source field was explicitly present.
type Presence struct {
	RateLimit       bool `json:"rateLimit"`
	Timeout         bool `json:"timeout"`
	CircuitWindow   bool `json:"circuitWindow"`
	RateWindow      bool `json:"rateWindow"`
	CapturedAt      bool `json:"capturedAt"`
	WindowAlignment bool `json:"windowAlignment"`
}

// RouteMatch is the policy's virtual-service routing predicate.
type RouteMatch struct {
	Method     string            `json:"method"`
	PathPrefix string            `json:"pathPrefix"`
	Headers    map[string]string `json:"headers,omitempty"`
}

// Rounding describes microsecond-to-millisecond timestamp handling.
type Rounding struct {
	Rule                 string `json:"rule"`
	SourceTimestampUs    int64  `json:"sourceTimestampUs"`
	RemainderUs          int64  `json:"remainderUs"`
	RoundingBit          int    `json:"roundingBit"`
	ErrorUs              int64  `json:"errorUs"`
	CumulativeErrorUs    int64  `json:"cumulativeErrorUs"`
	ConvertedTimestampMs int64  `json:"convertedTimestampMs"`
}

// RawPolicy is the lossless, source-unit policy record produced by a codec.
type RawPolicy struct {
	PolicyID          string            `json:"policyId"`
	VirtualServiceID  string            `json:"virtualServiceId"`
	PolicyRevision    uint64            `json:"policyRevision"`
	LastWriteMs       int64             `json:"lastWriteMs"`
	Match             RouteMatch        `json:"match"`
	RateLimitValue    uint64            `json:"rateLimitValue"`
	RateLimitUnit     string            `json:"rateLimitUnit"`
	TimeoutValue      uint64            `json:"timeoutValue"`
	TimeoutUnit       string            `json:"timeoutUnit"`
	CircuitWindowVal  uint64            `json:"circuitWindowValue"`
	CircuitWindowUnit string            `json:"circuitWindowUnit"`
	RateWindowValue   uint64            `json:"rateWindowValue"`
	RateWindowUnit    string            `json:"rateWindowUnit"`
	CapturedAtValue   int64             `json:"capturedAtValue"`
	CapturedAtUnit    string            `json:"capturedAtUnit"`
	WindowAlignment   string            `json:"windowAlignment"`
	Presence          Presence          `json:"presence"`
	SourceOffset      int64             `json:"sourceOffset"`
	SourceLine        int               `json:"sourceLine"`
	ExtraFields       map[string]string `json:"extraFields,omitempty"`
}

// ParsedSnapshot is a validated, integrity-checked source snapshot before
// compatibility and unit normalization.
type ParsedSnapshot struct {
	SnapshotVersion  uint64
	SnapshotID       string
	GeneratedAtMs    int64
	SourceFormat     string
	FormatVersion    int
	PolicyCount      int
	SourceDigest     string
	Policies         []RawPolicy
	UnknownFields    []string
	RequiredFeatures []string
}

// Policy is the normalized internal model.
type Policy struct {
	PolicyID               string     `json:"policyId"`
	VirtualServiceID       string     `json:"virtualServiceId"`
	PolicyRevision         uint64     `json:"policyRevision"`
	LastWriteMs            int64      `json:"lastWriteMs"`
	Match                  RouteMatch `json:"match"`
	RateLimitBps           uint64     `json:"rateLimitBps"`
	TimeoutMs              uint64     `json:"timeoutMs"`
	CircuitWindowMs        uint64     `json:"circuitWindowMs"`
	RateWindowMs           uint64     `json:"rateWindowMs"`
	CapturedAtMs           int64      `json:"capturedAtMs"`
	WindowAlignment        string     `json:"windowAlignment"`
	RateWindowStartMs      int64      `json:"rateWindowStartMs"`
	RateWindowEndMs        int64      `json:"rateWindowEndMs"`
	Rounding               Rounding   `json:"rounding"`
	DefaultsApplied        []string   `json:"defaultsApplied,omitempty"`
	SourceOffset           int64      `json:"sourceOffset,omitempty"`
	SourceLine             int        `json:"sourceLine,omitempty"`
	SourceOptionalPresence Presence   `json:"sourceOptionalPresence"`
}

// DefaultDetail records a compatibility downgrade action.
type DefaultDetail struct {
	PolicyID     string `json:"policyId"`
	Field        string `json:"field"`
	DefaultValue any    `json:"defaultValue"`
	Reason       string `json:"reason"`
}

// Snapshot is a normalized policy snapshot.
type Snapshot struct {
	SnapshotVersion         uint64          `json:"snapshotVersion"`
	SnapshotID              string          `json:"snapshotId"`
	GeneratedAtMs           int64           `json:"generatedAtMs"`
	SourceFormat            string          `json:"sourceFormat"`
	FormatVersion           int             `json:"formatVersion"`
	SourceDigest            string          `json:"sourceDigest"`
	ContentDigest           string          `json:"contentDigest"`
	Policies                []Policy        `json:"policies"`
	DefaultDetails          []DefaultDetail `json:"defaultDetails,omitempty"`
	CumulativeRoundingError int64           `json:"cumulativeRoundingErrorUs"`
}

// CanonicalPolicy returns stable JSON for conflict tie-breaking and storage.
func CanonicalPolicy(p Policy) ([]byte, error) {
	return json.Marshal(p)
}

// SetContentDigest computes the digest over the normalized, sorted policy set.
func (s *Snapshot) SetContentDigest() error {
	sorted := append([]Policy(nil), s.Policies...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].PolicyID < sorted[j].PolicyID })
	payload := struct {
		SnapshotVersion uint64          `json:"snapshotVersion"`
		SnapshotID      string          `json:"snapshotId"`
		GeneratedAtMs   int64           `json:"generatedAtMs"`
		Policies        []Policy        `json:"policies"`
		Defaults        []DefaultDetail `json:"defaultDetails,omitempty"`
	}{
		SnapshotVersion: s.SnapshotVersion,
		SnapshotID:      s.SnapshotID,
		GeneratedAtMs:   s.GeneratedAtMs,
		Policies:        sorted,
		Defaults:        s.DefaultDetails,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(b)
	s.ContentDigest = "sha256:" + hex.EncodeToString(sum[:])
	return nil
}

// PolicyDigest returns a stable digest for a normalized policy.
func PolicyDigest(p Policy) string {
	b, _ := json.Marshal(p)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ConflictDigest intentionally excludes import-local metadata (source offsets,
// rounding audit fields and default-detail annotations) so equal conflict
// tie-breakers compare policy semantics, not the route by which they arrived.
func ConflictDigest(p Policy) string {
	v := struct {
		PolicyID         string     `json:"policyId"`
		VirtualServiceID string     `json:"virtualServiceId"`
		PolicyRevision   uint64     `json:"policyRevision"`
		LastWriteMs      int64      `json:"lastWriteMs"`
		Match            RouteMatch `json:"match"`
		RateLimitBps     uint64     `json:"rateLimitBps"`
		TimeoutMs        uint64     `json:"timeoutMs"`
		CircuitWindowMs  uint64     `json:"circuitWindowMs"`
		RateWindowMs     uint64     `json:"rateWindowMs"`
		WindowAlignment  string     `json:"windowAlignment"`
	}{
		PolicyID: p.PolicyID, VirtualServiceID: p.VirtualServiceID,
		PolicyRevision: p.PolicyRevision, LastWriteMs: p.LastWriteMs, Match: p.Match,
		RateLimitBps: p.RateLimitBps, TimeoutMs: p.TimeoutMs,
		CircuitWindowMs: p.CircuitWindowMs, RateWindowMs: p.RateWindowMs,
		WindowAlignment: p.WindowAlignment,
	}
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// DuplicatePolicyIDs returns duplicate identifiers in deterministic order.
func DuplicatePolicyIDs(policies []Policy) []string {
	seen := make(map[string]int, len(policies))
	for _, p := range policies {
		seen[p.PolicyID]++
	}
	out := make([]string, 0)
	for id, n := range seen {
		if n > 1 {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// Validate performs model-level checks shared by all import paths.
func (s Snapshot) Validate() error {
	if s.SnapshotVersion == 0 {
		return fmt.Errorf("snapshot version must be greater than zero")
	}
	if s.SnapshotID == "" {
		return fmt.Errorf("snapshot id is required")
	}
	for _, p := range s.Policies {
		if p.PolicyID == "" {
			return fmt.Errorf("policy id is required")
		}
		if p.VirtualServiceID == "" {
			return fmt.Errorf("policy %s: virtual service id is required", p.PolicyID)
		}
		if p.Match.PathPrefix == "" {
			return fmt.Errorf("policy %s: match.pathPrefix is required", p.PolicyID)
		}
		if p.RateWindowMs == 0 {
			return fmt.Errorf("policy %s: rate window must be positive", p.PolicyID)
		}
		if p.WindowAlignment != AlignmentNone && p.WindowAlignment != AlignmentUp && p.WindowAlignment != AlignmentDown {
			return fmt.Errorf("policy %s: invalid window alignment %q", p.PolicyID, p.WindowAlignment)
		}
	}
	return nil
}
