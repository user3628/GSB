package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"replicationlag/internal/apperr"
)

// Observation is a normalized replica position snapshot at one sample time.
// FormatVersion and LastWriteMS are import metadata and do not change the
// position/lag semantics.
type Observation struct {
	ReplicaID       string  `json:"replica_id"`
	SampledAtMS     int64   `json:"sampled_at_ms"`
	GTID            GTIDSet `json:"gtid"`
	PositionName    string  `json:"position_name"`
	LagMS           int64   `json:"lag_ms"`
	ReportedVersion Version `json:"reported_version"`
	FormatVersion   int     `json:"format_version"`
	LastWriteMS     int64   `json:"last_write_ms"`
}

var replicaIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
var binlogPositionRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*:[0-9]+$`)

// PositionPolicy declares the accepted executed-position formats. Empty fields
// retain the built-in GTID-set and filename:offset syntaxes.
type PositionPolicy struct {
	Regex string `json:"regex,omitempty"`
}

func normalizePosition(raw string) (string, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return "", fmt.Errorf("position name is empty")
	}
	if set, err := ParseGTID(text); err == nil {
		return set.String(), nil
	}
	if binlogPositionRE.MatchString(text) {
		return text, nil
	}
	return "", fmt.Errorf("position name %q does not match a declared GTID-set or file:offset format", text)
}

// NormalizePosition canonicalizes a declared executed-position name.
func NormalizePosition(raw string, policy PositionPolicy) (string, error) {
	if policy.Regex != "" {
		re, err := regexp.Compile(policy.Regex)
		if err != nil {
			return "", fmt.Errorf("invalid position policy regex: %w", err)
		}
		if !re.MatchString(raw) {
			return "", fmt.Errorf("position name %q does not match configured pattern", raw)
		}
	}
	return normalizePosition(raw)
}

// Validate validates all required observation fields and attaches the replica
// identifier, sample time and failing field to every business error.
func (o Observation) Validate() error {
	if !replicaIDRE.MatchString(o.ReplicaID) {
		return invalid(o, "replica_id", "must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}")
	}
	if o.SampledAtMS <= 0 {
		return invalid(o, "sampled_at_ms", "must be a positive Unix millisecond timestamp")
	}
	if err := o.GTID.Validate(); err != nil {
		return invalid(o, "gtid", err.Error())
	}
	if _, err := normalizePosition(o.PositionName); err != nil {
		return invalid(o, "position_name", err.Error())
	}
	if o.LagMS < 0 {
		return invalid(o, "lag_ms", "must be a non-negative integer")
	}
	if !o.ReportedVersion.Valid() {
		return invalid(o, "reported_version", "must contain at least one numeric version segment")
	}
	if o.FormatVersion < 0 {
		return invalid(o, "format_version", "must be non-negative")
	}
	return nil
}

func invalid(o Observation, field, detail string) *apperr.Error {
	return apperr.New(apperr.CodeObservationInvalid, "observation field is invalid", map[string]any{
		"replica_id":    o.ReplicaID,
		"sampled_at_ms": o.SampledAtMS,
		"field":         field,
		"detail":        detail,
	})
}

// ContentFingerprint hashes semantics that must be identical for duplicate
// sample-time observations. Conflict metadata is intentionally excluded.
func (o Observation) ContentFingerprint() string {
	payload := struct {
		ReplicaID    string  `json:"replica_id"`
		SampledAtMS  int64   `json:"sampled_at_ms"`
		GTID         string  `json:"gtid"`
		PositionName string  `json:"position_name"`
		LagMS        int64   `json:"lag_ms"`
		Version      Version `json:"reported_version"`
	}{
		ReplicaID:    o.ReplicaID,
		SampledAtMS:  o.SampledAtMS,
		GTID:         o.GTID.Normalized().String(),
		PositionName: o.PositionName,
		LagMS:        o.LagMS,
		Version:      o.ReportedVersion,
	}
	data, _ := json.Marshal(payload)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ObservationView is the stable HTTP/storage representation.
type ObservationView struct {
	ReplicaID       string      `json:"replica_id"`
	SampledAtMS     int64       `json:"sampled_at_ms"`
	GTID            string      `json:"gtid"`
	GTIDIntervals   []SourceSet `json:"gtid_intervals"`
	PositionName    string      `json:"position_name"`
	LagMS           int64       `json:"lag_ms"`
	ReportedVersion string      `json:"reported_version"`
	FormatVersion   int         `json:"format_version"`
	LastWriteMS     int64       `json:"last_write_ms"`
	Fingerprint     string      `json:"fingerprint,omitempty"`
}

func (o Observation) View() ObservationView {
	return ObservationView{
		ReplicaID:       o.ReplicaID,
		SampledAtMS:     o.SampledAtMS,
		GTID:            o.GTID.Normalized().String(),
		GTIDIntervals:   o.GTID.Normalized().Sources,
		PositionName:    o.PositionName,
		LagMS:           o.LagMS,
		ReportedVersion: o.ReportedVersion.String(),
		FormatVersion:   o.FormatVersion,
		LastWriteMS:     o.LastWriteMS,
		Fingerprint:     o.ContentFingerprint(),
	}
}

// Policy drives delay grades and the replica state machine.
type Policy struct {
	LagWarnMS            int64 `json:"lag_warn_ms"`
	LagCriticalMS        int64 `json:"lag_critical_ms"`
	StallThreshold       int   `json:"stall_threshold"`
	RecoveryObservations int   `json:"recovery_observations"`
}

func DefaultPolicy() Policy {
	return Policy{LagWarnMS: 1000, LagCriticalMS: 5000, StallThreshold: 3, RecoveryObservations: 2}
}

func (p Policy) Validate() error {
	if p.LagWarnMS < 0 || p.LagCriticalMS <= p.LagWarnMS {
		return fmt.Errorf("lag_critical_ms must be greater than non-negative lag_warn_ms")
	}
	if p.StallThreshold < 1 {
		return fmt.Errorf("stall_threshold must be at least 1")
	}
	if p.RecoveryObservations < 1 {
		return fmt.Errorf("recovery_observations must be at least 1")
	}
	return nil
}
