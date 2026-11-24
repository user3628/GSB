// Package compat implements explicit, configuration-driven format/version
// compatibility decisions. Parsers never silently accept a historical syntax.
package compat

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"replicationlag/internal/apperr"
)

type Decision string

const (
	DecisionAccept  Decision = "ACCEPT"
	DecisionDegrade Decision = "DEGRADE"
	DecisionReject  Decision = "REJECT"
)

type TimePrecision string

const (
	PrecisionMS TimePrecision = "ms"
	PrecisionS  TimePrecision = "s"
	PrecisionUS TimePrecision = "us"
	PrecisionNS TimePrecision = "ns"
)

// Rule is the complete compatibility policy for one format/version pair.
type Rule struct {
	Decision              Decision      `json:"decision"`
	DefaultVersion        string        `json:"default_version,omitempty"`
	TimePrecision         TimePrecision `json:"time_precision,omitempty"`
	AllowTildeRanges      bool          `json:"allow_tilde_ranges,omitempty"`
	AllowIntegerShorthand bool          `json:"allow_integer_shorthand,omitempty"`
	DefaultGTIDSource     string        `json:"default_gtid_source,omitempty"`
	AllowMissingVersion   bool          `json:"allow_missing_version,omitempty"`
	PositionRegex         string        `json:"position_regex,omitempty"`
}

// Matrix maps detected formats and versions to decisions.
type Matrix struct {
	Rules               map[string]Rule `json:"rules"`
	Default             Rule            `json:"default"`
	KnownRequiredFields []string        `json:"known_required_fields,omitempty"`
}

func key(format string, version int) string {
	return fmt.Sprintf("%s/%d", strings.ToLower(strings.TrimSpace(format)), version)
}

// Default returns the built-in current-format matrix. Historical compatibility
// is intentionally opt-in through a sample config file.
func Default() Matrix {
	accept := Rule{Decision: DecisionAccept, TimePrecision: PrecisionMS}
	degradeBinary0 := Rule{
		Decision:              DecisionDegrade,
		DefaultVersion:        "5.7.0",
		TimePrecision:         PrecisionS,
		AllowTildeRanges:      true,
		AllowIntegerShorthand: true,
		AllowMissingVersion:   true,
	}
	degradeJSON0 := degradeBinary0
	return Matrix{
		Rules: map[string]Rule{
			"jsonl/1":  accept,
			"binary/1": accept,
			"jsonl/0":  degradeJSON0,
			"binary/0": degradeBinary0,
		},
		Default: Rule{Decision: DecisionReject},
		KnownRequiredFields: []string{
			"replica_id", "sampled_at_ms", "gtid_segments", "gtid",
			"position_name", "executed_position", "lag_ms",
			"reported_version", "last_write_ms",
		},
	}
}

// Load reads a matrix file. Empty path returns the built-in matrix.
func Load(path string) (Matrix, error) {
	if strings.TrimSpace(path) == "" {
		matrix := Default()
		if err := matrix.Validate(); err != nil {
			return Matrix{}, err
		}
		return matrix, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Matrix{}, apperr.Wrap(apperr.CodeBadRequest, "cannot read compatibility matrix", map[string]any{"path": path}, err)
	}
	var matrix Matrix
	if err := json.Unmarshal(data, &matrix); err != nil {
		return Matrix{}, apperr.Wrap(apperr.CodeBadRequest, "cannot parse compatibility matrix", map[string]any{"path": path}, err)
	}
	if matrix.Rules == nil {
		matrix.Rules = Default().Rules
	}
	if matrix.KnownRequiredFields == nil {
		matrix.KnownRequiredFields = Default().KnownRequiredFields
	}
	if err := matrix.Validate(); err != nil {
		return Matrix{}, apperr.New(apperr.CodeBadRequest, "compatibility matrix is invalid", map[string]any{"path": path, "detail": err.Error()})
	}
	return matrix, nil
}

func (m Matrix) Validate() error {
	for name, rule := range m.Rules {
		if !strings.Contains(name, "/") {
			return fmt.Errorf("rule key %q must be format/version", name)
		}
		if err := validateRule(rule); err != nil {
			return fmt.Errorf("rule %s: %w", name, err)
		}
	}
	if m.Default.Decision == "" {
		m.Default.Decision = DecisionReject
	}
	if err := validateRule(m.Default); err != nil {
		return fmt.Errorf("default rule: %w", err)
	}
	return nil
}

func validateRule(rule Rule) error {
	switch rule.Decision {
	case DecisionAccept, DecisionDegrade, DecisionReject:
	default:
		return fmt.Errorf("decision %q must be ACCEPT, DEGRADE or REJECT", rule.Decision)
	}
	if rule.TimePrecision != "" {
		switch rule.TimePrecision {
		case PrecisionMS, PrecisionS, PrecisionUS, PrecisionNS:
		default:
			return fmt.Errorf("unsupported time precision %q", rule.TimePrecision)
		}
	}
	if rule.AllowIntegerShorthand && strings.TrimSpace(rule.DefaultGTIDSource) == "" {
		// A source may still be supplied by the record. This is not an error.
	}
	return nil
}

// Decide resolves a format/version decision and checks required fields. Unknown
// required fields always reject with E_FORMAT_UNSUPPORTED, even if the base
// matrix accepts the version.
func (m Matrix) Decide(format string, version int, requiredFields []string) (Rule, *apperr.Error) {
	unknown := m.UnknownRequiredFields(requiredFields)
	if len(unknown) > 0 {
		return Rule{}, apperr.New(apperr.CodeFormatUnsupported, "input requires unknown mandatory fields", map[string]any{
			"format":          format,
			"format_version":  version,
			"required_fields": unknown,
		})
	}
	rule, ok := m.Rules[key(format, version)]
	if !ok {
		rule = m.Default
	}
	if rule.TimePrecision == "" {
		rule.TimePrecision = PrecisionMS
	}
	if rule.Decision == DecisionReject {
		return rule, apperr.New(apperr.CodeFormatUnsupported, "format version is rejected by compatibility matrix", map[string]any{
			"format":         format,
			"format_version": version,
			"decision":       rule.Decision,
		})
	}
	return rule, nil
}

func (m Matrix) UnknownRequiredFields(required []string) []string {
	known := map[string]bool{}
	for _, field := range m.KnownRequiredFields {
		known[strings.ToLower(field)] = true
	}
	unknown := make([]string, 0)
	seen := map[string]bool{}
	for _, field := range required {
		field = strings.ToLower(strings.TrimSpace(field))
		if field != "" && !known[field] && !seen[field] {
			unknown = append(unknown, field)
			seen[field] = true
		}
	}
	sort.Strings(unknown)
	return unknown
}

// TimestampToMS converts declared time precision to Unix milliseconds and
// reports whether lossy truncation occurred.
func TimestampToMS(value int64, precision TimePrecision) (int64, bool, error) {
	if value < 0 {
		return 0, false, fmt.Errorf("timestamp must be non-negative")
	}
	switch precision {
	case "", PrecisionMS:
		return value, false, nil
	case PrecisionS:
		if value > (1<<63-1)/1000 {
			return 0, false, fmt.Errorf("timestamp overflows milliseconds")
		}
		return value * 1000, false, nil
	case PrecisionUS:
		return value / 1000, value%1000 != 0, nil
	case PrecisionNS:
		return value / 1_000_000, value%1_000_000 != 0, nil
	default:
		return 0, false, fmt.Errorf("unsupported time precision %q", precision)
	}
}

// Description provides a stable import report record.
func (r Rule) Description(format string, version int) string {
	return fmt.Sprintf("%s/%d %s", format, version, r.Decision)
}
