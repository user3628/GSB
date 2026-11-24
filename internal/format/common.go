// Package format implements the interoperable JSON Lines and compact binary
// observation persistence formats.
package format

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"replicationlag/internal/apperr"
	"replicationlag/internal/compat"
	"replicationlag/internal/domain"
)

const (
	JSONLFormat          = "jsonl"
	BinaryFormat         = "binary"
	BinaryMagic          = "RLAG"
	CurrentJSONLVersion  = 1
	CurrentBinaryVersion = 1
)

type Degradation struct {
	Record int    `json:"record"`
	Field  string `json:"field"`
	Detail string `json:"detail"`
}

type ImportResult struct {
	Format         string               `json:"format"`
	FormatVersion  int                  `json:"format_version"`
	Decision       compat.Decision      `json:"decision"`
	Observations   []domain.Observation `json:"-"`
	Degradations   []Degradation        `json:"degradations"`
	RequiredFields []string             `json:"required_fields,omitempty"`
	SourceBytes    int                  `json:"source_bytes"`
}

// Detect identifies a format and advertised version from the leading bytes.
func Detect(data []byte) (string, int, *apperr.Error) {
	trimmed := bytes.TrimLeft(data, " \t\r\n\xef\xbb\xbf")
	if len(trimmed) >= len(BinaryMagic) && string(trimmed[:len(BinaryMagic)]) == BinaryMagic {
		if len(trimmed) < len(BinaryMagic)+1 {
			return "", 0, apperr.New(apperr.CodeFormatCorrupt, "binary header is truncated", map[string]any{"offset": len(BinaryMagic)})
		}
		return BinaryFormat, int(trimmed[len(BinaryMagic)]), nil
	}
	if len(trimmed) == 0 {
		return "", 0, apperr.New(apperr.CodeFormatCorrupt, "input is empty", map[string]any{"offset": 0})
	}
	if trimmed[0] != '{' && trimmed[0] != '[' {
		return "", 0, apperr.New(apperr.CodeFormatCorrupt, "input is neither JSON Lines nor compact binary", map[string]any{"offset": 0})
	}
	line, _, _ := bytes.Cut(trimmed, []byte("\n"))
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(line), &meta); err != nil {
		return "", 0, apperr.New(apperr.CodeFormatCorrupt, "first JSON line is malformed", map[string]any{"line": 1, "detail": err.Error()})
	}
	version := CurrentJSONLVersion
	if raw, ok := meta["format_version"]; ok {
		if err := json.Unmarshal(raw, &version); err != nil {
			return "", 0, apperr.New(apperr.CodeFormatCorrupt, "format_version is not an integer", map[string]any{"line": 1})
		}
	}
	if raw, ok := meta["format"]; ok {
		var name string
		if err := json.Unmarshal(raw, &name); err != nil {
			return "", 0, apperr.New(apperr.CodeFormatCorrupt, "format metadata is not a string", map[string]any{"line": 1})
		}
		lower := strings.ToLower(name)
		if !strings.Contains(lower, "json") {
			return "", 0, apperr.New(apperr.CodeFormatUnsupported, "JSON Lines format metadata names an unsupported format", map[string]any{"format": name})
		}
	}
	return JSONLFormat, version, nil
}

// Import parses and normalizes a complete format payload. fallbackWriteMS is
// used only when a record omits last_write_ms.
func Import(data []byte, matrix compat.Matrix, fallbackWriteMS int64) (ImportResult, error) {
	formatName, version, detectErr := Detect(data)
	if detectErr != nil {
		return ImportResult{}, detectErr
	}
	switch formatName {
	case JSONLFormat:
		return importJSONL(data, version, matrix, fallbackWriteMS)
	case BinaryFormat:
		return importBinary(data, version, matrix, fallbackWriteMS)
	default:
		return ImportResult{}, apperr.New(apperr.CodeFormatUnsupported, "unsupported input format", map[string]any{"format": formatName})
	}
}

func normalizeGTIDText(raw string, rule compat.Rule) (domain.GTIDSet, []string, error) {
	opts := domain.GTIDParseOptions{
		AllowTildeRanges:      rule.AllowTildeRanges,
		AllowIntegerShorthand: rule.AllowIntegerShorthand,
		DefaultSource:         rule.DefaultGTIDSource,
		AllowColonRangeList:   true,
		AllowCommaRangeList:   true,
	}
	set, err := domain.ParseGTIDWithOptions(raw, opts)
	if err != nil {
		return domain.GTIDSet{}, nil, err
	}
	details := make([]string, 0, 3)
	if rule.AllowTildeRanges && strings.Contains(raw, "~") {
		details = append(details, "tilde range syntax expanded to canonical hyphen ranges")
	}
	if rule.AllowIntegerShorthand && !strings.Contains(raw, ":") {
		details = append(details, "integer interval shorthand expanded with default GTID source")
	}
	canonical := set.Normalized()
	if canonical.String() != strings.TrimSpace(raw) {
		details = append(details, "GTID intervals sorted, merged and canonicalized")
	}
	return canonical, details, nil
}

func normalizePositionText(raw string, rule compat.Rule) (string, []string, error) {
	text := strings.TrimSpace(raw)
	if rule.AllowTildeRanges && strings.Contains(text, "~") {
		if set, err := domain.ParseGTIDWithOptions(text, domain.GTIDParseOptions{
			AllowTildeRanges:      true,
			AllowIntegerShorthand: rule.AllowIntegerShorthand,
			DefaultSource:         rule.DefaultGTIDSource,
			AllowColonRangeList:   true,
			AllowCommaRangeList:   true,
		}); err == nil {
			return set.String(), []string{"tilde position expanded to canonical GTID set"}, nil
		}
	}
	position, err := domain.NormalizePosition(text, domain.PositionPolicy{Regex: rule.PositionRegex})
	if err != nil {
		return "", nil, err
	}
	if position != text {
		return position, []string{"executed position canonicalized"}, nil
	}
	return position, nil, nil
}

func parseReportedVersion(raw string, present bool, rule compat.Rule) (domain.Version, []string, error) {
	if !present || strings.TrimSpace(raw) == "" {
		if !rule.AllowMissingVersion {
			return domain.Version{}, nil, fmt.Errorf("reported version is missing")
		}
		if strings.TrimSpace(rule.DefaultVersion) == "" {
			return domain.Version{}, nil, fmt.Errorf("reported version is missing and matrix has no default_version")
		}
		version, err := domain.ParseVersion(rule.DefaultVersion)
		if err != nil {
			return domain.Version{}, nil, fmt.Errorf("matrix default_version is invalid: %w", err)
		}
		return version, []string{"missing reported version filled from compatibility default"}, nil
	}
	version, err := domain.ParseVersion(raw)
	if err != nil {
		return domain.Version{}, nil, err
	}
	return version, nil, nil
}

func conversionDetail(precision compat.TimePrecision) string {
	switch precision {
	case compat.PrecisionS:
		return "legacy second timestamp converted to Unix milliseconds"
	case compat.PrecisionUS:
		return "microsecond timestamp converted to Unix milliseconds"
	case compat.PrecisionNS:
		return "nanosecond timestamp converted to Unix milliseconds"
	default:
		return "timestamp interpreted as Unix milliseconds"
	}
}

func observationInvalid(replicaID string, sampledAt int64, field string, err error) *apperr.Error {
	return apperr.New(apperr.CodeObservationInvalid, "observation field is invalid", map[string]any{
		"replica_id":    replicaID,
		"sampled_at_ms": sampledAt,
		"field":         field,
		"detail":        err.Error(),
	})
}
