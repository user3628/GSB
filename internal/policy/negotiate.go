package policy

import (
	"bytes"
	"encoding/json"
	"strings"

	"example.com/replicabudget/internal/apperr"
)

type InputInfo struct {
	Format  string `json:"format"`
	Version int    `json:"version"`
	Endian  string `json:"endian,omitempty"`
}

type MatrixEntry struct {
	Format            string `json:"format"`
	Version           int    `json:"version"`
	TargetVersion     int    `json:"target_version"`
	Decision          string `json:"decision"`
	RequiresMigration bool   `json:"requires_migration"`
	Rule              string `json:"rule"`
}

type Negotiation struct {
	Input             InputInfo   `json:"input"`
	TargetVersion     int         `json:"target_version"`
	Decision          string      `json:"decision"`
	RequiresMigration bool        `json:"requires_migration"`
	Rule              string      `json:"rule"`
	Matrix            MatrixEntry `json:"matrix_entry"`
}

func CompatibilityMatrix() []MatrixEntry {
	return []MatrixEntry{
		{Format: FormatLegacyJSON, Version: 0, TargetVersion: 1, Decision: "degrade_accept", RequiresMigration: true, Rule: "derive replica bounds from fixed_replicas; fill weight_permille=1000; map whitelisted deprecated fields; require quota reference"},
		{Format: FormatJSONL, Version: 1, TargetVersion: 1, Decision: "accept", RequiresMigration: false, Rule: "native hierarchical v1"},
		{Format: FormatBinary, Version: 1, TargetVersion: 1, Decision: "accept", RequiresMigration: false, Rule: "native hierarchical v1, little or big endian"},
		{Format: FormatJSONL, Version: 2, TargetVersion: 1, Decision: "reject", RequiresMigration: false, Rule: "unknown mandatory fields in newer version"},
		{Format: FormatBinary, Version: 2, TargetVersion: 1, Decision: "reject", RequiresMigration: false, Rule: "unknown mandatory fields in newer version"},
		{Format: FormatLegacyJSON, Version: 1, TargetVersion: 1, Decision: "reject", RequiresMigration: false, Rule: "no declared legacy v1 semantics"},
	}
}

func DetectFormat(data []byte, hint string) (InputInfo, error) {
	info, err := detect(data)
	if err != nil {
		return info, err
	}
	hint = strings.ToLower(strings.TrimSpace(hint))
	if hint == "" || hint == FormatAuto {
		return info, nil
	}
	aliases := map[string]string{
		"legacy": FormatLegacyJSON, "legacy-json": FormatLegacyJSON, "legacy_json": FormatLegacyJSON,
		"json": FormatJSONL, "jsonl": FormatJSONL, "strategy-jsonl": FormatJSONL,
		"bin": FormatBinary, "binary": FormatBinary,
		"strategy_binary": FormatBinary,
	}
	normalized, ok := aliases[hint]
	if !ok {
		return info, apperr.WithDetails(apperr.FormatUnsupported, "format hint is unknown", map[string]any{"format": hint})
	}
	if normalized != info.Format {
		return info, apperr.WithDetails(apperr.FormatUnsupported, "format hint does not match detected input", map[string]any{
			"hint": hint, "detected": info.Format, "detected_version": info.Version,
		})
	}
	return info, nil
}

func detect(data []byte) (InputInfo, error) {
	info := InputInfo{Format: "", Version: -1}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return info, apperr.New(apperr.SpecInvalid, "input is empty")
	}
	if len(trimmed) >= 4 && string(trimmed[:4]) == BinaryMagic {
		info.Format = FormatBinary
		if len(trimmed) >= 5 {
			info.Version = int(trimmed[4])
		}
		if len(trimmed) >= 6 {
			switch trimmed[5] {
			case 'L':
				info.Endian = "little"
			case 'B':
				info.Endian = "big"
			}
		}
		return info, nil
	}
	if trimmed[0] == '[' {
		info.Format = FormatLegacyJSON
		info.Version = LegacyFormatVersion
		return info, nil
	}
	if trimmed[0] == '{' {
		format, version, err := readJSONLHeader(trimmed)
		if err == nil && isJSONLFormat(format) {
			info.Format = FormatJSONL
			info.Version = version
			return info, nil
		}
		var probe map[string]json.RawMessage
		if json.Unmarshal(trimmed, &probe) == nil {
			if _, ok := probe["fixed_replicas"]; ok {
				info.Format = FormatLegacyJSON
				info.Version = LegacyFormatVersion
				return info, nil
			}
		}
		if err != nil {
			return info, apperr.New(apperr.SpecInvalid, "cannot detect JSON input: "+err.Error())
		}
		return info, apperr.WithDetails(apperr.FormatUnsupported, "JSON format marker is unknown", map[string]any{"format": format, "version": version})
	}
	return info, apperr.New(apperr.FormatUnsupported, "input format is not recognized")
}

func Negotiate(info InputInfo) (Negotiation, error) {
	for _, entry := range CompatibilityMatrix() {
		if entry.Format == info.Format && entry.Version == info.Version {
			neg := Negotiation{
				Input: info, TargetVersion: entry.TargetVersion, Decision: entry.Decision,
				RequiresMigration: entry.RequiresMigration, Rule: entry.Rule, Matrix: entry,
			}
			if entry.Decision == "reject" {
				return neg, apperr.WithDetails(apperr.FormatUnsupported, "format/version rejected by compatibility matrix", map[string]any{
					"format": info.Format, "version": info.Version, "rule": entry.Rule,
				})
			}
			return neg, nil
		}
	}
	return Negotiation{Input: info}, apperr.WithDetails(apperr.FormatUnsupported, "format/version has no compatibility matrix entry", map[string]any{
		"format": info.Format, "version": info.Version,
	})
}
