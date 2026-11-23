package codec

import (
	"fmt"
	"sort"

	"example.com/logicplan/editor/internal/bizerr"
)

type Format string

const (
	FormatBinary Format = "binary"
	FormatJSONL  Format = "jsonl"
)

type Decision string

const (
	DecisionAccept  Decision = "accept"
	DecisionDegrade Decision = "degrade"
	DecisionReject  Decision = "reject"
)

type CompatibilityRule struct {
	Format        Format         `json:"format"`
	Version       int            `json:"version"`
	Decision      Decision       `json:"decision"`
	MissingFields []string       `json:"missing_fields,omitempty"`
	Defaults      map[string]any `json:"defaults,omitempty"`
	Reason        string         `json:"reason"`
}

type Compatibility struct {
	Format        Format         `json:"format"`
	Version       int            `json:"version"`
	Decision      Decision       `json:"decision"`
	MissingFields []string       `json:"missing_fields,omitempty"`
	Defaults      map[string]any `json:"defaults,omitempty"`
	Reason        string         `json:"reason"`
}

var compatibilityMatrix = []CompatibilityRule{
	{
		Format: FormatBinary, Version: 1, Decision: DecisionDegrade,
		MissingFields: []string{"columns", "stats"},
		Defaults: map[string]any{
			"columns": []any{},
			"stats":   map[string]any{"row_count": 0, "distinct": map[string]int64{}},
		},
		Reason: "legacy binary schema predates column references and statistics bindings",
	},
	{
		Format: FormatJSONL, Version: 1, Decision: DecisionDegrade,
		MissingFields: []string{"columns", "stats"},
		Defaults: map[string]any{
			"columns": []any{},
			"stats":   map[string]any{"row_count": 0, "distinct": map[string]int64{}},
		},
		Reason: "legacy JSONL schema predates column references and statistics bindings",
	},
	{Format: FormatBinary, Version: 2, Decision: DecisionAccept, Reason: "current binary schema"},
	{Format: FormatJSONL, Version: 2, Decision: DecisionAccept, Reason: "current JSONL schema"},
	{
		Format: FormatBinary, Version: 3, Decision: DecisionReject,
		Reason: "version 3 contains required fields unknown to this editor",
	},
	{
		Format: FormatJSONL, Version: 3, Decision: DecisionReject,
		Reason: "version 3 contains required fields unknown to this editor",
	},
}

func Matrix() []CompatibilityRule {
	out := append([]CompatibilityRule(nil), compatibilityMatrix...)
	for i := range out {
		if out[i].Defaults != nil {
			cp := make(map[string]any, len(out[i].Defaults))
			for k, v := range out[i].Defaults {
				cp[k] = v
			}
			out[i].Defaults = cp
		}
	}
	return out
}

func Negotiate(format Format, version int) (Compatibility, error) {
	for _, rule := range compatibilityMatrix {
		if rule.Format == format && rule.Version == version {
			return Compatibility{
				Format: format, Version: version, Decision: rule.Decision,
				MissingFields: append([]string(nil), rule.MissingFields...),
				Defaults:      rule.Defaults, Reason: rule.Reason,
			}, nil
		}
	}
	return Compatibility{}, bizerr.New(bizerr.CodeFormatUnsupported,
		fmt.Sprintf("format %s version %d is not present in the compatibility matrix", format, version),
		map[string]any{"format": format, "format_version": version, "decision": DecisionReject})
}

func RejectUnknownRequired(format Format, version int, features []string) error {
	if len(features) == 0 {
		return nil
	}
	sorted := append([]string(nil), features...)
	sort.Strings(sorted)
	return bizerr.New(bizerr.CodeFormatUnsupported,
		"snapshot declares required fields unknown to this editor",
		map[string]any{
			"format": format, "format_version": version,
			"unknown_required_fields": sorted, "decision": DecisionReject,
		})
}
