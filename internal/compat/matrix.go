package compat

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"meshmigrate/internal/model"
)

const (
	DecisionAccept    = "accept"
	DecisionDowngrade = "downgrade_accept"
	DecisionReject    = "reject"

	CodeUnsupported = "E_FORMAT_UNSUPPORTED"
)

// Defaults are declared in the compatibility matrix, never inferred from the
// current process state.
type Defaults struct {
	RateLimitBps    uint64 `json:"rateLimitBps"`
	TimeoutMs       uint64 `json:"timeoutMs"`
	CircuitWindowMs uint64 `json:"circuitWindowMs"`
	RateWindowMs    uint64 `json:"rateWindowMs"`
	WindowAlignment string `json:"windowAlignment"`
	PolicyRevision  uint64 `json:"policyRevision"`
}

type VersionRule struct {
	Decision string `json:"decision"`
	Reason   string `json:"reason"`
}

type Matrix struct {
	MatrixVersion     string                            `json:"matrixVersion"`
	SupportedFeatures []string                          `json:"supportedFeatures"`
	Defaults          Defaults                          `json:"defaults"`
	Formats           map[string]map[string]VersionRule `json:"formats"`
	featureSet        map[string]struct{}
}

//go:embed matrix.json
var matrixJSON []byte

func LoadMatrix() (*Matrix, error) {
	var m Matrix
	if err := json.Unmarshal(matrixJSON, &m); err != nil {
		return nil, fmt.Errorf("decode embedded compatibility matrix: %w", err)
	}
	if m.MatrixVersion == "" {
		return nil, fmt.Errorf("compatibility matrix version is empty")
	}
	m.featureSet = make(map[string]struct{}, len(m.SupportedFeatures))
	for _, f := range m.SupportedFeatures {
		m.featureSet[f] = struct{}{}
	}
	if m.Defaults.RateWindowMs == 0 {
		return nil, fmt.Errorf("compatibility matrix rateWindowMs default must be positive")
	}
	return &m, nil
}

func (m *Matrix) DefaultsValue() Defaults { return m.Defaults }

type Decision struct {
	Decision         string   `json:"decision"`
	Format           string   `json:"format"`
	FormatVersion    int      `json:"formatVersion"`
	MatrixVersion    string   `json:"matrixVersion"`
	Reason           string   `json:"reason"`
	UnknownFields    []string `json:"unknownFields,omitempty"`
	RequiredFeatures []string `json:"requiredFeatures,omitempty"`
	ErrorCode        string   `json:"errorCode,omitempty"`
}

func (m *Matrix) Decide(parsed *model.ParsedSnapshot) Decision {
	d := Decision{
		Format:           parsed.SourceFormat,
		FormatVersion:    parsed.FormatVersion,
		MatrixVersion:    m.MatrixVersion,
		UnknownFields:    append([]string(nil), parsed.UnknownFields...),
		RequiredFeatures: append([]string(nil), parsed.RequiredFeatures...),
	}
	formatRules, ok := m.Formats[parsed.SourceFormat]
	if !ok {
		d.Decision = DecisionReject
		d.Reason = "format has no compatibility declarations"
		d.ErrorCode = CodeUnsupported
		d.UnknownFields = uniqueSorted(d.UnknownFields)
		return d
	}
	rule, ok := formatRules[strconv.Itoa(parsed.FormatVersion)]
	if !ok {
		d.Decision = DecisionReject
		d.Reason = "format version " + strconv.Itoa(parsed.FormatVersion) + " has no compatibility declaration"
		d.ErrorCode = CodeUnsupported
		d.UnknownFields = uniqueSorted(d.UnknownFields)
		return d
	}
	d.Decision = rule.Decision
	d.Reason = rule.Reason
	for _, f := range parsed.RequiredFeatures {
		if _, supported := m.featureSet[f]; !supported {
			d.UnknownFields = append(d.UnknownFields, f)
			d.Decision = DecisionReject
			d.Reason = "required feature " + f + " is not supported by compatibility matrix " + m.MatrixVersion
			d.ErrorCode = CodeUnsupported
		}
	}
	if parsed.SourceFormat == model.FormatBinary && len(parsed.UnknownFields) > 0 {
		d.Decision = DecisionReject
		d.Reason = "binary record declares unknown presence/version fields"
		d.ErrorCode = CodeUnsupported
	}
	if d.Decision == DecisionReject {
		d.ErrorCode = CodeUnsupported
	}
	d.UnknownFields = uniqueSorted(d.UnknownFields)
	return d
}

func uniqueSorted(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	sort.Strings(in)
	out := in[:0]
	for _, s := range in {
		if len(out) == 0 || out[len(out)-1] != s {
			out = append(out, s)
		}
	}
	return out
}
