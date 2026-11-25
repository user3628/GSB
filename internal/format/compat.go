package format

import (
	"encoding/json"
	"os"
	"strings"

	"mpckpt/internal/model"
)

type Verdict string

const (
	VerdictAccept        Verdict = "ACCEPT"
	VerdictDegradeAccept Verdict = "DEGRADE_ACCEPT"
	VerdictReject        Verdict = "REJECT"
)

type CompatibilityMatrix struct {
	Rules map[string]map[int]Verdict
}

type matrixFile struct {
	Rules map[string]map[string]Verdict `json:"rules"`
}

func DefaultCompatibilityMatrix() *CompatibilityMatrix {
	return &CompatibilityMatrix{Rules: map[string]map[int]Verdict{
		FormatBinary: {
			1: VerdictDegradeAccept,
			2: VerdictAccept,
			3: VerdictReject,
		},
		FormatJSONL: {
			1: VerdictDegradeAccept,
			2: VerdictAccept,
			3: VerdictReject,
		},
	}}
}

func LoadCompatibilityMatrix(path string) (*CompatibilityMatrix, error) {
	matrix := DefaultCompatibilityMatrix()
	if strings.TrimSpace(path) == "" {
		return matrix, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var override matrixFile
	if err := json.Unmarshal(raw, &override); err != nil {
		return nil, err
	}
	for rawFormat, versions := range override.Rules {
		formatName := model.NormalizeFormat(rawFormat)
		if matrix.Rules[formatName] == nil {
			matrix.Rules[formatName] = map[int]Verdict{}
		}
		for rawVersion, verdict := range versions {
			version, err := parseVersion(rawVersion)
			if err != nil {
				return nil, err
			}
			normalized := Verdict(strings.ToUpper(strings.TrimSpace(string(verdict))))
			if normalized != VerdictAccept && normalized != VerdictDegradeAccept && normalized != VerdictReject {
				return nil, &model.BizError{Code: model.CodeSpecInvalid, Message: "invalid compatibility verdict", Context: map[string]any{
					"format": formatName, "version": version, "verdict": verdict,
				}}
			}
			matrix.Rules[formatName][version] = normalized
		}
	}
	return matrix, nil
}

func (m *CompatibilityMatrix) Decide(formatName string, version int) (Verdict, *model.BizError) {
	formatName = model.NormalizeFormat(formatName)
	versions, ok := m.Rules[formatName]
	if !ok {
		return "", model.NewError(model.CodeFormatUnsupported, "format is not declared in the compatibility matrix", map[string]any{
			"format": formatName,
		})
	}
	verdict, ok := versions[version]
	if !ok {
		return "", model.NewError(model.CodeFormatUnsupported, "format version is not declared in the compatibility matrix", map[string]any{
			"format": formatName, "format_version": version,
		})
	}
	if verdict == VerdictReject {
		return verdict, model.NewError(model.CodeFormatUnsupported, "format version rejected by compatibility matrix", map[string]any{
			"format": formatName, "format_version": version, "verdict": verdict,
		})
	}
	return verdict, nil
}

func parseVersion(value string) (int, error) {
	var version int
	if err := json.Unmarshal([]byte(value), &version); err == nil {
		return version, nil
	}
	return 0, &model.BizError{Code: model.CodeSpecInvalid, Message: "compatibility version key must be an integer", Context: map[string]any{"version": value}}
}
