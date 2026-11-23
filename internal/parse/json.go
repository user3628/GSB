package parse

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"baselineworkbench/internal/codes"
	"baselineworkbench/internal/policy"
)

type wireItem struct {
	ID     string          `json:"id"`
	Unit   string          `json:"unit"`
	Value  json.RawMessage `json:"value"`
	Weight json.RawMessage `json:"weight,omitempty"`
}

type snapshotWire struct {
	Schema       string     `json:"schema,omitempty"`
	PackageID    string     `json:"package_id,omitempty"`
	SampleMarker string     `json:"sample_marker,omitempty"`
	Items        []wireItem `json:"items"`
}

func Snapshot(data []byte) policy.ParseResult {
	var wire snapshotWire
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&wire); err != nil {
		return policy.ParseResult{Issues: []policy.Issue{{
			Code:    codes.PolicyMalformed,
			Message: "invalid JSON baseline: " + err.Error(),
			Offset:  0,
			Source:  "baseline",
		}}}
	}
	if err := ensureEOF(decoder); err != nil {
		return policy.ParseResult{Issues: []policy.Issue{{
			Code:    codes.PolicyMalformed,
			Message: "trailing content in JSON baseline: " + err.Error(),
			Source:  "baseline",
		}}}
	}
	result := policy.ParseResult{}
	seen := make(map[string]bool, len(wire.Items))
	cursor := 0
	for _, raw := range wire.Items {
		offset := findOffset(data, cursor, raw.ID)
		if offset >= 0 {
			cursor = offset + len(raw.ID)
		}
		record, recordErr := wireToRecord(raw, "baseline", offset)
		if recordErr != nil {
			if raw.ID != "" {
				seen[raw.ID] = true
			}
			result.Issues = append(result.Issues, *recordErr)
			continue
		}
		if seen[record.ID] {
			result.Issues = append(result.Issues, policy.Issue{
				Code:    codes.PolicyMalformed,
				Message: "duplicate policy id",
				ItemID:  record.ID,
				Offset:  offset,
				Source:  "baseline",
			})
			continue
		}
		item, itemErr := policy.NormalizeRecord(record)
		if itemErr != nil {
			seen[record.ID] = true
			result.Issues = append(result.Issues, *itemErr)
			continue
		}
		seen[record.ID] = true
		result.Items = append(result.Items, item)
	}
	return result
}

func wireToRecord(raw wireItem, source string, offset int) (policy.RawRecord, *policy.Issue) {
	value, err := scalarString(raw.Value)
	if err != nil {
		return policy.RawRecord{}, &policy.Issue{Code: codes.PolicyMalformed, Message: "value must be a decimal string or number", ItemID: raw.ID, Offset: offset, Source: source}
	}
	weight := ""
	if len(raw.Weight) != 0 {
		weight, err = scalarString(raw.Weight)
		if err != nil {
			return policy.RawRecord{}, &policy.Issue{Code: codes.PolicyMalformed, Message: "weight must be a decimal string or number", ItemID: raw.ID, Offset: offset, Source: source}
		}
	}
	return policy.RawRecord{
		ID:     strings.TrimSpace(raw.ID),
		Unit:   strings.TrimSpace(raw.Unit),
		Value:  value,
		Weight: weight,
		Offset: offset,
		Source: source,
	}, nil
}

func scalarString(raw json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", errors.New("missing value")
	}
	if trimmed[0] == '"' {
		var value string
		if err := json.Unmarshal(trimmed, &value); err != nil {
			return "", err
		}
		return value, nil
	}
	for _, r := range string(trimmed) {
		if (r < '0' || r > '9') && r != '-' && r != '+' && r != '.' {
			return "", errors.New("non-decimal numeric token")
		}
	}
	return string(trimmed), nil
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func findOffset(data []byte, from int, id string) int {
	if id == "" || from >= len(data) {
		return -1
	}
	idx := bytes.Index(data[from:], []byte(id))
	if idx < 0 {
		return -1
	}
	return from + idx
}
