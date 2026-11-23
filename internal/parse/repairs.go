package parse

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"

	"baselineworkbench/internal/codes"
	"baselineworkbench/internal/policy"
)

const maxBlockBytes = 16 << 20

// Repairs parses length-prefixed JSON text blocks: <decimal-length>:<json>\n.
// A truncated block returns E_POLICY_MALFORMED and preserves earlier items.
func Repairs(data []byte) policy.ParseResult {
	result := policy.ParseResult{}
	seen := make(map[string]bool)
	pos := 0
	for pos < len(data) {
		for pos < len(data) && (data[pos] == '\n' || data[pos] == '\r') {
			pos++
		}
		if pos >= len(data) {
			break
		}
		blockOffset := pos
		startDigits := pos
		for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			pos++
		}
		if pos == startDigits || pos >= len(data) || data[pos] != ':' {
			result.Issues = append(result.Issues, policy.Issue{
				Code:    codes.PolicyMalformed,
				Message: "missing decimal length prefix or colon",
				Offset:  blockOffset,
				Source:  "repair",
			})
			break
		}
		lengthText := string(data[startDigits:pos])
		pos++
		declared, err := strconv.ParseInt(lengthText, 10, 64)
		if err != nil || declared <= 0 || declared > maxBlockBytes {
			result.Issues = append(result.Issues, policy.Issue{
				Code:    codes.PolicyMalformed,
				Message: "invalid length prefix",
				Offset:  blockOffset,
				Source:  "repair",
			})
			break
		}
		if int64(len(data)-pos) < declared {
			result.Issues = append(result.Issues, policy.Issue{
				Code:    codes.PolicyMalformed,
				Message: "truncated length-prefixed repair block",
				Offset:  blockOffset,
				Source:  "repair",
			})
			break
		}
		payload := data[pos : pos+int(declared)]
		payloadOffset := pos
		pos += int(declared)
		if pos < len(data) {
			if data[pos] == '\r' {
				pos++
			}
			if pos >= len(data) || data[pos] != '\n' {
				result.Issues = append(result.Issues, policy.Issue{
					Code:    codes.PolicyMalformed,
					Message: "repair block must end with LF",
					Offset:  blockOffset,
					Source:  "repair",
				})
				break
			}
			pos++
		}
		var wire wireItem
		decoder := json.NewDecoder(bytes.NewReader(payload))
		if err := decoder.Decode(&wire); err != nil {
			result.Issues = append(result.Issues, policy.Issue{
				Code:    codes.PolicyMalformed,
				Message: "invalid repair block JSON: " + err.Error(),
				Offset:  payloadOffset,
				Source:  "repair",
			})
			continue
		}
		if err := ensureEOF(decoder); err != nil {
			result.Issues = append(result.Issues, policy.Issue{
				Code:    codes.PolicyMalformed,
				Message: "repair block contains trailing JSON content",
				Offset:  payloadOffset,
				Source:  "repair",
			})
			continue
		}
		record, recordErr := wireToRecord(wire, "repair", payloadOffset)
		if recordErr != nil {
			if wire.ID != "" {
				seen[wire.ID] = true
			}
			result.Issues = append(result.Issues, *recordErr)
			continue
		}
		if seen[record.ID] {
			result.Issues = append(result.Issues, policy.Issue{
				Code:    codes.PolicyMalformed,
				Message: "duplicate policy id in repair blocks",
				ItemID:  record.ID,
				Offset:  payloadOffset,
				Source:  "repair",
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

func RepairBlock(record policy.RawRecord) []byte {
	payload := struct {
		ID     string `json:"id"`
		Unit   string `json:"unit"`
		Value  string `json:"value"`
		Weight string `json:"weight,omitempty"`
	}{
		ID:     record.ID,
		Unit:   record.Unit,
		Value:  record.Value,
		Weight: record.Weight,
	}
	encoded, _ := json.Marshal(payload)
	var builder strings.Builder
	builder.WriteString(strconv.Itoa(len(encoded)))
	builder.WriteByte(':')
	builder.Write(encoded)
	builder.WriteByte('\n')
	return []byte(builder.String())
}
