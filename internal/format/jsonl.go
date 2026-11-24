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

type jsonlLine struct {
	number int
	offset int
	object map[string]json.RawMessage
}

// importJSONL parses a versioned JSON Lines stream. The optional first line is
// metadata; every subsequent non-empty line contains one complete record.
func importJSONL(data []byte, detectedVersion int, matrix compat.Matrix, fallbackWriteMS int64) (ImportResult, error) {
	lines, err := parseJSONLLines(data)
	if err != nil {
		return ImportResult{}, err
	}
	if len(lines) == 0 {
		return ImportResult{}, apperr.New(apperr.CodeFormatCorrupt, "JSON Lines input contains no metadata or observations", map[string]any{"line": 1, "offset": 0})
	}

	version := detectedVersion
	recordStart := 0
	requiredFields := []string{}
	header := lines[0]
	if !looksLikeObservation(header.object) {
		recordStart = 1
		if raw, ok := header.object["format_version"]; ok {
			if err := json.Unmarshal(raw, &version); err != nil {
				return ImportResult{}, apperr.New(apperr.CodeFormatCorrupt, "format_version is not an integer", map[string]any{"line": header.number, "offset": header.offset})
			}
		}
		fields, fieldErr := parseRequiredFields(header.object)
		if fieldErr != nil {
			return ImportResult{}, apperr.New(apperr.CodeFormatCorrupt, "required_fields must be a string array", map[string]any{"line": header.number, "offset": header.offset})
		}
		requiredFields = append(requiredFields, fields...)
	}
	for _, line := range lines[recordStart:] {
		fields, fieldErr := parseRequiredFields(line.object)
		if fieldErr != nil {
			return ImportResult{}, apperr.New(apperr.CodeFormatCorrupt, "required_fields must be a string array", map[string]any{"line": line.number, "offset": line.offset})
		}
		requiredFields = append(requiredFields, fields...)
		if raw, ok := line.object["format_version"]; ok {
			var recordVersion int
			if err := json.Unmarshal(raw, &recordVersion); err != nil {
				return ImportResult{}, apperr.New(apperr.CodeFormatCorrupt, "record format_version is not an integer", map[string]any{"line": line.number, "offset": line.offset})
			}
			if recordVersion != version {
				return ImportResult{}, apperr.New(apperr.CodeFormatCorrupt, "record format_version differs from stream version", map[string]any{
					"line": line.number, "offset": line.offset, "stream_version": version, "record_version": recordVersion,
				})
			}
		}
	}

	rule, decisionErr := matrix.Decide(JSONLFormat, version, requiredFields)
	if decisionErr != nil {
		return ImportResult{}, decisionErr
	}
	result := ImportResult{
		Format:         JSONLFormat,
		FormatVersion:  version,
		Decision:       rule.Decision,
		Observations:   make([]domain.Observation, 0, len(lines)-recordStart),
		Degradations:   []Degradation{},
		RequiredFields: dedupeStrings(requiredFields),
		SourceBytes:    len(data),
	}
	if rule.Decision == compat.DecisionDegrade {
		result.Degradations = append(result.Degradations, Degradation{
			Record: 0, Field: "format_version", Detail: fmt.Sprintf("jsonl/%d accepted with explicit degradation", version),
		})
	}

	for _, line := range lines[recordStart:] {
		observation, details, parseErr := parseJSONLObservation(line, version, rule, fallbackWriteMS)
		if parseErr != nil {
			return ImportResult{}, parseErr
		}
		for _, detail := range details {
			result.Degradations = append(result.Degradations, Degradation{Record: line.number, Field: detail.Field, Detail: detail.Detail})
		}
		result.Observations = append(result.Observations, observation)
	}
	return result, nil
}

func parseJSONLLines(data []byte) ([]jsonlLine, *apperr.Error) {
	rawLines := bytes.Split(data, []byte("\n"))
	lines := make([]jsonlLine, 0, len(rawLines))
	offset := 0
	for index, raw := range rawLines {
		lineOffset := offset
		offset += len(raw) + 1
		raw = bytes.TrimSpace(raw)
		if index == 0 {
			raw = bytes.TrimPrefix(raw, []byte("\xef\xbb\xbf"))
			raw = bytes.TrimSpace(raw)
		}
		if len(raw) == 0 {
			continue
		}
		if raw[0] != '{' {
			return nil, apperr.New(apperr.CodeFormatCorrupt, "JSON Lines record must be an object", map[string]any{
				"line": index + 1, "offset": lineOffset,
			})
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return nil, apperr.New(apperr.CodeFormatCorrupt, "malformed JSON Lines record", map[string]any{
				"line": index + 1, "offset": lineOffset, "detail": err.Error(),
			})
		}
		lines = append(lines, jsonlLine{number: index + 1, offset: lineOffset, object: object})
	}
	return lines, nil
}

func looksLikeObservation(object map[string]json.RawMessage) bool {
	for _, key := range []string{"replica_id", "sampled_at_ms", "sampled_at", "gtid", "gtid_segments", "lag_ms", "position_name"} {
		if _, ok := object[key]; ok {
			return true
		}
	}
	return false
}

func parseRequiredFields(object map[string]json.RawMessage) ([]string, error) {
	raw, ok := object["required_fields"]
	if !ok {
		return nil, nil
	}
	var fields []string
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	return fields, nil
}

func dedupeStrings(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}

type fieldDetail struct {
	Field  string
	Detail string
}

func parseJSONLObservation(line jsonlLine, formatVersion int, rule compat.Rule, fallbackWriteMS int64) (domain.Observation, []fieldDetail, error) {
	replicaID := rawString(line.object, "replica_id")
	sampledAt, timeDetails, timeErr := parseJSONLTimestamp(line, rule)
	if timeErr != nil {
		return domain.Observation{}, nil, observationInvalid(replicaID, sampledAt, "sampled_at_ms", timeErr)
	}

	lagMS, lagErr := rawInt64(line.object, "lag_ms")
	if lagErr != nil {
		return domain.Observation{}, nil, observationInvalid(replicaID, sampledAt, "lag_ms", lagErr)
	}
	if lagMS < 0 {
		return domain.Observation{}, nil, observationInvalid(replicaID, sampledAt, "lag_ms", fmt.Errorf("must be non-negative"))
	}

	gtidRaw, gtidErr := rawGTIDSegments(line.object)
	if gtidErr != nil {
		return domain.Observation{}, nil, observationInvalid(replicaID, sampledAt, "gtid_segments", gtidErr)
	}
	gtid, gtidDetails, gtidErr := normalizeGTIDText(gtidRaw, rule)
	if gtidErr != nil {
		return domain.Observation{}, nil, observationInvalid(replicaID, sampledAt, "gtid_segments", gtidErr)
	}

	positionRaw := rawStringAlias(line.object, "position_name", "executed_position", "executed_gtid_name")
	position, positionDetails, positionErr := normalizePositionText(positionRaw, rule)
	if positionErr != nil {
		return domain.Observation{}, nil, observationInvalid(replicaID, sampledAt, "position_name", positionErr)
	}

	reportedRaw := rawStringAlias(line.object, "reported_version", "replica_version")
	_, versionPresent := firstRaw(line.object, "reported_version", "replica_version")
	reportedVersion, versionDetails, versionErr := parseReportedVersion(reportedRaw, versionPresent, rule)
	if versionErr != nil {
		return domain.Observation{}, nil, observationInvalid(replicaID, sampledAt, "reported_version", versionErr)
	}

	lastWriteMS := fallbackWriteMS
	if value, err := rawInt64(line.object, "last_write_ms"); err == nil {
		lastWriteMS = value
	} else if rawPresent(line.object, "last_write_ms") {
		return domain.Observation{}, nil, observationInvalid(replicaID, sampledAt, "last_write_ms", err)
	}
	observation := domain.Observation{
		ReplicaID:       replicaID,
		SampledAtMS:     sampledAt,
		GTID:            gtid,
		PositionName:    position,
		LagMS:           lagMS,
		ReportedVersion: reportedVersion,
		FormatVersion:   formatVersion,
		LastWriteMS:     lastWriteMS,
	}
	if err := observation.Validate(); err != nil {
		return domain.Observation{}, nil, err
	}
	details := append([]fieldDetail{}, timeDetails...)
	for _, detail := range append(append(gtidDetails, positionDetails...), versionDetails...) {
		details = append(details, fieldDetail{Field: "compatibility", Detail: detail})
	}
	return observation, details, nil
}

func parseJSONLTimestamp(line jsonlLine, rule compat.Rule) (int64, []fieldDetail, error) {
	if raw, ok := line.object["sampled_at_ms"]; ok {
		var value int64
		if err := json.Unmarshal(raw, &value); err != nil {
			return 0, nil, fmt.Errorf("must be an integer Unix millisecond timestamp")
		}
		return value, nil, nil
	}
	raw, ok := line.object["sampled_at"]
	if !ok {
		return 0, nil, fmt.Errorf("sampled_at_ms or sampled_at is required")
	}
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, nil, fmt.Errorf("must be an integer timestamp")
	}
	ms, lossy, err := compat.TimestampToMS(value, rule.TimePrecision)
	if err != nil {
		return 0, nil, err
	}
	details := []fieldDetail{{Field: "sampled_at", Detail: conversionDetail(rule.TimePrecision)}}
	if lossy {
		details = append(details, fieldDetail{Field: "sampled_at", Detail: "sub-millisecond precision truncated during normalization"})
	}
	return ms, details, nil
}

func rawGTIDSegments(object map[string]json.RawMessage) (string, error) {
	if raw, ok := object["gtid_segments"]; ok {
		var segments []string
		if err := json.Unmarshal(raw, &segments); err == nil {
			if len(segments) == 0 {
				return "", fmt.Errorf("gtid_segments is empty")
			}
			return strings.Join(segments, ","), nil
		}
		var numbers []json.Number
		if err := json.Unmarshal(raw, &numbers); err != nil {
			return "", fmt.Errorf("gtid_segments must be a string or number array")
		}
		if len(numbers) == 0 {
			return "", fmt.Errorf("gtid_segments is empty")
		}
		parts := make([]string, len(numbers))
		for i, number := range numbers {
			parts[i] = number.String()
		}
		return strings.Join(parts, ","), nil
	}
	if value := rawStringAlias(object, "gtid", "gtid_set"); value != "" {
		return value, nil
	}
	return "", fmt.Errorf("gtid_segments is required")
}

func rawString(object map[string]json.RawMessage, key string) string {
	value, _ := rawStringOK(object, key)
	return value
}

func rawStringAlias(object map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		if value, ok := rawStringOK(object, key); ok {
			return value
		}
	}
	return ""
}

func rawStringOK(object map[string]json.RawMessage, key string) (string, bool) {
	raw, ok := object[key]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false
	}
	return value, true
}

func firstRaw(object map[string]json.RawMessage, keys ...string) (json.RawMessage, bool) {
	for _, key := range keys {
		if value, ok := object[key]; ok && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return value, true
		}
	}
	return nil, false
}

func rawPresent(object map[string]json.RawMessage, key string) bool {
	_, ok := object[key]
	return ok
}

func rawInt64(object map[string]json.RawMessage, key string) (int64, error) {
	raw, ok := object[key]
	if !ok {
		return 0, fmt.Errorf("%s is required", key)
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, fmt.Errorf("%s must not be null", key)
	}
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	return value, nil
}

// EncodeJSONL emits the current v1 JSON Lines representation in deterministic
// replica/time order.
func EncodeJSONL(observations []domain.Observation) ([]byte, error) {
	ordered := domain.SortObservations(observations)
	var buffer bytes.Buffer
	header := map[string]any{
		"format":          "replication-observation-jsonl",
		"format_version":  CurrentJSONLVersion,
		"required_fields": []string{"replica_id", "sampled_at_ms", "gtid_segments", "position_name", "lag_ms", "reported_version"},
	}
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(header); err != nil {
		return nil, err
	}
	for _, observation := range ordered {
		segments := make([]string, 0, len(observation.GTID.Normalized().Sources))
		for _, source := range observation.GTID.Normalized().Sources {
			parts := make([]string, len(source.Intervals))
			for i, interval := range source.Intervals {
				parts[i] = interval.String()
			}
			segments = append(segments, source.Source+":"+strings.Join(parts, ":"))
		}
		record := map[string]any{
			"replica_id":       observation.ReplicaID,
			"sampled_at_ms":    observation.SampledAtMS,
			"gtid_segments":    segments,
			"position_name":    observation.PositionName,
			"lag_ms":           observation.LagMS,
			"reported_version": observation.ReportedVersion.String(),
			"last_write_ms":    observation.LastWriteMS,
			"format_version":   CurrentJSONLVersion,
		}
		if err := encoder.Encode(record); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}
