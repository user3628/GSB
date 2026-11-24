package codec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"mitmpipeline/internal/domain"
)

var knownJSONLFields = map[string]bool{
	"format": true, "format_version": true, "connection_id": true, "sni": true,
	"protocol_version": true, "observed_start_unix_ms": true, "cert_chain_pem": true,
	"chain": true, "policy": true, "policy_timeline": true, "events": true,
	"future_required": true,
}

func EncodeJSONL(records []domain.Record) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	for i := range records {
		record := records[i]
		if record.Format == "" {
			record.Format = domain.FormatJSONL
		}
		if record.FormatVersion == 0 {
			record.FormatVersion = CurrentVersion
		}
		if err := encoder.Encode(&record); err != nil {
			return nil, domain.WrapError(domain.CodeRecordCorrupt, err, "encode JSON line").WithContext("record_index", i+1)
		}
	}
	return buf.Bytes(), nil
}

func DecodeJSONL(data []byte) ([]domain.Record, []domain.Degradation, error) {
	lines := bytes.Split(data, []byte{'\n'})
	records := []domain.Record{}
	degradations := []domain.Degradation{}
	recordIndex := 0

	for lineNumber, rawLine := range lines {
		line := bytes.TrimSpace(rawLine)
		if len(line) == 0 {
			continue
		}
		recordIndex++
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(line, &fields); err != nil {
			return nil, nil, domain.WrapError(domain.CodeRecordCorrupt, err, "invalid JSON object").
				WithContext("record_index", recordIndex).WithContext("line_number", lineNumber+1)
		}
		version := jsonVersion(fields["format_version"])
		if version == 0 {
			version = 1
		}

		for key := range fields {
			if knownJSONLFields[key] || strings.HasPrefix(key, "x_") {
				continue
			}
			if version > CurrentVersion {
				return nil, nil, domain.NewError(domain.CodeFormatUnsupported, "new format version contains unexplainable required field").
					WithContextMap(map[string]any{
						"record_index": recordIndex, "field": key,
						"requested_version": version, "supported_versions": []int{1, 2},
					})
			}
			return nil, nil, domain.NewError(domain.CodeRecordCorrupt, "unknown JSON field").
				WithContext("record_index", recordIndex).WithContext("field", key)
		}
		if version > CurrentVersion {
			if _, hasFuture := fields["future_required"]; hasFuture {
				return nil, nil, domain.NewError(domain.CodeFormatUnsupported, "new format version contains unexplainable required field").
					WithContextMap(map[string]any{
						"record_index": recordIndex, "field": "future_required",
						"requested_version": version, "supported_versions": []int{1, 2},
					})
			}
			return nil, nil, domain.NewError(domain.CodeFormatUnsupported, "unsupported JSONL format version").
				WithContextMap(map[string]any{"record_index": recordIndex, "requested_version": version, "supported_versions": []int{1, 2}})
		}

		var record domain.Record
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, nil, domain.WrapError(domain.CodeRecordCorrupt, err, "invalid JSON record").
				WithContext("record_index", recordIndex).WithContext("line_number", lineNumber+1)
		}
		record.Format = domain.FormatJSONL
		record.FormatVersion = version
		if record.CertChainPEM == "" {
			var pem strings.Builder
			for _, entry := range record.Chain {
				pem.WriteString(entry.PEM)
				if !strings.HasSuffix(entry.PEM, "\n") {
					pem.WriteByte('\n')
				}
			}
			record.CertChainPEM = pem.String()
		}
		if record.ProtocolVersion == "" && version == 1 {
			record.ProtocolVersion = DefaultProtocolVersion
			degradations = append(degradations, domain.Degradation{
				RecordIndex: recordIndex, ConnectionID: record.ConnectionID, Field: "protocol_version",
				Rule: "fill_default_protocol", Filled: DefaultProtocolVersion,
				Reason: "v1 JSONL records do not carry protocol_version",
			})
		}
		if version == 1 {
			degradations = append(degradations, domain.Degradation{
				RecordIndex: recordIndex, ConnectionID: record.ConnectionID, Field: "events.time",
				Rule: "parse_rfc3339_seconds_as_rfc3339nano", Supplied: "second_precision",
				Filled: "zero_submillisecond_fraction", Reason: "v1 JSONL timestamps have lower precision",
			})
		}
		if err := ValidateRecord(&record); err != nil {
			if de, ok := domain.AsError(err); ok {
				de.Context["record_index"] = recordIndex
			}
			return nil, nil, err
		}
		records = append(records, record)
	}
	if recordIndex == 0 {
		return nil, nil, domain.NewError(domain.CodeRecordCorrupt, "JSONL input contains no records")
	}
	return records, degradations, nil
}

func ValidateRecord(record *domain.Record) error {
	if strings.TrimSpace(record.ConnectionID) == "" {
		return domain.NewError(domain.CodeRecordCorrupt, "connection_id is required")
	}
	if strings.TrimSpace(record.SNI) == "" {
		return domain.NewError(domain.CodeRecordCorrupt, "sni is required")
	}
	if strings.TrimSpace(record.CertChainPEM) == "" {
		return domain.NewError(domain.CodeRecordCorrupt, "certificate chain is required")
	}
	if record.Policy.Version == "" || record.Policy.Mode == "" {
		return domain.NewError(domain.CodeRecordCorrupt, "policy version and mode are required")
	}
	if record.Policy.Mode != domain.PolicyModeSPKI && record.Policy.Mode != domain.PolicyModeRoot {
		return domain.NewError(domain.CodeRecordCorrupt, "unsupported policy mode").WithContext("mode", record.Policy.Mode)
	}
	if len(record.Events) == 0 {
		return domain.NewError(domain.CodeRecordCorrupt, "at least one event is required")
	}
	seenSeq := map[int]bool{}
	for i, event := range record.Events {
		if event.Type == "" {
			return domain.NewError(domain.CodeRecordCorrupt, "event type is required").WithContext("event_index", i)
		}
		if event.Seq == 0 {
			event.Seq = i + 1
		}
		if seenSeq[event.Seq] {
			return domain.NewError(domain.CodeRecordCorrupt, "duplicate event sequence").WithContext("event_seq", event.Seq)
		}
		seenSeq[event.Seq] = true
	}
	return nil
}

func jsonVersion(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0
	}
	switch version := value.(type) {
	case float64:
		return int(version)
	case string:
		var parsed int
		_, _ = fmt.Sscanf(version, "%d", &parsed)
		return parsed
	default:
		return 0
	}
}
