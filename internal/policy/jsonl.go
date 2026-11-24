package policy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"example.com/replicabudget/internal/apperr"
)

var jsonlAllowedFields = map[string]struct{}{
	"format": {}, "format_version": {}, "record_type": {}, "service_id": {},
	"min_replicas": {}, "max_replicas": {}, "target_concurrency": {},
	"latency_budget_ms": {}, "quota_group_ref": {}, "weight_permille": {},
	"vram_bytes_per_replica": {}, "compute_milli_per_replica": {},
	"elastic_enabled": {}, "placement_zone": {},
}

func EncodeJSONL(strategies []NewStrategy) ([]byte, error) {
	copied := append([]NewStrategy(nil), strategies...)
	SortStrategies(copied)
	buf := &bytes.Buffer{}
	for _, strategy := range copied {
		record := struct {
			Format        string `json:"format"`
			FormatVersion int    `json:"format_version"`
			RecordType    string `json:"record_type"`
			NewStrategy
		}{
			Format: "strategy-jsonl", FormatVersion: CurrentFormatVersion, RecordType: "strategy",
			NewStrategy: strategy,
		}
		line, err := json.Marshal(record)
		if err != nil {
			return nil, err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

func DecodeJSONL(data []byte) ([]NewStrategy, InputInfo, error) {
	info := InputInfo{Format: FormatJSONL, Version: -1}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), 16<<20)
	var out []NewStrategy
	lineNo := 0
	recordNo := 0
	for scanner.Scan() {
		lineNo++
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			continue
		}
		recordNo++
		obj := map[string]json.RawMessage{}
		if err := json.Unmarshal([]byte(raw), &obj); err != nil {
			return nil, info, apperr.Record(apperr.SpecInvalid, "invalid JSONL record: "+err.Error(), recordNo, map[string]any{"line": lineNo})
		}
		for key := range obj {
			if _, ok := jsonlAllowedFields[key]; !ok {
				return nil, info, apperr.Record(apperr.FormatUnsupported, "unknown mandatory JSONL field", recordNo, map[string]any{
					"field": key, "line": lineNo,
				})
			}
		}
		var envelope struct {
			Format        string `json:"format"`
			FormatVersion int    `json:"format_version"`
			RecordType    string `json:"record_type"`
		}
		if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
			return nil, info, apperr.Record(apperr.SpecInvalid, "invalid JSONL envelope", recordNo, nil)
		}
		if !isJSONLFormat(envelope.Format) {
			return nil, info, apperr.Record(apperr.FormatUnsupported, "JSONL format marker is missing or unsupported", recordNo, map[string]any{"format": envelope.Format})
		}
		if info.Version < 0 {
			info.Version = envelope.FormatVersion
		}
		if envelope.FormatVersion != info.Version {
			return nil, info, apperr.Record(apperr.SpecInvalid, "JSONL records mix format versions", recordNo, map[string]any{"expected": info.Version, "actual": envelope.FormatVersion})
		}
		if info.Version != CurrentFormatVersion {
			return nil, info, apperr.Record(apperr.FormatUnsupported, "JSONL format version is unsupported", recordNo, map[string]any{"version": info.Version})
		}
		if envelope.RecordType != "" && envelope.RecordType != "strategy" {
			return nil, info, apperr.Record(apperr.RecordCorrupt, "unknown JSONL record type", recordNo, map[string]any{"record_type": envelope.RecordType})
		}
		var strategy NewStrategy
		if err := json.Unmarshal([]byte(raw), &strategy); err != nil {
			return nil, info, apperr.Record(apperr.SpecInvalid, "invalid strategy fields: "+err.Error(), recordNo, map[string]any{"line": lineNo})
		}
		out = append(out, strategy)
	}
	if err := scanner.Err(); err != nil {
		return nil, info, apperr.New(apperr.SpecInvalid, "reading JSONL: "+err.Error())
	}
	if recordNo == 0 {
		return nil, info, apperr.New(apperr.SpecInvalid, "JSONL batch is empty")
	}
	if info.Version < 0 {
		info.Version = -1
	}
	return out, info, nil
}

func isJSONLFormat(format string) bool {
	return format == FormatJSONL || format == "strategy-jsonl"
}

func readJSONLHeader(data []byte) (string, int, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), 16<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var header struct {
			Format        string          `json:"format"`
			FormatVersion json.RawMessage `json:"format_version"`
		}
		if err := json.Unmarshal([]byte(line), &header); err != nil {
			return "", -1, err
		}
		version, err := jsonInt(header.FormatVersion)
		if err != nil {
			return header.Format, -1, err
		}
		return header.Format, version, nil
	}
	return "", -1, fmt.Errorf("empty JSONL")
}

func jsonInt(raw json.RawMessage) (int, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("missing integer")
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(n.String(), 10, 32)
	if err != nil {
		return 0, err
	}
	return int(v), nil
}
