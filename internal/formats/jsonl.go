package formats

import (
	"bytes"
	"encoding/json"

	"supplyaudit/internal/model"
)

type jsonRecord struct {
	Name       string `json:"name"`
	Component  string `json:"component"`
	Version    string `json:"version"`
	Digest     string `json:"digest"`
	Integrity  string `json:"integrity"`
	Source     string `json:"source"`
	Repository string `json:"repository"`
	Signature  string `json:"signature"`
}

func parseJSONL(data []byte, result *model.SourceResult, options ParseOptions) {
	result.FormatVersion = "1"
	lineStart := 0
	recordIndex := 0
	for offset := 0; offset <= len(data); offset++ {
		if offset != len(data) && data[offset] != '\n' {
			continue
		}
		line := data[lineStart:offset]
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			lineStart = offset + 1
			continue
		}
		lineOffset := int64(lineStart)
		var record jsonRecord
		if err := json.Unmarshal(trimmed, &record); err != nil {
			result.AddIssue(model.CodeRecordInvalid, "invalid JSON line: "+err.Error(), lineOffset, int64(len(data)), recordIndex)
			recordIndex++
			lineStart = offset + 1
			continue
		}
		name := record.Name
		if name == "" {
			name = record.Component
		}
		digest := record.Digest
		if digest == "" {
			digest = record.Integrity
		}
		origin := record.Repository
		if origin == "" {
			origin = record.Source
		}
		if name == "" || record.Version == "" || digest == "" {
			result.AddIssue(model.CodeRecordInvalid, "JSON record requires name/component, version, and digest/integrity", lineOffset, int64(len(data)), recordIndex)
			recordIndex++
			lineStart = offset + 1
			continue
		}
		decl, err := buildDeclaration(result.Spec, lineOffset, recordIndex, name, record.Version, digest, origin, record.Signature)
		if err != nil {
			issueFromError(result, err, lineOffset, int64(len(data)), recordIndex)
			recordIndex++
			lineStart = offset + 1
			continue
		}
		if !acceptDeclaration(result, decl, options) {
			return
		}
		recordIndex++
		lineStart = offset + 1
	}
}
