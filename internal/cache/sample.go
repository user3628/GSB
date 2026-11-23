package cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

type sampleBatch struct {
	Entries []sampleEntry `json:"entries"`
}

type sampleEntry struct {
	StageID          string `json:"stage_id"`
	InputFingerprint string `json:"input_fingerprint"`
	ToolchainVersion string `json:"toolchain_version"`
	ArtifactContent  string `json:"artifact_content"`
	ArtifactDigest   string `json:"artifact_digest"`
	ArtifactSize     int64  `json:"artifact_size"`
	RetainUntil      string `json:"retain_until"`
}

// ParseSampleBatch validates the complete batch before returning any entry.
// Callers therefore cannot accidentally persist the valid prefix of an invalid
// deterministic sample file.
func ParseSampleBatch(data []byte) ([]EntryInput, error) {
	var batch sampleBatch
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&batch); err != nil {
		return nil, wrapError(CodeEntryInvalid, "invalid sample JSON", nil, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err != nil {
			return nil, wrapError(CodeEntryInvalid, "sample JSON contains invalid trailing data", nil, err)
		}
		return nil, newError(CodeEntryInvalid, "sample JSON contains trailing values", nil)
	}
	if len(batch.Entries) == 0 {
		return nil, newError(CodeEntryInvalid, "sample batch must contain at least one entry", nil)
	}

	inputs := make([]EntryInput, 0, len(batch.Entries))
	for index, sample := range batch.Entries {
		retainUntil, err := time.Parse(time.RFC3339, sample.RetainUntil)
		if err != nil {
			return nil, wrapError(CodeEntryInvalid, "retain_until must be RFC3339", map[string]any{
				"index": index,
				"field": "retain_until",
				"value": sample.RetainUntil,
			}, err)
		}
		input, _, _, err := NormalizeAndValidate(EntryInput{
			StageID:          sample.StageID,
			InputFingerprint: sample.InputFingerprint,
			ToolchainVersion: sample.ToolchainVersion,
			ArtifactContent:  []byte(sample.ArtifactContent),
			ArtifactDigest:   sample.ArtifactDigest,
			ArtifactSize:     sample.ArtifactSize,
			RetainUntil:      retainUntil,
		})
		if err != nil {
			return nil, withContext(err, map[string]any{"index": index})
		}
		inputs = append(inputs, input)
	}
	return inputs, nil
}

func LoadSampleFile(path string) ([]EntryInput, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, wrapError(CodeEntryInvalid, "cannot read sample file", map[string]any{"path": path}, err)
	}
	inputs, err := ParseSampleBatch(data)
	if err != nil {
		return nil, withContext(err, map[string]any{"path": path})
	}
	return inputs, nil
}

func SampleByName(inputs []EntryInput, stage string) (EntryInput, error) {
	for _, input := range inputs {
		if input.StageID == stage {
			return input, nil
		}
	}
	return EntryInput{}, newError(CodeEntryInvalid, fmt.Sprintf("sample stage %q not found", stage), map[string]any{"stage_id": stage})
}
