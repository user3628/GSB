package rewardpack

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
)

// ExportBundleFiles writes canonical, byte-stable review artifacts.
func ExportBundleFiles(dir string, bundle ArtifactBundle) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return errorWith(CodeInvalidInput, err)
	}
	if err := writeJSONLValues(filepath.Join(dir, "normalized.jsonl"), bundle.Normalized); err != nil {
		return err
	}
	if err := writeJSONLValues(filepath.Join(dir, "scores.jsonl"), bundle.Scores); err != nil {
		return err
	}
	if err := writeJSONLValues(filepath.Join(dir, "features.jsonl"), bundle.Features); err != nil {
		return err
	}
	if err := writeJSONLValues(filepath.Join(dir, "retained.jsonl"), bundle.Budget.Retained); err != nil {
		return err
	}
	if err := writeJSONLValues(filepath.Join(dir, "eliminated.jsonl"), bundle.Budget.Eliminated); err != nil {
		return err
	}
	for name, value := range map[string]any{
		"budget.json": bundle.Budget,
		"report.json": bundle.Report,
		"bundle.json": bundle,
	} {
		payload, err := CanonicalBytes(value)
		if err != nil {
			return errorWith(CodeInternal, err)
		}
		payload = append(payload, '\n')
		if err := os.WriteFile(filepath.Join(dir, name), payload, 0o644); err != nil {
			return &BusinessError{Code: CodeInvalidInput, Context: name, Message: err.Error()}
		}
	}
	return nil
}

func writeJSONLValues[T any](path string, values []T) error {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	for _, value := range values {
		if err := encoder.Encode(value); err != nil {
			return errorWith(CodeInternal, err)
		}
	}
	if err := os.WriteFile(path, buffer.Bytes(), 0o644); err != nil {
		return &BusinessError{Code: CodeInvalidInput, Context: filepath.Base(path), Message: err.Error()}
	}
	return nil
}
