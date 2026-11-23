package policy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"sandboxpolicy/business"
)

// ParsePolicy decodes and strictly normalizes a standalone policy file.
func ParsePolicy(data []byte) (*Policy, error) {
	var p Policy
	if err := decodeStrict(data, &p); err != nil {
		return nil, business.At(business.CodePolicyMalformed, "policy", business.ReasonInvalidField, err.Error())
	}
	if err := NormalizeStrict(&p); err != nil {
		return nil, err
	}
	return &p, nil
}

// ParsePolicyFile loads a deterministic local policy sample.
func ParsePolicyFile(path string) (*Policy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read policy %s: %w", path, err)
	}
	return ParsePolicy(data)
}

// ParseChangeSet decodes and normalizes a change-set file. Complete state-aware
// validation happens in Apply while the SQLite transaction is held.
func ParseChangeSet(data []byte) (*ChangeSet, error) {
	var cs ChangeSet
	if err := decodeStrict(data, &cs); err != nil {
		return nil, business.At(business.CodePolicyMalformed, "changeset", business.ReasonInvalidField, err.Error())
	}
	if err := NormalizeChangeSet(&cs); err != nil {
		return nil, err
	}
	return &cs, nil
}

func ParseChangeSetFile(path string) (*ChangeSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read changeset %s: %w", path, err)
	}
	return ParseChangeSet(data)
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON documents are not allowed")
		}
		return fmt.Errorf("trailing content: %w", err)
	}
	return nil
}
