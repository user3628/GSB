package configimpact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var (
	keyPathPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*(\.[A-Za-z_][A-Za-z0-9_-]*)*$`)
	idPattern      = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,63}$`)
)

// DecodeDocument decodes and validates a JSON document. Unknown fields are
// rejected so a misspelled safety-related field cannot be silently ignored.
func DecodeDocument(data []byte) (*Document, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var doc Document
	if err := dec.Decode(&doc); err != nil {
		return nil, invalidDocument("document JSON decode failed: %v", err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return nil, invalidDocument("document JSON has trailing content: %v", err)
	}
	return validateDocument(&doc)
}

func ensureJSONEOF(dec *json.Decoder) error {
	var extra any
	err := dec.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return fmt.Errorf("unexpected second JSON value")
	}
	return err
}

func invalidDocument(format string, args ...any) *BusinessError {
	return businessError(ErrDocInvalid, format, args...)
}

// validateDocument takes ownership of doc and returns the normalized copy.
func validateDocument(doc *Document) (*Document, error) {
	if doc == nil {
		return nil, invalidDocument("document is nil")
	}
	if doc.Version <= 0 {
		return nil, invalidDocument("document version must be positive")
	}
	if len(doc.Components) == 0 {
		return nil, invalidDocument("document must contain at least one component")
	}

	groupIDs := make(map[string]struct{}, len(doc.Groups))
	for gi := range doc.Groups {
		group := &doc.Groups[gi]
		if !idPattern.MatchString(group.ID) {
			return nil, invalidDocument("invalid group id %q", group.ID)
		}
		if _, exists := groupIDs[group.ID]; exists {
			return nil, invalidDocument("duplicate group id %q", group.ID)
		}
		groupIDs[group.ID] = struct{}{}
		if err := normalizeKeys(group.Defaults, "group "+group.ID); err != nil {
			return nil, err
		}
	}

	components := make(map[string]*Component, len(doc.Components))
	for ci := range doc.Components {
		component := &doc.Components[ci]
		if !idPattern.MatchString(component.ID) {
			return nil, invalidDocument("invalid component id %q", component.ID)
		}
		if _, exists := components[component.ID]; exists {
			return nil, invalidDocument("duplicate component id %q", component.ID)
		}
		if component.Group == "" {
			return nil, invalidDocument("component %q has no group", component.ID)
		}
		if _, exists := groupIDs[component.Group]; !exists {
			return nil, invalidDocument("component %q references missing group %q", component.ID, component.Group)
		}
		if err := normalizeKeys(component.Config, "component "+component.ID); err != nil {
			return nil, err
		}

		seenDeps := make(map[string]struct{}, len(component.DependsOn))
		for _, dep := range component.DependsOn {
			if dep == component.ID {
				return nil, invalidDocument("component %q depends on itself", component.ID)
			}
			if _, exists := seenDeps[dep]; exists {
				return nil, invalidDocument("component %q has duplicate dependency %q", component.ID, dep)
			}
			seenDeps[dep] = struct{}{}
		}
		seenWatches := make(map[string]struct{}, len(component.Watches))
		for _, key := range component.Watches {
			if !keyPathPattern.MatchString(key) {
				return nil, invalidDocument("component %q has invalid watched key path %q", component.ID, key)
			}
			if _, exists := seenWatches[key]; exists {
				return nil, invalidDocument("component %q has duplicate watched key %q", component.ID, key)
			}
			seenWatches[key] = struct{}{}
		}
		components[component.ID] = component
	}

	for _, component := range components {
		for _, dep := range component.DependsOn {
			if _, exists := components[dep]; !exists {
				return nil, invalidDocument("component %q references missing dependency %q", component.ID, dep)
			}
		}
	}
	if cycle := findDependencyCycle(components); len(cycle) > 0 {
		return nil, invalidDocument("dependency cycle: %s", strings.Join(cycle, " -> "))
	}

	return doc, nil
}

func normalizeKeys(keys []ConfigKey, owner string) error {
	seen := make(map[string]struct{}, len(keys))
	for i := range keys {
		key := &keys[i]
		if !keyPathPattern.MatchString(key.Key) {
			return invalidDocument("%s contains invalid key path %q", owner, key.Key)
		}
		if _, exists := seen[key.Key]; exists {
			return invalidDocument("%s declares key %q more than once", owner, key.Key)
		}
		seen[key.Key] = struct{}{}
		if key.Order < 0 {
			return invalidDocument("%s key %q has negative order", owner, key.Key)
		}
		normalizedType, normalizedValue, err := normalizeTypedValue(key.Type, key.Value)
		if err != nil {
			return invalidDocument("%s key %q: %v", owner, key.Key, err)
		}
		key.Type = normalizedType
		key.Value = normalizedValue
	}
	return nil
}

func normalizeType(valueType string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(valueType)) {
	case "string":
		return "string", nil
	case "int", "integer":
		return "int", nil
	case "float", "number":
		return "float", nil
	case "bool", "boolean":
		return "bool", nil
	case "json":
		return "json", nil
	default:
		return "", fmt.Errorf("unsupported type %q", valueType)
	}
}

func normalizeTypedValue(valueType string, raw json.RawMessage) (string, json.RawMessage, error) {
	normalizedType, err := normalizeType(valueType)
	if err != nil {
		return "", nil, err
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return "", nil, fmt.Errorf("value is required")
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return "", nil, fmt.Errorf("invalid JSON value: %w", err)
	}
	value := append(json.RawMessage(nil), compact.Bytes()...)
	if bytes.Equal(value, []byte("null")) {
		return "", nil, fmt.Errorf("null is not a defined value; omit the key instead")
	}

	dec := json.NewDecoder(bytes.NewReader(value))
	dec.UseNumber()
	var decoded any
	if err := dec.Decode(&decoded); err != nil {
		return "", nil, fmt.Errorf("invalid JSON value: %w", err)
	}
	switch normalizedType {
	case "string":
		if _, ok := decoded.(string); !ok {
			return "", nil, fmt.Errorf("value is not a string")
		}
	case "bool":
		if _, ok := decoded.(bool); !ok {
			return "", nil, fmt.Errorf("value is not a bool")
		}
	case "int":
		number, ok := decoded.(json.Number)
		if !ok {
			return "", nil, fmt.Errorf("value is not an integer")
		}
		if _, err := strconv.ParseInt(number.String(), 10, 64); err != nil {
			asFloat, floatErr := strconv.ParseFloat(number.String(), 64)
			if floatErr != nil || math.IsInf(asFloat, 0) || math.IsNaN(asFloat) || math.Trunc(asFloat) != asFloat || asFloat < math.MinInt64 || asFloat > math.MaxInt64 {
				return "", nil, fmt.Errorf("value is not a 64-bit integer")
			}
		}
	case "float":
		number, ok := decoded.(json.Number)
		if !ok {
			return "", nil, fmt.Errorf("value is not a number")
		}
		if _, err := strconv.ParseFloat(number.String(), 64); err != nil {
			return "", nil, fmt.Errorf("value is not a finite number")
		}
	case "json":
		// Any valid, non-null JSON is accepted.
	}
	return normalizedType, value, nil
}

func findDependencyCycle(components map[string]*Component) []string {
	state := make(map[string]uint8, len(components))
	stack := make([]string, 0, len(components))
	var visit func(string) []string
	visit = func(id string) []string {
		switch state[id] {
		case 1:
			start := 0
			for i, item := range stack {
				if item == id {
					start = i
					break
				}
			}
			cycle := append([]string(nil), stack[start:]...)
			return append(cycle, id)
		case 2:
			return nil
		}
		state[id] = 1
		stack = append(stack, id)
		for _, dep := range components[id].DependsOn {
			if cycle := visit(dep); len(cycle) > 0 {
				return cycle
			}
		}
		stack = stack[:len(stack)-1]
		state[id] = 2
		return nil
	}
	ids := make([]string, 0, len(components))
	for id := range components {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if cycle := visit(id); len(cycle) > 0 {
			return cycle
		}
	}
	return nil
}

func cloneDocument(doc *Document) (*Document, error) {
	payload, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	var clone Document
	if err := json.Unmarshal(payload, &clone); err != nil {
		return nil, err
	}
	return &clone, nil
}

func documentHash(doc *Document) (string, []byte, error) {
	payload, err := json.Marshal(doc)
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), payload, nil
}

func patchHash(patch *Patch) (string, error) {
	canonical := *patch
	canonical.Changes = make([]PatchChange, len(patch.Changes))
	for i, change := range patch.Changes {
		change.Scope = strings.ToLower(strings.TrimSpace(change.Scope))
		if change.Type != "" {
			if normalizedType, err := normalizeType(change.Type); err == nil {
				change.Type = normalizedType
			}
		}
		if len(bytes.TrimSpace(change.Value)) > 0 {
			var compact bytes.Buffer
			if err := json.Compact(&compact, change.Value); err == nil {
				change.Value = append(json.RawMessage(nil), compact.Bytes()...)
			}
		}
		canonical.Changes[i] = change
	}
	payload, err := json.Marshal(&canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
