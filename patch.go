package configimpact

import (
	"bytes"
	"encoding/json"
	"strings"
)

func invalidPatch(format string, args ...any) *BusinessError {
	return businessError(ErrPatchInvalid, format, args...)
}

// ApplyPatch validates and applies a patch to a cloned document. It does not
// mutate the input document and does not touch persisted state.
func ApplyPatch(doc *Document, patch Patch) (*Document, error) {
	if strings.TrimSpace(patch.ID) == "" {
		return nil, invalidPatch("patch id is required")
	}
	if patch.BaseVersion <= 0 {
		return nil, invalidPatch("base_version must be positive")
	}
	if len(patch.Changes) == 0 {
		return nil, invalidPatch("patch must contain at least one change")
	}
	if doc.Version != patch.BaseVersion {
		return nil, withDetails(
			businessError(ErrStaleBase, "patch base version %d does not match current version %d", patch.BaseVersion, doc.Version),
			map[string]any{"base_version": patch.BaseVersion, "current_version": doc.Version},
		)
	}
	clone, err := cloneDocument(doc)
	if err != nil {
		return nil, storageError(err)
	}

	targets := make(map[string]struct{}, len(patch.Changes))
	for i := range patch.Changes {
		change := &patch.Changes[i]
		if !keyPathPattern.MatchString(change.Key) {
			return nil, invalidPatch("change %d has invalid key path %q", i, change.Key)
		}
		scope, target, declaration, err := findPatchTarget(clone, change)
		if err != nil {
			return nil, err
		}
		duplicateID := scope + "\x00" + target + "\x00" + change.Key
		if _, exists := targets[duplicateID]; exists {
			return nil, invalidPatch("patch targets %s %s key %q more than once", scope, target, change.Key)
		}
		targets[duplicateID] = struct{}{}

		valueType := strings.TrimSpace(change.Type)
		if declaration != nil && valueType == "" {
			valueType = declaration.Type
		}
		if valueType == "" {
			return nil, invalidPatch("change %d key %q requires type for a new declaration", i, change.Key)
		}
		normalizedType, normalizedValue, err := normalizeTypedValue(valueType, change.Value)
		if err != nil {
			return nil, invalidPatch("change %d key %q: %v", i, change.Key, err)
		}
		if change.Order < 0 {
			return nil, invalidPatch("change %d key %q has negative order", i, change.Key)
		}

		newKey := ConfigKey{Key: change.Key, Type: normalizedType, Value: normalizedValue, Order: change.Order}
		if scope == "component" {
			component := findComponent(clone, target)
			if component == nil {
				return nil, invalidPatch("component %q not found", target)
			}
			replaceOrAppendKey(&component.Config, newKey)
		} else {
			group := findGroup(clone, target)
			if group == nil {
				return nil, invalidPatch("group %q not found", target)
			}
			replaceOrAppendKey(&group.Defaults, newKey)
		}
	}
	clone.Version = patch.BaseVersion + 1
	if _, err := validateDocument(clone); err != nil {
		return nil, err
	}
	return clone, nil
}

func findPatchTarget(doc *Document, change *PatchChange) (string, string, *ConfigKey, error) {
	scope := strings.ToLower(strings.TrimSpace(change.Scope))
	switch scope {
	case "component":
		if change.Component == "" {
			return "", "", nil, invalidPatch("key %q uses component scope without component", change.Key)
		}
		component := findComponent(doc, change.Component)
		if component == nil {
			return "", "", nil, invalidPatch("component %q not found", change.Component)
		}
		return "component", change.Component, findKey(component.Config, change.Key), nil
	case "group":
		if change.Group == "" {
			return "", "", nil, invalidPatch("key %q uses group scope without group", change.Key)
		}
		group := findGroup(doc, change.Group)
		if group == nil {
			return "", "", nil, invalidPatch("group %q not found", change.Group)
		}
		return "group", change.Group, findKey(group.Defaults, change.Key), nil
	case "":
		type match struct {
			scope string
			id    string
			key   *ConfigKey
		}
		matches := make([]match, 0, 2)
		for gi := range doc.Groups {
			if key := findKey(doc.Groups[gi].Defaults, change.Key); key != nil {
				matches = append(matches, match{scope: "group", id: doc.Groups[gi].ID, key: key})
			}
		}
		for ci := range doc.Components {
			if key := findKey(doc.Components[ci].Config, change.Key); key != nil {
				matches = append(matches, match{scope: "component", id: doc.Components[ci].ID, key: key})
			}
		}
		if len(matches) == 0 {
			return "", "", nil, invalidPatch("key %q has no existing declaration; scope, group or component is required", change.Key)
		}
		if len(matches) > 1 {
			return "", "", nil, invalidPatch("key %q is ambiguous across %d declarations; set scope and target", change.Key, len(matches))
		}
		return matches[0].scope, matches[0].id, matches[0].key, nil
	default:
		return "", "", nil, invalidPatch("unsupported scope %q", change.Scope)
	}
}

func replaceOrAppendKey(keys *[]ConfigKey, replacement ConfigKey) {
	for i := range *keys {
		if (*keys)[i].Key == replacement.Key {
			(*keys)[i] = replacement
			return
		}
	}
	*keys = append(*keys, replacement)
}

func findComponent(doc *Document, id string) *Component {
	for i := range doc.Components {
		if doc.Components[i].ID == id {
			return &doc.Components[i]
		}
	}
	return nil
}

func findGroup(doc *Document, id string) *Group {
	for i := range doc.Groups {
		if doc.Groups[i].ID == id {
			return &doc.Groups[i]
		}
	}
	return nil
}

func findKey(keys []ConfigKey, key string) *ConfigKey {
	for i := range keys {
		if keys[i].Key == key {
			return &keys[i]
		}
	}
	return nil
}

// DecodePatch decodes a patch strict-JSON payload.
func DecodePatch(data []byte) (*Patch, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var patch Patch
	if err := dec.Decode(&patch); err != nil {
		return nil, invalidPatch("patch JSON decode failed: %v", err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return nil, invalidPatch("patch JSON has trailing content: %v", err)
	}
	return &patch, nil
}
