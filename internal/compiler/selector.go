package compiler

import (
	"encoding/json"
	"sort"

	"netpolcompiler/internal/model"
)

func SelectorMatches(selector model.LabelSelector, labels map[string]string) bool {
	for key, expected := range selector.MatchLabels {
		actual, ok := labels[key]
		if !ok || actual != expected {
			return false
		}
	}
	for _, key := range selector.Exists {
		if _, ok := labels[key]; !ok {
			return false
		}
	}
	for _, requirement := range selector.NotIn {
		value, ok := labels[requirement.Key]
		if !ok {
			continue
		}
		for _, blocked := range requirement.Values {
			if value == blocked {
				return false
			}
		}
	}
	return true
}

func CanonicalSelector(selector model.LabelSelector) string {
	normalized := NormalizeSelector(selector)
	data, err := json.Marshal(normalized)
	if err != nil {
		return ""
	}
	return string(data)
}

func NormalizeSelector(selector model.LabelSelector) model.LabelSelector {
	result := model.LabelSelector{}
	if len(selector.MatchLabels) > 0 {
		result.MatchLabels = make(map[string]string, len(selector.MatchLabels))
		for key, value := range selector.MatchLabels {
			result.MatchLabels[key] = value
		}
	}
	result.Exists = append([]string(nil), selector.Exists...)
	sort.Strings(result.Exists)
	if len(selector.NotIn) > 0 {
		result.NotIn = make([]model.NotInRequirement, len(selector.NotIn))
		for i, requirement := range selector.NotIn {
			values := append([]string(nil), requirement.Values...)
			sort.Strings(values)
			result.NotIn[i] = model.NotInRequirement{Key: requirement.Key, Values: values}
		}
		sort.SliceStable(result.NotIn, func(i, j int) bool {
			return result.NotIn[i].Key < result.NotIn[j].Key
		})
	}
	return result
}

func selectorConstraintCount(selector model.LabelSelector) int {
	return len(selector.MatchLabels) + len(selector.Exists) + len(selector.NotIn)
}

// SelectorSubsetOf reports whether every label set matched by specific is also
// matched by general. The comparison is structural but sound for MatchLabels,
// Exists and NotIn predicates.
func SelectorSubsetOf(specific, general model.LabelSelector) bool {
	for key, expected := range general.MatchLabels {
		actual, ok := specific.MatchLabels[key]
		if !ok || actual != expected {
			return false
		}
	}
	for _, key := range general.Exists {
		if value, ok := specific.MatchLabels[key]; ok && value != "" {
			continue
		}
		if _, ok := specific.MatchLabels[key]; ok {
			continue
		}
		if containsString(specific.Exists, key) {
			continue
		}
		return false
	}
	for _, generalNotIn := range general.NotIn {
		if specificValue, ok := specific.MatchLabels[generalNotIn.Key]; ok {
			for _, blocked := range generalNotIn.Values {
				if specificValue == blocked {
					return false
				}
			}
			continue
		}
		specificNotIn, found := findNotIn(specific.NotIn, generalNotIn.Key)
		if !found {
			return false
		}
		for _, blockedByGeneral := range generalNotIn.Values {
			if !containsString(specificNotIn.Values, blockedByGeneral) {
				return false
			}
		}
	}
	return true
}

func SelectorsCanOverlap(left, right model.LabelSelector) bool {
	for key, leftValue := range left.MatchLabels {
		if rightValue, ok := right.MatchLabels[key]; ok && leftValue != rightValue {
			return false
		}
		for _, requirement := range right.NotIn {
			if requirement.Key == key && containsString(requirement.Values, leftValue) {
				return false
			}
		}
	}
	for key, rightValue := range right.MatchLabels {
		for _, requirement := range left.NotIn {
			if requirement.Key == key && containsString(requirement.Values, rightValue) {
				return false
			}
		}
	}
	return true
}

func findNotIn(requirements []model.NotInRequirement, key string) (model.NotInRequirement, bool) {
	for _, requirement := range requirements {
		if requirement.Key == key {
			return requirement, true
		}
	}
	return model.NotInRequirement{}, false
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
