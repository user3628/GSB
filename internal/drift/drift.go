package drift

import (
	"encoding/json"
	"fmt"
	"sort"

	"driftengine/internal/fixednum"
	"driftengine/internal/spec"
)

type Difference struct {
	ObjectID         string `json:"object_id,omitempty"`
	Field            string `json:"field"`
	Category         string `json:"category"`
	Desired          any    `json:"desired,omitempty"`
	Actual           any    `json:"actual,omitempty"`
	Message          string `json:"message"`
	Tolerance        string `json:"tolerance,omitempty"`
	BoundaryPolicy   string `json:"boundary_policy,omitempty"`
	DifferenceExact  string `json:"difference_exact,omitempty"`
	AllowedExact     string `json:"allowed_exact,omitempty"`
	BoundaryIncluded *bool  `json:"boundary_included,omitempty"`
}

type Report struct {
	ObjectID        string       `json:"object_id"`
	PrimaryCategory string       `json:"primary_category"`
	Categories      []string     `json:"categories"`
	Differences     []Difference `json:"differences"`
	InSync          bool         `json:"in_sync"`
}

func EvaluateAll(objects []spec.Object, policy spec.Policy) map[string]Report {
	reports := make(map[string]Report, len(objects))
	for _, object := range objects {
		reports[object.ID] = Evaluate(object, policy)
	}
	return reports
}

func Evaluate(object spec.Object, policy spec.Policy) Report {
	tolerance, _ := fixednum.Parse(policy.NumericRelativeTolerance)
	inclusive := policy.ToleranceBoundary != "exclusive"
	priority := policy.CategoryPriority
	if len(priority) == 0 {
		priority = spec.DefaultPriority()
	}
	rank := map[string]int{}
	for i, category := range priority {
		rank[category] = i
	}

	differences := []Difference{}
	fieldSet := map[string]bool{}
	for field := range object.Desired {
		fieldSet[field] = true
	}
	for field := range object.Actual {
		fieldSet[field] = true
	}
	fields := make([]string, 0, len(fieldSet))
	for field := range fieldSet {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	for _, field := range fields {
		desired, desiredOK := object.Desired[field]
		actual, actualOK := object.Actual[field]
		marker, hasMarker := object.SourceMarkers[field]

		if hasMarker && marker.Desired != "" && marker.Actual != "" && marker.Desired != marker.Actual {
			differences = append(differences, Difference{
				ObjectID: object.ID,
				Field:    field,
				Category: spec.CategorySourceConflict,
				Desired:  marker.Desired,
				Actual:   marker.Actual,
				Message:  "field is managed by conflicting sources",
			})
		}

		switch {
		case desiredOK && !actualOK:
			differences = append(differences, Difference{
				ObjectID: object.ID,
				Field:    field,
				Category: spec.CategoryFieldMissing,
				Desired:  desired,
				Actual:   nil,
				Message:  "desired field is missing from actual configuration",
			})
		case !desiredOK && actualOK:
			differences = append(differences, Difference{
				ObjectID: object.ID,
				Field:    field,
				Category: spec.CategoryFieldExtra,
				Desired:  nil,
				Actual:   actual,
				Message:  "actual field is not present in desired configuration",
			})
		case desiredOK && actualOK:
			if diff, ok := compareValue(object.ID, field, desired, actual, tolerance, inclusive); ok {
				differences = append(differences, diff)
			}
		}
	}

	sort.SliceStable(differences, func(i, j int) bool {
		leftRank := rank[differences[i].Category]
		rightRank := rank[differences[j].Category]
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		if differences[i].Field != differences[j].Field {
			return differences[i].Field < differences[j].Field
		}
		return differences[i].Message < differences[j].Message
	})

	hit := map[string]bool{}
	categories := []string{}
	for _, diff := range differences {
		if !hit[diff.Category] {
			hit[diff.Category] = true
			categories = append(categories, diff.Category)
		}
	}
	primary := spec.CategoryConverged
	if len(categories) > 0 {
		primary = categories[0]
	}
	return Report{
		ObjectID:        object.ID,
		PrimaryCategory: primary,
		Categories:      categories,
		Differences:     differences,
		InSync:          len(differences) == 0,
	}
}

func compareValue(objectID, field string, desired, actual any, tolerance fixednum.Rational, inclusive bool) (Difference, bool) {
	diff := Difference{
		ObjectID: objectID,
		Field:    field,
		Category: spec.CategoryValueOffset,
		Desired:  desired,
		Actual:   actual,
	}
	desiredNumber, desiredNumeric := fixednum.FromValue(desired)
	actualNumber, actualNumeric := fixednum.FromValue(actual)
	if desiredNumeric && actualNumeric {
		result := fixednum.RelativeWithin(actualNumber, desiredNumber, tolerance, inclusive)
		diff.Tolerance = tolerance.String()
		diff.BoundaryPolicy = result.BoundaryPolicy
		diff.DifferenceExact = result.DifferenceExact
		diff.AllowedExact = result.AllowedExact
		diff.BoundaryIncluded = &result.Inclusive
		if result.Within {
			return Difference{}, false
		}
		diff.Message = "numeric value exceeds declared relative tolerance"
		return diff, true
	}
	if ValuesEqual(desired, actual) {
		return Difference{}, false
	}
	diff.Message = "value differs from desired configuration"
	return diff, true
}

func ValuesEqual(a, b any) bool {
	if an, ok := fixednum.FromValue(a); ok {
		if bn, ok := fixednum.FromValue(b); ok {
			return an.Cmp(bn) == 0
		}
	}
	ar, errA := canonical(a)
	br, errB := canonical(b)
	if errA == nil && errB == nil {
		return string(ar) == string(br)
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}

func canonical(v any) ([]byte, error) {
	return json.Marshal(v)
}
