package convergence

import (
	"fmt"
	"math/big"
	"sort"
)

type DriftDetail struct {
	Category       string `json:"category"`
	CategoryZh     string `json:"category_zh"`
	Field          string `json:"field"`
	ReasonCode     string `json:"reason_code"`
	Basis          string `json:"basis"`
	Expected       any    `json:"expected,omitempty"`
	Actual         any    `json:"actual,omitempty"`
	ExpectedSource string `json:"expected_source,omitempty"`
	ActualSource   string `json:"actual_source,omitempty"`
	Tolerance      string `json:"tolerance,omitempty"`
	AbsoluteDelta  string `json:"absolute_delta,omitempty"`
	RelativeDelta  string `json:"relative_delta,omitempty"`
}

type FieldCheck struct {
	Field       string `json:"field"`
	Present     bool   `json:"present"`
	ValueMatch  bool   `json:"value_match"`
	SourceMatch bool   `json:"source_match"`
	Matched     bool   `json:"matched"`
	Basis       string `json:"basis"`
}

type DriftReport struct {
	ObjectID        string        `json:"object_id"`
	ClusterID       string        `json:"cluster_id"`
	Kind            string        `json:"kind"`
	Namespace       string        `json:"namespace"`
	DriftCategory   string        `json:"drift_category"`
	PrimaryCategory string        `json:"primary_category"`
	Categories      []string      `json:"categories"`
	Converged       bool          `json:"converged"`
	AffectedFields  []string      `json:"affected_fields"`
	Details         []DriftDetail `json:"details"`
	FieldChecks     []FieldCheck  `json:"field_checks,omitempty"`
}

// DriftObject judges one object. A nil/incomplete field collection is an error rather than a silent in-sync result.
func DriftObject(object Object, policy Policy) (*DriftReport, error) {
	if object.Expected == nil {
		return nil, NewError(CodeSpecInvalid, "expected field collection is nil/incomplete", map[string]any{"object_id": object.ID})
	}
	if object.Actual == nil {
		return nil, NewError(CodeSpecInvalid, "actual field collection is nil/incomplete", map[string]any{"object_id": object.ID})
	}
	expected, err := normalizeFieldSet(object.Expected)
	if err != nil {
		return nil, err
	}
	actual, err := normalizeFieldSet(object.Actual)
	if err != nil {
		return nil, err
	}
	normalizePolicy(&policy)

	names := make(map[string]struct{})
	for name := range expected {
		names[name] = struct{}{}
	}
	for name := range actual {
		names[name] = struct{}{}
	}
	sortedNames := make([]string, 0, len(names))
	for name := range names {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)

	report := &DriftReport{
		ObjectID:       object.ID,
		ClusterID:      object.ClusterID,
		Kind:           object.Kind,
		Namespace:      object.Namespace,
		Categories:     []string{},
		AffectedFields: []string{},
		Details:        []DriftDetail{},
		FieldChecks:    []FieldCheck{},
	}

	categorySet := map[string]bool{}
	affectedSet := map[string]bool{}

	addDetail := func(detail DriftDetail) {
		detail.CategoryZh = CategoryLabel(detail.Category)
		report.Details = append(report.Details, detail)
		categorySet[detail.Category] = true
		if !affectedSet[detail.Field] {
			affectedSet[detail.Field] = true
			report.AffectedFields = append(report.AffectedFields, detail.Field)
		}
	}

	for _, name := range sortedNames {
		expectedField, expectedOK := expected[name]
		actualField, actualOK := actual[name]

		if expectedOK && !actualOK {
			addDetail(DriftDetail{
				Category:       CategoryMissing,
				Field:          name,
				ReasonCode:     "ACTUAL_FIELD_MISSING",
				Basis:          "期望字段存在，但实际字段集合中不存在该字段",
				Expected:       expectedField.Value,
				ExpectedSource: expectedField.Source,
			})
			report.FieldChecks = append(report.FieldChecks, FieldCheck{Field: name, Present: false, Matched: false, Basis: "actual field missing"})
			continue
		}
		if !expectedOK && actualOK {
			addDetail(DriftDetail{
				Category:     CategoryExtra,
				Field:        name,
				ReasonCode:   "UNEXPECTED_ACTUAL_FIELD",
				Basis:        "实际字段存在，但期望字段集合中未声明该字段",
				Actual:       actualField.Value,
				ActualSource: actualField.Source,
			})
			report.FieldChecks = append(report.FieldChecks, FieldCheck{Field: name, Present: false, Matched: false, Basis: "extra actual field"})
			continue
		}

		sourceMatch := expectedField.Source == actualField.Source
		valueMatch, basis, detailExtras := compareFieldValue(expectedField.Value, actualField.Value, policy.DriftTolerance)
		check := FieldCheck{
			Field:       name,
			Present:     true,
			ValueMatch:  valueMatch,
			SourceMatch: sourceMatch,
			Matched:     valueMatch && sourceMatch,
			Basis:       basis,
		}
		report.FieldChecks = append(report.FieldChecks, check)

		if !sourceMatch {
			detail := DriftDetail{
				Category:       CategorySourceConflict,
				Field:          name,
				ReasonCode:     "SOURCE_MARKER_CONFLICT",
				Basis:          "同名字段的来源标记不一致",
				Expected:       expectedField.Value,
				Actual:         actualField.Value,
				ExpectedSource: expectedField.Source,
				ActualSource:   actualField.Source,
			}
			applyDetailExtras(&detail, detailExtras)
			addDetail(detail)
		}
		if !valueMatch {
			detail := DriftDetail{
				Category:       CategoryValueShift,
				Field:          name,
				ReasonCode:     "VALUE_OUTSIDE_RELATIVE_TOLERANCE",
				Basis:          basis,
				Expected:       expectedField.Value,
				Actual:         actualField.Value,
				ExpectedSource: expectedField.Source,
				ActualSource:   actualField.Source,
			}
			applyDetailExtras(&detail, detailExtras)
			addDetail(detail)
		}
	}

	for _, category := range policy.DriftPriority {
		if categorySet[category] {
			report.Categories = append(report.Categories, category)
		}
	}
	// Defensive stable order if policy omitted a category accepted by normalization.
	if len(report.Categories) == 0 && len(report.Details) > 0 {
		report.Categories = append(report.Categories, DefaultDriftPriority...)
	}
	if len(report.Categories) == 0 {
		report.DriftCategory = CategoryInSync
		report.PrimaryCategory = CategoryInSync
		report.Converged = true
	} else {
		report.DriftCategory = report.Categories[0]
		report.PrimaryCategory = report.Categories[0]
	}
	sort.Strings(report.AffectedFields)
	return report, nil
}

type valueComparison struct {
	tolerance     string
	absoluteDelta string
	relativeDelta string
}

func compareFieldValue(expected, actual any, tolerance Decimal) (bool, string, valueComparison) {
	expectedDecimal, expectedNumeric := decimalFromValue(expected)
	actualDecimal, actualNumeric := decimalFromValue(actual)
	if expectedNumeric && actualNumeric {
		expectedRat := expectedDecimal.Rat()
		actualRat := actualDecimal.Rat()
		difference := new(big.Rat).Sub(actualRat, expectedRat)
		absolute := new(big.Rat).Abs(difference)
		base := new(big.Rat).Set(expectedRat)
		if actualAbs := new(big.Rat).Abs(actualRat); actualAbs.Cmp(new(big.Rat).Abs(base)) > 0 {
			base = actualAbs
		} else {
			base = new(big.Rat).Abs(base)
		}
		allowed := new(big.Rat).Mul(tolerance.Rat(), base)
		within := absolute.Cmp(allowed) <= 0
		relative := new(big.Rat)
		if base.Sign() != 0 {
			relative.Quo(absolute, base)
		} else if absolute.Sign() != 0 {
			relative = absolute
		}
		extras := valueComparison{
			tolerance:     tolerance.String(),
			absoluteDelta: absolute.FloatString(8),
			relativeDelta: relative.FloatString(8),
		}
		basis := fmt.Sprintf("定点十进制比较：|actual-expected|=%s；tolerance(%s)×max(|expected|,|actual|)=%s；relative_delta=%s",
			absolute.FloatString(8), tolerance.String(), allowed.FloatString(8), relative.FloatString(8))
		if within {
			return true, "数值偏差在声明的相对容差内；" + basis, extras
		}
		return false, "数值偏差超过声明的相对容差；" + basis, extras
	}

	expectedCanonical, err1 := CanonicalJSON(expected)
	actualCanonical, err2 := CanonicalJSON(actual)
	if err1 == nil && err2 == nil && string(expectedCanonical) == string(actualCanonical) {
		return true, "规范化后字段值完全一致", valueComparison{}
	}
	return false, "非数值字段或数值/非数值类型不匹配，规范化后不完全一致", valueComparison{}
}

func applyDetailExtras(detail *DriftDetail, extras valueComparison) {
	if extras.tolerance != "" {
		detail.Tolerance = extras.tolerance
		detail.AbsoluteDelta = extras.absoluteDelta
		detail.RelativeDelta = extras.relativeDelta
	}
}

func JudgeSnapshot(snapshot *Snapshot) ([]DriftReport, map[string]DriftReport, error) {
	if snapshot == nil {
		return nil, nil, NewError(CodeSpecInvalid, "snapshot is nil", nil)
	}
	ordered := make([]DriftReport, 0, len(snapshot.Objects))
	byID := make(map[string]DriftReport, len(snapshot.Objects))
	for _, object := range snapshot.Objects {
		report, err := DriftObject(object, snapshot.Policy)
		if err != nil {
			return nil, nil, err
		}
		ordered = append(ordered, *report)
		byID[object.ID] = *report
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ObjectID < ordered[j].ObjectID })
	return ordered, byID, nil
}
