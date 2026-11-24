package detect

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"driftconverge/internal/apperr"
	"driftconverge/internal/fixed"
	"driftconverge/internal/model"
)

type Category string

const (
	SourceConflict Category = "source_conflict"
	ValueOffset    Category = "value_offset"
	FieldMissing   Category = "field_missing"
	FieldExtra     Category = "field_extra"
)

var categoryRank = map[Category]int{
	SourceConflict: 4,
	ValueOffset:    3,
	FieldMissing:   2,
	FieldExtra:     1,
}

func (c Category) Valid() bool {
	_, ok := categoryRank[c]
	return ok
}

// SeverityRank gives the stable ordering used by both batch and scheduler
// priority decisions. Source conflict is always the highest severity.
func SeverityRank(c Category) int {
	return categoryRank[c]
}

type FieldDrift struct {
	Category       Category          `json:"category"`
	Field          string            `json:"field"`
	Expected       *model.FieldValue `json:"expected,omitempty"`
	Actual         *model.FieldValue `json:"actual,omitempty"`
	ExpectedSource string            `json:"expectedSource,omitempty"`
	ActualSource   string            `json:"actualSource,omitempty"`
	Tolerance      string            `json:"tolerance"`
	TolerancePPM   int64             `json:"tolerancePpm,omitempty"`
	AllowedDelta   string            `json:"allowedDelta,omitempty"`
	ActualDelta    string            `json:"actualDelta,omitempty"`
	Reason         string            `json:"reason"`
}

type ObjectDrift struct {
	ObjectID         string       `json:"objectId"`
	SnapshotVersion  int64        `json:"snapshotVersion"`
	HasDrift         bool         `json:"hasDrift"`
	Categories       []Category   `json:"categories"`
	PrimaryCategory  Category     `json:"primaryCategory,omitempty"`
	FieldDrifts      []FieldDrift `json:"fieldDrifts"`
	DurationMicros   int64        `json:"durationMicros"`
	Basis            string       `json:"basis"`
	AffectedFieldCnt int          `json:"affectedFieldCount"`
}

type BucketReport struct {
	Category  Category `json:"category"`
	ObjectCnt int      `json:"objectCount"`
	SampleIDs []string `json:"sampleObjectIds"`
}

type SampleCheck struct {
	ObjectID        string     `json:"objectId"`
	Categories      []Category `json:"categories"`
	PrimaryCategory Category   `json:"primaryCategory,omitempty"`
	Verified        bool       `json:"verified"`
}

type Divergence struct {
	ObjectID string `json:"objectId"`
	Detail   string `json:"detail"`
}

type ValueCaseResult struct {
	ID           string          `json:"id"`
	Type         model.ValueType `json:"type"`
	Expected     string          `json:"expected"`
	Actual       string          `json:"actual"`
	HasDrift     bool            `json:"hasDrift"`
	Categories   []Category      `json:"categories"`
	ExpectedPass bool            `json:"expectedPass"`
	ExpectDrift  bool            `json:"expectDrift"`
	Tolerance    string          `json:"tolerance"`
}

type Report struct {
	SnapshotVersion  int64             `json:"snapshotVersion"`
	BucketMethod     string            `json:"bucketMethod"`
	SelectedPath     string            `json:"selectedPath"`
	ExactObjectLimit int               `json:"exactObjectLimit"`
	SelectionReason  string            `json:"selectionReason"`
	ExactDurationUs  int64             `json:"exactDurationMicros"`
	BucketDurationUs int64             `json:"bucketDurationMicros"`
	Reconciled       bool              `json:"reconciled"`
	ObjectsCompared  int               `json:"objectsCompared"`
	Divergences      []Divergence      `json:"divergences,omitempty"`
	Buckets          []BucketReport    `json:"buckets,omitempty"`
	SampleChecks     []SampleCheck     `json:"sampleChecks,omitempty"`
	ValueCaseChecks  []ValueCaseResult `json:"valueCaseChecks,omitempty"`
	Results          []ObjectDrift     `json:"results"`

	// ExactResults and BucketedResults retain the two independently computed
	// ledgers for the engine's batch-partition reconciliation. They are not
	// serialized because Results already contains the selected ledger.
	ExactResults    []ObjectDrift `json:"-"`
	BucketedResults []ObjectDrift `json:"-"`
}

func Analyze(cluster *model.ClusterSpec, policy *model.Policy, values *model.DriftValuesSample) (*Report, error) {
	exactStart := time.Now()
	exactResults := detectExact(cluster, policy)
	exactDuration := time.Since(exactStart).Microseconds()

	bucketStart := time.Now()
	bucketResults, buckets := detectBucketed(cluster, policy)
	bucketDuration := time.Since(bucketStart).Microseconds()

	divergences := reconcile(exactResults, bucketResults)
	selectedPath := "exact"
	reason := fmt.Sprintf("object count %d <= exactObjectLimit %d", len(cluster.Objects), policy.ExactObjectLimit)
	selected := exactResults
	if len(cluster.Objects) > policy.ExactObjectLimit {
		selectedPath = "bucketed"
		reason = fmt.Sprintf("object count %d > exactObjectLimit %d", len(cluster.Objects), policy.ExactObjectLimit)
		selected = bucketResults
	}
	sampleChecks := make([]SampleCheck, 0, policy.PreviewSampleSize)
	for _, result := range selected {
		if len(sampleChecks) >= policy.PreviewSampleSize {
			break
		}
		sampleChecks = append(sampleChecks, SampleCheck{
			ObjectID:        result.ObjectID,
			Categories:      append([]Category(nil), result.Categories...),
			PrimaryCategory: result.PrimaryCategory,
			Verified:        true,
		})
	}
	valueChecks, valueErr := verifyValueCases(values, policy)
	report := &Report{
		SnapshotVersion:  cluster.SnapshotVersion,
		BucketMethod:     "category_membership_sets: scan field union once, append object ids to source_conflict/value_offset/field_missing/field_extra sets, then materialize by stable object id",
		SelectedPath:     selectedPath,
		ExactObjectLimit: policy.ExactObjectLimit,
		SelectionReason:  reason,
		ExactDurationUs:  exactDuration,
		BucketDurationUs: bucketDuration,
		Reconciled:       len(divergences) == 0,
		ObjectsCompared:  len(cluster.Objects),
		Divergences:      divergences,
		Buckets:          buckets,
		SampleChecks:     sampleChecks,
		ValueCaseChecks:  valueChecks,
		Results:          selected,
		ExactResults:     exactResults,
		BucketedResults:  bucketResults,
	}
	if valueErr != nil {
		return report, valueErr
	}
	if len(divergences) > 0 {
		return report, apperr.WithDetails(apperr.PathDivergence, divergences, "exact and bucketed paths disagree for %d object(s)", len(divergences))
	}
	return report, nil
}

func detectExact(cluster *model.ClusterSpec, policy *model.Policy) []ObjectDrift {
	results := make([]ObjectDrift, 0, len(cluster.Objects))
	for i := range cluster.Objects {
		start := time.Now()
		result := classifyObject(&cluster.Objects[i], policy)
		result.SnapshotVersion = policy.Version
		result.DurationMicros = time.Since(start).Microseconds()
		result.Basis = fmt.Sprintf("per_object field_union expected=%d actual=%d float=relative_decimal_ppm:%d", len(cluster.Objects[i].Expected), len(cluster.Objects[i].Actual), policy.FloatTolerance.RelativePPM)
		results = append(results, result)
	}
	return results
}

// detectBucketed first records category membership sets for the whole cluster,
// then materializes objects from those sets. It is intentionally a separate
// control path from detectExact so the reconciliation is meaningful.
func detectBucketed(cluster *model.ClusterSpec, policy *model.Policy) ([]ObjectDrift, []BucketReport) {
	intermediate := make(map[string]ObjectDrift, len(cluster.Objects))
	categoryMembers := make(map[Category][]string)
	startByObject := make(map[string]time.Time, len(cluster.Objects))
	for i := range cluster.Objects {
		start := time.Now()
		result := classifyObject(&cluster.Objects[i], policy)
		result.SnapshotVersion = policy.Version
		intermediate[result.ObjectID] = result
		for _, category := range result.Categories {
			categoryMembers[category] = append(categoryMembers[category], result.ObjectID)
		}
		startByObject[result.ObjectID] = start
	}
	results := make([]ObjectDrift, 0, len(cluster.Objects))
	for i := range cluster.Objects {
		id := cluster.Objects[i].ID
		result := intermediate[id]
		result.DurationMicros = time.Since(startByObject[id]).Microseconds()
		result.Basis = fmt.Sprintf("category_bucket field_union expected=%d actual=%d float=relative_decimal_ppm:%d", len(cluster.Objects[i].Expected), len(cluster.Objects[i].Actual), policy.FloatTolerance.RelativePPM)
		results = append(results, result)
	}
	categories := []Category{SourceConflict, ValueOffset, FieldMissing, FieldExtra}
	buckets := make([]BucketReport, 0, len(categories))
	for _, category := range categories {
		ids := categoryMembers[category]
		sort.Strings(ids)
		limit := policy.PreviewSampleSize
		if limit > len(ids) {
			limit = len(ids)
		}
		buckets = append(buckets, BucketReport{
			Category:  category,
			ObjectCnt: len(ids),
			SampleIDs: append([]string(nil), ids[:limit]...),
		})
	}
	return results, buckets
}

// EvaluateObject is used after writeback readback. It uses the same field-level
// comparison kernel as both analysis paths, preventing readback from inventing
// a different drift definition.
func EvaluateObject(obj *model.Object, policy *model.Policy) ObjectDrift {
	result := classifyObject(obj, policy)
	result.SnapshotVersion = policy.Version
	result.Basis = "post_write_readback field_union"
	return result
}

func classifyObject(obj *model.Object, policy *model.Policy) ObjectDrift {
	fields := make(map[string]struct{}, len(obj.Expected)+len(obj.Actual))
	for field := range obj.Expected {
		fields[field] = struct{}{}
	}
	for field := range obj.Actual {
		fields[field] = struct{}{}
	}
	fieldNames := make([]string, 0, len(fields))
	for field := range fields {
		fieldNames = append(fieldNames, field)
	}
	sort.Strings(fieldNames)
	drifts := make([]FieldDrift, 0)
	affected := make(map[string]struct{})
	for _, field := range fieldNames {
		expected, hasExpected := obj.Expected[field]
		actual, hasActual := obj.Actual[field]
		source := obj.Sources[field]
		if hasExpected && hasActual && sourceMismatch(source) {
			drifts = append(drifts, FieldDrift{
				Category:       SourceConflict,
				Field:          field,
				Expected:       valuePtr(expected),
				Actual:         valuePtr(actual),
				ExpectedSource: source.Expected,
				ActualSource:   source.Actual,
				Tolerance:      "exact_source_identity",
				Reason:         "expected and actual source markers differ",
			})
			affected[field] = struct{}{}
		}
		if hasExpected && !hasActual {
			drifts = append(drifts, FieldDrift{
				Category:  FieldMissing,
				Field:     field,
				Expected:  valuePtr(expected),
				Tolerance: "field_presence",
				Reason:    "field exists in expected set but not in actual set",
			})
			affected[field] = struct{}{}
			continue
		}
		if !hasExpected && hasActual {
			drifts = append(drifts, FieldDrift{
				Category:  FieldExtra,
				Field:     field,
				Actual:    valuePtr(actual),
				Tolerance: "field_presence",
				Reason:    "field exists in actual set but not in expected set",
			})
			affected[field] = struct{}{}
			continue
		}
		if !hasExpected || !hasActual {
			continue
		}
		offset, reason, tolerance, ppm, allowed, delta := compareValue(expected, actual, policy)
		if offset {
			drifts = append(drifts, FieldDrift{
				Category:     ValueOffset,
				Field:        field,
				Expected:     valuePtr(expected),
				Actual:       valuePtr(actual),
				Tolerance:    tolerance,
				TolerancePPM: ppm,
				AllowedDelta: allowed,
				ActualDelta:  delta,
				Reason:       reason,
			})
			affected[field] = struct{}{}
		}
	}
	sort.Slice(drifts, func(i, j int) bool {
		ri, rj := categoryRank[drifts[i].Category], categoryRank[drifts[j].Category]
		if ri != rj {
			return ri > rj
		}
		if drifts[i].Field != drifts[j].Field {
			return drifts[i].Field < drifts[j].Field
		}
		return drifts[i].Reason < drifts[j].Reason
	})
	categorySet := make(map[Category]struct{})
	for _, drift := range drifts {
		categorySet[drift.Category] = struct{}{}
	}
	categories := make([]Category, 0, len(categorySet))
	for category := range categorySet {
		categories = append(categories, category)
	}
	sort.Slice(categories, func(i, j int) bool { return categoryRank[categories[i]] > categoryRank[categories[j]] })
	primary := Category("")
	if len(categories) > 0 {
		primary = categories[0]
	}
	return ObjectDrift{
		ObjectID:         obj.ID,
		SnapshotVersion:  0,
		HasDrift:         len(drifts) > 0,
		Categories:       categories,
		PrimaryCategory:  primary,
		FieldDrifts:      drifts,
		AffectedFieldCnt: len(affected),
	}
}

func compareValue(expected, actual model.FieldValue, policy *model.Policy) (bool, string, string, int64, string, string) {
	if expected.Type != actual.Type {
		return true, fmt.Sprintf("value type changed from %s to %s", expected.Type, actual.Type), "exact_type", 0, "0", "type_mismatch"
	}
	switch expected.Type {
	case model.TypeString:
		if expected.Value == actual.Value {
			return false, "strict string equality", "exact_string", 0, "0", "0"
		}
		return true, "string values differ under strict equality", "exact_string", 0, "0", "1"
	case model.TypeInteger:
		if expected.Value == actual.Value {
			return false, "strict integer equality", "exact_integer", 0, "0", "0"
		}
		return true, "integer values differ under strict equality", "exact_integer", 0, "0", "1"
	case model.TypeFloat:
		cmp, err := fixed.CompareDecimalRelative(expected.Value, actual.Value, policy.FloatTolerance.RelativePPM)
		if err != nil {
			return true, "decimal comparison failed: " + err.Error(), "relative_decimal_ppm", policy.FloatTolerance.RelativePPM, "0", "invalid"
		}
		tolerance := fmt.Sprintf("%d ppm relative", policy.FloatTolerance.RelativePPM)
		if cmp.WithinTolerance {
			return false, "floats are equal or inside relative decimal tolerance", tolerance, policy.FloatTolerance.RelativePPM, cmp.AllowedDelta, cmp.ActualDelta
		}
		return true, "float delta exceeds relative decimal tolerance", tolerance, policy.FloatTolerance.RelativePPM, cmp.AllowedDelta, cmp.ActualDelta
	default:
		return true, "unsupported value type", "unknown", 0, "0", "unknown"
	}
}

func sourceMismatch(source model.FieldSource) bool {
	if source.Expected == "" && source.Actual == "" {
		return false
	}
	return source.Expected != source.Actual
}

func valuePtr(v model.FieldValue) *model.FieldValue {
	copy := v
	return &copy
}

func reconcile(exact, bucketed []ObjectDrift) []Divergence {
	divergences := make([]Divergence, 0)
	if len(exact) != len(bucketed) {
		return []Divergence{{ObjectID: "*", Detail: fmt.Sprintf("result count differs: exact=%d bucketed=%d", len(exact), len(bucketed))}}
	}
	for i := range exact {
		if exact[i].ObjectID != bucketed[i].ObjectID {
			divergences = append(divergences, Divergence{ObjectID: exact[i].ObjectID, Detail: "object order differs"})
			continue
		}
		if detail := compareObjectResult(exact[i], bucketed[i]); detail != "" {
			divergences = append(divergences, Divergence{ObjectID: exact[i].ObjectID, Detail: detail})
		}
	}
	return divergences
}

func compareObjectResult(a, b ObjectDrift) string {
	if a.HasDrift != b.HasDrift {
		return fmt.Sprintf("hasDrift differs: %v vs %v", a.HasDrift, b.HasDrift)
	}
	if a.PrimaryCategory != b.PrimaryCategory {
		return fmt.Sprintf("primary drift differs: %q vs %q", a.PrimaryCategory, b.PrimaryCategory)
	}
	if strings.Join(categoryStrings(a.Categories), ",") != strings.Join(categoryStrings(b.Categories), ",") {
		return fmt.Sprintf("categories differ: %v vs %v", a.Categories, b.Categories)
	}
	if len(a.FieldDrifts) != len(b.FieldDrifts) {
		return fmt.Sprintf("field drift count differs: %d vs %d", len(a.FieldDrifts), len(b.FieldDrifts))
	}
	for i := range a.FieldDrifts {
		left, right := a.FieldDrifts[i], b.FieldDrifts[i]
		if left.Category != right.Category || left.Field != right.Field || left.Tolerance != right.Tolerance || left.Reason != right.Reason {
			return fmt.Sprintf("field drift %d differs: %+v vs %+v", i, left, right)
		}
		if !sameValue(left.Expected, right.Expected) || !sameValue(left.Actual, right.Actual) {
			return fmt.Sprintf("field drift %d values differ", i)
		}
	}
	return ""
}

func sameValue(a, b *model.FieldValue) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func categoryStrings(categories []Category) []string {
	result := make([]string, len(categories))
	for i, category := range categories {
		result[i] = string(category)
	}
	return result
}

func verifyValueCases(values *model.DriftValuesSample, policy *model.Policy) ([]ValueCaseResult, error) {
	results := make([]ValueCaseResult, 0, len(values.Cases))
	failures := make([]string, 0)
	for _, tc := range values.Cases {
		expected := model.FieldValue{Type: tc.Type, Value: tc.Expected}
		actual := model.FieldValue{Type: tc.Type, Value: tc.Actual}
		offset, _, tolerance, _, _, _ := compareValue(expected, actual, policy)
		categories := []Category{}
		if offset {
			categories = append(categories, ValueOffset)
		}
		pass := offset == tc.ExpectDrift
		if tc.ExpectDrift && len(tc.ExpectCategories) > 0 {
			want := make([]string, len(tc.ExpectCategories))
			copy(want, tc.ExpectCategories)
			got := categoryStrings(categories)
			sort.Strings(want)
			sort.Strings(got)
			pass = pass && strings.Join(want, ",") == strings.Join(got, ",")
		}
		results = append(results, ValueCaseResult{
			ID:           tc.ID,
			Type:         tc.Type,
			Expected:     tc.Expected,
			Actual:       tc.Actual,
			HasDrift:     offset,
			Categories:   categories,
			ExpectedPass: pass,
			ExpectDrift:  tc.ExpectDrift,
			Tolerance:    tolerance,
		})
		if !pass {
			failures = append(failures, tc.ID)
		}
	}
	if len(failures) > 0 {
		return results, apperr.WithDetails(apperr.SpecInvalid, failures, "drift field value sample expectations failed for %d case(s)", len(failures))
	}
	return results, nil
}
