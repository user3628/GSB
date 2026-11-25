package model

import (
	"fmt"
	"sort"
)

type Category string

const (
	CategoryAccountPolicy  Category = "account_policy"
	CategoryAuditPolicy    Category = "audit_policy"
	CategoryServiceStartup Category = "service_startup"
	CategoryRegistryACL    Category = "registry_acl"
)

func (c Category) Valid() bool {
	switch c {
	case CategoryAccountPolicy, CategoryAuditPolicy, CategoryServiceStartup, CategoryRegistryACL:
		return true
	default:
		return false
	}
}

type ValueKind string

const (
	KindNumber ValueKind = "number"
	KindString ValueKind = "string"
	KindSet    ValueKind = "set"
)

type Value struct {
	Kind   ValueKind `json:"kind"`
	Number int64     `json:"number,omitempty"`
	Text   string    `json:"text,omitempty"`
	Set    []string  `json:"set,omitempty"`
}

func CanonicalValue(v Value) Value {
	out := v
	switch v.Kind {
	case KindSet:
		seen := make(map[string]struct{}, len(v.Set))
		set := make([]string, 0, len(v.Set))
		for _, item := range v.Set {
			if _, ok := seen[item]; ok {
				continue
			}
			seen[item] = struct{}{}
			set = append(set, item)
		}
		sort.Strings(set)
		out.Set = set
	case KindString:
		out.Text = v.Text
	}
	return out
}

type Interval struct {
	Min          int64 `json:"min"`
	Max          int64 `json:"max"`
	MinInclusive bool  `json:"min_inclusive"`
	MaxInclusive bool  `json:"max_inclusive"`
}

func (in Interval) Contains(v int64) bool {
	if in.MinInclusive {
		if v < in.Min {
			return false
		}
	} else if v <= in.Min {
		return false
	}
	if in.MaxInclusive {
		return v <= in.Max
	}
	return v < in.Max
}

func (in Interval) String() string {
	left := "("
	right := ")"
	if in.MinInclusive {
		left = "["
	}
	if in.MaxInclusive {
		right = "]"
	}
	return fmt.Sprintf("%s%d,%d%s", left, in.Min, in.Max, right)
}

type Expectation struct {
	Interval     *Interval `json:"interval,omitempty"`
	AllowedEnum  []string  `json:"allowed_enum,omitempty"`
	ExpectedText string    `json:"expected_text,omitempty"`
	RequiredSet  []string  `json:"required_set,omitempty"`
}

type Baseline struct {
	ID             string      `json:"id"`
	Category       Category    `json:"category"`
	Expected       Expectation `json:"expected"`
	SeverityWeight int         `json:"severity_weight"`
	DependsOn      []string    `json:"depends_on,omitempty"`
}

func ValidateBaselines(items []Baseline) error {
	problems := make([]string, 0)
	byID := make(map[string]Baseline, len(items))
	for i, b := range items {
		if b.ID == "" {
			problems = append(problems, fmt.Sprintf("baselines[%d]: id is required", i))
			continue
		}
		if _, exists := byID[b.ID]; exists {
			problems = append(problems, fmt.Sprintf("%s: duplicate baseline id", b.ID))
		}
		if !b.Category.Valid() {
			problems = append(problems, fmt.Sprintf("%s: invalid category %q", b.ID, b.Category))
		}
		if b.SeverityWeight <= 0 {
			problems = append(problems, fmt.Sprintf("%s: severity_weight must be a positive integer", b.ID))
		}
		if err := validateExpectation(b); err != nil {
			problems = append(problems, err.Error())
		}
		byID[b.ID] = b
	}
	for _, b := range items {
		for _, dep := range b.DependsOn {
			if dep == b.ID {
				problems = append(problems, fmt.Sprintf("%s: dependency must not reference itself", b.ID))
			}
			if _, ok := byID[dep]; !ok {
				problems = append(problems, fmt.Sprintf("%s: dependency reference %s does not exist", b.ID, dep))
			}
		}
	}
	if len(problems) > 0 {
		return NewCodedError(ErrBaselineInvalid, "baseline batch failed validation").
			withDetails(map[string]any{"problems": problems})
	}
	return nil
}

func (e *CodedError) withDetails(details any) *CodedError {
	e.Details = details
	return e
}

func validateExpectation(b Baseline) error {
	switch b.Category {
	case CategoryAccountPolicy, CategoryAuditPolicy:
		if b.Expected.Interval == nil {
			return fmt.Errorf("%s: numeric interval expectation is required", b.ID)
		}
		iv := b.Expected.Interval
		if iv.Min > iv.Max || (iv.Min == iv.Max && (!iv.MinInclusive || !iv.MaxInclusive)) {
			return fmt.Errorf("%s: interval expectation is empty or invalid", b.ID)
		}
	case CategoryServiceStartup:
		if b.Expected.ExpectedText == "" {
			return fmt.Errorf("%s: expected service startup type is required", b.ID)
		}
		if len(b.Expected.AllowedEnum) == 0 {
			return fmt.Errorf("%s: declared startup enum is required", b.ID)
		}
		if !containsString(b.Expected.AllowedEnum, b.Expected.ExpectedText) {
			return fmt.Errorf("%s: expected startup type is outside declared enum", b.ID)
		}
	case CategoryRegistryACL:
		if len(b.Expected.RequiredSet) == 0 {
			return fmt.Errorf("%s: required registry permission set is required", b.ID)
		}
	}
	return nil
}

func containsString(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

type Observation struct {
	Host         string `json:"host"`
	BaselineID   string `json:"baseline_id"`
	Actual       Value  `json:"actual"`
	SampledAtMS  int64  `json:"sampled_at_ms"`
	Source       string `json:"source,omitempty"`
	OptionalNote string `json:"optional_note,omitempty"`
}

type RoundingRule string

const (
	RoundTruncate RoundingRule = "truncate_millisecond"
	RoundNearest  RoundingRule = "nearest_millisecond"
	RoundCeiling  RoundingRule = "ceiling_millisecond"
)

type RoundingInfo struct {
	Host                  string       `json:"host"`
	BaselineID            string       `json:"baseline_id"`
	SourceFormat          string       `json:"source_format"`
	Rule                  RoundingRule `json:"rule"`
	SourceTimestampMicros int64        `json:"source_timestamp_micros,omitempty"`
	SourceTimestampMS     int64        `json:"source_timestamp_ms,omitempty"`
	NormalizedTimestampMS int64        `json:"normalized_timestamp_ms"`
	PrecisionFrom         string       `json:"precision_from"`
	PrecisionTo           string       `json:"precision_to"`
	DiscardedDigits       int          `json:"discarded_digits"`
	RemainderMicros       int64        `json:"remainder_micros"`
	RoundedUp             bool         `json:"rounded_up"`
	RoundingErrorMicros   int64        `json:"rounding_error_micros"`
	CumulativeErrorMicros int64        `json:"cumulative_rounding_error_micros"`
}

type ParseResult struct {
	Observations []Observation  `json:"observations"`
	Rounding     []RoundingInfo `json:"rounding"`
}

type DriftCategory string

const (
	DriftNone           DriftCategory = "conformant"
	DriftMissing        DriftCategory = "missing"
	DriftExtra          DriftCategory = "extra"
	DriftValueOffset    DriftCategory = "value_offset"
	DriftSourceConflict DriftCategory = "source_conflict"
)

type TaskStatus string

const (
	TaskPending  TaskStatus = "PENDING"
	TaskRunning  TaskStatus = "RUNNING"
	TaskDone     TaskStatus = "DONE"
	TaskDeferred TaskStatus = "DEFERRED"
	TaskError    TaskStatus = "ERROR"
)

type RoundingExplanation struct {
	Method      string `json:"method"`
	Numerator   int64  `json:"numerator"`
	Denominator int64  `json:"denominator"`
	Quotient    int64  `json:"quotient"`
	Remainder   int64  `json:"remainder"`
	Note        string `json:"note"`
}

type PropagationHop struct {
	FromBaselineID string   `json:"from_baseline_id"`
	ToBaselineID   string   `json:"to_baseline_id"`
	Hop            int      `json:"hop"`
	SourceLevel    int      `json:"source_level"`
	Boost          int      `json:"boost"`
	Path           []string `json:"path"`
}

type Conclusion struct {
	SessionID       string              `json:"session_id"`
	Host            string              `json:"host"`
	BaselineID      string              `json:"baseline_id"`
	Category        Category            `json:"category"`
	Status          TaskStatus          `json:"status"`
	DriftCategory   DriftCategory       `json:"drift_category"`
	OwnLevel        int                 `json:"own_level"`
	DriftLevel      int                 `json:"drift_level"`
	ScoreScaled     int64               `json:"score_scaled"`
	OffsetPermille  int64               `json:"offset_permille"`
	LevelRounding   RoundingExplanation `json:"level_rounding"`
	MissingElements []string            `json:"missing_elements,omitempty"`
	ExtraElements   []string            `json:"extra_elements,omitempty"`
	Recommendation  string              `json:"recommendation,omitempty"`
	Propagation     []PropagationHop    `json:"propagation,omitempty"`
	RepairCost      int                 `json:"repair_cost"`
	Reason          string              `json:"reason,omitempty"`
	ErrorCode       ErrorCode           `json:"error_code,omitempty"`
	Details         map[string]any      `json:"details,omitempty"`
	ViewVersion     int64               `json:"view_version"`
}

type HostSummary struct {
	SessionID      string `json:"session_id"`
	Host           string `json:"host"`
	ViewVersion    int64  `json:"view_version"`
	Total          int    `json:"total"`
	Done           int    `json:"done"`
	Conformant     int    `json:"conformant"`
	Drift          int    `json:"drift"`
	Deferred       int    `json:"deferred"`
	Errors         int    `json:"errors"`
	MaxDriftLevel  int    `json:"max_drift_level"`
	WeightedScore  int64  `json:"weighted_score_scaled"`
	RemainingQuota int    `json:"remaining_repair_quota"`
}
