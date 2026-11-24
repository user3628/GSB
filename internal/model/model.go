package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"driftconverge/internal/apperr"
)

const (
	TypeString  ValueType = "string"
	TypeInteger ValueType = "int"
	TypeFloat   ValueType = "float"
)

type ValueType string

// FieldValue uses an explicit scalar type. Values are kept as canonical text so
// the sample and persisted representation never depend on host float formatting.
type FieldValue struct {
	Type  ValueType `json:"type"`
	Value string    `json:"value"`
}

func (v *FieldValue) UnmarshalJSON(data []byte) error {
	var aux struct {
		Type  ValueType       `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&aux); err != nil {
		return err
	}
	v.Type = aux.Type
	if len(aux.Value) == 0 {
		return fmt.Errorf("field value is required")
	}
	if aux.Value[0] == '"' {
		return json.Unmarshal(aux.Value, &v.Value)
	}
	// Numbers are accepted as a convenience but are immediately canonicalized.
	var number json.Number
	if err := json.Unmarshal(aux.Value, &number); err != nil {
		return fmt.Errorf("field value must be a string or number: %w", err)
	}
	v.Value = number.String()
	return nil
}

func (v FieldValue) String() string {
	return v.Value
}

type FieldSource struct {
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
}

type Object struct {
	ID             string                 `json:"id"`
	Kind           string                 `json:"kind"`
	Namespace      string                 `json:"namespace"`
	Expected       map[string]FieldValue  `json:"expected"`
	Actual         map[string]FieldValue  `json:"actual"`
	Sources        map[string]FieldSource `json:"sources,omitempty"`
	DependsOn      []string               `json:"dependsOn,omitempty"`
	ResourceBudget int64                  `json:"resourceBudget"`
}

type ClusterSpec struct {
	SnapshotVersion int64    `json:"snapshotVersion"`
	Description     string   `json:"description,omitempty"`
	Objects         []Object `json:"objects"`
}

type FloatTolerance struct {
	// RelativePPM is parts-per-million, for example 10_000 means 1%.
	RelativePPM int64  `json:"relativePpm"`
	Mode        string `json:"mode"`
}

type Policy struct {
	Version                        int64          `json:"version"`
	GeneratedAt                    string         `json:"generatedAt"`
	Checksum                       string         `json:"checksum,omitempty"`
	ExactObjectLimit               int            `json:"exactObjectLimit"`
	BatchSizeLimit                 int            `json:"batchSizeLimit"`
	DependencyOrderRule            string         `json:"dependencyOrderRule"`
	RoundTimeBudgetMs              int64          `json:"roundTimeBudgetMs"`
	TimeBudgetRelativeTolerancePPM int64          `json:"timeBudgetRelativeTolerancePpm"`
	FloatTolerance                 FloatTolerance `json:"floatTolerance"`
	MaxParallelBatches             int            `json:"maxParallelBatches"`
	WriteQueueCapacity             int            `json:"writeQueueCapacity"`
	RetryLimit                     int            `json:"retryLimit"`
	WriteTimeoutMs                 int64          `json:"writeTimeoutMs"`
	PreviewSampleSize              int            `json:"previewSampleSize"`
}

type FieldValueCase struct {
	ID               string    `json:"id"`
	Type             ValueType `json:"type"`
	Expected         string    `json:"expected"`
	Actual           string    `json:"actual"`
	ExpectDrift      bool      `json:"expectDrift"`
	ExpectCategories []string  `json:"expectCategories,omitempty"`
}

type DriftValuesSample struct {
	Version int64            `json:"version"`
	Cases   []FieldValueCase `json:"cases"`
}

func (p *Policy) ComputeChecksum() string {
	view := *p
	view.Checksum = ""
	canonical, _ := json.Marshal(view)
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func DecodePolicy(data []byte) (*Policy, error) {
	var p Policy
	if err := decodeStrict(data, &p); err != nil {
		return nil, invalid("cannot decode policy snapshot: %v", err)
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return &p, nil
}

func (p *Policy) Validate() error {
	if p.Version <= 0 {
		return invalid("policy.version must be greater than zero")
	}
	if _, err := time.Parse(time.RFC3339, p.GeneratedAt); err != nil {
		return invalid("policy.generatedAt must be RFC3339: %v", err)
	}
	if p.ExactObjectLimit <= 0 {
		return invalid("policy.exactObjectLimit must be positive")
	}
	if p.BatchSizeLimit <= 0 {
		return invalid("policy.batchSizeLimit must be positive")
	}
	if p.DependencyOrderRule != "dependencies_first" {
		return invalid("policy.dependencyOrderRule must be dependencies_first")
	}
	if p.RoundTimeBudgetMs <= 0 || p.RoundTimeBudgetMs > math.MaxInt64/1000 {
		return invalid("policy.roundTimeBudgetMs is out of range")
	}
	if p.TimeBudgetRelativeTolerancePPM < 0 || p.TimeBudgetRelativeTolerancePPM > 1_000_000 {
		return invalid("policy.timeBudgetRelativeTolerancePpm must be in [0,1000000]")
	}
	if p.FloatTolerance.RelativePPM < 0 || p.FloatTolerance.RelativePPM > 1_000_000 {
		return invalid("policy.floatTolerance.relativePpm must be in [0,1000000]")
	}
	if p.FloatTolerance.Mode != "relative_decimal_ppm" {
		return invalid("policy.floatTolerance.mode must be relative_decimal_ppm")
	}
	if p.MaxParallelBatches <= 0 || p.MaxParallelBatches > 1024 {
		return invalid("policy.maxParallelBatches must be in [1,1024]")
	}
	if p.WriteQueueCapacity < 0 || p.WriteQueueCapacity > 1_000_000 {
		return invalid("policy.writeQueueCapacity must be in [0,1000000]")
	}
	if p.RetryLimit < 0 || p.RetryLimit > 100 {
		return invalid("policy.retryLimit must be in [0,100]")
	}
	if p.WriteTimeoutMs <= 0 || p.WriteTimeoutMs > math.MaxInt64/1_000_000 {
		return invalid("policy.writeTimeoutMs must be positive")
	}
	if p.PreviewSampleSize <= 0 {
		return invalid("policy.previewSampleSize must be positive")
	}
	if strings.TrimSpace(p.Checksum) == "" {
		return invalid("policy.checksum is required")
	}
	if got := p.ComputeChecksum(); got != p.Checksum {
		return invalid("policy.checksum mismatch: declared %s, computed %s", p.Checksum, got)
	}
	return nil
}

func DecodeCluster(data []byte) (*ClusterSpec, error) {
	var c ClusterSpec
	if err := decodeStrict(data, &c); err != nil {
		return nil, invalid("cannot decode cluster snapshot: %v", err)
	}
	if err := c.NormalizeAndValidate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *ClusterSpec) NormalizeAndValidate() error {
	if c.SnapshotVersion <= 0 {
		return invalid("cluster.snapshotVersion must be greater than zero")
	}
	seen := make(map[string]struct{}, len(c.Objects))
	var budgetTotal int64
	for i := range c.Objects {
		obj := &c.Objects[i]
		obj.ID = strings.TrimSpace(obj.ID)
		obj.Kind = strings.TrimSpace(obj.Kind)
		obj.Namespace = strings.TrimSpace(obj.Namespace)
		if obj.ID == "" || obj.Kind == "" || obj.Namespace == "" {
			return invalid("object[%d] id, kind and namespace are required", i)
		}
		if _, ok := seen[obj.ID]; ok {
			return invalid("duplicate object id %q", obj.ID)
		}
		seen[obj.ID] = struct{}{}
		if obj.ResourceBudget < 0 {
			return invalid("object %q resourceBudget must be non-negative", obj.ID)
		}
		if obj.ResourceBudget > math.MaxInt64-budgetTotal {
			return invalid("resourceBudget sum overflows int64 at object %q", obj.ID)
		}
		budgetTotal += obj.ResourceBudget
		if obj.Expected == nil {
			obj.Expected = map[string]FieldValue{}
		}
		if obj.Actual == nil {
			obj.Actual = map[string]FieldValue{}
		}
		if obj.Sources == nil {
			obj.Sources = map[string]FieldSource{}
		}
		for field, value := range obj.Expected {
			if strings.TrimSpace(field) == "" {
				return invalid("object %q contains an empty expected field name", obj.ID)
			}
			if err := normalizeValue(&value); err != nil {
				return invalid("object %q expected.%s: %v", obj.ID, field, err)
			}
			obj.Expected[field] = value
		}
		for field, value := range obj.Actual {
			if strings.TrimSpace(field) == "" {
				return invalid("object %q contains an empty actual field name", obj.ID)
			}
			if err := normalizeValue(&value); err != nil {
				return invalid("object %q actual.%s: %v", obj.ID, field, err)
			}
			obj.Actual[field] = value
		}
		for field, source := range obj.Sources {
			if _, expected := obj.Expected[field]; !expected {
				if _, actual := obj.Actual[field]; !actual {
					return invalid("object %q source marker references unknown field %q", obj.ID, field)
				}
			}
			source.Expected = strings.TrimSpace(source.Expected)
			source.Actual = strings.TrimSpace(source.Actual)
			obj.Sources[field] = source
		}
		seenDeps := map[string]struct{}{}
		deps := make([]string, 0, len(obj.DependsOn))
		for _, dep := range obj.DependsOn {
			dep = strings.TrimSpace(dep)
			if dep == "" {
				return invalid("object %q has an empty dependency id", obj.ID)
			}
			if _, ok := seenDeps[dep]; ok {
				continue
			}
			seenDeps[dep] = struct{}{}
			deps = append(deps, dep)
		}
		sort.Strings(deps)
		obj.DependsOn = deps
	}
	for _, obj := range c.Objects {
		for _, dep := range obj.DependsOn {
			if _, ok := seen[dep]; !ok {
				return invalid("object %q depends on missing object %q", obj.ID, dep)
			}
		}
	}
	sort.Slice(c.Objects, func(i, j int) bool { return c.Objects[i].ID < c.Objects[j].ID })
	return nil
}

func DecodeValues(data []byte) (*DriftValuesSample, error) {
	var sample DriftValuesSample
	if err := decodeStrict(data, &sample); err != nil {
		return nil, invalid("cannot decode drift field values sample: %v", err)
	}
	if sample.Version <= 0 {
		return nil, invalid("drift field values version must be positive")
	}
	seen := map[string]struct{}{}
	for i := range sample.Cases {
		c := &sample.Cases[i]
		c.ID = strings.TrimSpace(c.ID)
		if c.ID == "" {
			return nil, invalid("drift field case[%d] id is required", i)
		}
		if _, ok := seen[c.ID]; ok {
			return nil, invalid("duplicate drift field case id %q", c.ID)
		}
		seen[c.ID] = struct{}{}
		expected := FieldValue{Type: c.Type, Value: c.Expected}
		actual := FieldValue{Type: c.Type, Value: c.Actual}
		if err := normalizeValue(&expected); err != nil {
			return nil, invalid("drift field case %q expected: %v", c.ID, err)
		}
		if err := normalizeValue(&actual); err != nil {
			return nil, invalid("drift field case %q actual: %v", c.ID, err)
		}
		c.Expected, c.Actual = expected.Value, actual.Value
	}
	return &sample, nil
}

func normalizeValue(v *FieldValue) error {
	switch v.Type {
	case TypeString:
		return nil
	case TypeInteger:
		n, err := strconv.ParseInt(strings.TrimSpace(v.Value), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid 64-bit integer %q", v.Value)
		}
		v.Value = strconv.FormatInt(n, 10)
		return nil
	case TypeFloat:
		rat, ok := new(big.Rat).SetString(strings.TrimSpace(v.Value))
		if !ok {
			return fmt.Errorf("invalid decimal float %q", v.Value)
		}
		// Keep the exact supplied decimal text. big.Rat is only the validator and
		// comparison representation; rendering it back to a fixed decimal could
		// silently discard significant digits.
		v.Value = strings.TrimSpace(v.Value)
		if rat.Sign() == 0 {
			v.Value = "0"
		}
		return nil
	default:
		return fmt.Errorf("unsupported field type %q", v.Type)
	}
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON data")
		}
		return err
	}
	return nil
}

func invalid(format string, args ...any) *apperr.Error {
	return apperr.New(apperr.SpecInvalid, format, args...)
}

func (c *ClusterSpec) ObjectByID() map[string]*Object {
	result := make(map[string]*Object, len(c.Objects))
	for i := range c.Objects {
		result[c.Objects[i].ID] = &c.Objects[i]
	}
	return result
}

func BudgetTotal(objects []Object) (int64, error) {
	var total int64
	for _, obj := range objects {
		if obj.ResourceBudget < 0 || total > math.MaxInt64-obj.ResourceBudget {
			return 0, invalid("resource budget total overflow")
		}
		total += obj.ResourceBudget
	}
	return total, nil
}

func (p *Policy) ValidateAgainstCluster(c *ClusterSpec) error {
	if c.SnapshotVersion != p.Version {
		return invalid("cluster.snapshotVersion %d does not match policy.version %d", c.SnapshotVersion, p.Version)
	}
	return nil
}
