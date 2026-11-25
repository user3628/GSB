package spec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"driftengine/internal/errs"
	"driftengine/internal/fixednum"
)

const (
	CategorySourceConflict = "source_conflict"
	CategoryFieldMissing   = "field_missing"
	CategoryFieldExtra     = "field_extra"
	CategoryValueOffset    = "value_offset"
	CategoryConverged      = "converged"
)

func DefaultPriority() []string {
	return []string{
		CategorySourceConflict,
		CategoryFieldMissing,
		CategoryFieldExtra,
		CategoryValueOffset,
	}
}

type Fields map[string]any

type SourceMarker struct {
	Desired string `json:"desired,omitempty"`
	Actual  string `json:"actual,omitempty"`
}

func (m *SourceMarker) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		m.Desired = ""
		m.Actual = text
		return nil
	}
	type alias SourceMarker
	var value alias
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*m = SourceMarker(value)
	return nil
}

type Budget struct {
	Units      int64 `json:"units,omitempty"`
	CPUMillis  int64 `json:"cpu_millis,omitempty"`
	MemoryMiB  int64 `json:"memory_mib,omitempty"`
	StorageMiB int64 `json:"storage_mib,omitempty"`
}

type Object struct {
	ID             string                  `json:"id"`
	Category       string                  `json:"category"`
	Kind           string                  `json:"kind,omitempty"`
	Namespace      string                  `json:"namespace"`
	Desired        Fields                  `json:"desired"`
	Actual         Fields                  `json:"actual"`
	SourceMarkers  map[string]SourceMarker `json:"source_markers,omitempty"`
	Dependencies   []string                `json:"dependencies,omitempty"`
	ResourceBudget Budget                  `json:"resource_budget"`
}

type Policy struct {
	NumericRelativeTolerance string   `json:"numeric_relative_tolerance"`
	ToleranceBoundary        string   `json:"tolerance_boundary,omitempty"`
	CategoryPriority         []string `json:"category_priority,omitempty"`
	MaxBatchSize             int      `json:"max_batch_size"`
	MaxBatchBudgetUnits      int64    `json:"max_batch_budget_units"`
	DependencyOrderRule      string   `json:"dependency_order_rule,omitempty"`
	RoundTimeBudgetMS        int64    `json:"round_time_budget_ms"`
	WriteTimeoutMS           int64    `json:"write_timeout_ms"`
	MaxRetries               int      `json:"max_retries"`
	MaxParallelBatches       int      `json:"max_parallel_batches"`
	WriteQueueSize           int      `json:"write_queue_size"`
}

type Snapshot struct {
	Schema      string    `json:"schema"`
	Version     int64     `json:"version"`
	GeneratedAt time.Time `json:"generated_at"`
	Policy      Policy    `json:"policy"`
	Objects     []Object  `json:"objects"`
	Digest      string    `json:"digest,omitempty"`
}

type Increment struct {
	Schema         string    `json:"schema"`
	BaseVersion    int64     `json:"base_version"`
	GeneratedAt    time.Time `json:"generated_at"`
	Objects        []Object  `json:"objects"`
	Digest         string    `json:"digest,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
}

func DecodeJSON(data []byte, target any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(target); err != nil {
		return err
	}
	if dec.More() {
		return fmt.Errorf("unexpected trailing JSON value")
	}
	return nil
}

func DigestOf(value any) (string, []byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), raw, nil
}

func ParseSnapshot(data []byte) (*Snapshot, error) {
	var snapshot Snapshot
	if err := DecodeJSON(data, &snapshot); err != nil {
		return nil, errs.New(errs.SpecInvalid, "snapshot JSON cannot be parsed", err.Error())
	}
	declaredDigest := snapshot.Digest
	snapshot.Digest = ""
	if declaredDigest != "" {
		actual, _, err := DigestOf(&snapshot)
		if err != nil {
			return nil, errs.New(errs.SpecInvalid, "snapshot cannot be canonicalized", err.Error())
		}
		if declaredDigest != actual {
			return nil, errs.New(errs.SpecInvalid, "snapshot digest mismatch", map[string]string{
				"declared": declaredDigest,
				"actual":   actual,
			})
		}
		snapshot.Digest = declaredDigest
	}
	if err := NormalizeSnapshot(&snapshot); err != nil {
		return nil, err
	}
	if declaredDigest == "" {
		digest, _, err := DigestOf(&snapshot)
		if err != nil {
			return nil, errs.New(errs.SpecInvalid, err.Error())
		}
		snapshot.Digest = digest
	}
	return &snapshot, nil
}

func ParseIncrement(data []byte) (*Increment, error) {
	var increment Increment
	if err := DecodeJSON(data, &increment); err != nil {
		return nil, errs.New(errs.SpecInvalid, "increment JSON cannot be parsed", err.Error())
	}
	declared := increment.Digest
	increment.Digest = ""
	if declared != "" {
		actual, _, err := DigestOf(&increment)
		if err != nil {
			return nil, errs.New(errs.SpecInvalid, err.Error())
		}
		if declared != actual {
			return nil, errs.New(errs.SpecInvalid, "increment digest mismatch", map[string]string{"declared": declared, "actual": actual})
		}
		increment.Digest = declared
	}
	if increment.BaseVersion <= 0 {
		return nil, errs.New(errs.SpecInvalid, "increment base_version must be positive")
	}
	if len(increment.Objects) == 0 {
		return nil, errs.New(errs.SpecInvalid, "increment must contain at least one object")
	}
	if err := ValidateObjectBasics(increment.Objects); err != nil {
		return nil, err
	}
	if declared == "" {
		digest, _, err := DigestOf(&increment)
		if err != nil {
			return nil, errs.New(errs.SpecInvalid, err.Error())
		}
		increment.Digest = digest
	}
	return &increment, nil
}

func NormalizeSnapshot(s *Snapshot) error {
	if s.Schema == "" {
		s.Schema = "drift.snapshot/v1"
	}
	if s.Schema != "drift.snapshot/v1" {
		return errs.New(errs.SpecInvalid, "unsupported snapshot schema", s.Schema)
	}
	if s.Version <= 0 {
		return errs.New(errs.SpecInvalid, "snapshot version must be positive")
	}
	if s.GeneratedAt.IsZero() {
		return errs.New(errs.SpecInvalid, "generated_at is required")
	}
	s.GeneratedAt = s.GeneratedAt.UTC()
	if err := NormalizePolicy(&s.Policy); err != nil {
		return err
	}
	if len(s.Objects) == 0 {
		return errs.New(errs.SpecInvalid, "snapshot must contain objects")
	}
	return ValidateObjects(s.Objects)
}

func NormalizePolicy(p *Policy) error {
	if strings.TrimSpace(p.NumericRelativeTolerance) == "" {
		p.NumericRelativeTolerance = "0"
	}
	tolerance, err := fixednum.Parse(p.NumericRelativeTolerance)
	if err != nil || tolerance.N.Sign() < 0 {
		return errs.New(errs.SpecInvalid, "invalid numeric_relative_tolerance", p.NumericRelativeTolerance)
	}
	p.NumericRelativeTolerance = tolerance.String()
	switch p.ToleranceBoundary {
	case "", "inclusive":
		p.ToleranceBoundary = "inclusive"
	case "exclusive":
	default:
		return errs.New(errs.SpecInvalid, "policy.tolerance_boundary must be inclusive or exclusive", p.ToleranceBoundary)
	}
	if len(p.CategoryPriority) == 0 {
		p.CategoryPriority = DefaultPriority()
	}
	if err := ValidatePriority(p.CategoryPriority); err != nil {
		return err
	}
	if p.MaxBatchSize <= 0 {
		return errs.New(errs.SpecInvalid, "max_batch_size must be positive")
	}
	if p.MaxBatchBudgetUnits <= 0 {
		return errs.New(errs.SpecInvalid, "max_batch_budget_units must be positive")
	}
	if p.DependencyOrderRule == "" {
		p.DependencyOrderRule = "dependencies_before_dependents"
	}
	if p.DependencyOrderRule != "dependencies_before_dependents" {
		return errs.New(errs.SpecInvalid, "unsupported dependency_order_rule", p.DependencyOrderRule)
	}
	if p.RoundTimeBudgetMS <= 0 {
		p.RoundTimeBudgetMS = 30000
	}
	if p.WriteTimeoutMS <= 0 {
		p.WriteTimeoutMS = 2000
	}
	if p.MaxRetries < 0 {
		return errs.New(errs.SpecInvalid, "max_retries must be non-negative")
	}
	if p.MaxParallelBatches <= 0 {
		p.MaxParallelBatches = 2
	}
	if p.WriteQueueSize < 0 {
		return errs.New(errs.SpecInvalid, "write_queue_size must be non-negative")
	}
	if p.WriteQueueSize == 0 {
		p.WriteQueueSize = p.MaxParallelBatches * 4
	}
	return nil
}

func ValidatePriority(priority []string) error {
	expected := map[string]bool{}
	for _, category := range DefaultPriority() {
		expected[category] = true
	}
	seen := map[string]bool{}
	for _, category := range priority {
		if !expected[category] {
			return errs.New(errs.SpecInvalid, "unknown drift category in priority", category)
		}
		if seen[category] {
			return errs.New(errs.SpecInvalid, "duplicate category in priority", category)
		}
		seen[category] = true
	}
	if len(seen) != len(expected) {
		return errs.New(errs.SpecInvalid, "category_priority must contain all four drift categories", priority)
	}
	return nil
}

func ValidateObjects(objects []Object) error {
	if err := ValidateObjectBasics(objects); err != nil {
		return err
	}
	ids := make(map[string]int, len(objects))
	for _, object := range objects {
		ids[object.ID]++
	}
	missing := map[string][]string{}
	for _, o := range objects {
		for _, dep := range o.Dependencies {
			if _, ok := ids[dep]; !ok {
				missing[o.ID] = append(missing[o.ID], dep)
			}
		}
	}
	if len(missing) > 0 {
		return errs.New(errs.SpecInvalid, "dependency reference does not exist", missing)
	}
	return nil
}

func ValidateObjectBasics(objects []Object) error {
	ids := make(map[string]int, len(objects))
	duplicates := []string{}
	for i := range objects {
		o := &objects[i]
		o.ID = strings.TrimSpace(o.ID)
		o.Category = strings.TrimSpace(o.Category)
		o.Namespace = strings.TrimSpace(o.Namespace)
		if o.ID == "" {
			return errs.New(errs.SpecInvalid, "object id is required", map[string]int{"index": i})
		}
		if o.Category == "" {
			return errs.New(errs.SpecInvalid, "object category is required", o.ID)
		}
		if o.Namespace == "" {
			return errs.New(errs.SpecInvalid, "object namespace is required", o.ID)
		}
		if o.Kind == "" {
			o.Kind = o.Category
		}
		if o.Desired == nil {
			o.Desired = Fields{}
		}
		if o.Actual == nil {
			o.Actual = Fields{}
		}
		if o.ResourceBudget.Units < 0 || o.ResourceBudget.CPUMillis < 0 || o.ResourceBudget.MemoryMiB < 0 || o.ResourceBudget.StorageMiB < 0 {
			return errs.New(errs.SpecInvalid, "resource budgets must be non-negative", o.ID)
		}
		if o.ResourceBudget.Units == 0 {
			o.ResourceBudget.Units = 1
		}
		if ids[o.ID] > 0 {
			duplicates = append(duplicates, o.ID)
		}
		ids[o.ID]++
		o.Dependencies = NormalizeDependencies(o.Dependencies)
	}
	if len(duplicates) > 0 {
		return errs.New(errs.SpecInvalid, "duplicate object id", duplicates)
	}
	return nil
}

func NormalizeDependencies(deps []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, dep := range deps {
		dep = strings.TrimSpace(dep)
		if dep == "" || seen[dep] {
			continue
		}
		seen[dep] = true
		out = append(out, dep)
	}
	sort.Strings(out)
	return out
}

func SortedObjects(objects []Object) []Object {
	out := append([]Object(nil), objects...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func ObjectByID(objects []Object) map[string]Object {
	out := make(map[string]Object, len(objects))
	for _, object := range objects {
		out[object.ID] = object
	}
	return out
}

func ObjectDigest(object Object) string {
	object.Dependencies = NormalizeDependencies(object.Dependencies)
	digest, _, _ := DigestOf(object)
	return digest
}

func EdgeDigest(objectID, dependencyID string) string {
	digest, _, _ := DigestOf(map[string]string{"object_id": objectID, "depends_on_id": dependencyID})
	return digest
}

func TimePtr(t time.Time) *time.Time { return &t }
