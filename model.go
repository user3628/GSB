package convergence

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	CategorySourceConflict = "source_conflict"
	CategoryMissing        = "field_missing"
	CategoryExtra          = "field_extra"
	CategoryValueShift     = "value_shift"
	CategoryInSync         = "in_sync"
)

var DefaultDriftPriority = []string{
	CategorySourceConflict,
	CategoryMissing,
	CategoryExtra,
	CategoryValueShift,
}

var categoryChinese = map[string]string{
	CategorySourceConflict: "来源冲突",
	CategoryMissing:        "字段缺失",
	CategoryExtra:          "字段多余",
	CategoryValueShift:     "取值偏移",
	CategoryInSync:         "一致",
}

func CategoryLabel(category string) string { return categoryChinese[category] }

// Field is one normalized configuration value and its provenance marker.
type Field struct {
	Value  any    `json:"value"`
	Source string `json:"source,omitempty"`
}

func (f *Field) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if bytes.Equal(trimmed, []byte("null")) {
		return NewError(CodeSpecInvalid, "field must not be null", nil)
	}
	if len(trimmed) > 0 && trimmed[0] == '{' {
		var keys map[string]json.RawMessage
		if err := decodeUseNumber(data, &keys); err != nil {
			return err
		}
		if rawValue, ok := keys["value"]; ok {
			var value any
			if err := decodeUseNumber(rawValue, &value); err != nil {
				return err
			}
			var source string
			if rawSource, ok := keys["source"]; ok {
				if err := json.Unmarshal(rawSource, &source); err != nil {
					return err
				}
			}
			f.Value = value
			f.Source = source
			return nil
		}
	}
	var value any
	if err := decodeUseNumber(data, &value); err != nil {
		return err
	}
	f.Value = value
	f.Source = ""
	return nil
}

type FieldSet map[string]Field

type ResourceBudget struct {
	BatchUnits  int64 `json:"batch_units"`
	CPUMillis   int64 `json:"cpu_millis,omitempty"`
	MemoryBytes int64 `json:"memory_bytes,omitempty"`
}

func (b ResourceBudget) SchedulingUnits() int64 {
	if b.BatchUnits <= 0 {
		return 1
	}
	return b.BatchUnits
}

type Object struct {
	ClusterID   string         `json:"cluster_id"`
	ID          string         `json:"id"`
	Kind        string         `json:"kind"`
	Namespace   string         `json:"namespace"`
	Expected    FieldSet       `json:"expected_fields"`
	Actual      FieldSet       `json:"actual_fields"`
	DependsOn   []string       `json:"depends_on,omitempty"`
	Budget      ResourceBudget `json:"resource_budget,omitempty"`
	Endpoint    string         `json:"endpoint,omitempty"`
	ResourceVer string         `json:"resource_version,omitempty"`
}

type Cluster struct {
	ID       string `json:"id"`
	Endpoint string `json:"endpoint"`
	Region   string `json:"region,omitempty"`
}

type Policy struct {
	PolicyVersion      int64     `json:"policy_version"`
	GeneratedAt        time.Time `json:"generated_at"`
	DriftTolerance     Decimal   `json:"drift_tolerance"`
	DriftPriority      []string  `json:"drift_priority,omitempty"`
	MaxBatchSize       int       `json:"max_batch_size"`
	MaxBatchUnits      int64     `json:"max_batch_resource_units"`
	RoundBudget        Duration  `json:"round_budget"`
	WriteTimeout       Duration  `json:"write_timeout"`
	RetryLimit         int       `json:"retry_limit"`
	MaxParallelBatches int       `json:"max_parallel_batches"`
	WriteQueueSize     int       `json:"write_queue_size"`
	Checksum           string    `json:"checksum,omitempty"`
}

type Snapshot struct {
	SchemaVersion   string    `json:"schema_version,omitempty"`
	SnapshotVersion int64     `json:"snapshot_version"`
	GeneratedAt     time.Time `json:"generated_at"`
	Policy          Policy    `json:"policy"`
	Clusters        []Cluster `json:"clusters"`
	Objects         []Object  `json:"objects"`
	Checksum        string    `json:"checksum,omitempty"`
}

type Delta struct {
	BaseVersion      int64     `json:"base_version"`
	Version          int64     `json:"version,omitempty"`
	GeneratedAt      time.Time `json:"generated_at"`
	Reason           string    `json:"reason,omitempty"`
	Upserts          []Object  `json:"upserts,omitempty"`
	RemovedObjectIDs []string  `json:"removed_object_ids,omitempty"`
}

func decodeUseNumber(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return decoder.Decode(target)
}

func CanonicalJSON(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buffer.Bytes(), "\n"), nil
}

func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ContentHash(value any) (string, error) {
	data, err := CanonicalJSON(value)
	if err != nil {
		return "", err
	}
	return SHA256Hex(data), nil
}

type policyChecksumPayload struct {
	PolicyVersion      int64     `json:"policy_version"`
	GeneratedAt        time.Time `json:"generated_at"`
	DriftTolerance     Decimal   `json:"drift_tolerance"`
	DriftPriority      []string  `json:"drift_priority"`
	MaxBatchSize       int       `json:"max_batch_size"`
	MaxBatchUnits      int64     `json:"max_batch_resource_units"`
	RoundBudget        Duration  `json:"round_budget"`
	WriteTimeout       Duration  `json:"write_timeout"`
	RetryLimit         int       `json:"retry_limit"`
	MaxParallelBatches int       `json:"max_parallel_batches"`
	WriteQueueSize     int       `json:"write_queue_size"`
}

func (p Policy) checksumPayload() policyChecksumPayload {
	priority := append([]string(nil), p.DriftPriority...)
	if len(priority) == 0 {
		priority = append([]string(nil), DefaultDriftPriority...)
	}
	return policyChecksumPayload{
		PolicyVersion:      p.PolicyVersion,
		GeneratedAt:        p.GeneratedAt.UTC(),
		DriftTolerance:     p.DriftTolerance,
		DriftPriority:      priority,
		MaxBatchSize:       p.MaxBatchSize,
		MaxBatchUnits:      p.MaxBatchUnits,
		RoundBudget:        p.RoundBudget,
		WriteTimeout:       p.WriteTimeout,
		RetryLimit:         p.RetryLimit,
		MaxParallelBatches: p.MaxParallelBatches,
		WriteQueueSize:     p.WriteQueueSize,
	}
}

func (p Policy) ComputeChecksum() (string, error) {
	hash, err := ContentHash(p.checksumPayload())
	if err != nil {
		return "", err
	}
	return hash, nil
}

func (p Policy) VerifyChecksum() error {
	if strings.TrimSpace(p.Checksum) == "" {
		return NewError(CodeSpecInvalid, "policy checksum is missing", nil)
	}
	expected, err := p.ComputeChecksum()
	if err != nil {
		return WrapError(CodeSpecInvalid, "cannot compute policy checksum", err, nil)
	}
	if !strings.EqualFold(expected, p.Checksum) {
		return NewError(CodeSpecInvalid, "policy checksum mismatch", map[string]any{
			"policy_version": p.PolicyVersion,
			"expected":       expected,
			"actual":         p.Checksum,
		})
	}
	return nil
}

type snapshotChecksumPayload struct {
	SnapshotVersion int64     `json:"snapshot_version"`
	GeneratedAt     time.Time `json:"generated_at"`
	Policy          Policy    `json:"policy"`
	Clusters        []Cluster `json:"clusters"`
	Objects         []Object  `json:"objects"`
}

func (s Snapshot) checksumPayload() snapshotChecksumPayload {
	return snapshotChecksumPayload{
		SnapshotVersion: s.SnapshotVersion,
		GeneratedAt:     s.GeneratedAt.UTC(),
		Policy:          s.Policy,
		Clusters:        s.Clusters,
		Objects:         s.Objects,
	}
}

func (s Snapshot) ComputeChecksum() (string, error) {
	return ContentHash(s.checksumPayload())
}

func (s Snapshot) VerifyChecksum() error {
	if strings.TrimSpace(s.Checksum) == "" {
		return NewError(CodeSpecInvalid, "snapshot checksum is missing", map[string]any{"snapshot_version": s.SnapshotVersion})
	}
	expected, err := s.ComputeChecksum()
	if err != nil {
		return WrapError(CodeSpecInvalid, "cannot compute snapshot checksum", err, nil)
	}
	if !strings.EqualFold(expected, s.Checksum) {
		return NewError(CodeSpecInvalid, "snapshot checksum mismatch", map[string]any{
			"snapshot_version": s.SnapshotVersion,
			"expected":         expected,
			"actual":           s.Checksum,
		})
	}
	return nil
}

// PrepareSnapshot normalizes values and fills deterministic policy/snapshot checksums.
func PrepareSnapshot(snapshot *Snapshot) error {
	if err := NormalizeSnapshot(snapshot); err != nil {
		return err
	}
	policyChecksum, err := snapshot.Policy.ComputeChecksum()
	if err != nil {
		return err
	}
	snapshot.Policy.Checksum = policyChecksum
	snapshotChecksum, err := snapshot.ComputeChecksum()
	if err != nil {
		return err
	}
	snapshot.Checksum = snapshotChecksum
	return nil
}

func ParseSnapshot(data []byte) (*Snapshot, error) {
	var snapshot Snapshot
	if err := decodeUseNumber(data, &snapshot); err != nil {
		return nil, NewError(CodeSpecInvalid, "cannot parse snapshot JSON", map[string]any{"cause": err.Error()})
	}
	if err := NormalizeSnapshot(&snapshot); err != nil {
		return nil, err
	}
	if strings.TrimSpace(snapshot.Policy.Checksum) != "" {
		if err := snapshot.Policy.VerifyChecksum(); err != nil {
			return nil, err
		}
	}
	if strings.TrimSpace(snapshot.Checksum) != "" {
		if err := snapshot.VerifyChecksum(); err != nil {
			return nil, err
		}
	}
	return &snapshot, nil
}

func ParseDelta(data []byte) (*Delta, error) {
	var delta Delta
	if err := decodeUseNumber(data, &delta); err != nil {
		return nil, NewError(CodeSpecInvalid, "cannot parse delta JSON", map[string]any{"cause": err.Error()})
	}
	if err := NormalizeDelta(&delta); err != nil {
		return nil, err
	}
	return &delta, nil
}

func ParsePolicy(data []byte) (*Policy, error) {
	var policy Policy
	if err := decodeUseNumber(data, &policy); err != nil {
		return nil, NewError(CodeSpecInvalid, "cannot parse policy JSON", map[string]any{"cause": err.Error()})
	}
	normalizePolicy(&policy)
	if problems := validatePolicy(policy); len(problems) > 0 {
		return nil, NewError(CodeSpecInvalid, "invalid policy", map[string]any{"problems": problems})
	}
	if err := policy.VerifyChecksum(); err != nil {
		return nil, err
	}
	return &policy, nil
}

func NormalizeDelta(delta *Delta) error {
	if delta == nil {
		return NewError(CodeSpecInvalid, "delta is nil", nil)
	}
	if delta.BaseVersion < 0 {
		return NewError(CodeSpecInvalid, "base_version must be non-negative", nil)
	}
	seenUpsertIDs := make(map[string]struct{}, len(delta.Upserts))
	for i := range delta.Upserts {
		if err := normalizeObject(&delta.Upserts[i]); err != nil {
			return err
		}
		if _, exists := seenUpsertIDs[delta.Upserts[i].ID]; exists {
			return NewError(CodeSpecInvalid, "duplicate upsert object id in delta: "+delta.Upserts[i].ID, nil)
		}
		seenUpsertIDs[delta.Upserts[i].ID] = struct{}{}
	}
	sort.Strings(delta.RemovedObjectIDs)
	delta.RemovedObjectIDs = compactStrings(delta.RemovedObjectIDs)
	return nil
}

func NormalizeSnapshot(snapshot *Snapshot) error {
	if snapshot == nil {
		return NewError(CodeSpecInvalid, "snapshot is nil", nil)
	}
	normalizePolicy(&snapshot.Policy)
	problems := validatePolicy(snapshot.Policy)

	if snapshot.SnapshotVersion <= 0 {
		problems = append(problems, "snapshot_version must be a positive monotonic integer")
	}
	if snapshot.GeneratedAt.IsZero() {
		problems = append(problems, "generated_at is required")
	}

	clusterIDs := make(map[string]struct{}, len(snapshot.Clusters))
	for i := range snapshot.Clusters {
		cluster := &snapshot.Clusters[i]
		cluster.ID = strings.TrimSpace(cluster.ID)
		cluster.Endpoint = strings.TrimSpace(cluster.Endpoint)
		if cluster.ID == "" {
			problems = append(problems, fmt.Sprintf("clusters[%d].id is required", i))
			continue
		}
		if _, exists := clusterIDs[cluster.ID]; exists {
			problems = append(problems, "duplicate cluster id: "+cluster.ID)
		}
		clusterIDs[cluster.ID] = struct{}{}
	}

	seenIDs := make(map[string]struct{}, len(snapshot.Objects))
	for i := range snapshot.Objects {
		object := &snapshot.Objects[i]
		if err := normalizeObject(object); err != nil {
			problems = append(problems, err.Error())
			continue
		}
		if _, exists := seenIDs[object.ID]; exists {
			problems = append(problems, "duplicate object id: "+object.ID)
		}
		seenIDs[object.ID] = struct{}{}
		if _, ok := clusterIDs[object.ClusterID]; !ok {
			problems = append(problems, fmt.Sprintf("object %s references missing cluster %s", object.ID, object.ClusterID))
		}
		if object.Budget.CPUMillis < 0 || object.Budget.MemoryBytes < 0 || object.Budget.BatchUnits < 0 {
			problems = append(problems, "resource budget must be non-negative: "+object.ID)
		}
	}

	for _, object := range snapshot.Objects {
		for _, dep := range object.DependsOn {
			if _, ok := seenIDs[dep]; !ok {
				problems = append(problems, fmt.Sprintf("object %s references missing dependency %s", object.ID, dep))
			}
		}
	}

	if aggregateBudgetOverflows(snapshot.Objects) {
		problems = append(problems, "aggregate resource budget overflows int64")
	}

	if len(problems) > 0 {
		return NewError(CodeSpecInvalid, "specification validation failed", map[string]any{"problems": compactStrings(problems)})
	}
	return nil
}

func normalizePolicy(policy *Policy) {
	if policy.MaxBatchSize <= 0 {
		policy.MaxBatchSize = 10
	}
	if policy.MaxBatchUnits <= 0 {
		policy.MaxBatchUnits = int64(policy.MaxBatchSize)
	}
	if policy.RoundBudget <= 0 {
		policy.RoundBudget = Duration(30 * time.Second)
	}
	if policy.WriteTimeout <= 0 {
		policy.WriteTimeout = Duration(5 * time.Second)
	}
	if policy.MaxParallelBatches <= 0 {
		policy.MaxParallelBatches = 2
	}
	if policy.WriteQueueSize < 0 {
		policy.WriteQueueSize = 0
	}
	if len(policy.DriftPriority) == 0 {
		policy.DriftPriority = append([]string(nil), DefaultDriftPriority...)
	}
}

func validatePolicy(policy Policy) []string {
	var problems []string
	if policy.PolicyVersion <= 0 {
		problems = append(problems, "policy_version must be a positive monotonic integer")
	}
	if policy.GeneratedAt.IsZero() {
		problems = append(problems, "policy.generated_at is required")
	}
	if policy.DriftTolerance.Rat().Sign() < 0 {
		problems = append(problems, "drift_tolerance must be non-negative")
	}
	if policy.MaxBatchSize <= 0 {
		problems = append(problems, "max_batch_size must be positive")
	}
	if policy.MaxBatchUnits <= 0 {
		problems = append(problems, "max_batch_resource_units must be positive")
	}
	if policy.RoundBudget <= 0 {
		problems = append(problems, "round_budget must be positive")
	}
	if policy.WriteTimeout <= 0 {
		problems = append(problems, "write_timeout must be positive")
	}
	if policy.RetryLimit < 0 {
		problems = append(problems, "retry_limit must be non-negative")
	}
	if policy.MaxParallelBatches <= 0 {
		problems = append(problems, "max_parallel_batches must be positive")
	}
	validPriority := map[string]bool{}
	for _, category := range DefaultDriftPriority {
		validPriority[category] = true
	}
	seen := map[string]bool{}
	for _, category := range policy.DriftPriority {
		if !validPriority[category] {
			problems = append(problems, "unknown drift priority category: "+category)
		}
		if seen[category] {
			problems = append(problems, "duplicate drift priority category: "+category)
		}
		seen[category] = true
	}
	return problems
}

func normalizeObject(object *Object) error {
	if object == nil {
		return NewError(CodeSpecInvalid, "object is nil", nil)
	}
	object.ID = strings.TrimSpace(object.ID)
	object.ClusterID = strings.TrimSpace(object.ClusterID)
	object.Kind = strings.TrimSpace(object.Kind)
	object.Namespace = strings.TrimSpace(object.Namespace)
	object.Endpoint = strings.TrimSpace(object.Endpoint)
	if object.ID == "" {
		return NewError(CodeSpecInvalid, "object id is required", object)
	}
	if object.ClusterID == "" {
		return NewError(CodeSpecInvalid, "object cluster_id is required: "+object.ID, nil)
	}
	if object.Kind == "" {
		return NewError(CodeSpecInvalid, "object kind is required: "+object.ID, nil)
	}
	if object.Namespace == "" {
		object.Namespace = "default"
	}
	if object.Expected == nil {
		return NewError(CodeSpecInvalid, "expected_fields collection is missing/incomplete: "+object.ID, nil)
	}
	if object.Actual == nil {
		return NewError(CodeSpecInvalid, "actual_fields collection is missing/incomplete: "+object.ID, nil)
	}

	var err error
	object.Expected, err = normalizeFieldSet(object.Expected)
	if err != nil {
		return WrapError(CodeSpecInvalid, "invalid expected_fields for "+object.ID, err, nil)
	}
	object.Actual, err = normalizeFieldSet(object.Actual)
	if err != nil {
		return WrapError(CodeSpecInvalid, "invalid actual_fields for "+object.ID, err, nil)
	}

	object.DependsOn = compactStrings(object.DependsOn)
	if object.Budget.BatchUnits < 0 || object.Budget.CPUMillis < 0 || object.Budget.MemoryBytes < 0 {
		return NewError(CodeSpecInvalid, "resource budget must be non-negative: "+object.ID, object.Budget)
	}
	return nil
}

func normalizeFieldSet(fields FieldSet) (FieldSet, error) {
	if fields == nil {
		return nil, NewError(CodeSpecInvalid, "field set is nil", nil)
	}
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	normalized := make(FieldSet, len(fields))
	for _, name := range names {
		field := fields[name]
		value, err := NormalizeValue(field.Value)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", name, err)
		}
		field.Value = value
		field.Source = strings.TrimSpace(field.Source)
		normalized[name] = field
	}
	return normalized, nil
}

func NormalizeValue(value any) (any, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case string, bool, Decimal:
		return typed, nil
	case json.Number:
		decimal, err := ParseDecimal(typed.String())
		if err != nil {
			return nil, err
		}
		return decimal, nil
	case float64:
		decimal, err := ParseDecimal(formatFloat(typed))
		if err != nil {
			return nil, err
		}
		return decimal, nil
	case float32:
		decimal, err := ParseDecimal(formatFloat(float64(typed)))
		if err != nil {
			return nil, err
		}
		return decimal, nil
	case int:
		return NewDecimal(int64(typed)), nil
	case int8:
		return NewDecimal(int64(typed)), nil
	case int16:
		return NewDecimal(int64(typed)), nil
	case int32:
		return NewDecimal(int64(typed)), nil
	case int64:
		return NewDecimal(typed), nil
	case uint:
		return ParseDecimal(fmt.Sprint(typed))
	case uint8:
		return NewDecimal(int64(typed)), nil
	case uint16:
		return NewDecimal(int64(typed)), nil
	case uint32:
		return NewDecimal(int64(typed)), nil
	case uint64:
		return ParseDecimal(fmt.Sprint(typed))
	case []any:
		result := make([]any, len(typed))
		for i := range typed {
			normalized, err := NormalizeValue(typed[i])
			if err != nil {
				return nil, err
			}
			result[i] = normalized
		}
		return result, nil
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		result := make(map[string]any, len(typed))
		for _, key := range keys {
			normalized, err := NormalizeValue(typed[key])
			if err != nil {
				return nil, err
			}
			result[key] = normalized
		}
		return result, nil
	default:
		data, err := json.Marshal(typed)
		if err != nil {
			return nil, fmt.Errorf("unsupported value type %T", value)
		}
		var decoded any
		if err := decodeUseNumber(data, &decoded); err != nil {
			return nil, err
		}
		return NormalizeValue(decoded)
	}
}

func compactStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func addInt64Checked(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, false
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}

func aggregateBudgetOverflows(objects []Object) bool {
	var batchUnits int64
	var cpuMillis int64
	var memoryBytes int64
	for _, object := range objects {
		var ok bool
		batchUnits, ok = addInt64Checked(batchUnits, object.Budget.SchedulingUnits())
		if !ok {
			return true
		}
		cpuMillis, ok = addInt64Checked(cpuMillis, object.Budget.CPUMillis)
		if !ok {
			return true
		}
		memoryBytes, ok = addInt64Checked(memoryBytes, object.Budget.MemoryBytes)
		if !ok {
			return true
		}
	}
	return false
}
