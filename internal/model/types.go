package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const FixedScale int64 = 1_000_000

const (
	ColumnInt   = "int"
	ColumnFixed = "fixed"
	ColumnDict  = "dict"
)

const (
	PathSmall = "small_vector"
	PathLarge = "merged_large_vector"

	StatusRunning   = "running"
	StatusCommitted = "committed"
	StatusFailed    = "failed"
	StatusAborted   = "aborted"
	StatusRecovered = "recovered"

	BatchPending     = "pending"
	BatchProvisional = "provisional"
	BatchCommitted   = "committed"
)

// Column is a typed column with a packed null bitmap (one bit per row).
type Column struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	ScaleUnits  int64    `json:"scale_units,omitempty"`
	IntValues   []int64  `json:"int_values,omitempty"`
	FixedValues []int64  `json:"fixed_values,omitempty"`
	DictIndices []int32  `json:"dict_indices,omitempty"`
	Dictionary  []string `json:"dictionary,omitempty"`
	Nulls       []byte   `json:"nulls"`
}

func (c Column) NullAt(row int) bool {
	if row < 0 || row/8 >= len(c.Nulls) {
		return false
	}
	return c.Nulls[row/8]&(byte(1)<<uint(row%8)) != 0
}

// Snapshot is an immutable, versioned columnar snapshot.
type Snapshot struct {
	Version      int64             `json:"version"`
	GeneratedAt  string            `json:"generated_at"`
	Seed         uint64            `json:"seed"`
	RowCount     int               `json:"row_count"`
	Columns      []Column          `json:"columns"`
	Selections   map[string]Bitmap `json:"selections"`
	SHA256Digest string            `json:"sha256"`
}

func (s Snapshot) Clone() Snapshot {
	raw, _ := json.Marshal(s)
	var out Snapshot
	_ = json.Unmarshal(raw, &out)
	return out
}

func (s Snapshot) Column(name string) (Column, bool) {
	for _, c := range s.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

func (s Snapshot) Selection(form string) (Bitmap, bool) {
	bm, ok := s.Selections[form]
	return bm, ok
}

// ComputeSnapshotDigest is canonical for this Go model: encoding/json sorts map
// keys, and the digest field itself is excluded from the payload.
func ComputeSnapshotDigest(s Snapshot) (string, error) {
	copy := s
	copy.SHA256Digest = ""
	raw, err := json.Marshal(copy)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

type SnapshotGenerateRequest struct {
	Version int64  `json:"version"`
	Rows    int    `json:"rows"`
	Seed    uint64 `json:"seed"`
}

type Predicate struct {
	Column       string  `json:"column"`
	Op           string  `json:"op"`
	ValueInt     *int64  `json:"value_int,omitempty"`
	ValueFixed   *int64  `json:"value_fixed,omitempty"`
	ValueString  *string `json:"value_string,omitempty"`
	OtherColumn  string  `json:"other_column,omitempty"`
	FactorUnits  int64   `json:"factor_units,omitempty"`
	DivisorUnits int64   `json:"divisor_units,omitempty"`
}

type ScanOperator struct {
	Columns []string `json:"columns,omitempty"`
}

type FilterOperator struct {
	Predicate Predicate `json:"predicate"`
}

type ProjectOperator struct {
	Columns []string `json:"columns"`
}

type AggregateSpec struct {
	Name   string `json:"name"`
	Column string `json:"column,omitempty"`
	Func   string `json:"func"`
}

type AggregateOperator struct {
	Specs []AggregateSpec `json:"specs"`
}

type Operator struct {
	Kind      string             `json:"kind"`
	Scan      *ScanOperator      `json:"scan,omitempty"`
	Filter    *FilterOperator    `json:"filter,omitempty"`
	Project   *ProjectOperator   `json:"project,omitempty"`
	Aggregate *AggregateOperator `json:"aggregate,omitempty"`
}

type Chain struct {
	ID                  string     `json:"id"`
	Version             int64      `json:"version"`
	BaseSnapshotVersion int64      `json:"base_snapshot_version"`
	Operators           []Operator `json:"operators"`
	CreatedAt           string     `json:"created_at,omitempty"`
}

const (
	FaultBitmapMismatch = "bitmap_mismatch"
	FaultFixedOverflow  = "fixed_overflow"
	FaultDictIndex      = "dict_index"
	FaultDivideByZero   = "divide_by_zero"
	FaultExecAborted    = "exec_aborted"
)

type FaultSpec struct {
	Kind         string `json:"kind"`
	Batch        int    `json:"batch,omitempty"`
	Column       string `json:"column,omitempty"`
	Row          int    `json:"row,omitempty"`
	DictIndex    int    `json:"dict_index,omitempty"`
	ExpectedRows int    `json:"expected_rows,omitempty"`
	ActualRows   int    `json:"actual_rows,omitempty"`
}

func BusinessCodeForFault(kind string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case FaultBitmapMismatch:
		return CodeBitmapMismatch, true
	case FaultFixedOverflow:
		return CodeNumericOverflow, true
	case FaultDictIndex:
		return CodeDictIndex, true
	case FaultDivideByZero:
		return CodeDivideByZero, true
	case FaultExecAborted:
		return CodeExecAborted, true
	default:
		return "", false
	}
}

type FaultProbe struct {
	Kind   string `json:"kind"`
	Batch  int    `json:"batch,omitempty"`
	Column string `json:"column,omitempty"`
}

type ExecuteRequest struct {
	ExecutionID          string      `json:"execution_id,omitempty"`
	ChainVersion         int64       `json:"chain_version"`
	BitmapForm           string      `json:"bitmap_form"`
	BatchSize            int         `json:"batch_size"`
	LargeVectorThreshold int         `json:"large_vector_threshold,omitempty"`
	Injections           []FaultSpec `json:"injections,omitempty"`
	// CrashAfterBatch is a demo-only forced-termination hook. The server only
	// honors it when COLUMNAR_ALLOW_CRASH=1.
	CrashAfterBatch int `json:"crash_after_batch,omitempty"`
}

type PathDecision struct {
	Path           string `json:"path"`
	Reason         string `json:"reason"`
	SelectedRows   int    `json:"selected_rows"`
	BatchSize      int    `json:"batch_size"`
	LargeThreshold int    `json:"large_threshold"`
	BatchCount     int    `json:"batch_count"`
}

type ProjectedValue struct {
	Column    string `json:"column"`
	Type      string `json:"type"`
	Null      bool   `json:"null"`
	Int       *int64 `json:"int,omitempty"`
	Units     *int64 `json:"units,omitempty"`
	DictIndex *int32 `json:"dict_index,omitempty"`
	Text      string `json:"text,omitempty"`
}

type ProjectedRow struct {
	RowID  int64            `json:"row_id"`
	Values []ProjectedValue `json:"values"`
}

type AggregateValue struct {
	Name               string `json:"name"`
	Function           string `json:"function"`
	Column             string `json:"column,omitempty"`
	Type               string `json:"type"`
	Count              int64  `json:"count"`
	NullsSkipped       int64  `json:"nulls_skipped"`
	ValueUnits         int64  `json:"value_units"`
	ValueText          string `json:"value_text"`
	ScaleUnits         int64  `json:"scale_units,omitempty"`
	Rounding           string `json:"rounding,omitempty"`
	RoundingErrorUnits int64  `json:"rounding_error_units,omitempty"`
}

type FilterStage struct {
	Column      string `json:"column"`
	Op          string `json:"op"`
	InputRows   int    `json:"input_rows"`
	MatchedRows int    `json:"matched_rows"`
	OutputRows  int    `json:"output_rows"`
}

type BatchTrace struct {
	BatchNo      int              `json:"batch_no"`
	RowStart     int              `json:"row_start"`
	RowEnd       int              `json:"row_end"`
	InputRows    int              `json:"input_rows"`
	FilteredRows int              `json:"filtered_rows"`
	OutputRows   int              `json:"output_rows"`
	RowIDs       []int64          `json:"row_ids"`
	Projection   []string         `json:"projection,omitempty"`
	Rows         []ProjectedRow   `json:"rows,omitempty"`
	Aggregates   []AggregateValue `json:"aggregates,omitempty"`
	FilterStages []FilterStage    `json:"filter_stages,omitempty"`
}

type ExecutionView struct {
	ExecutionID     string           `json:"execution_id"`
	ChainID         string           `json:"chain_id"`
	ChainVersion    int64            `json:"chain_version"`
	SnapshotVersion int64            `json:"snapshot_version"`
	BitmapForm      string           `json:"bitmap_form"`
	Bitmap          Bitmap           `json:"bitmap"`
	BatchSize       int              `json:"batch_size"`
	LargeThreshold  int              `json:"large_threshold"`
	Path            PathDecision     `json:"path"`
	Status          string           `json:"status"`
	Published       bool             `json:"published"`
	ErrorCode       string           `json:"error_code,omitempty"`
	ErrorContext    map[string]any   `json:"error_context,omitempty"`
	StartedAt       string           `json:"started_at"`
	FinishedAt      string           `json:"finished_at,omitempty"`
	Batches         []BatchTrace     `json:"batches,omitempty"`
	Aggregates      []AggregateValue `json:"aggregates,omitempty"`
}

type ExecutionSummary struct {
	ExecutionID     string       `json:"execution_id"`
	ChainID         string       `json:"chain_id"`
	ChainVersion    int64        `json:"chain_version"`
	SnapshotVersion int64        `json:"snapshot_version"`
	BitmapForm      string       `json:"bitmap_form"`
	Path            PathDecision `json:"path"`
	Status          string       `json:"status"`
	Published       bool         `json:"published"`
	ErrorCode       string       `json:"error_code,omitempty"`
	StartedAt       string       `json:"started_at"`
	FinishedAt      string       `json:"finished_at,omitempty"`
}

type RecoveryDiff struct {
	Kind     string `json:"kind"`
	BatchNo  int    `json:"batch_no,omitempty"`
	Before   string `json:"before"`
	After    string `json:"after"`
	Evidence string `json:"evidence"`
}

type IntentFact struct {
	Kind      string         `json:"kind"`
	Status    string         `json:"status"`
	Payload   map[string]any `json:"payload,omitempty"`
	CreatedAt string         `json:"created_at"`
}

type RecoveryReport struct {
	ID                  int64          `json:"id"`
	ExecutionID         string         `json:"execution_id"`
	ChainID             string         `json:"chain_id"`
	ChainVersion        int64          `json:"chain_version"`
	PreviousStatus      string         `json:"previous_status"`
	RecoveredStatus     string         `json:"recovered_status"`
	Path                string         `json:"path"`
	CommittedBatches    []int          `json:"committed_batches"`
	RolledBackBatches   []int          `json:"rolled_back_batches"`
	DiscardedAggregates []int          `json:"discarded_aggregate_batches"`
	Intents             []IntentFact   `json:"intents"`
	Differences         []RecoveryDiff `json:"differences"`
	CreatedAt           string         `json:"created_at"`
}

func NowUTC() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func FormatFixed(units int64) string {
	sign := ""
	if units < 0 {
		sign = "-"
		// Avoid negating MinInt64.
		u := uint64(-(units + 1)) + 1
		return fmt.Sprintf("%s%d.%06d", sign, u/uint64(FixedScale), u%uint64(FixedScale))
	}
	u := uint64(units)
	return fmt.Sprintf("%d.%06d", u/uint64(FixedScale), u%uint64(FixedScale))
}

func FormatScaled(units, scale int64) string {
	if scale <= 0 {
		scale = 1
	}
	digits := 0
	for n := scale; n > 1; n /= 10 {
		digits++
	}
	if units < 0 {
		u := uint64(-(units + 1)) + 1
		whole := u / uint64(scale)
		frac := u % uint64(scale)
		if digits == 0 {
			return fmt.Sprintf("-%d", whole)
		}
		return fmt.Sprintf("-%d.%0*d", whole, digits, frac)
	}
	u := uint64(units)
	whole := u / uint64(scale)
	frac := u % uint64(scale)
	if digits == 0 {
		return fmt.Sprintf("%d", whole)
	}
	return fmt.Sprintf("%d.%0*d", whole, digits, frac)
}
