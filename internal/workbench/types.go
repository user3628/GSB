package workbench

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	StateFormat      = "recordlab-state-v1"
	ReportFormat     = "recordlab-report-v1"
	InjectionFormat  = "recordlab-injection-v1"
	SamplingMarker   = "FIXED_SAMPLE_V1"
	DefaultSnapshot  = "deterministic-snapshot-v1"
	GenesisDigest    = "recordlab-genesis-v1"
	BaselineScenario = "baseline"
)

const (
	CodeOK              = "E_OK"
	CodeTruncated       = "E_TRUNCATED"
	CodeLengthWrap      = "E_LENGTH_WRAP"
	CodeLengthOverflow  = "E_LENGTH_OVERFLOW"
	CodeDepthExceeded   = "E_DEPTH_EXCEEDED"
	CodeNestedBudget    = "E_NESTED_BUDGET"
	CodeTaskMemory      = "E_TASK_MEMORY_BUDGET"
	CodeGlobalBudget    = "E_GLOBAL_BUDGET"
	CodeSnapshotVersion = "E_SNAPSHOT_VERSION"
	CodeNotRun          = "E_NOT_RUN"
)

const (
	SampleValid          = "valid"
	SampleTruncated      = "truncated"
	SampleLengthWrap     = "length_wrap"
	SampleLengthOverflow = "length_overflow"
	SampleDepth          = "deep_nesting"
	SampleNestedBudget   = "nested_budget"
)

const (
	FinalOK             = "ok"
	FinalDecodeFinding  = "decode_finding"
	FinalBudgetExceeded = "budget_exceeded"
	FinalRejected       = "rejected"
	FinalPending        = "pending"
)

const (
	InjectionLengthWrap   = "length_wrap"
	InjectionDeepNesting  = "deep_nesting"
	InjectionTaskBudget   = "task_budget"
	InjectionGlobalBudget = "global_budget"
	InjectionSnapshotGap  = "snapshot_gap"
)

type Config struct {
	Seed               uint64 `json:"seed"`
	RecordCount        int    `json:"record_count"`
	PayloadSize        int    `json:"payload_size"`
	MaxDepth           int    `json:"max_depth"`
	NestedBudget       int64  `json:"nested_budget"`
	GlobalMemoryBudget int64  `json:"global_memory_budget"`
	TaskMemoryBudget   int64  `json:"task_memory_budget"`
	Scenario           string `json:"scenario"`
	Injection          bool   `json:"injection"`
}

func DefaultConfig() Config {
	return Config{
		Seed:               1,
		RecordCount:        3,
		PayloadSize:        64,
		MaxDepth:           4,
		NestedBudget:       4096,
		GlobalMemoryBudget: 262144,
		TaskMemoryBudget:   131072,
		Scenario:           BaselineScenario,
		Injection:          false,
	}
}

func (c Config) Validate() error {
	if c.RecordCount < 1 || c.RecordCount > 256 {
		return fmt.Errorf("record_count must be between 1 and 256")
	}
	if c.PayloadSize < 0 || c.PayloadSize > 1<<20 {
		return fmt.Errorf("payload_size must be between 0 and 1048576")
	}
	if c.MaxDepth < 1 || c.MaxDepth > 64 {
		return fmt.Errorf("max_depth must be between 1 and 64")
	}
	if c.NestedBudget < 0 {
		return fmt.Errorf("nested_budget must be non-negative")
	}
	if c.GlobalMemoryBudget < 0 {
		return fmt.Errorf("global_memory_budget must be non-negative")
	}
	if c.TaskMemoryBudget < 1 {
		return fmt.Errorf("task_memory_budget must be positive")
	}
	if !KnownScenario(c.Scenario) {
		return fmt.Errorf("unknown scenario %q", c.Scenario)
	}
	return nil
}

func KnownScenario(s string) bool {
	switch s {
	case BaselineScenario,
		"inject-length-wrap",
		"inject-deep-nesting",
		"inject-task-budget",
		"inject-global-budget",
		"inject-snapshot-gap":
		return true
	default:
		return false
	}
}

type TaskSpec struct {
	TaskID         string `json:"task_id"`
	SampleKind     string `json:"sample_kind"`
	DeclaredBudget int64  `json:"declared_budget"`
	Description    string `json:"description"`
}

type TraceEvent struct {
	Seq         int64  `json:"seq"`
	Action      string `json:"action"`
	RecordIndex int    `json:"record_index"`
	RecordID    uint16 `json:"record_id"`
	Depth       int    `json:"depth"`
	Offset      int64  `json:"offset"`
	EndOffset   int64  `json:"end_offset"`
	Code        string `json:"code,omitempty"`
	Detail      string `json:"detail,omitempty"`
}

type Issue struct {
	Code        string `json:"code"`
	RecordIndex int    `json:"record_index"`
	RecordID    uint16 `json:"record_id"`
	Depth       int    `json:"depth"`
	Offset      int64  `json:"offset"`
	ParentEnd   int64  `json:"parent_end"`
	Declared    uint64 `json:"declared_length"`
	Cumulative  uint64 `json:"cumulative_length,omitempty"`
	Limit       uint64 `json:"limit,omitempty"`
	Detail      string `json:"detail"`
}

type RecordSummary struct {
	Index          int    `json:"index"`
	RecordID       uint16 `json:"record_id"`
	DeclaredLength uint64 `json:"declared_length"`
	BodyBytes      int64  `json:"body_bytes"`
	FieldCount     int    `json:"field_count"`
	NestedFields   int    `json:"nested_fields"`
	ChildRecords   int    `json:"child_records"`
	MaxDepth       int    `json:"max_depth"`
	PayloadBytes   int64  `json:"payload_bytes"`
	Digest         string `json:"digest"`
	Code           string `json:"code"`
	PeakBytes      int64  `json:"peak_memory_bytes"`
	Allocations    int64  `json:"allocations"`
}

type FailureSummary struct {
	RecordIndex    int    `json:"record_index"`
	RecordID       uint16 `json:"record_id"`
	Code           string `json:"code"`
	Offset         int64  `json:"offset"`
	ParentEnd      int64  `json:"parent_end"`
	DeclaredLength uint64 `json:"declared_length"`
	Detail         string `json:"detail"`
	PeakBytes      int64  `json:"peak_memory_bytes"`
	Allocations    int64  `json:"allocations"`
}

type DecodeResult struct {
	Code            string          `json:"code"`
	ContainerDigest string          `json:"container_digest"`
	ContainerBytes  int64           `json:"container_bytes"`
	DeclaredRecords int             `json:"declared_records"`
	SafePrefix      int             `json:"safe_record_prefix"`
	Records         []RecordSummary `json:"records"`
	Failure         *FailureSummary `json:"failure,omitempty"`
	Issues          []Issue         `json:"issues"`
	Trajectory      []TraceEvent    `json:"trajectory"`
	PeakBytes       int64           `json:"peak_memory_bytes"`
	Allocations     int64           `json:"allocations"`
}

type TaskResult struct {
	TaskID              string          `json:"task_id"`
	SampleKind          string          `json:"sample_kind"`
	Description         string          `json:"description"`
	DeclaredBudget      int64           `json:"declared_budget"`
	AdmissionCode       string          `json:"admission_code"`
	Admitted            bool            `json:"admitted"`
	DecodeCode          string          `json:"decode_code"`
	DecisionCode        string          `json:"decision_code"`
	FinalState          string          `json:"final_state"`
	RejectionReason     string          `json:"rejection_reason,omitempty"`
	SampleDigest        string          `json:"sample_digest,omitempty"`
	SampleBytes         int64           `json:"sample_bytes"`
	PeakMemoryBytes     int64           `json:"peak_memory_bytes"`
	Allocations         int64           `json:"allocations"`
	GlobalReservedAfter int64           `json:"global_reserved_after"`
	Records             []RecordSummary `json:"records"`
	Failure             *FailureSummary `json:"failure,omitempty"`
	Issues              []Issue         `json:"issues"`
	Trajectory          []TraceEvent    `json:"trajectory"`
	SnapshotVersion     int64           `json:"snapshot_version"`
}

type State struct {
	Format  string       `json:"format"`
	Version int64        `json:"version"`
	Config  Config       `json:"config"`
	Tasks   []TaskSpec   `json:"tasks"`
	Results []TaskResult `json:"results"`
}

func NewState(cfg Config, tasks []TaskSpec) State {
	return State{
		Format:  StateFormat,
		Version: 0,
		Config:  cfg,
		Tasks:   append([]TaskSpec(nil), tasks...),
		Results: []TaskResult{},
	}
}

func (s State) SortedTasks() []TaskSpec {
	tasks := append([]TaskSpec(nil), s.Tasks...)
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].TaskID < tasks[j].TaskID })
	return tasks
}

func (s State) SortedResults() []TaskResult {
	results := append([]TaskResult(nil), s.Results...)
	sort.Slice(results, func(i, j int) bool { return results[i].TaskID < results[j].TaskID })
	return results
}

func (s State) Result(taskID string) (TaskResult, bool) {
	for _, result := range s.Results {
		if result.TaskID == taskID {
			return result, true
		}
	}
	return TaskResult{}, false
}

func (s *State) RemoveTaskResult(taskID string) {
	out := s.Results[:0]
	for _, result := range s.Results {
		if result.TaskID != taskID {
			out = append(out, result)
		}
	}
	s.Results = out
}

func (s *State) PutTaskResult(result TaskResult) {
	s.RemoveTaskResult(result.TaskID)
	s.Results = append(s.Results, result)
	sort.Slice(s.Results, func(i, j int) bool { return s.Results[i].TaskID < s.Results[j].TaskID })
}

func CodeRank(code string) int {
	switch code {
	case CodeLengthWrap:
		return 70
	case CodeLengthOverflow:
		return 60
	case CodeTruncated:
		return 50
	case CodeDepthExceeded:
		return 40
	case CodeNestedBudget:
		return 30
	case CodeTaskMemory:
		return 20
	case CodeGlobalBudget:
		return 10
	default:
		return 0
	}
}

func WorseCode(a, b string) string {
	if CodeRank(b) > CodeRank(a) {
		return b
	}
	return a
}

func IsBusinessCode(code string) bool {
	return CodeRank(code) > 0
}

type Snapshot struct {
	Version      int64  `json:"version"`
	Kind         string `json:"kind"`
	ParentDigest string `json:"parent_digest"`
	StateSHA256  string `json:"state_sha256"`
	ChainDigest  string `json:"chain_digest"`
	StateJSON    []byte `json:"-"`
	State        State  `json:"state"`
}

type ChainLoad struct {
	Valid              []Snapshot
	State              State
	HasState           bool
	WarningCode        string
	WarningDetail      string
	InvalidFromVersion int64
}

func (l ChainLoad) LatestVersion() int64 {
	if len(l.Valid) == 0 {
		return 0
	}
	return l.Valid[len(l.Valid)-1].Version
}

func (l ChainLoad) LatestDigest() string {
	if len(l.Valid) == 0 {
		return GenesisDigest
	}
	return l.Valid[len(l.Valid)-1].ChainDigest
}

type RunOptions struct {
	Concurrency int
	StopAfter   int
	Reset       bool
	Config      Config
	ConfigSet   bool
}

type RunOutcome struct {
	State       State
	Chain       []Snapshot
	NewTasks    int
	Interrupted bool
	WarningCode string
	Report      Report
}

type Report struct {
	Format         string        `json:"format"`
	SamplingMarker string        `json:"sampling_marker"`
	SnapshotMarker string        `json:"snapshot_marker"`
	Scenario       string        `json:"scenario"`
	Config         Config        `json:"config"`
	Tasks          []TaskReport  `json:"tasks"`
	RejectedTasks  []TaskReport  `json:"rejected_tasks"`
	SnapshotChain  []ChainEntry  `json:"snapshot_chain"`
	Summary        ReportSummary `json:"summary"`
}

type TaskReport struct {
	TaskID              string          `json:"task_id"`
	SampleKind          string          `json:"sample_kind"`
	Description         string          `json:"description"`
	DeclaredBudget      int64           `json:"declared_budget"`
	AdmissionCode       string          `json:"admission_code"`
	Admitted            bool            `json:"admitted"`
	DecodeCode          string          `json:"decode_code"`
	DecisionCode        string          `json:"decision_code"`
	FinalState          string          `json:"final_state"`
	RejectionReason     string          `json:"rejection_reason,omitempty"`
	SampleDigest        string          `json:"sample_digest,omitempty"`
	SampleBytes         int64           `json:"sample_bytes"`
	PeakMemoryBytes     int64           `json:"peak_memory_bytes"`
	Allocations         int64           `json:"allocations"`
	GlobalReservedAfter int64           `json:"global_reserved_after"`
	SnapshotVersion     int64           `json:"snapshot_version"`
	Records             []RecordSummary `json:"records"`
	Failure             *FailureSummary `json:"failure,omitempty"`
	Issues              []Issue         `json:"issues"`
	Trajectory          []TraceEvent    `json:"trajectory"`
}

type ChainEntry struct {
	Version      int64  `json:"version"`
	Kind         string `json:"kind"`
	ParentDigest string `json:"parent_digest"`
	StateSHA256  string `json:"state_sha256"`
	ChainDigest  string `json:"chain_digest"`
}

type ReportSummary struct {
	TaskCount           int   `json:"task_count"`
	CompletedCount      int   `json:"completed_count"`
	RejectedCount       int   `json:"rejected_count"`
	FindingCount        int   `json:"finding_count"`
	GlobalBudget        int64 `json:"global_memory_budget"`
	GlobalReservedBytes int64 `json:"global_reserved_bytes"`
	GlobalPeakSumBytes  int64 `json:"global_peak_sum_bytes"`
	TaskBudgetLimit     int64 `json:"task_memory_budget"`
	SnapshotCount       int   `json:"snapshot_count"`
}

type InjectionCase struct {
	Kind               string `json:"kind"`
	ExpectedTargetCode string `json:"expected_target_code"`
	ActualTargetCode   string `json:"actual_target_code"`
	ControlsUnaffected bool   `json:"controls_unaffected"`
	SnapshotsValid     bool   `json:"snapshots_valid"`
	SnapshotFallback   int64  `json:"snapshot_fallback_version,omitempty"`
	ReportDigest       string `json:"report_digest,omitempty"`
	Passed             bool   `json:"passed"`
	Detail             string `json:"detail"`
}

type InjectionReport struct {
	Format         string          `json:"format"`
	SamplingMarker string          `json:"sampling_marker"`
	Cases          []InjectionCase `json:"cases"`
	AllPassed      bool            `json:"all_passed"`
}

var ErrInterrupted = errors.New("run interrupted before all tasks committed")

func cleanTaskID(id string) string {
	return strings.TrimSpace(id)
}
