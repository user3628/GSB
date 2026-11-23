package domain

import (
	"encoding/json"
	"fmt"
	"sort"
)

const (
	ModeFast = "fast"
	ModeFull = "full"

	LatePolicyReject    = "reject"
	LatePolicyNewBatch  = "new_batch"
	LatePolicyMergeSeal = "merge_sealed"
)

type Faults struct {
	LateRequestIDs        []string `json:"late_request_ids,omitempty"`
	DuplicateRequestID    string   `json:"duplicate_request_id,omitempty"`
	VramShortfallBytes    int64    `json:"vram_shortfall_bytes,omitempty"`
	PrecisionOffsetUS     int64    `json:"precision_offset_us,omitempty"`
	InterruptAfterBatches *int     `json:"interrupt_after_batches,omitempty"`
	InterruptAfterQuant   bool     `json:"interrupt_after_quantiles,omitempty"`
}

type OrganizationOptions struct {
	Mode                    string `json:"mode"`
	OrganizationOrder       string `json:"organization_order"`
	NodeVRAMBytes           int64  `json:"node_vram_bytes"`
	OrganizationBudgetMS    int64  `json:"organization_budget_ms"`
	AllowMixedModels        bool   `json:"allow_mixed_models"`
	LatencyDecimals         int    `json:"latency_decimals"`
	MaxAbsDeviationUS       int64  `json:"max_abs_deviation_us"`
	MaxRelativeDeviationPPM int64  `json:"max_relative_deviation_ppm"`
	LatePolicy              string `json:"late_policy"`
	Faults                  Faults `json:"faults,omitempty"`
}

func DefaultOrganizationOptions() OrganizationOptions {
	return OrganizationOptions{
		Mode: ModeFull, OrganizationOrder: "arrival", NodeVRAMBytes: 512 << 20, OrganizationBudgetMS: 50,
		LatencyDecimals: 3, MaxAbsDeviationUS: 100, MaxRelativeDeviationPPM: 10000,
		LatePolicy: LatePolicyReject,
	}
}

func NormalizeOptions(in OrganizationOptions) (OrganizationOptions, error) {
	out := in
	if out.Mode == "" {
		out.Mode = ModeFull
	}
	if out.Mode == "low_latency" || out.Mode == "low-latency" {
		out.Mode = ModeFast
	}
	if out.Mode != ModeFast && out.Mode != ModeFull {
		return out, NewBizError(CodeBadRequest, "mode must be fast or full", map[string]any{"mode": out.Mode})
	}
	if out.OrganizationOrder == "" {
		out.OrganizationOrder = "arrival"
	}
	switch out.OrganizationOrder {
	case "arrival":
	case "priority", "priority_desc", "priority-desc":
		out.OrganizationOrder = "priority"
	default:
		return out, NewBizError(CodeBadRequest, "organization_order must be arrival or priority", map[string]any{"organization_order": out.OrganizationOrder})
	}
	if out.NodeVRAMBytes <= 0 {
		return out, NewBizError(CodeBadRequest, "node_vram_bytes must be positive", map[string]any{"node_vram_bytes": out.NodeVRAMBytes})
	}
	if out.OrganizationBudgetMS < 0 {
		return out, NewBizError(CodeBadRequest, "organization_budget_ms must not be negative", nil)
	}
	if out.LatencyDecimals < 0 || out.LatencyDecimals > 3 {
		return out, NewBizError(CodeBadRequest, "latency_decimals must be between 0 and 3", map[string]any{"latency_decimals": out.LatencyDecimals})
	}
	if out.MaxAbsDeviationUS < 0 || out.MaxRelativeDeviationPPM < 0 {
		return out, NewBizError(CodeBadRequest, "precision limits must not be negative", nil)
	}
	if out.LatePolicy == "" {
		out.LatePolicy = LatePolicyReject
	}
	switch out.LatePolicy {
	case LatePolicyReject, LatePolicyNewBatch:
	case LatePolicyMergeSeal:
		return out, NewBizError(CodeConflicting, "sealed batches cannot accept late requests", map[string]any{"late_policy": out.LatePolicy})
	default:
		return out, NewBizError(CodeBadRequest, "invalid late_policy", map[string]any{"late_policy": out.LatePolicy})
	}
	if out.Faults.VramShortfallBytes < 0 {
		return out, NewBizError(CodeBadRequest, "vram_shortfall_bytes must not be negative", nil)
	}
	if out.Faults.VramShortfallBytes >= out.NodeVRAMBytes {
		return out, NewBizError(CodeInsufficientCap, "injected VRAM shortfall leaves no usable capacity", map[string]any{
			"node_vram_bytes": out.NodeVRAMBytes, "vram_shortfall_bytes": out.Faults.VramShortfallBytes,
		})
	}
	if len(out.Faults.LateRequestIDs) > 1 {
		ids := append([]string(nil), out.Faults.LateRequestIDs...)
		sort.Strings(ids)
		out.Faults.LateRequestIDs = ids
	}
	return out, nil
}

func (o OrganizationOptions) AvailableVRAM() int64 {
	return o.NodeVRAMBytes - o.Faults.VramShortfallBytes
}

func (o OrganizationOptions) OptionsJSON() string {
	b, _ := json.Marshal(o)
	return string(b)
}

type DeferredRequest struct {
	RequestID string `json:"request_id"`
	ModelID   string `json:"model_id"`
	Reason    string `json:"reason"`
	Detail    string `json:"detail"`
}

type MixRejection struct {
	RequestID      string   `json:"request_id"`
	ExistingModels []string `json:"existing_models"`
	CandidateModel string   `json:"candidate_model"`
	Reason         string   `json:"reason"`
	Detail         string   `json:"detail"`
}

type PlannedMember struct {
	RequestID        string `json:"request_id"`
	Sequence         int    `json:"sequence"`
	ModelID          string `json:"model_id"`
	AllocatedSamples int64  `json:"allocated_samples"`
	OriginalSamples  int64  `json:"original_samples"`
	AllocationReason string `json:"allocation_reason"`
}

type PlannedBatch struct {
	BatchID        string          `json:"batch_id"`
	RunID          string          `json:"run_id"`
	Sequence       int             `json:"batch_sequence"`
	RootSequence   int             `json:"root_batch_sequence"`
	SplitIndex     int             `json:"split_index"`
	SplitReason    string          `json:"split_reason"`
	SplitDetail    string          `json:"split_detail"`
	Status         string          `json:"status"`
	Mode           string          `json:"mode"`
	Priority       int64           `json:"priority"`
	Models         []string        `json:"models"`
	Members        []PlannedMember `json:"members"`
	TotalSamples   int64           `json:"total_samples"`
	VRAMBytes      int64           `json:"vram_bytes"`
	LatencyUS      int64           `json:"latency_us"`
	CompensationUS int64           `json:"compensation_us"`
	LatencyLimitUS int64           `json:"latency_limit_us"`
	LatencyOK      bool            `json:"latency_ok"`
	Sealed         bool            `json:"sealed"`
	LateSealed     bool            `json:"late_sealed,omitempty"`
	SealReason     string          `json:"seal_reason,omitempty"`
}

type QuantileResult struct {
	Quantile    int    `json:"quantile"`
	Method      string `json:"method"`
	Available   bool   `json:"available"`
	LatencyUS   int64  `json:"latency_us"`
	LatencyMS   string `json:"latency_ms"`
	Rank        int    `json:"rank"`
	SampleCount int    `json:"sample_count"`
	MinSamples  int    `json:"min_samples"`
	ErrorCode   string `json:"error_code,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

type PrecisionResult struct {
	TruthUS                 int64  `json:"truth_latency_us"`
	ReportedUS              int64  `json:"reported_latency_us"`
	ReportedMS              string `json:"reported_latency_ms"`
	RoundingPlace           string `json:"rounding_place"`
	InternalFixedUS         int64  `json:"internal_fixed_us"`
	CompensationUS          int64  `json:"compensation_us"`
	RoundingErrorUS         int64  `json:"rounding_error_us"`
	CumulativeRoundingErrUS int64  `json:"cumulative_rounding_error_us"`
	AbsoluteDeviationUS     int64  `json:"absolute_deviation_us"`
	RelativeDeviationPPM    int64  `json:"relative_deviation_ppm"`
	MaxAbsDeviationUS       int64  `json:"max_abs_deviation_us"`
	MaxRelativeDeviationPPM int64  `json:"max_relative_deviation_ppm"`
	PrecisionOK             bool   `json:"precision_ok"`
	Explanation             string `json:"explanation"`
}

type BatchPlan struct {
	PlannedBatch
	Quantiles []QuantileResult `json:"quantiles"`
	Precision PrecisionResult  `json:"precision"`
}

type Plan struct {
	Mode                 string              `json:"mode"`
	ModeLabel            string              `json:"mode_label"`
	Options              OrganizationOptions `json:"options"`
	Batches              []BatchPlan         `json:"batches"`
	Deferred             []DeferredRequest   `json:"deferred_requests"`
	DeferredCount        int                 `json:"deferred_request_count"`
	DeferredReasonCounts map[string]int      `json:"deferred_reason_counts"`
	MixRejections        []MixRejection      `json:"mix_rejections"`
	SelectedRequests     int                 `json:"selected_request_count"`
	TotalInputSamples    int64               `json:"total_input_samples"`
	TotalPlannedSamples  int64               `json:"total_planned_samples"`
	SampleConservationOK bool                `json:"sample_conservation_ok"`
	TotalVRAMBytes       int64               `json:"total_vram_bytes"`
	TotalLatencyUS       int64               `json:"total_latency_us"`
	PrecisionOK          bool                `json:"precision_ok"`
	LatencyOK            bool                `json:"latency_ok"`
	PlanHash             string              `json:"plan_hash"`
}

type RunResult struct {
	RunID                string              `json:"run_id"`
	TraceID              string              `json:"trace_id"`
	Version              string              `json:"version"`
	ContentHash          string              `json:"content_hash"`
	CatalogHash          string              `json:"catalog_hash"`
	Mode                 string              `json:"mode"`
	ModeLabel            string              `json:"mode_label"`
	Status               string              `json:"status"`
	Options              OrganizationOptions `json:"options"`
	OptionsHash          string              `json:"options_hash"`
	PlanHash             string              `json:"plan_hash"`
	ResultHash           string              `json:"result_hash"`
	NodeVRAMBytes        int64               `json:"node_vram_bytes"`
	AvailableVRAMBytes   int64               `json:"available_vram_bytes"`
	UsedVRAMBytes        int64               `json:"used_vram_bytes"`
	BatchCount           int                 `json:"batch_count"`
	Batches              []BatchPlan         `json:"batches"`
	DeferredCount        int                 `json:"deferred_request_count"`
	DeferredReasonCounts map[string]int      `json:"deferred_reason_counts"`
	Deferred             []DeferredRequest   `json:"deferred_requests"`
	MixRejections        []MixRejection      `json:"mix_rejections"`
	SelectedRequests     int                 `json:"selected_request_count"`
	TotalInputSamples    int64               `json:"total_input_samples"`
	TotalPlannedSamples  int64               `json:"total_planned_samples"`
	SampleConservationOK bool                `json:"sample_conservation_ok"`
	PrecisionOK          bool                `json:"precision_ok"`
	LatencyOK            bool                `json:"latency_ok"`
	FailureCode          string              `json:"failure_code,omitempty"`
	CreatedAt            string              `json:"created_at"`
	CompletedAt          string              `json:"completed_at,omitempty"`
}

type VRAMBudget struct {
	NodeVRAMBytes      int64 `json:"node_vram_bytes"`
	InjectedShortfall  int64 `json:"injected_shortfall_bytes"`
	AvailableVRAMBytes int64 `json:"available_vram_bytes"`
	BatchVRAMBytes     int64 `json:"batch_vram_bytes"`
	UsedThroughBatch   int64 `json:"used_through_batch_bytes"`
	RemainingAfter     int64 `json:"remaining_after_batch_bytes"`
}

type BatchEnvelope struct {
	Batch     BatchPlan  `json:"batch"`
	Budget    VRAMBudget `json:"vram_budget"`
	TraceID   string     `json:"trace_id"`
	Version   string     `json:"version"`
	RunStatus string     `json:"run_status"`
	Mode      string     `json:"mode"`
	ModeLabel string     `json:"mode_label"`
	NodeVRAM  int64      `json:"node_vram_bytes"`
}

type IntentFact struct {
	ID        int64  `json:"id"`
	Kind      string `json:"kind"`
	TraceID   string `json:"trace_id,omitempty"`
	RunID     string `json:"run_id,omitempty"`
	BatchID   string `json:"batch_id,omitempty"`
	State     string `json:"state"`
	CreatedAt string `json:"created_at"`
	Detail    string `json:"detail,omitempty"`
}

type RecoveryReport struct {
	ReportID         string       `json:"report_id"`
	CreatedAt        string       `json:"created_at"`
	Clean            bool         `json:"clean"`
	StartedIntents   []IntentFact `json:"started_intents"`
	AbortedRuns      []string     `json:"aborted_runs"`
	AbortedBatches   []string     `json:"aborted_batches"`
	PreservedBatches []string     `json:"preserved_complete_batches"`
	PersistedFacts   []string     `json:"persisted_facts"`
	CompletionClaim  string       `json:"completion_claim"`
	Conclusion       string       `json:"conclusion"`
}

type Comparison struct {
	TraceID    string           `json:"trace_id"`
	Version    string           `json:"version"`
	LeftRunID  string           `json:"left_run_id"`
	RightRunID string           `json:"right_run_id"`
	LeftMode   string           `json:"left_mode"`
	RightMode  string           `json:"right_mode"`
	LeftHash   string           `json:"left_result_hash"`
	RightHash  string           `json:"right_result_hash"`
	Pairs      []ComparisonPair `json:"batch_pairs"`
	Summary    map[string]any   `json:"summary"`
}

type ComparisonPair struct {
	Index           int      `json:"index"`
	LeftBatchID     string   `json:"left_batch_id,omitempty"`
	RightBatchID    string   `json:"right_batch_id,omitempty"`
	LeftSamples     int64    `json:"left_samples"`
	RightSamples    int64    `json:"right_samples"`
	LeftVRAMBytes   int64    `json:"left_vram_bytes"`
	RightVRAMBytes  int64    `json:"right_vram_bytes"`
	LeftLatencyUS   int64    `json:"left_latency_us"`
	RightLatencyUS  int64    `json:"right_latency_us"`
	LeftModels      []string `json:"left_models"`
	RightModels     []string `json:"right_models"`
	LeftStatus      string   `json:"left_status"`
	RightStatus     string   `json:"right_status"`
	SameComposition bool     `json:"same_composition"`
}

func (p *Plan) RecomputeSummary() {
	p.DeferredCount = len(p.Deferred)
	p.DeferredReasonCounts = make(map[string]int)
	for _, d := range p.Deferred {
		p.DeferredReasonCounts[d.Reason]++
	}
	p.TotalPlannedSamples = 0
	p.TotalVRAMBytes = 0
	p.PrecisionOK = true
	p.LatencyOK = true
	for i := range p.Batches {
		p.TotalPlannedSamples += p.Batches[i].TotalSamples
		p.TotalVRAMBytes += p.Batches[i].VRAMBytes
		p.PrecisionOK = p.PrecisionOK && p.Batches[i].Precision.PrecisionOK
		p.LatencyOK = p.LatencyOK && p.Batches[i].LatencyOK
		_ = i
	}
}

func (p *Plan) SummaryString() string {
	return fmt.Sprintf("mode=%s batches=%d selected=%d deferred=%d samples=%d precision_ok=%t latency_ok=%t",
		p.Mode, len(p.Batches), p.SelectedRequests, p.DeferredCount, p.TotalPlannedSamples, p.PrecisionOK, p.LatencyOK)
}
