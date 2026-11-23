package scheduler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"localinfer/workbench/internal/domain"
)

type Planner struct{}

func NewPlanner() *Planner { return &Planner{} }

func (p *Planner) BuildPlan(runID string, trace *domain.Trace, catalog *domain.ModelCatalog, opts domain.OrganizationOptions) (*domain.Plan, error) {
	if trace == nil || catalog == nil {
		return nil, domain.NewBizError(domain.CodeInternal, "trace and catalog are required", nil)
	}
	normalized, err := domain.NormalizeOptions(opts)
	if err != nil {
		return nil, err
	}
	opts = normalized
	lateSet := make(map[string]struct{}, len(opts.Faults.LateRequestIDs))
	for _, id := range opts.Faults.LateRequestIDs {
		if _, ok := trace.RequestByID(id); !ok {
			return nil, domain.NewBizError(domain.CodeTraceInvalid, "late_request_id is not present in trace", map[string]any{"request_id": id})
		}
		lateSet[id] = struct{}{}
	}
	if opts.Faults.DuplicateRequestID != "" {
		if _, ok := trace.RequestByID(opts.Faults.DuplicateRequestID); !ok {
			return nil, domain.NewBizError(domain.CodeTraceInvalid, "duplicate_request_id is not present in trace", map[string]any{"request_id": opts.Faults.DuplicateRequestID})
		}
		return nil, domain.NewBizError(domain.CodeConflicting, "request already exists in the persisted trace", map[string]any{
			"request_id": opts.Faults.DuplicateRequestID, "trace_id": trace.TraceID, "version": trace.Version,
		})
	}

	available := opts.AvailableVRAM()
	cutoff := int64(0)
	if opts.Mode == domain.ModeFast && len(trace.Requests) > 0 {
		var err error
		cutoff, err = checkedAdd(trace.Requests[0].ArrivalMS, opts.OrganizationBudgetMS)
		if err != nil {
			return nil, domain.NewBizError(domain.CodeBadRequest, "fast-mode organization cutoff overflows int64", map[string]any{
				"first_arrival_ms": trace.Requests[0].ArrivalMS, "organization_budget_ms": opts.OrganizationBudgetMS,
			})
		}
	}
	plan := &domain.Plan{
		Mode: opts.Mode, Options: opts, MixRejections: []domain.MixRejection{}, Deferred: []domain.DeferredRequest{},
	}
	if opts.Mode == domain.ModeFast {
		plan.ModeLabel = "low_latency"
	} else {
		plan.ModeLabel = "full"
	}
	selected := make([]domain.TraceRequest, 0, len(trace.Requests))
	var selectedVRAM int64
	var selectedSamples int64
	for _, req := range trace.Requests {
		plan.TotalInputSamples += req.Samples
		if opts.Mode == domain.ModeFast && req.ArrivalMS > cutoff {
			plan.Deferred = append(plan.Deferred, domain.DeferredRequest{
				RequestID: req.RequestID, ModelID: req.ModelID, Reason: "ORGANIZATION_BUDGET_EXCEEDED",
				Detail: fmt.Sprintf("arrival_ms=%d exceeds fast-mode cutoff=%d", req.ArrivalMS, cutoff),
			})
			continue
		}
		if _, isLate := lateSet[req.RequestID]; isLate && opts.LatePolicy == domain.LatePolicyReject {
			plan.Deferred = append(plan.Deferred, domain.DeferredRequest{
				RequestID: req.RequestID, ModelID: req.ModelID, Reason: "LATE_AFTER_SEAL",
				Detail: "late_policy=reject; existing sealed batches are immutable",
			})
			continue
		}
		model := catalog.ByID[req.ModelID]
		reqVRAM, err := checkedMul(req.Samples, model.VRAMPerSample)
		if err != nil {
			return nil, domain.NewBizError(domain.CodeInsufficientCap, "requested VRAM overflows int64", map[string]any{"request_id": req.RequestID})
		}
		selectedVRAM, err = checkedAdd(selectedVRAM, reqVRAM)
		if err != nil {
			return nil, domain.NewBizError(domain.CodeInsufficientCap, "total requested VRAM overflows int64", nil)
		}
		selectedSamples += req.Samples
		selected = append(selected, req)
	}
	plan.SelectedRequests = len(selected)
	if selectedVRAM > available {
		return nil, domain.NewBizError(domain.CodeInsufficientCap, "selected requests exceed available node VRAM; no batch was committed", map[string]any{
			"available_vram_bytes": available, "requested_vram_bytes": selectedVRAM,
			"shortfall_bytes": selectedVRAM - available, "node_vram_bytes": opts.NodeVRAMBytes,
			"vram_shortfall_bytes": opts.Faults.VramShortfallBytes, "planning_preserved": true,
		})
	}
	if opts.OrganizationOrder == "priority" {
		sort.SliceStable(selected, func(i, j int) bool {
			if selected[i].Priority != selected[j].Priority {
				return selected[i].Priority > selected[j].Priority
			}
			if selected[i].ArrivalMS != selected[j].ArrivalMS {
				return selected[i].ArrivalMS < selected[j].ArrivalMS
			}
			return selected[i].Sequence < selected[j].Sequence
		})
	}

	builder := &planBuilder{
		runID: runID, catalog: catalog, opts: opts, plan: plan, availableVRAM: available,
	}
	for _, req := range selected {
		_, isLate := lateSet[req.RequestID]
		if err := builder.addRequest(req, isLate); err != nil {
			return nil, err
		}
	}
	builder.closeCurrent("END_OF_TRACE", "end of selected arrival window")
	if builder.err != nil {
		return nil, builder.err
	}
	if err := builder.finalize(); err != nil {
		return nil, err
	}

	plan.RecomputeSummary()
	if plan.TotalPlannedSamples != selectedSamples {
		return nil, domain.NewBizError(domain.CodeInternal, "sample conservation check failed before commit", map[string]any{
			"selected_samples": selectedSamples, "planned_samples": plan.TotalPlannedSamples,
		})
	}
	plan.SampleConservationOK = true
	hash, err := planDigest(plan)
	if err != nil {
		return nil, err
	}
	plan.PlanHash = hash
	return plan, nil
}

type planBuilder struct {
	runID         string
	catalog       *domain.ModelCatalog
	opts          domain.OrganizationOptions
	plan          *domain.Plan
	availableVRAM int64
	usedVRAM      int64
	rootSeq       int
	batchSeq      int
	current       *openBatch
	pendingSplit  *splitOrigin
	err           error
}

type splitOrigin struct {
	rootSeq int
	index   int
	reason  string
	detail  string
}

type openBatch struct {
	batch       *domain.BatchPlan
	modelCounts map[string]int64
	latency     compensatedInt
	vram        int64
	lateSealed  bool
}

func (b *planBuilder) currentIsLate() bool {
	return b.current != nil && b.current.lateSealed
}

func (b *planBuilder) hasModel(modelID string) bool {
	return b.current != nil && b.current.modelCounts[modelID] > 0
}

func (b *planBuilder) startBatch(req domain.TraceRequest, reason, detail string, late bool) {
	var rootSeq, splitIndex int
	if b.pendingSplit != nil {
		rootSeq = b.pendingSplit.rootSeq
		splitIndex = b.currentSplitIndex(rootSeq) + 1
		if b.pendingSplit.index >= 0 {
			splitIndex = b.pendingSplit.index
		}
	} else {
		b.rootSeq++
		rootSeq = b.rootSeq
		splitIndex = 0
	}
	b.batchSeq++
	batch := &domain.BatchPlan{PlannedBatch: domain.PlannedBatch{
		BatchID: fmt.Sprintf("%s:b%04d", b.runID, b.batchSeq), RunID: b.runID,
		Sequence: b.batchSeq, RootSequence: rootSeq, SplitIndex: splitIndex,
		SplitReason: reason, SplitDetail: detail, Status: "planned", Mode: b.opts.Mode,
		Priority: req.Priority, Models: []string{}, Members: []domain.PlannedMember{},
		LatencyLimitUS: req.LatencyLimitUS, Sealed: true, LateSealed: late,
		SealReason: sealReasonFor(reason, b.opts.Mode),
	}}
	b.current = &openBatch{batch: batch, modelCounts: map[string]int64{}, lateSealed: late}
	b.pendingSplit = nil
}

func (b *planBuilder) currentSplitIndex(rootSeq int) int {
	max := -1
	for _, batch := range b.plan.Batches {
		if batch.RootSequence == rootSeq && batch.SplitIndex > max {
			max = batch.SplitIndex
		}
	}
	return max
}

func (b *planBuilder) closeCurrent(reason, detail string) {
	if b == nil || b.current == nil || b.current.batch.TotalSamples == 0 {
		return
	}
	latency, compensation, err := b.current.latency.Result()
	if err != nil {
		b.err = domain.NewBizError(domain.CodePrecisionBreach, "batch latency sum overflows int64", map[string]any{"batch_id": b.current.batch.BatchID})
		b.current = nil
		return
	}
	b.current.batch.LatencyUS = latency
	b.current.batch.CompensationUS = compensation
	b.current.batch.LatencyOK = latency <= b.current.batch.LatencyLimitUS
	if !b.current.batch.LatencyOK {
		b.current.batch.Status = "latency_failed"
	}
	if reason != "" && reason != "END_OF_TRACE" && reason != "END_OF_WINDOW" {
		b.current.batch.SealReason = reason + ": " + detail
	}
	b.plan.Batches = append(b.plan.Batches, *b.current.batch)
	b.current = nil
}

func (b *planBuilder) finalize() error {
	cumulative := int64(0)
	for i := range b.plan.Batches {
		batch := &b.plan.Batches[i]
		sort.SliceStable(batch.Members, func(i, j int) bool { return batch.Members[i].Sequence < batch.Members[j].Sequence })
		var check compensatedInt
		for _, m := range batch.Members {
			model := b.catalog.ByID[m.ModelID]
			term, err := checkedMul(m.AllocatedSamples, model.CostPerSample)
			if err != nil {
				return domain.NewBizError(domain.CodePrecisionBreach, "batch latency overflows int64", map[string]any{"batch_id": batch.BatchID})
			}
			check.Add(term)
		}
		sum, comp, err := check.Result()
		if err != nil {
			return domain.NewBizError(domain.CodePrecisionBreach, "batch latency overflows int64", map[string]any{"batch_id": batch.BatchID})
		}
		batch.LatencyUS = sum
		batch.CompensationUS = comp
		batch.LatencyOK = sum <= batch.LatencyLimitUS
		batch.Quantiles = computeQuantiles(batch.PlannedBatch, b.catalog, b.opts.LatencyDecimals)
		if err := applyPrecision(batch, b.catalog, b.opts, &cumulative); err != nil {
			return err
		}
		if !batch.Precision.PrecisionOK {
			batch.Status = "precision_failed"
		} else if !batch.LatencyOK {
			batch.Status = "latency_failed"
		} else {
			batch.Status = "complete"
		}
	}
	return nil
}

func sortedModels(counts map[string]int64) []string {
	models := make([]string, 0, len(counts))
	for model, count := range counts {
		if count > 0 {
			models = append(models, model)
		}
	}
	sort.Strings(models)
	return models
}

func joinHits(hits []string) string {
	if len(hits) == 0 {
		return ""
	}
	seen := make(map[string]struct{}, len(hits))
	out := make([]string, 0, len(hits))
	for _, h := range hits {
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}
	return strings.Join(out, "+")
}

func sealReasonFor(reason, mode string) string {
	switch reason {
	case "LATE_AFTER_SEAL_NEW_BATCH":
		return "late-after-seal: isolated immutable batch"
	case "PRIORITY_CHANGE":
		return "priority boundary: no cross-priority mixing"
	case "MODEL_ISOLATION":
		return "model boundary: mixed-model batching not declared"
	}
	if mode == domain.ModeFast {
		return "fast mode organization budget / capacity boundary"
	}
	return "full mode capacity boundary"
}

func planDigest(plan *domain.Plan) (string, error) {
	b, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
