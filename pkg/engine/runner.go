// Package engine executes shards through calibration, forward,
// dequantization, and verification while persisting each committed stage.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"local/quantops/pkg/dataset"
	"local/quantops/pkg/qerr"
	"local/quantops/pkg/quant"
	"local/quantops/pkg/scheduler"
	"local/quantops/pkg/store"
)

type RunOptions struct {
	Limits       scheduler.Limits
	ShardIDs     []string
	StaleRetries int
	CommitHook   func(committed int)
}

type Result struct {
	Summary       Summary
	SummaryBytes  []byte
	NumericErrors []*qerr.Error
	BudgetErrors  []*qerr.Error
	Concurrency   int
}

type Runner struct {
	Dataset *dataset.Loaded
	Store   *store.Store
}

func New(ds *dataset.Loaded, st *store.Store) (*Runner, error) {
	if ds == nil || st == nil {
		return nil, qerr.New(qerr.CodeInvalidInput, "", "", "", "dataset and store are required", nil)
	}
	return &Runner{Dataset: ds, Store: st}, nil
}

type shardOutcome struct {
	shardID string
	summary *ShardSummary
	numeric *qerr.Error
	err     error
}

func (r *Runner) Run(ctx context.Context, opts RunOptions) (*Result, error) {
	if opts.Limits == (scheduler.Limits{}) {
		opts.Limits = scheduler.DefaultLimits()
	}
	if opts.StaleRetries <= 0 {
		opts.StaleRetries = 4
	}
	if err := opts.Limits.Validate(); err != nil {
		return nil, qerr.New(qerr.CodeInvalidInput, "", "", "", "invalid run budget", err)
	}
	if err := r.Store.SetManifestFingerprint(ctx, r.Dataset.Fingerprint); err != nil {
		return nil, err
	}

	shards := r.Dataset.Manifest.Shards
	if len(opts.ShardIDs) > 0 {
		allowed := make(map[string]bool, len(opts.ShardIDs))
		known := make(map[string]bool, len(r.Dataset.Manifest.Shards))
		for _, sh := range r.Dataset.Manifest.Shards {
			known[sh.ID] = true
		}
		for _, id := range opts.ShardIDs {
			if !known[id] {
				return nil, qerr.New(qerr.CodeInvalidInput, id, "", "", "unknown requested shard", nil)
			}
			allowed[id] = true
		}
		shards = nil
		for _, sh := range r.Dataset.Manifest.Shards {
			if allowed[sh.ID] {
				shards = append(shards, sh)
			}
		}
	}
	plan, err := scheduler.PlanBatches(shards, opts.Limits)
	if err != nil {
		return nil, err
	}
	rejections := make([]store.BudgetRejection, 0, len(plan.Rejected))
	for _, rj := range plan.Rejected {
		rejections = append(rejections, store.BudgetRejection{
			ShardID: rj.ShardID, ShardOrdinal: rj.ShardOrdinal,
			EstimatedElements: rj.EstimatedElements, WorkingSetBytes: rj.WorkingSetBytes,
			BudgetRemaining: rj.BudgetRemaining, MemoryRemaining: rj.MemoryRemaining,
			Reason: rj.Reason, Code: rj.Code,
		})
	}
	if err := r.Store.SaveBudgetRejections(ctx, rejections); err != nil {
		return nil, err
	}

	runtimes := make(map[string]*shardRuntime, len(shards))
	for _, sh := range shards {
		rt, err := prepareRuntime(r.Dataset, sh)
		if err != nil {
			return nil, qerr.New(qerr.CodeInvalidInput, sh.ID, "", "", "prepare shard", err)
		}
		runtimes[sh.ID] = rt
	}
	var committed atomic.Int64
	hook := func() {
		n := committed.Add(1)
		if opts.CommitHook != nil {
			opts.CommitHook(int(n))
		}
	}
	outcomes := make(chan shardOutcome, len(shards))
	var wg sync.WaitGroup
	for worker, queue := range plan.WorkerQueues {
		if len(queue) == 0 {
			continue
		}
		wg.Add(1)
		go func(worker int, queue []string) {
			defer wg.Done()
			for _, shardID := range queue {
				if err := ctx.Err(); err != nil {
					outcomes <- shardOutcome{shardID: shardID, err: err}
					return
				}
				out := r.runShardWithRetry(ctx, runtimes[shardID], opts.StaleRetries, hook)
				outcomes <- out
			}
		}(worker, queue)
	}
	wg.Wait()
	close(outcomes)

	byShard := make(map[string]shardOutcome, len(shards))
	var fatal []*qerr.Error
	for out := range outcomes {
		if out.shardID == "" && out.summary != nil {
			out.shardID = out.summary.ShardID
		}
		if out.err != nil {
			var be *qerr.Error
			if errors.As(out.err, &be) {
				fatal = append(fatal, be)
			} else {
				fatal = append(fatal, qerr.New(qerr.CodeInternal, out.shardID, "", "", "shard execution failed", out.err))
			}
		}
		if out.shardID != "" {
			byShard[out.shardID] = out
		}
	}

	res, runErr := r.finish(plan, byShard, fatal)
	if runErr != nil {
		return res, runErr
	}
	if len(fatal) > 0 {
		sort.Slice(fatal, func(i, j int) bool {
			if fatal[i].ShardID != fatal[j].ShardID {
				return fatal[i].ShardID < fatal[j].ShardID
			}
			return fatal[i].Stage < fatal[j].Stage
		})
		return res, &qerr.Aggregate{Errors: fatal}
	}
	// Numeric and budget outcomes are business failures, not process
	// failures: the deterministic summary is still complete and callers can
	// inspect Result before handling the aggregated error.
	if res != nil && (len(res.NumericErrors) > 0 || len(res.BudgetErrors) > 0) {
		errs := make([]*qerr.Error, 0, len(res.NumericErrors)+len(res.BudgetErrors))
		errs = append(errs, res.NumericErrors...)
		errs = append(errs, res.BudgetErrors...)
		return res, &qerr.Aggregate{Errors: errs}
	}
	return res, nil
}

func (r *Runner) runShardWithRetry(ctx context.Context, rt *shardRuntime, retries int, hook func()) shardOutcome {
	for attempt := 0; attempt <= retries; attempt++ {
		out := r.runShardOnce(ctx, rt, hook)
		out.shardID = rt.spec.ID
		if out.err == nil {
			return out
		}
		if qerr.IsCode(out.err, qerr.CodeStaleCheckpoint) && attempt < retries {
			time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
			continue
		}
		return out
	}
	return shardOutcome{shardID: rt.spec.ID, err: qerr.New(qerr.CodeInternal, rt.spec.ID, "", "", "retry loop exhausted", nil)}
}

func (r *Runner) runShardOnce(ctx context.Context, rt *shardRuntime, hook func()) shardOutcome {
	sh := rt.spec
	expected := expectedStageConfigs(rt)
	if _, err := r.Store.Reconcile(ctx, store.ReconcileRequest{
		ShardID: sh.ID, ShardOrdinal: sh.Ordinal,
		BaseInputFingerprint: rt.contentFP, Expected: expected,
	}); err != nil {
		return shardOutcome{err: err}
	}
	progress, err := r.Store.Progress(ctx, sh.ID)
	if err != nil {
		return shardOutcome{err: err}
	}
	states, err := r.Store.States(ctx, sh.ID)
	if err != nil {
		return shardOutcome{err: err}
	}
	artifacts, err := decodeArtifacts(states)
	if err != nil {
		return shardOutcome{err: qerr.Wrap(qerr.CodeStaleCheckpoint, sh.ID, "", "", "decode committed artifacts", err)}
	}
	nextSeq := len(states) + 1
	if progress != nil {
		nextSeq = progress.NextStageSeq
	}
	version := ""
	if progress != nil {
		version = progress.VersionFingerprint
	}

	for nextSeq <= len(store.Stages()) {
		stage, ok := store.StageAt(nextSeq)
		if !ok {
			break
		}
		payload, err := r.computeStage(rt, stage, artifacts)
		if err != nil {
			return shardOutcome{err: err}
		}
		inputFP := rt.contentFP
		if nextSeq > 1 {
			inputFP = states[nextSeq-2].StageDigest
		}
		published, err := r.Store.Publish(ctx, store.PublishRequest{
			ShardID: sh.ID, ShardOrdinal: sh.Ordinal, Stage: stage, StageSeq: nextSeq,
			ExpectedVersion: version, ConfigVersion: rt.stageConfigs[nextSeq-1],
			InputFingerprint: inputFP, Payload: payload,
		})
		if err != nil {
			return shardOutcome{err: err}
		}
		states = append(states, published.State)
		version = published.Progress.VersionFingerprint
		progress = &published.Progress
		if hook != nil {
			hook()
		}
		nextSeq++
	}
	if progress == nil || progress.NextStageSeq != len(store.Stages())+1 || len(states) != len(store.Stages()) {
		return shardOutcome{err: qerr.New(qerr.CodeStaleCheckpoint, sh.ID, "", "",
			"shard did not reach a complete checkpoint chain", nil)}
	}
	return r.buildOutcome(ctx, rt, states, progress)
}

func (r *Runner) computeStage(rt *shardRuntime, stage store.Stage, artifacts *artifactSet) ([]byte, error) {
	switch stage {
	case store.Calibration:
		cal, err := computeCalibration(rt)
		if err != nil {
			return nil, err
		}
		artifacts.cal = cal
		return encodeArtifact(cal)
	case store.Forward:
		if artifacts.cal == nil {
			return nil, qerr.New(qerr.CodeMissingArtifact, rt.spec.ID, string(stage), "",
				"calibration artifact is missing", nil)
		}
		fwd, err := computeForward(rt, artifacts.cal)
		if err != nil {
			return nil, err
		}
		artifacts.fwd = fwd
		return encodeArtifact(fwd)
	case store.Dequantization:
		if artifacts.fwd == nil {
			return nil, qerr.New(qerr.CodeMissingArtifact, rt.spec.ID, string(stage), "",
				"forward artifact is missing", nil)
		}
		deq, err := computeDequant(rt, artifacts.fwd)
		if err != nil {
			return nil, err
		}
		artifacts.deq = deq
		return encodeArtifact(deq)
	case store.Verification:
		if artifacts.deq == nil {
			return nil, qerr.New(qerr.CodeMissingArtifact, rt.spec.ID, string(stage), "",
				"dequantization artifact is missing", nil)
		}
		ver, err := computeVerify(rt, artifacts.deq)
		if err != nil {
			return nil, err
		}
		artifacts.verify = ver
		return encodeArtifact(ver)
	default:
		return nil, qerr.New(qerr.CodeInvalidTransition, rt.spec.ID, string(stage), "", "unknown stage", nil)
	}
}

func computeCalibration(rt *shardRuntime) (*CalibrationArtifact, error) {
	inputParams, err := quant.Calibrate(rt.activation, rt.spec.Chain[0].Activation)
	if err != nil {
		return nil, qerr.New(qerr.CodeInvalidInput, rt.spec.ID, string(store.Calibration), "", "calibrate activation", err)
	}
	cal := &CalibrationArtifact{
		ContentFingerprint: rt.contentFP,
		InputParams:        inputParams,
		NonFiniteInputs:    append([]NonFiniteEvent(nil), rt.nonFinite...),
	}
	for i, layer := range rt.spec.Chain {
		params, err := quant.Calibrate(rt.weights[i], layer.Weight)
		if err != nil {
			return nil, qerr.New(qerr.CodeInvalidInput, rt.spec.ID, string(store.Calibration), layer.Name, "calibrate weight", err)
		}
		cal.WeightParams = append(cal.WeightParams, params)
	}
	return cal, nil
}

func computeForward(rt *shardRuntime, cal *CalibrationArtifact) (*ForwardArtifact, error) {
	out := &ForwardArtifact{}
	cur := rt.activation.Clone()
	for i, layer := range rt.spec.Chain {
		var aParams []quant.Params
		if i == 0 {
			aParams = cal.InputParams
		} else {
			params, err := quant.Calibrate(cur, layer.Activation)
			if err != nil {
				return nil, qerr.New(qerr.CodeInvalidInput, rt.spec.ID, string(store.Forward), layer.Name,
					"calibrate intermediate activation", err)
			}
			aParams = params
		}
		if i >= len(cal.WeightParams) {
			return nil, qerr.New(qerr.CodeMissingArtifact, rt.spec.ID, string(store.Forward), layer.Name,
				"calibrated weight parameters are missing", nil)
		}
		res, err := quant.LinearWithParams(cur, rt.weights[i], layer.Activation, layer.Weight, aParams, cal.WeightParams[i])
		if err != nil {
			return nil, qerr.New(qerr.CodeInvalidInput, rt.spec.ID, string(store.Forward), layer.Name,
				"quantized linear operator failed", err)
		}
		next, err := quant.DequantizeLinear(res)
		if err != nil {
			return nil, qerr.New(qerr.CodeInvalidInput, rt.spec.ID, string(store.Forward), layer.Name,
				"forward activation dequantization failed", err)
		}
		out.Layers = append(out.Layers, ForwardLayerArtifact{
			Index: i, Name: layer.Name,
			InputShape: res.InputShape, WeightShape: res.WeightShape, OutputShape: res.OutputShape,
			Accum: res.Accum, AScales: res.AScales, AZero: res.AZero,
			WScales: res.WScales, WZero: res.WZero,
			InputParams: res.InputParams, WeightParams: res.WeightParams,
			InputClamps: res.InputClamps, WeightClamps: res.WeightClamps,
			InputClampCount: len(res.InputClamps), WeightClampCount: len(res.WeightClamps),
		})
		cur = next
	}
	if !equalInts(cur.Shape, rt.spec.ExpectedOutputShape) {
		return nil, qerr.New(qerr.CodeInvalidInput, rt.spec.ID, string(store.Forward), "",
			"forward output shape differs from manifest", nil)
	}
	return out, nil
}

func computeDequant(rt *shardRuntime, fwd *ForwardArtifact) (*DequantArtifact, error) {
	out := &DequantArtifact{}
	for _, layer := range fwd.Layers {
		res := quant.LinearResult{
			InputShape: layer.InputShape, WeightShape: layer.WeightShape, OutputShape: layer.OutputShape,
			Accum: layer.Accum, AScales: layer.AScales, AZero: layer.AZero,
			WScales: layer.WScales, WZero: layer.WZero,
		}
		t, err := quant.DequantizeLinear(res)
		if err != nil {
			return nil, qerr.New(qerr.CodeInvalidInput, rt.spec.ID, string(store.Dequantization), layer.Name,
				"dequantize linear output", err)
		}
		out.Layers = append(out.Layers, DequantLayerArtifact{Index: layer.Index, Name: layer.Name, Shape: t.Shape, Values: t.Values})
	}
	return out, nil
}

func computeVerify(rt *shardRuntime, deq *DequantArtifact) (*VerifyArtifact, error) {
	if len(deq.Layers) == 0 {
		return nil, qerr.New(qerr.CodeMissingArtifact, rt.spec.ID, string(store.Verification), "",
			"no dequantized layer outputs", nil)
	}
	final := deq.Layers[len(deq.Layers)-1]
	if !equalInts(final.Shape, rt.spec.ExpectedOutputShape) {
		return nil, qerr.New(qerr.CodeInvalidInput, rt.spec.ID, string(store.Verification), final.Name,
			"dequantized output shape differs from manifest", nil)
	}
	report, err := buildDiagnostics(rt.spec, rt.activation, rt.weights, *deq, rt.nonFinite)
	if err != nil {
		return nil, err
	}
	return &VerifyArtifact{Report: report}, nil
}

func (r *Runner) buildOutcome(ctx context.Context, rt *shardRuntime, states []store.State, progress *store.Progress) shardOutcome {
	arts, err := decodeArtifacts(states)
	if err != nil {
		return shardOutcome{err: err}
	}
	if arts.deq == nil || arts.verify == nil || arts.fwd == nil || arts.cal == nil {
		return shardOutcome{err: qerr.New(qerr.CodeMissingArtifact, rt.spec.ID, "", "", "stage artifacts incomplete", nil)}
	}
	summary := buildShardSummary(rt, states, progress, arts)
	out := shardOutcome{summary: &summary}
	if !arts.verify.Report.Passed {
		be := numericMismatchError(rt.spec.ID, arts.verify.Report)
		out.numeric = be
	}
	return out
}

func (r *Runner) finish(plan *scheduler.Plan, byShard map[string]shardOutcome, fatal []*qerr.Error) (*Result, error) {
	result := &Result{Concurrency: plan.Concurrency}
	shardSummaries := []ShardSummary{}
	numericErrs := []*qerr.Error{}
	for _, as := range plan.Accepted {
		out, ok := byShard[as.ShardID]
		if ok && out.summary != nil {
			shardSummaries = append(shardSummaries, *out.summary)
			if out.numeric != nil {
				numericErrs = append(numericErrs, out.numeric)
			}
		}
	}
	sort.Slice(shardSummaries, func(i, j int) bool {
		if shardSummaries[i].ShardOrdinal != shardSummaries[j].ShardOrdinal {
			return shardSummaries[i].ShardOrdinal < shardSummaries[j].ShardOrdinal
		}
		return shardSummaries[i].ShardID < shardSummaries[j].ShardID
	})
	sort.Slice(numericErrs, func(i, j int) bool { return numericErrs[i].ShardID < numericErrs[j].ShardID })
	for _, rj := range plan.Rejected {
		result.BudgetErrors = append(result.BudgetErrors, budgetError(rj))
	}
	status := "ok"
	if len(fatal) > 0 {
		status = "failed"
	} else if len(numericErrs) > 0 {
		status = "numeric_mismatch"
	} else if len(result.BudgetErrors) > 0 {
		status = "budget_limited"
	}
	result.NumericErrors = numericErrs
	summary := Summary{
		SchemaVersion:       SummarySchemaVersion,
		ManifestFingerprint: r.Dataset.Fingerprint,
		Limits:              plan.Limits,
		Scheduler:           *plan,
		Shards:              shardSummaries,
		Untreated:           plan.Rejected,
		Status:              status,
	}
	aggregate, err := summaryAggregate(summary)
	if err != nil {
		return nil, err
	}
	summary.AggregateDigest = aggregate
	result.Summary = summary
	b, err := marshalSummary(summary)
	if err != nil {
		return nil, err
	}
	result.SummaryBytes = b
	return result, nil
}

func budgetError(r scheduler.Rejected) *qerr.Error {
	e := qerr.New(qerr.CodeBudgetExceeded, r.ShardID, "", "",
		"shard not admitted by deterministic budget scheduler", nil)
	e.Details = mustJSON(map[string]any{
		"estimated_elements": r.EstimatedElements,
		"working_set_bytes":  r.WorkingSetBytes,
		"budget_remaining":   r.BudgetRemaining,
		"memory_remaining":   r.MemoryRemaining,
		"reason":             r.Reason,
	})
	return e
}

func decodeArtifacts(states []store.State) (*artifactSet, error) {
	arts := &artifactSet{}
	for _, st := range states {
		switch st.Stage {
		case store.Calibration:
			var v CalibrationArtifact
			if err := json.Unmarshal(st.Payload, &v); err != nil {
				return nil, err
			}
			arts.cal = &v
		case store.Forward:
			var v ForwardArtifact
			if err := json.Unmarshal(st.Payload, &v); err != nil {
				return nil, err
			}
			arts.fwd = &v
		case store.Dequantization:
			var v DequantArtifact
			if err := json.Unmarshal(st.Payload, &v); err != nil {
				return nil, err
			}
			arts.deq = &v
		case store.Verification:
			var v VerifyArtifact
			if err := json.Unmarshal(st.Payload, &v); err != nil {
				return nil, err
			}
			arts.verify = &v
		}
	}
	return arts, nil
}

type artifactSet struct {
	cal    *CalibrationArtifact
	fwd    *ForwardArtifact
	deq    *DequantArtifact
	verify *VerifyArtifact
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
