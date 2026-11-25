package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"driftengine/internal/batch"
	"driftengine/internal/drift"
	"driftengine/internal/engine"
	"driftengine/internal/errs"
	"driftengine/internal/fixednum"
	"driftengine/internal/loopendpoint"
	"driftengine/internal/sample"
	"driftengine/internal/spec"
	"driftengine/internal/store"
)

type Options struct {
	Seed       int64
	WorkDir    string
	ExePath    string
	ReportPath string
}

type Report struct {
	OK        bool      `json:"ok"`
	Seed      int64     `json:"seed"`
	WorkDir   string    `json:"work_dir"`
	ExePath   string    `json:"exe_path"`
	Checks    []Check   `json:"checks"`
	StartedAt time.Time `json:"started_at"`
}

type Check struct {
	Name       string `json:"name"`
	OK         bool   `json:"ok"`
	DurationMS int64  `json:"duration_ms"`
	Detail     any    `json:"detail"`
	Error      string `json:"error,omitempty"`
}

func Run(ctx context.Context, opts Options) (*Report, error) {
	if opts.Seed == 0 {
		opts.Seed = sample.FixedSeed
	}
	if opts.WorkDir == "" {
		opts.WorkDir = filepath.Join(".verify-work", fmt.Sprint(time.Now().UnixNano()))
	}
	if opts.ExePath == "" {
		opts.ExePath = currentExe()
	}
	report := &Report{OK: true, Seed: opts.Seed, WorkDir: opts.WorkDir, ExePath: opts.ExePath, StartedAt: time.Now().UTC()}

	checks := []struct {
		name string
		fn   func(context.Context, Options) (any, error)
	}{
		{"drift_categories_and_inclusive_boundary", checkDrift},
		{"dependency_batches_and_cycle_outside_layers", checkBatches},
		{"deterministic_seed_and_large_graph", checkLarge},
		{"real_loopback_write_and_readback_convergence", checkLoopbackConvergence},
		{"concurrent_increments_optimistic_base", checkConcurrentIncrements},
		{"invalid_spec_preserves_committed_version", checkInvalidSpec},
		{"write_queue_full_timeout_releases_slot", checkQueueTimeout},
		{"readback_mismatch_retry_exhausted", checkRetryExhausted},
		{"forced_kill_sqlite_recovery", checkForcedKill},
	}
	for _, candidate := range checks {
		start := time.Now()
		detail, err := candidate.fn(ctx, opts)
		check := Check{Name: candidate.name, DurationMS: time.Since(start).Milliseconds(), Detail: detail}
		if err != nil {
			check.OK = false
			check.Error = err.Error()
			if apiErr := errs.AsError(err); apiErr != nil {
				check.Error = apiErr.Code + ": " + apiErr.Message
				if detail == nil {
					detail = apiErr.Details
					check.Detail = detail
				}
			}
			report.OK = false
		} else {
			check.OK = true
		}
		report.Checks = append(report.Checks, check)
	}
	return report, nil
}

func currentExe() string {
	if exe, err := executablePath(); err == nil {
		return exe
	}
	return "drift-engine"
}

func checkDrift(_ context.Context, _ Options) (any, error) {
	snapshot, err := sample.StandardSnapshot()
	if err != nil {
		return nil, err
	}
	reports := drift.EvaluateAll(snapshot.Objects, snapshot.Policy)
	multi := reports["obj-multi-drift"]
	expected := []string{spec.CategorySourceConflict, spec.CategoryFieldMissing, spec.CategoryFieldExtra, spec.CategoryValueOffset}
	if multi.PrimaryCategory != spec.CategorySourceConflict || len(multi.Categories) != 4 {
		return nil, fmt.Errorf("multi drift categories=%v primary=%s", multi.Categories, multi.PrimaryCategory)
	}
	for i, category := range expected {
		if multi.Categories[i] != category {
			return nil, fmt.Errorf("category priority mismatch: got %v", multi.Categories)
		}
	}
	missing := reports["obj-missing"]
	extra := reports["obj-extra"]
	value := reports["obj-value-offset"]
	source := reports["obj-source-conflict"]
	if missing.PrimaryCategory != spec.CategoryFieldMissing || extra.PrimaryCategory != spec.CategoryFieldExtra ||
		value.PrimaryCategory != spec.CategoryValueOffset || source.PrimaryCategory != spec.CategorySourceConflict {
		return nil, fmt.Errorf("primary categories mismatch: missing=%s extra=%s value=%s source=%s",
			missing.PrimaryCategory, extra.PrimaryCategory, value.PrimaryCategory, source.PrimaryCategory)
	}
	tolerance, _ := fixednum.Parse("1/100")
	result := fixednum.RelativeWithin(fixednum.Rat(202, 1), fixednum.Rat(200, 1), tolerance, true)
	if !result.Within {
		return nil, fmt.Errorf("inclusive boundary should be within: %+v", result)
	}
	exclusive := fixednum.RelativeWithin(fixednum.Rat(202, 1), fixednum.Rat(200, 1), tolerance, false)
	if exclusive.Within {
		return nil, fmt.Errorf("exclusive boundary should drift")
	}
	return map[string]any{
		"reports":          len(reports),
		"multi_categories": multi.Categories,
		"boundary_rule":    "|actual-desired| <= tolerance*|desired|; equality is inside for inclusive policy",
	}, nil
}

func checkBatches(_ context.Context, _ Options) (any, error) {
	standard, err := sample.StandardSnapshot()
	if err != nil {
		return nil, err
	}
	plan, err := batch.Build(standard.Version, standard.Objects, standard.Policy)
	if err != nil {
		return nil, err
	}
	if plan.Layers != 4 || len(plan.Batches) != 15 {
		return nil, fmt.Errorf("standard plan layers=%d batches=%d", plan.Layers, len(plan.Batches))
	}
	cycleSnapshot, err := sample.CycleSnapshot()
	if err != nil {
		return nil, err
	}
	cyclePlan, cycleErr := batch.Build(cycleSnapshot.Version, cycleSnapshot.Objects, cycleSnapshot.Policy)
	if cycleErr == nil || errs.Code(cycleErr) != errs.DependencyCycle {
		return nil, fmt.Errorf("expected E_DEPENDENCY_CYCLE, got %v", cycleErr)
	}
	if cyclePlan == nil || len(cyclePlan.CycleIDs) != 3 || len(cyclePlan.Batches) != 15 {
		return nil, fmt.Errorf("cycle outside plan invalid: %+v", cyclePlan)
	}
	return map[string]any{
		"layers":           plan.Layers,
		"standard_batches": len(plan.Batches),
		"cycle_ids":        cyclePlan.CycleIDs,
		"outside_batches":  len(cyclePlan.Batches),
	}, nil
}

func checkLarge(_ context.Context, _ Options) (any, error) {
	standard, err := sample.StandardSnapshot()
	if err != nil {
		return nil, err
	}
	large, err := sample.LargeSnapshot()
	if err != nil {
		return nil, err
	}
	start := time.Now()
	plan, planErr := batch.Build(large.Version, large.Objects, large.Policy)
	planMS := time.Since(start).Milliseconds()
	if planErr != nil {
		return nil, planErr
	}
	canonicalA, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}
	plan2, err := batch.Build(large.Version, large.Objects, large.Policy)
	if err != nil {
		return nil, err
	}
	canonicalB, _ := json.Marshal(plan2)
	if string(canonicalA) != string(canonicalB) {
		return nil, fmt.Errorf("large graph plan is not deterministic")
	}
	return map[string]any{
		"large_objects":    len(large.Objects),
		"large_batches":    len(plan.Batches),
		"plan_duration_ms": planMS,
		"digest":           standard.Digest,
	}, nil
}

func openStore(path string) (*store.Store, error) {
	return store.Open(path)
}

var _ = runtime.GOOS
var _ = engine.PhaseConverged
var _ = loopendpoint.ObjectState{}
