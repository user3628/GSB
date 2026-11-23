package workbench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"internalpki/workbench/internal/checkpoint"
	"internalpki/workbench/internal/engine"
	"internalpki/workbench/internal/model"
	"internalpki/workbench/internal/sampleio"
	"internalpki/workbench/internal/scheduler"
)

type Options struct {
	SamplesDir      string
	CheckpointPath  string
	Concurrency     int
	SampleCPUBudget time.Duration
	GlobalCPUBudget time.Duration
	MemoryBudget    int64
	ValidationTime  time.Time
}

func DefaultOptions() Options {
	at, _ := time.Parse(time.RFC3339, model.DefaultValidationAt)
	return Options{
		Concurrency:     4,
		SampleCPUBudget: 5 * time.Millisecond,
		GlobalCPUBudget: 500 * time.Millisecond,
		MemoryBudget:    128 * 1024 * 1024,
		ValidationTime:  at,
	}
}

type taskKind int

const (
	taskRun taskKind = iota
	taskSampleBudget
	taskGlobalBudget
)

type plannedTask struct {
	sample *sampleio.Sample
	cost   engine.Cost
	kind   taskKind
}

// Run loads all samples, applies deterministic admission control, executes
// admitted work through a bounded worker pool, and returns a canonical report.
func Run(ctx context.Context, options Options) (model.Report, error) {
	if options.SamplesDir == "" {
		return model.Report{}, errors.New("samples directory is required")
	}
	if options.Concurrency < 1 {
		return model.Report{}, errors.New("concurrency must be at least 1")
	}
	if options.SampleCPUBudget <= 0 {
		return model.Report{}, errors.New("sample CPU budget must be positive")
	}
	if options.GlobalCPUBudget <= 0 {
		return model.Report{}, errors.New("global CPU budget must be positive")
	}
	if options.MemoryBudget <= 0 {
		return model.Report{}, errors.New("memory budget must be positive")
	}
	if options.ValidationTime.IsZero() {
		options.ValidationTime = DefaultOptions().ValidationTime
	}

	paths, err := sampleio.Discover(options.SamplesDir)
	if err != nil {
		return model.Report{}, err
	}
	samples := make([]*sampleio.Sample, 0, len(paths))
	digests := make(map[string]string, len(paths))
	for _, path := range paths {
		sample := sampleio.Load(options.SamplesDir, path)
		samples = append(samples, sample)
		digests[path] = sample.InputDigest
	}

	store, diagnostics, err := checkpoint.Open(options.CheckpointPath, digests)
	if err != nil {
		return model.Report{}, err
	}
	restored := store.TerminalResults(digests)
	results := make([]model.SampleResult, 0, len(samples))
	consumedCPU, consumedMemory := consumedResources(restored)

	remaining := make([]*sampleio.Sample, 0, len(samples))
	for _, sample := range samples {
		if result, ok := restored[sample.Path]; ok {
			results = append(results, result)
			continue
		}
		remaining = append(remaining, sample)
	}

	sampleBudgetUnits := durationUnits(options.SampleCPUBudget)
	globalBudgetUnits := durationUnits(options.GlobalCPUBudget)
	tasks := planTasks(remaining, consumedCPU, consumedMemory, sampleBudgetUnits, globalBudgetUnits, options.MemoryBudget)

	jobs := make([]scheduler.Job, 0, len(tasks))
	for _, planned := range tasks {
		task := planned
		jobs = append(jobs, scheduler.Job{
			Path:  task.sample.Path,
			Depth: task.sample.EffectiveDepth(),
			Execute: func(ctx context.Context, queuePosition int, waitTicks int64) (model.SampleResult, error) {
				return executeTask(ctx, store, task, options.ValidationTime, sampleBudgetUnits, globalBudgetUnits, options.MemoryBudget, queuePosition, waitTicks)
			},
		})
	}
	executed, err := scheduler.Run(ctx, jobs, options.Concurrency)
	if err != nil {
		return model.Report{}, err
	}
	results = append(results, executed...)

	report := model.Report{
		SchemaVersion: model.SchemaVersion,
		SampleMarker:  model.FixedSampleMarker,
		Samples:       results,
		Diagnostics:   diagnostics,
	}
	report.Finalize()
	if !report.Summary.FullyAccounted {
		report.Diagnostics = append(report.Diagnostics, model.Diagnostic{
			Code:    model.CodeReportNotFullyAccounted,
			Message: "completed, failed, and degraded counts do not equal the sample total",
		})
		return report, errors.New("report accounting invariant failed")
	}
	return report, nil
}

func planTasks(samples []*sampleio.Sample, consumedCPU, consumedMemory, sampleBudgetUnits, globalBudgetUnits, memoryBudget int64) []plannedTask {
	tasks := make([]plannedTask, 0, len(samples))
	for _, sample := range samples {
		tasks = append(tasks, plannedTask{
			sample: sample,
			cost:   engine.EstimateCost(sample.EffectiveDepth()),
		})
	}
	sort.SliceStable(tasks, func(i, j int) bool {
		left, right := tasks[i], tasks[j]
		if left.sample.EffectiveDepth() == right.sample.EffectiveDepth() {
			return left.sample.Path < right.sample.Path
		}
		return left.sample.EffectiveDepth() < right.sample.EffectiveDepth()
	})

	usedCPU, usedMemory := consumedCPU, consumedMemory
	for i := range tasks {
		switch {
		case tasks[i].cost.CPUUnits > sampleBudgetUnits:
			tasks[i].kind = taskSampleBudget
		case usedCPU+tasks[i].cost.CPUUnits > globalBudgetUnits || usedMemory+tasks[i].cost.MemoryBytes > memoryBudget:
			for j := i; j < len(tasks); j++ {
				tasks[j].kind = taskGlobalBudget
			}
			return tasks
		default:
			tasks[i].kind = taskRun
			usedCPU += tasks[i].cost.CPUUnits
			usedMemory += tasks[i].cost.MemoryBytes
		}
	}
	return tasks
}

func executeTask(
	ctx context.Context,
	store *checkpoint.Store,
	task plannedTask,
	at time.Time,
	sampleBudgetUnits, globalBudgetUnits, memoryBudget int64,
	queuePosition int,
	waitTicks int64,
) (model.SampleResult, error) {
	if err := ctx.Err(); err != nil {
		return model.SampleResult{}, err
	}
	digest := task.sample.InputDigest
	var result model.SampleResult
	switch task.kind {
	case taskSampleBudget:
		result = engine.BudgetResult(
			task.sample,
			model.CodeSampleBudgetExceeded,
			"sample_cpu_budget",
			fmt.Sprintf("<= %d CPU units", sampleBudgetUnits),
			fmt.Sprintf("%d CPU units", task.cost.CPUUnits),
			"sample exceeded its per-sample CPU budget; reserved resources were released",
			model.StateBudgetTerminated,
			model.OutcomeFailed,
		)
		result.Metrics = model.BudgetMetrics{
			QueuePosition: queuePosition,
			WaitTicks:     waitTicks,
			CPUUnits:      0,
			MemoryBytes:   0,
			CPUCharged:    false,
			MemoryCharged: false,
		}
		if err := store.Commit(task.sample.Path, digest, model.StateBudgetTerminated, result); err != nil {
			return model.SampleResult{}, err
		}
		return result, nil
	case taskGlobalBudget:
		result = engine.BudgetResult(
			task.sample,
			model.CodeGlobalBudgetExhausted,
			"global_budget",
			fmt.Sprintf("admit within global CPU=%d units and memory=%d bytes", globalBudgetUnits, memoryBudget),
			fmt.Sprintf("estimated cpu=%d units memory=%d bytes", task.cost.CPUUnits, task.cost.MemoryBytes),
			"deeper sample degraded after shallower samples consumed the remaining global budget",
			model.StateBudgetTerminated,
			model.OutcomeDegraded,
		)
		result.Metrics = model.BudgetMetrics{
			QueuePosition: queuePosition,
			WaitTicks:     waitTicks,
			CPUUnits:      0,
			MemoryBytes:   0,
			CPUCharged:    false,
			MemoryCharged: false,
		}
		if err := store.Commit(task.sample.Path, digest, model.StateBudgetTerminated, result); err != nil {
			return model.SampleResult{}, err
		}
		return result, nil
	default:
		if err := store.MarkVerifying(task.sample.Path, digest); err != nil {
			return model.SampleResult{}, err
		}
		result = engine.Evaluate(task.sample, at)
		result.Metrics = model.BudgetMetrics{
			QueuePosition: queuePosition,
			WaitTicks:     waitTicks,
			CPUUnits:      task.cost.CPUUnits,
			MemoryBytes:   task.cost.MemoryBytes,
			CPUCharged:    true,
			MemoryCharged: true,
		}
		state := model.StateCompleted
		if result.Outcome != model.OutcomePassed {
			state = model.StateFailed
		}
		if err := store.Commit(task.sample.Path, digest, state, result); err != nil {
			return model.SampleResult{}, err
		}
		return result, nil
	}
}

func consumedResources(restored map[string]model.SampleResult) (int64, int64) {
	var cpu, memory int64
	for _, result := range restored {
		if result.Metrics.CPUCharged {
			cpu += result.Metrics.CPUUnits
		}
		if result.Metrics.MemoryCharged {
			memory += result.Metrics.MemoryBytes
		}
	}
	return cpu, memory
}

func durationUnits(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	units := int64(duration / time.Microsecond)
	if duration%time.Microsecond != 0 {
		units++
	}
	return units
}

// MarshalReport emits canonical JSON with one trailing newline. The data model
// contains no maps, so encoding/json preserves the same field order every run.
func MarshalReport(report model.Report) ([]byte, error) {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode report: %w", err)
	}
	return append(data, '\n'), nil
}
