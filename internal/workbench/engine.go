package workbench

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
)

type AdmissionPlan struct {
	Admitted      bool
	Code          string
	ReservedAfter int64
}

func PlanAdmissions(tasks []TaskSpec, cfg Config) map[string]AdmissionPlan {
	sorted := append([]TaskSpec(nil), tasks...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].TaskID < sorted[j].TaskID })
	plans := make(map[string]AdmissionPlan, len(tasks))
	var reserved int64
	for _, task := range sorted {
		plan := AdmissionPlan{Code: CodeOK, ReservedAfter: reserved}
		switch {
		case task.DeclaredBudget < 0 || task.DeclaredBudget > cfg.TaskMemoryBudget:
			plan.Code = CodeTaskMemory
		case reserved > cfg.GlobalMemoryBudget-task.DeclaredBudget:
			plan.Code = CodeGlobalBudget
		default:
			reserved += task.DeclaredBudget
			plan.Admitted = true
			plan.ReservedAfter = reserved
		}
		plans[task.TaskID] = plan
	}
	return plans
}

func RunWorkbench(ctx context.Context, dbPath string, opts RunOptions) (RunOutcome, error) {
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}
	store, err := OpenStore(dbPath)
	if err != nil {
		return RunOutcome{}, err
	}
	defer store.Close()
	if opts.Reset {
		if err := store.Reset(); err != nil {
			return RunOutcome{}, err
		}
	}
	load, err := store.Load(true)
	if err != nil {
		return RunOutcome{}, err
	}
	outcome := RunOutcome{WarningCode: load.WarningCode}
	var state State
	var chain []Snapshot
	var latestVersion int64
	if load.HasState {
		state = load.State
		chain = append([]Snapshot(nil), load.Valid...)
		latestVersion = load.LatestVersion()
		if opts.ConfigSet && opts.Config != state.Config {
			return RunOutcome{}, errors.New("existing workspace has a different configuration; use the edit command or --reset")
		}
	} else {
		cfg := DefaultConfig()
		if opts.ConfigSet {
			cfg = opts.Config
		}
		if err := cfg.Validate(); err != nil {
			return RunOutcome{}, err
		}
		tasks, err := GenerateTasks(cfg)
		if err != nil {
			return RunOutcome{}, err
		}
		state = NewState(cfg, tasks)
		snapshot, err := store.Commit(state, "policy", 1)
		if err != nil {
			return RunOutcome{}, err
		}
		state = snapshot.State
		chain = append(chain, snapshot)
		latestVersion = snapshot.Version
	}
	runConfig := state.Config

	plans := PlanAdmissions(state.Tasks, runConfig)
	results := make(map[string]TaskResult, len(state.Tasks))
	persisted := make(map[string]bool, len(state.Results))
	for _, result := range state.Results {
		results[result.TaskID] = result
		persisted[result.TaskID] = true
	}
	var jobs []TaskSpec
	for _, task := range state.SortedTasks() {
		if persisted[task.TaskID] {
			continue
		}
		plan := plans[task.TaskID]
		if !plan.Admitted {
			results[task.TaskID] = rejectedTaskResult(task, plan)
			continue
		}
		jobs = append(jobs, task)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	newTasks := 0
	stopAfterTriggered := false
	flush := func() error {
		if stopAfterTriggered {
			return nil
		}
		for _, task := range state.SortedTasks() {
			if persisted[task.TaskID] {
				continue
			}
			result, available := results[task.TaskID]
			if !available {
				break
			}
			nextVersion := latestVersion + 1
			result.SnapshotVersion = nextVersion
			state.PutTaskResult(result)
			snapshot, err := store.Commit(state, "task", nextVersion)
			if err != nil {
				return err
			}
			state = snapshot.State
			chain = append(chain, snapshot)
			latestVersion = nextVersion
			results[task.TaskID] = result
			persisted[task.TaskID] = true
			newTasks++
			if opts.StopAfter > 0 && newTasks >= opts.StopAfter {
				stopAfterTriggered = true
				cancel()
				return nil
			}
		}
		return nil
	}
	if err := flush(); err != nil {
		return RunOutcome{}, err
	}

	if len(jobs) > 0 && !stopAfterTriggered {
		jobQueue := make(chan TaskSpec, len(jobs))
		resultQueue := make(chan TaskResult, len(jobs))
		workerCount := opts.Concurrency
		if workerCount > len(jobs) {
			workerCount = len(jobs)
		}
		var wg sync.WaitGroup
		worker := func() {
			defer wg.Done()
			for {
				select {
				case <-runCtx.Done():
					return
				case task, ok := <-jobQueue:
					if !ok {
						return
					}
					select {
					case <-runCtx.Done():
						return
					default:
					}
					result := analyzeTask(task, runConfig, plans[task.TaskID])
					select {
					case resultQueue <- result:
					case <-runCtx.Done():
						return
					}
				}
			}
		}
		wg.Add(workerCount)
		for i := 0; i < workerCount; i++ {
			go worker()
		}
	producer:
		for _, task := range jobs {
			select {
			case jobQueue <- task:
			case <-runCtx.Done():
				break producer
			}
		}
		close(jobQueue)
		received := 0
	loop:
		for received < len(jobs) {
			select {
			case result := <-resultQueue:
				results[result.TaskID] = result
				received++
				if err := flush(); err != nil {
					cancel()
					wg.Wait()
					return RunOutcome{}, err
				}
				if stopAfterTriggered {
					break loop
				}
			case <-runCtx.Done():
				break loop
			}
		}
		cancel()
		wg.Wait()
	}
	if stopAfterTriggered || ctx.Err() != nil {
		if err := flush(); err != nil {
			return RunOutcome{}, err
		}
		outcome.Interrupted = true
	}

	finalLoad, err := store.Load(false)
	if err != nil {
		return RunOutcome{}, err
	}
	if finalLoad.WarningCode != "" {
		return RunOutcome{}, fmt.Errorf("%s: %s", finalLoad.WarningCode, finalLoad.WarningDetail)
	}
	outcome.State = finalLoad.State
	outcome.Chain = finalLoad.Valid
	outcome.NewTasks = newTasks
	outcome.Report = BuildReport(finalLoad.State, finalLoad.Valid)
	if ctx.Err() != nil && !stopAfterTriggered {
		return outcome, ctx.Err()
	}
	return outcome, nil
}

func rejectedTaskResult(task TaskSpec, plan AdmissionPlan) TaskResult {
	reason := "declared task budget exceeds the configured single-task budget"
	if plan.Code == CodeGlobalBudget {
		reason = "declared task budget cannot be admitted inside the remaining global budget"
	}
	return TaskResult{
		TaskID:              task.TaskID,
		SampleKind:          task.SampleKind,
		Description:         task.Description,
		DeclaredBudget:      task.DeclaredBudget,
		AdmissionCode:       plan.Code,
		Admitted:            false,
		DecodeCode:          CodeNotRun,
		DecisionCode:        plan.Code,
		FinalState:          FinalRejected,
		RejectionReason:     reason,
		GlobalReservedAfter: plan.ReservedAfter,
		Records:             []RecordSummary{},
		Issues:              []Issue{},
		Trajectory:          []TraceEvent{},
	}
}

func analyzeTask(task TaskSpec, cfg Config, plan AdmissionPlan) TaskResult {
	sample := BuildSample(task, cfg)
	digest := sha256.Sum256(sample)
	decoded := DecodeContainer(sample, DecodeLimits{MaxDepth: cfg.MaxDepth, NestedBudget: cfg.NestedBudget})
	result := TaskResult{
		TaskID:              task.TaskID,
		SampleKind:          task.SampleKind,
		Description:         task.Description,
		DeclaredBudget:      task.DeclaredBudget,
		AdmissionCode:       plan.Code,
		Admitted:            plan.Admitted,
		DecodeCode:          decoded.Code,
		DecisionCode:        decoded.Code,
		FinalState:          FinalOK,
		SampleDigest:        hex.EncodeToString(digest[:]),
		SampleBytes:         int64(len(sample)),
		PeakMemoryBytes:     decoded.PeakBytes,
		Allocations:         decoded.Allocations,
		GlobalReservedAfter: plan.ReservedAfter,
		Records:             append([]RecordSummary(nil), decoded.Records...),
		Failure:             decoded.Failure,
		Issues:              append([]Issue(nil), decoded.Issues...),
		Trajectory:          append([]TraceEvent(nil), decoded.Trajectory...),
	}
	if result.Records == nil {
		result.Records = []RecordSummary{}
	}
	if result.Issues == nil {
		result.Issues = []Issue{}
	}
	if result.Trajectory == nil {
		result.Trajectory = []TraceEvent{}
	}
	if decoded.PeakBytes > task.DeclaredBudget || decoded.PeakBytes > cfg.TaskMemoryBudget {
		result.DecisionCode = CodeTaskMemory
		result.FinalState = FinalBudgetExceeded
		result.RejectionReason = fmt.Sprintf("measured peak memory %d exceeds declared budget %d or task limit %d", decoded.PeakBytes, task.DeclaredBudget, cfg.TaskMemoryBudget)
		return result
	}
	if decoded.Code != CodeOK {
		result.FinalState = FinalDecodeFinding
	}
	return result
}

func EditWorkspace(dbPath string, patch Config) (ChainLoad, Snapshot, error) {
	store, err := OpenStore(dbPath)
	if err != nil {
		return ChainLoad{}, Snapshot{}, err
	}
	defer store.Close()
	load, err := store.Load(true)
	if err != nil {
		return ChainLoad{}, Snapshot{}, err
	}
	if !load.HasState {
		return ChainLoad{}, Snapshot{}, errors.New("workspace has no snapshot to edit")
	}
	if err := patch.Validate(); err != nil {
		return ChainLoad{}, Snapshot{}, err
	}
	tasks, err := GenerateTasks(patch)
	if err != nil {
		return ChainLoad{}, Snapshot{}, err
	}
	nextState := NewState(patch, tasks)
	snapshot, err := store.Commit(nextState, "policy-edit", load.LatestVersion()+1)
	if err != nil {
		return ChainLoad{}, Snapshot{}, err
	}
	load.Valid = append(load.Valid, snapshot)
	load.State = snapshot.State
	load.HasState = true
	load.WarningCode = ""
	load.WarningDetail = ""
	return load, snapshot, nil
}

func LoadWorkspace(dbPath string) (ChainLoad, error) {
	store, err := OpenStore(dbPath)
	if err != nil {
		return ChainLoad{}, err
	}
	defer store.Close()
	return store.Load(false)
}

func WorkspaceReport(dbPath string) (Report, ChainLoad, error) {
	load, err := LoadWorkspace(dbPath)
	if err != nil {
		return Report{}, ChainLoad{}, err
	}
	if !load.HasState {
		return Report{}, load, errors.New("workspace has no committed state")
	}
	return BuildReport(load.State, load.Valid), load, nil
}
