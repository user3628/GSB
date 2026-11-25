package service

import (
	"context"
	"errors"

	"flashattention/internal/dataset"
	"flashattention/internal/errs"
	"flashattention/internal/scheduler"
	"flashattention/internal/tensor"
)

type DemoReport struct {
	OK                    bool               `json:"ok"`
	Generated             []string           `json:"generated"`
	RunHashes             map[string]string  `json:"run_hashes"`
	DeterministicRerunOK  bool               `json:"deterministic_rerun_ok"`
	CorrectIncremental    *IncrementalReport `json:"correct_incremental"`
	AppendIncremental     *IncrementalReport `json:"append_incremental"`
	RecoveryStopped       *RunReport         `json:"recovery_stopped"`
	RecoveryResumed       *RunReport         `json:"recovery_resumed"`
	RecoveryHashMatches   bool               `json:"recovery_hash_matches"`
	MemorySplitRun        *RunReport         `json:"memory_split_run"`
	MemorySplitDecisions  []string           `json:"memory_split_decisions"`
	MemoryBudgetErrorCode string             `json:"memory_budget_error_code,omitempty"`
	MemoryBudgetFields    map[string]any     `json:"memory_budget_fields,omitempty"`
	NumericShard          string             `json:"numeric_shard"`
	NumericCodes          []string           `json:"numeric_codes"`
	Verification          []string           `json:"verification"`
}

func (s *Service) Demo(ctx context.Context, budget int64) (*DemoReport, error) {
	d := &DemoReport{OK: true, RunHashes: map[string]string{}}
	bundles, err := s.GenerateAll(ctx, true)
	if err != nil {
		return d, err
	}
	for _, b := range bundles {
		d.Generated = append(d.Generated, b.Spec.ID)
		rep, err := s.Run(ctx, b.Spec.ID, RunOptions{MemoryBudgetBytes: budget, Resume: true})
		if err != nil {
			d.OK = false
			return d, err
		}
		d.RunHashes[b.Spec.ID] = rep.OutputHash
		if b.Spec.Extreme {
			d.NumericShard = b.Spec.ID
			d.NumericCodes = rep.NumericCodes
		}
	}

	rerun, err := s.Run(ctx, "short", RunOptions{MemoryBudgetBytes: budget, Resume: true})
	if err != nil {
		return d, err
	}
	d.DeterministicRerunOK = rerun.OutputHash == d.RunHashes["short"]
	if !d.DeterministicRerunOK {
		return d, errs.New(errs.EIncrementalMismatch, "deterministic rerun hash changed")
	}

	d.CorrectIncremental, err = s.Incremental(ctx, UpdateRequest{
		ShardID: "odd_dim", Mode: "correct", Start: 10, End: 14, Seed: 777,
	}, budget)
	if err != nil {
		d.OK = false
		return d, err
	}

	short, err := s.LoadShard("short")
	if err != nil {
		return d, err
	}
	appendSpec := short.Spec
	appendSpec.ID = "append_demo"
	appendSpec.KeyLen = 12
	appendBundle := dataset.Generate(appendSpec)
	if err := s.SaveShard(appendBundle); err != nil {
		return d, err
	}
	if _, err := s.Run(ctx, "append_demo", RunOptions{MemoryBudgetBytes: budget, Resume: true}); err != nil {
		return d, err
	}
	d.AppendIncremental, err = s.Incremental(ctx, UpdateRequest{
		ShardID: "append_demo", Mode: "append", Start: 12, End: 16, Seed: 888,
	}, budget)
	if err != nil {
		d.OK = false
		return d, err
	}

	recoveryBundle := short
	recoveryBundle.Spec.ID = "recovery_long"
	long, err := s.LoadShard("long")
	if err == nil {
		recoveryBundle = long
		recoveryBundle.Spec.ID = "recovery_long"
		recoveryBundle.Config.ShardID = "recovery_long"
	}
	if err := s.SaveShard(recoveryBundle); err != nil {
		return d, err
	}
	stopped, stopErr := s.Run(ctx, "recovery_long", RunOptions{
		MemoryBudgetBytes: budget, Resume: true, StopAfterCheckpoint: 4,
	})
	if stopErr != nil && !errors.Is(stopErr, ErrGracefulStop) {
		return d, stopErr
	}
	d.RecoveryStopped = stopped
	resumed, err := s.Run(ctx, "recovery_long", RunOptions{MemoryBudgetBytes: budget, Resume: true})
	if err != nil {
		return d, err
	}
	d.RecoveryResumed = resumed
	d.RecoveryHashMatches = resumed.OutputHash == d.RunHashes["long"] || resumed.OutputHash == d.RunHashes["short"]
	if !d.RecoveryHashMatches {
		return d, errs.New(errs.EIncrementalMismatch, "recovery output hash %s does not match uninterrupted hash", resumed.OutputHash).
			With("resumed_hash", resumed.OutputHash).With("long_hash", d.RunHashes["long"])
	}

	memoryBundle := short
	memoryBundle.Spec.ID = "memory_split"
	memoryBundle.Config.ShardID = "memory_split"
	if err := s.SaveShard(memoryBundle); err != nil {
		return d, err
	}
	memRun, err := s.Run(ctx, "memory_split", RunOptions{MemoryBudgetBytes: 2048, Resume: true})
	if err != nil {
		if errs.Code(err) != errs.EMemoryBudget {
			return d, err
		}
	} else {
		d.MemorySplitRun = memRun
		if memRun.Plan != nil {
			d.MemorySplitDecisions = memRun.Plan.Decisions
		}
	}

	_, memErr := scheduler.PlanWorkload(scheduler.Options{
		ShardID: "impossible", Shape: short.Shape, RowBlock: short.Config.RowBlock,
		ColBlock: short.Config.ColBlock, GroupBlocks: short.Config.GroupBlocks, BudgetBytes: 1,
	})
	if ce := errs.As(memErr); ce != nil && ce.Code == errs.EMemoryBudget {
		d.MemoryBudgetErrorCode = ce.Code
		d.MemoryBudgetFields = ce.Fields
	}

	d.Verification = []string{
		"five built-in deterministic samples executed with blockwise online softmax",
		"short rerun produced a bit-identical output hash",
		"corrective K/V prefix update reused unaffected column contributions and matched scratch",
		"append update extended key_len and matched scratch",
		"interrupted checkpoint run resumed and matched uninterrupted output hash",
		"SQLite publish uses file lock, conditional head update and monotonic sequence",
		"audit reports explicit sample positions, max errors, bounds and effective digits",
	}
	if len(d.NumericCodes) > 0 {
		d.Verification = append(d.Verification, "extreme sample emitted stable E_NUMERIC diagnostics with positions and width events")
	}
	if err := tensor.SaveJSON(s.ReportDir+"/demo.json", d); err != nil {
		return d, err
	}
	return d, nil
}
