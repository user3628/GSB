package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"protocolfuzzlab/internal/codes"
	"protocolfuzzlab/internal/demo"
	"protocolfuzzlab/internal/model"
	"protocolfuzzlab/internal/replay"
	"protocolfuzzlab/internal/store"
)

const (
	Format       = "protocol-fuzzlab.verification/v1"
	SampleMarker = store.SampleMarker
)

var ScenarioOrder = []string{
	"missing-seed",
	"operator-cycle",
	"bad-checksum",
	"snapshot-gap",
	"concurrent-race",
	"forced-restart",
}

type Report struct {
	Format    string           `json:"format"`
	SampledAt string           `json:"sampled_at"`
	Scenarios []ScenarioResult `json:"scenarios"`
}

type ScenarioResult struct {
	Scenario             string   `json:"scenario"`
	Passed               bool     `json:"passed"`
	Code                 string   `json:"code"`
	ExpectedCode         string   `json:"expected_code"`
	CaseSetVersionBefore int      `json:"caseset_version_before"`
	CaseSetVersionAfter  int      `json:"caseset_version_after"`
	SnapshotCountBefore  int      `json:"snapshot_count_before"`
	SnapshotCountAfter   int      `json:"snapshot_count_after"`
	MaxSnapshotBefore    int      `json:"max_snapshot_before"`
	MaxSnapshotAfter     int      `json:"max_snapshot_after"`
	PeakInFlight         int      `json:"peak_in_flight,omitempty"`
	PeakQueued           int      `json:"peak_queued,omitempty"`
	Details              []string `json:"details"`
}

func Run(ctx context.Context, exe string, selected []string) (Report, error) {
	selected, err := selectScenarios(selected)
	if err != nil {
		return Report{}, err
	}
	root, err := os.MkdirTemp("", "protocol-fuzzlab-verify-*")
	if err != nil {
		return Report{}, err
	}
	defer os.RemoveAll(root)

	report := Report{Format: Format, SampledAt: SampleMarker}
	for _, scenario := range selected {
		path := filepath.Join(root, scenario+".db")
		var result ScenarioResult
		switch scenario {
		case "missing-seed":
			result = rejectChange(path, scenario, demo.MissingSeedChange(), codes.DepInvalidated)
		case "operator-cycle":
			result = rejectChange(path, scenario, demo.OperatorCycleChange(), codes.DepCycle)
		case "bad-checksum":
			result = verifyBadChecksum(ctx, path, scenario)
		case "snapshot-gap":
			result = verifySnapshotGap(ctx, path, scenario)
		case "concurrent-race":
			result = verifyConcurrency(ctx, path, scenario)
		case "forced-restart":
			if exe == "" {
				result = ScenarioResult{Scenario: scenario, ExpectedCode: codes.OK, Code: codes.BadRequest, Details: []string{"executable path unavailable"}}
				break
			}
			result = verifyForcedRestart(ctx, exe, path, scenario)
		}
		if !result.Passed && len(result.Details) == 0 {
			result.Details = []string{"scenario did not satisfy assertions"}
		}
		sort.Strings(result.Details)
		report.Scenarios = append(report.Scenarios, result)
	}
	return report, nil
}

func Marshal(r Report) ([]byte, error) {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

func selectScenarios(selected []string) ([]string, error) {
	enabled := map[string]bool{}
	for _, item := range selected {
		for _, part := range strings.Split(item, ",") {
			part = strings.TrimSpace(part)
			if part == "" || part == "all" {
				for _, scenario := range ScenarioOrder {
					enabled[scenario] = true
				}
				continue
			}
			found := false
			for _, scenario := range ScenarioOrder {
				if part == scenario {
					found = true
					enabled[scenario] = true
				}
			}
			if !found {
				return nil, fmt.Errorf("unknown injection scenario %q", part)
			}
		}
	}
	if len(enabled) == 0 {
		for _, scenario := range ScenarioOrder {
			enabled[scenario] = true
		}
	}
	var out []string
	for _, scenario := range ScenarioOrder {
		if enabled[scenario] {
			out = append(out, scenario)
		}
	}
	return out, nil
}

func initDemo(path string) (*store.Store, error) {
	st, err := store.Open(path)
	if err != nil {
		return nil, err
	}
	if _, _, err := st.Initialize(demo.BaseCaseSet(), "verification baseline"); err != nil {
		st.Close()
		return nil, err
	}
	return st, nil
}

func snapshotState(st *store.Store) (count, max int, err error) {
	count, err = st.SnapshotCount()
	if err != nil {
		return
	}
	max, err = st.MaxSnapshotVersion()
	return
}

func rejectChange(path, scenario string, changeSet model.ChangeSet, expected string) ScenarioResult {
	result := ScenarioResult{Scenario: scenario, ExpectedCode: expected}
	st, err := initDemo(path)
	if err != nil {
		result.Code = codes.BadRequest
		result.Details = []string{err.Error()}
		return result
	}
	defer st.Close()
	headBefore, err := st.Head()
	if err != nil {
		result.Code = codes.BadRequest
		result.Details = []string{err.Error()}
		return result
	}
	beforeCount, beforeMax, err := snapshotState(st)
	if err != nil {
		result.Code = codes.BadRequest
		result.Details = []string{err.Error()}
		return result
	}
	result.CaseSetVersionBefore = headBefore.Version
	result.SnapshotCountBefore = beforeCount
	result.MaxSnapshotBefore = beforeMax

	_, applyErr := st.ApplyChangeSet(changeSet)
	code := codes.OK
	if applyErr != nil {
		code = codes.From(applyErr).Code
	}
	result.Code = code
	headAfter, headErr := st.Head()
	if headErr != nil {
		result.Details = append(result.Details, headErr.Error())
	}
	afterCount, afterMax, stateErr := snapshotState(st)
	if stateErr != nil {
		result.Details = append(result.Details, stateErr.Error())
	}
	result.CaseSetVersionAfter = headAfter.Version
	result.SnapshotCountAfter = afterCount
	result.MaxSnapshotAfter = afterMax
	result.Passed = code == expected &&
		result.CaseSetVersionAfter == result.CaseSetVersionBefore &&
		result.MaxSnapshotAfter == result.MaxSnapshotBefore &&
		result.SnapshotCountAfter == result.SnapshotCountBefore
	if code == expected {
		result.Details = append(result.Details, "expected business code observed")
	}
	if result.CaseSetVersionAfter == result.CaseSetVersionBefore {
		result.Details = append(result.Details, "case set version remained unchanged")
	}
	if result.MaxSnapshotAfter == result.MaxSnapshotBefore {
		result.Details = append(result.Details, "snapshot version remained unchanged")
	}
	return result
}

func verifyBadChecksum(ctx context.Context, path, scenario string) ScenarioResult {
	result := ScenarioResult{Scenario: scenario, ExpectedCode: codes.BadChecksum}
	st, err := initDemo(path)
	if err != nil {
		result.Code = codes.BadRequest
		result.Details = []string{err.Error()}
		return result
	}
	head, _ := st.Head()
	beforeCount, beforeMax, _ := snapshotState(st)
	result.CaseSetVersionBefore = head.Version
	result.SnapshotCountBefore = beforeCount
	result.MaxSnapshotBefore = beforeMax
	runResult, err := replay.Run(ctx, st, replay.Options{
		CaseSetVersion: head.Version, BatchSize: 100, MaxConcurrency: 1, CaseBudget: 1000,
		NoCommit: true, TamperCaseID: "case-basic", TamperStep: 0,
	})
	code := codes.OK
	if err != nil {
		code = codes.From(err).Code
	} else {
		code = batchCaseCode(runResult, "case-basic")
	}
	headAfter, _ := st.Head()
	afterCount, afterMax, _ := snapshotState(st)
	_ = st.Close()
	result.Code = code
	result.CaseSetVersionAfter = headAfter.Version
	result.SnapshotCountAfter = afterCount
	result.MaxSnapshotAfter = afterMax
	result.Passed = code == codes.BadChecksum && result.CaseSetVersionAfter == result.CaseSetVersionBefore && afterMax == beforeMax && afterCount == beforeCount
	if code == codes.BadChecksum {
		result.Details = append(result.Details, "tampered checksum produced E_BAD_CHECKSUM")
	}
	if afterMax == beforeMax {
		result.Details = append(result.Details, "dry-run injection did not commit a snapshot")
	}
	return result
}

func verifySnapshotGap(ctx context.Context, path, scenario string) ScenarioResult {
	result := ScenarioResult{Scenario: scenario, ExpectedCode: codes.SnapshotVersion}
	st, err := initDemo(path)
	if err != nil {
		result.Code = codes.BadRequest
		result.Details = []string{err.Error()}
		return result
	}
	head, _ := st.Head()
	result.CaseSetVersionBefore = head.Version
	initial, err := replay.Run(ctx, st, replay.Options{CaseSetVersion: head.Version, BatchSize: 1, MaxConcurrency: 1})
	if err != nil {
		result.Code = codes.From(err).Code
		result.Details = []string{err.Error()}
		st.Close()
		return result
	}
	if len(initial.Batches) != 3 {
		result.Code = codes.BadRequest
		result.Details = []string{fmt.Sprintf("expected 3 deterministic batches, got %d", len(initial.Batches))}
		st.Close()
		return result
	}
	countBefore, maxBefore, _ := snapshotState(st)
	result.SnapshotCountBefore = countBefore
	result.MaxSnapshotBefore = maxBefore
	if err := st.InjectSnapshotGap(2); err != nil {
		result.Code = codes.From(err).Code
		result.Details = []string{err.Error()}
		st.Close()
		return result
	}
	chain, chainErr := st.SnapshotChain()
	code := codes.OK
	if chainErr != nil {
		code = codes.From(chainErr).Code
	}
	// A fresh replay key with different batching must be rejected because the
	// physical version chain is no longer contiguous.
	_, replayErr := replay.Run(ctx, st, replay.Options{CaseSetVersion: head.Version, BatchSize: 2, MaxConcurrency: 1, CaseBudget: 1})
	if replayErr != nil {
		if coded := codes.From(replayErr); coded.Code == codes.SnapshotVersion {
			code = coded.Code
		}
	}
	headAfter, _ := st.Head()
	afterCount, afterMax, _ := snapshotState(st)
	_ = st.Close()
	result.Code = code
	result.CaseSetVersionAfter = headAfter.Version
	result.SnapshotCountAfter = afterCount
	result.MaxSnapshotAfter = afterMax
	result.Passed = code == codes.SnapshotVersion && len(chain) == 1 && result.CaseSetVersionAfter == result.CaseSetVersionBefore && result.MaxSnapshotAfter == result.MaxSnapshotBefore
	if chainErr != nil {
		result.Details = append(result.Details, "chain loader returned E_SNAPSHOT_VERSION")
	}
	if len(chain) == 1 {
		result.Details = append(result.Details, "loader fell back to the latest contiguous version 1")
	}
	if result.MaxSnapshotAfter == result.MaxSnapshotBefore {
		result.Details = append(result.Details, "failed commit did not advance the maximum snapshot version")
	}
	return result
}

func verifyConcurrency(ctx context.Context, path, scenario string) ScenarioResult {
	result := ScenarioResult{Scenario: scenario, ExpectedCode: codes.OK}
	st, err := initDemo(path)
	if err != nil {
		result.Code = codes.BadRequest
		result.Details = []string{err.Error()}
		return result
	}
	head, _ := st.Head()
	beforeCount, beforeMax, _ := snapshotState(st)
	result.CaseSetVersionBefore = head.Version
	result.SnapshotCountBefore = beforeCount
	result.MaxSnapshotBefore = beforeMax
	runResult, err := replay.Run(ctx, st, replay.Options{
		CaseSetVersion: head.Version, BatchSize: 1, MaxConcurrency: 2, CaseBudget: 1000,
	})
	code := codes.OK
	if err != nil {
		code = codes.From(err).Code
	} else {
		for _, batch := range runResult.Batches {
			if batch.Execution.Code != codes.OK {
				code = batch.Execution.Code
				break
			}
		}
	}
	headAfter, _ := st.Head()
	afterCount, afterMax, _ := snapshotState(st)
	chain, chainErr := st.SnapshotChain()
	_ = st.Close()
	result.Code = code
	result.CaseSetVersionAfter = headAfter.Version
	result.SnapshotCountAfter = afterCount
	result.MaxSnapshotAfter = afterMax
	result.PeakInFlight = runResult.Stats.PeakInFlight
	result.PeakQueued = runResult.Stats.PeakQueued
	contiguous := chainErr == nil && len(chain) == 3
	result.Passed = code == codes.OK && headAfter.Version == head.Version &&
		afterCount == beforeCount+3 && afterMax == beforeMax+3 &&
		runResult.Stats.PeakInFlight <= 2 && runResult.Stats.PeakQueued >= 1 && contiguous
	if runResult.Stats.PeakInFlight <= 2 {
		result.Details = append(result.Details, "in-flight sessions never exceeded concurrency limit 2")
	}
	if runResult.Stats.PeakQueued >= 1 {
		result.Details = append(result.Details, "excess batch waited in the scheduler queue")
	}
	if contiguous {
		result.Details = append(result.Details, "three batch snapshots committed exactly once with a contiguous chain")
	}
	return result
}

func verifyForcedRestart(ctx context.Context, exe, path, scenario string) ScenarioResult {
	result := ScenarioResult{Scenario: scenario, ExpectedCode: codes.OK}
	st, err := initDemo(path)
	if err != nil {
		result.Code = codes.BadRequest
		result.Details = []string{err.Error()}
		return result
	}
	head, _ := st.Head()
	beforeCount, beforeMax, _ := snapshotState(st)
	result.CaseSetVersionBefore = head.Version
	result.SnapshotCountBefore = beforeCount
	result.MaxSnapshotBefore = beforeMax
	if err := st.Close(); err != nil {
		result.Code = codes.BadRequest
		result.Details = []string{err.Error()}
		return result
	}
	crashCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(crashCtx, exe, "replay", "--db", path, "--batch-size", "1", "--concurrency", "1", "--crash-after-batches", "1")
	crashErr := cmd.Run()
	exitCode := 0
	if crashErr != nil {
		if coded, ok := crashErr.(*exec.ExitError); ok {
			exitCode = coded.ExitCode()
		} else {
			result.Code = codes.BadRequest
			result.Details = []string{crashErr.Error()}
			return result
		}
	}
	st, err = store.Open(path)
	if err != nil {
		result.Code = codes.BadRequest
		result.Details = []string{err.Error()}
		return result
	}
	midCount, midMax, _ := snapshotState(st)
	midChain, midChainErr := st.SnapshotChain()
	headMid, _ := st.Head()
	_ = st.Close()
	if midCount != 1 || midMax != 1 || midChainErr != nil || len(midChain) != 1 || headMid.Version != head.Version {
		result.Code = codes.SnapshotVersion
		result.Details = []string{fmt.Sprintf("post-crash state count=%d max=%d chain=%d head=%d", midCount, midMax, len(midChain), headMid.Version)}
		return result
	}

	resumeCtx, resumeCancel := context.WithTimeout(ctx, 20*time.Second)
	defer resumeCancel()
	resumeCmd := exec.CommandContext(resumeCtx, exe, "replay", "--db", path, "--batch-size", "1", "--concurrency", "1")
	if out, err := resumeCmd.CombinedOutput(); err != nil {
		result.Code = codes.BadRequest
		result.Details = []string{fmt.Sprintf("resume failed: %v: %s", err, strings.TrimSpace(string(out)))}
		return result
	}
	st, err = store.Open(path)
	if err != nil {
		result.Code = codes.BadRequest
		result.Details = []string{err.Error()}
		return result
	}
	headAfter, _ := st.Head()
	afterCount, afterMax, _ := snapshotState(st)
	chain, chainErr := st.SnapshotChain()
	_ = st.Close()
	result.Code = codes.OK
	if exitCode != 99 {
		result.Code = codes.BadRequest
		result.Details = append(result.Details, fmt.Sprintf("crash subprocess exit code %d, expected 99", exitCode))
	} else {
		result.Details = append(result.Details, "child process terminated after the first committed batch")
	}
	result.CaseSetVersionAfter = headAfter.Version
	result.SnapshotCountAfter = afterCount
	result.MaxSnapshotAfter = afterMax
	result.Passed = result.Code == codes.OK && headAfter.Version == head.Version &&
		afterCount == beforeCount+3 && afterMax == beforeMax+3 && chainErr == nil && len(chain) == 3 &&
		exitCode == 99
	if result.Passed {
		result.Details = append(result.Details, "committed batch survived and the two uncommitted batches were replayed")
	}
	return result
}

func batchCaseCode(runResult replay.RunResult, caseID string) string {
	for _, batch := range runResult.Batches {
		for _, c := range batch.Execution.Cases {
			if c.CaseID == caseID {
				return c.Code
			}
		}
	}
	return codes.BadRequest
}
