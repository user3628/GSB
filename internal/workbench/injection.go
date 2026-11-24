package workbench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

func RunInjections(baseDir string) (InjectionReport, error) {
	return RunInjection(baseDir, "all")
}

func RunInjection(baseDir, kind string) (InjectionReport, error) {
	if baseDir == "" {
		baseDir = os.TempDir()
	}
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return InjectionReport{}, err
	}
	allCases := []string{
		InjectionLengthWrap,
		InjectionDeepNesting,
		InjectionTaskBudget,
		InjectionGlobalBudget,
		InjectionSnapshotGap,
	}
	selected := make([]string, 0, len(allCases))
	switch kind {
	case "", "all":
		selected = allCases
	default:
		for _, candidate := range allCases {
			if candidate == normalizeInjectionKind(kind) {
				selected = append(selected, candidate)
			}
		}
		if len(selected) == 0 {
			return InjectionReport{}, fmt.Errorf("unknown injection kind %q", kind)
		}
	}
	report := InjectionReport{Format: InjectionFormat, SamplingMarker: SamplingMarker, Cases: []InjectionCase{}}
	for _, kind := range selected {
		var result InjectionCase
		var err error
		if kind == InjectionSnapshotGap {
			result, err = runSnapshotGapInjection(baseDir)
		} else {
			result, err = runDataInjection(baseDir, kind)
		}
		if err != nil {
			return InjectionReport{}, err
		}
		result.Kind = kind
		report.Cases = append(report.Cases, result)
	}
	sort.Slice(report.Cases, func(i, j int) bool { return report.Cases[i].Kind < report.Cases[j].Kind })
	report.AllPassed = true
	for _, item := range report.Cases {
		if !item.Passed {
			report.AllPassed = false
		}
	}
	return report, nil
}

func normalizeInjectionKind(kind string) string {
	switch kind {
	case "length-wrap", "length_wrap":
		return InjectionLengthWrap
	case "deep-nesting", "deep_nesting":
		return InjectionDeepNesting
	case "task-budget", "task_budget":
		return InjectionTaskBudget
	case "global-budget", "global_budget":
		return InjectionGlobalBudget
	case "snapshot-gap", "snapshot_gap":
		return InjectionSnapshotGap
	default:
		return kind
	}
}

func runDataInjection(baseDir, kind string) (InjectionCase, error) {
	cfg, expected := injectionConfig(kind)
	dir, err := os.MkdirTemp(baseDir, "recordlab-inject-*")
	if err != nil {
		return InjectionCase{}, err
	}
	defer os.RemoveAll(dir)
	injectedDB := filepath.Join(dir, "injected.db")
	controlDB := filepath.Join(dir, "control.db")
	injected, err := RunWorkbench(context.Background(), injectedDB, RunOptions{Concurrency: 4, Config: cfg, ConfigSet: true})
	if err != nil {
		return InjectionCase{}, err
	}
	controlCfg := cfg
	controlCfg.Injection = false
	control, err := RunWorkbench(context.Background(), controlDB, RunOptions{Concurrency: 2, Config: controlCfg, ConfigSet: true})
	if err != nil {
		return InjectionCase{}, err
	}
	target, ok := FindTaskReport(injected.Report, "inject-target")
	if !ok {
		return InjectionCase{}, fmt.Errorf("injection target missing from report")
	}
	loaded, err := LoadWorkspace(injectedDB)
	if err != nil {
		return InjectionCase{}, err
	}
	cases := InjectionCase{
		ExpectedTargetCode: expected,
		ActualTargetCode:   target.DecisionCode,
		ControlsUnaffected: controlsMatch(control.Report, injected.Report),
		SnapshotsValid:     snapshotsValid(loaded, 3),
	}
	data, err := MarshalReport(injected.Report)
	if err != nil {
		return InjectionCase{}, err
	}
	cases.ReportDigest = ReportDigest(data)
	cases.Passed = cases.ActualTargetCode == cases.ExpectedTargetCode && cases.ControlsUnaffected && cases.SnapshotsValid
	cases.Detail = fmt.Sprintf("target decision %s; committed snapshots validated", cases.ActualTargetCode)
	return cases, nil
}

func injectionConfig(kind string) (Config, string) {
	cfg := DefaultConfig()
	cfg.Injection = true
	switch kind {
	case InjectionLengthWrap:
		cfg.Scenario = "inject-length-wrap"
		return cfg, CodeLengthWrap
	case InjectionDeepNesting:
		cfg.Scenario = "inject-deep-nesting"
		return cfg, CodeDepthExceeded
	case InjectionTaskBudget:
		cfg.Scenario = "inject-task-budget"
		return cfg, CodeTaskMemory
	case InjectionGlobalBudget:
		cfg.Scenario = "inject-global-budget"
		cfg.GlobalMemoryBudget = 98304
		return cfg, CodeGlobalBudget
	default:
		cfg.Scenario = "inject-snapshot-gap"
		return cfg, CodeSnapshotVersion
	}
}

func runSnapshotGapInjection(baseDir string) (InjectionCase, error) {
	cfg := DefaultConfig()
	cfg.Injection = true
	cfg.Scenario = "inject-snapshot-gap"
	dir, err := os.MkdirTemp(baseDir, "recordlab-inject-*")
	if err != nil {
		return InjectionCase{}, err
	}
	defer os.RemoveAll(dir)
	freshDB := filepath.Join(dir, "fresh.db")
	recoverDB := filepath.Join(dir, "recover.db")
	fresh, err := RunWorkbench(context.Background(), freshDB, RunOptions{Concurrency: 3, Config: cfg, ConfigSet: true})
	if err != nil {
		return InjectionCase{}, err
	}
	if _, err := RunWorkbench(context.Background(), recoverDB, RunOptions{Concurrency: 3, Config: cfg, ConfigSet: true, StopAfter: 3}); err != nil {
		return InjectionCase{}, err
	}
	store, err := OpenStore(recoverDB)
	if err != nil {
		return InjectionCase{}, err
	}
	if err := store.DeleteSnapshot(2); err != nil {
		store.Close()
		return InjectionCase{}, err
	}
	broken, err := store.Load(false)
	if err != nil {
		store.Close()
		return InjectionCase{}, err
	}
	store.Close()
	genesisHash := ""
	if len(broken.Valid) > 0 {
		genesisHash = broken.Valid[0].StateSHA256
	}
	recovered, err := RunWorkbench(context.Background(), recoverDB, RunOptions{Concurrency: 3, Config: cfg, ConfigSet: true})
	if err != nil {
		return InjectionCase{}, err
	}
	finalLoad, err := LoadWorkspace(recoverDB)
	if err != nil {
		return InjectionCase{}, err
	}
	freshBytes, err := MarshalReport(fresh.Report)
	if err != nil {
		return InjectionCase{}, err
	}
	recoveredBytes, err := MarshalReport(recovered.Report)
	if err != nil {
		return InjectionCase{}, err
	}
	genesisClean := finalLoad.WarningCode == "" && len(finalLoad.Valid) > 0 && finalLoad.Valid[0].StateSHA256 == genesisHash
	result := InjectionCase{
		ExpectedTargetCode: CodeSnapshotVersion,
		ActualTargetCode:   broken.WarningCode,
		ControlsUnaffected: bytes.Equal(freshBytes, recoveredBytes),
		SnapshotsValid:     snapshotsValid(finalLoad, 5) && genesisClean,
		SnapshotFallback:   broken.LatestVersion(),
		ReportDigest:       ReportDigest(recoveredBytes),
	}
	result.Passed = result.ActualTargetCode == result.ExpectedTargetCode && result.ControlsUnaffected && result.SnapshotsValid && result.SnapshotFallback == 1
	result.Detail = fmt.Sprintf("gap detected at fallback version %d; resumed report matches fresh report", result.SnapshotFallback)
	return result, nil
}

func controlsMatch(control, injected Report) bool {
	injectedByID := make(map[string]TaskReport, len(injected.Tasks))
	for _, task := range injected.Tasks {
		injectedByID[task.TaskID] = task
	}
	for _, expected := range control.Tasks {
		if expected.TaskID == "inject-target" {
			continue
		}
		actual, ok := injectedByID[expected.TaskID]
		if !ok {
			return false
		}
		expected.GlobalReservedAfter = 0
		actual.GlobalReservedAfter = 0
		expectedJSON, err := json.Marshal(expected)
		if err != nil {
			return false
		}
		actualJSON, err := json.Marshal(actual)
		if err != nil {
			return false
		}
		if !bytes.Equal(expectedJSON, actualJSON) {
			return false
		}
	}
	return true
}

func snapshotsValid(load ChainLoad, expectedResults int) bool {
	if load.WarningCode != "" || !load.HasState || len(load.State.Results) != expectedResults {
		return false
	}
	if len(load.Valid) != expectedResults+1 {
		return false
	}
	for index, snapshot := range load.Valid {
		if snapshot.Version != int64(index+1) {
			return false
		}
	}
	return true
}
