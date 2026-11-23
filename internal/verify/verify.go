package verify

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"dns-cache-workbench/internal/sample"
	"dns-cache-workbench/internal/workbench"
)

type Check struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail,omitempty"`
}

type CaseResult struct {
	Case             string  `json:"case"`
	ExpectedCode     string  `json:"expected_code"`
	ObservedCode     string  `json:"observed_code,omitempty"`
	Passed           bool    `json:"passed"`
	BaselineSnapshot int     `json:"baseline_snapshot"`
	FinalSnapshot    int     `json:"final_snapshot"`
	ReportPath       string  `json:"report_path,omitempty"`
	Checks           []Check `json:"checks"`
}

type Result struct {
	AllPassed bool         `json:"all_passed"`
	Cases     []CaseResult `json:"cases"`
}

type Options struct {
	InputRoot string
	WorkDir   string
	Target    string
	Window    uint64
	Flip      uint64
}

var suspiciousCodes = []string{
	workbench.CodeUnsolicited,
	workbench.CodeAdditionalInject,
	workbench.CodeTTLMismatch,
	workbench.CodeFlipFlop,
}

func Run(ctx context.Context, options Options) (Result, error) {
	if options.Target == "" {
		options.Target = "v2"
	}
	if options.Window == 0 {
		options.Window = 10
	}
	if options.Flip == 0 {
		options.Flip = 3
	}
	if err := os.RemoveAll(options.WorkDir); err != nil {
		return Result{}, err
	}
	if err := os.MkdirAll(options.WorkDir, 0o755); err != nil {
		return Result{}, err
	}

	baselineDir := filepath.Join(options.InputRoot, "clean")
	baselineDB := filepath.Join(options.WorkDir, "baseline", "baseline.db")
	baselineReportPath := filepath.Join(options.WorkDir, "baseline", "report.json")
	baselineReport, err := workbench.Scan(ctx, workbench.ScanOptions{
		InputDir:        baselineDir,
		DBPath:          baselineDB,
		ReportPath:      baselineReportPath,
		TargetParser:    options.Target,
		MaxConcurrency:  3,
		MaxEvents:       10000,
		WindowTicks:     options.Window,
		FlipWindowTicks: options.Flip,
	})
	if err != nil {
		return Result{}, err
	}
	baselineStore, err := workbench.OpenStore(baselineDB)
	if err != nil {
		return Result{}, err
	}
	baselineDescriptors, err := baselineStore.ListSnapshots(options.Target)
	if err != nil {
		baselineStore.Close()
		return Result{}, err
	}
	baselineSnapshot := baselineDescriptors[len(baselineDescriptors)-1].Version
	baselineContentHash := baselineDescriptors[len(baselineDescriptors)-1].ContentHash
	baselineStore.Close()

	result := Result{AllPassed: true, Cases: make([]CaseResult, 0, len(sample.InjectionKinds))}
	for _, kind := range sample.InjectionKinds {
		caseResult, err := runCase(ctx, options, kind, baselineReport, baselineContentHash, baselineSnapshot)
		if err != nil {
			return Result{}, err
		}
		result.Cases = append(result.Cases, caseResult)
		if !caseResult.Passed {
			result.AllPassed = false
		}
	}
	return result, nil
}

func runCase(ctx context.Context, options Options, kind string, baselineReport workbench.Report, baselineContentHash string, baselineSnapshot int) (CaseResult, error) {
	caseDir := filepath.Join(options.WorkDir, "cases", kind)
	if err := os.MkdirAll(caseDir, 0o755); err != nil {
		return CaseResult{}, err
	}
	dbPath := filepath.Join(caseDir, "case.db")
	cleanReportPath := filepath.Join(caseDir, "clean-report.json")
	_, err := workbench.Scan(ctx, workbench.ScanOptions{
		InputDir:        filepath.Join(options.InputRoot, "clean"),
		DBPath:          dbPath,
		ReportPath:      cleanReportPath,
		TargetParser:    options.Target,
		MaxConcurrency:  3,
		MaxEvents:       10000,
		WindowTicks:     options.Window,
		FlipWindowTicks: options.Flip,
	})
	if err != nil {
		return CaseResult{}, err
	}

	expectedCodes := map[string]string{
		sample.InjectionTruncatedBinary: workbench.CodeTruncated,
		sample.InjectionUnknownVersion:  workbench.CodeVersionUnsupported,
		sample.InjectionTextOverflow:    workbench.CodeLengthOverflow,
		sample.InjectionNameConflict:    workbench.CodeNameConflict,
		sample.InjectionSnapshotJump:    workbench.CodeSnapshotVersion,
	}
	expected := expectedCodes[kind]
	caseReport := baselineReport
	reportPath := filepath.Join(caseDir, "injected-report.json")
	if kind == sample.InjectionSnapshotJump {
		_, err = workbench.Scan(ctx, workbench.ScanOptions{
			InputDir:        filepath.Join(options.InputRoot, "clean"),
			DBPath:          dbPath,
			ReportPath:      reportPath,
			TargetParser:    options.Target,
			MaxConcurrency:  3,
			MaxEvents:       10000,
			WindowTicks:     options.Window,
			FlipWindowTicks: options.Flip,
			SnapshotJump:    2,
		})
		if err == nil {
			return CaseResult{}, fmt.Errorf("snapshot jump did not fail")
		}
	} else {
		caseReport, err = workbench.Scan(ctx, workbench.ScanOptions{
			InputDir:        filepath.Join(options.InputRoot, "injections", kind),
			DBPath:          dbPath,
			ReportPath:      reportPath,
			TargetParser:    options.Target,
			MaxConcurrency:  3,
			MaxEvents:       10000,
			WindowTicks:     options.Window,
			FlipWindowTicks: options.Flip,
		})
		if err != nil {
			return CaseResult{}, err
		}
	}

	observed := expected
	if kind == sample.InjectionSnapshotJump {
		businessErr, ok := err.(*workbench.BusinessError)
		if !ok || businessErr.Code != workbench.CodeSnapshotVersion {
			return CaseResult{}, fmt.Errorf("snapshot jump returned unexpected error: %v", err)
		}
	} else if !reportHasCode(caseReport, expected) {
		observed = ""
	}
	store, err := workbench.OpenStore(dbPath)
	if err != nil {
		return CaseResult{}, err
	}
	defer store.Close()
	descriptors, err := store.ListSnapshots(options.Target)
	if err != nil {
		return CaseResult{}, err
	}
	if len(descriptors) == 0 {
		return CaseResult{}, fmt.Errorf("case %s lost its baseline snapshot", kind)
	}
	finalSnapshot := descriptors[len(descriptors)-1].Version
	loadedBaseline, err := store.LoadSnapshot(baselineSnapshot, options.Target)
	if err != nil {
		return CaseResult{}, err
	}
	baselineHashAfter := ""
	if loadedBaseline.Snapshot != nil {
		baselineHashAfter = loadedBaseline.Snapshot.ContentHash
	}

	checks := []Check{
		{Name: "expected_code", Passed: observed == expected, Detail: expected},
		{Name: "all_suspicious_verdicts_preserved", Passed: hasAllSuspicious(caseReport), Detail: strings.Join(suspiciousCodes, ",")},
		{Name: "baseline_snapshot_content_unchanged", Passed: baselineHashAfter == baselineContentHash, Detail: baselineContentHash},
		{Name: "unaffected_source_event_counts_preserved", Passed: sourceCountsPreserved(baselineReport, caseReport), Detail: "clean source events"},
	}
	if kind == sample.InjectionSnapshotJump {
		checks = append(checks, Check{
			Name: "snapshot_jump_rolled_back", Passed: len(descriptors) == baselineSnapshot && finalSnapshot == baselineSnapshot,
			Detail: fmt.Sprintf("snapshot count=%d version=%d", len(descriptors), finalSnapshot),
		})
	}
	caseResult := CaseResult{
		Case:             kind,
		ExpectedCode:     expected,
		ObservedCode:     observed,
		BaselineSnapshot: baselineSnapshot,
		FinalSnapshot:    finalSnapshot,
		ReportPath:       reportPath,
		Checks:           checks,
	}
	caseResult.Passed = true
	for _, check := range checks {
		if !check.Passed {
			caseResult.Passed = false
			break
		}
	}
	return caseResult, nil
}

func reportHasCode(report workbench.Report, code string) bool {
	for _, finding := range report.Findings {
		if finding.Code == code {
			return true
		}
	}
	for _, source := range report.Sources {
		for _, issue := range source.Issues {
			if issue.Code == code {
				return true
			}
		}
	}
	return false
}

func hasAllSuspicious(report workbench.Report) bool {
	for _, code := range suspiciousCodes {
		if !reportHasCode(report, code) {
			return false
		}
	}
	return true
}

func sourceCountsPreserved(baseline, candidate workbench.Report) bool {
	candidateCounts := make(map[string]int)
	for _, source := range candidate.Sources {
		candidateCounts[source.Path] = source.Events
	}
	for _, source := range baseline.Sources {
		if candidateCounts[source.Path] != source.Events {
			return false
		}
	}
	return true
}
