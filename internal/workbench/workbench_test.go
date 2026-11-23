package workbench_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"svcworkbench/internal/domain"
	"svcworkbench/internal/samplegen"
	"svcworkbench/internal/snapshot"
	"svcworkbench/internal/workbench"
)

func TestNormalRunCommitsAndReplays(t *testing.T) {
	state := t.TempDir()
	options := defaultOptions(state)
	outcome, err := workbench.Run(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	report := outcome.Report
	if report.Snapshot == nil || !report.Snapshot.Committed || report.Snapshot.ActiveVersion != 1 {
		t.Fatalf("unexpected snapshot: %+v", report.Snapshot)
	}
	if len(report.Services) != 7 {
		t.Fatalf("services = %d", len(report.Services))
	}
	if !contains(report.BusinessCodes, string(domain.CodePrivilegeExposure)) {
		t.Fatalf("missing exposure code: %#v", report.BusinessCodes)
	}
	if contains(report.BusinessCodes, string(domain.CodeDeclarationConflict)) {
		t.Fatalf("normal sample conflicts: %#v", report.BusinessCodes)
	}
	if report.Metrics.MergeMode != "serial-service-partition" || report.Metrics.CompletedSources != 3 {
		t.Fatalf("unexpected metrics: %+v", report.Metrics)
	}
	replay, err := workbench.Replay(context.Background(), state, 1, "v2", domain.DefaultSourcePriorities)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Snapshot == nil || replay.Snapshot.Version != 1 || len(replay.Report) == 0 {
		t.Fatalf("unexpected replay: %+v", replay)
	}
	trace, err := workbench.Trace(context.Background(), state, 1, "Writable Agent", "v2", domain.DefaultSourcePriorities)
	if err != nil {
		t.Fatal(err)
	}
	if trace.Service == nil || !contains(trace.Service.RiskFlags, "PRIVILEGE_EXPOSURE") {
		t.Fatalf("unexpected trace: %+v", trace)
	}
	if len(trace.Service.Declarations) != 3 {
		t.Fatalf("trace declarations = %d", len(trace.Service.Declarations))
	}
}

func TestReplayAndTraceOnEmptyStateReturnBusinessCode(t *testing.T) {
	state := t.TempDir()
	replay, err := workbench.Replay(context.Background(), state, 0, "v2", domain.DefaultSourcePriorities)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(replay.Codes, string(domain.CodeSnapshotVersion)) {
		t.Fatalf("replay codes = %#v", replay.Codes)
	}
	trace, err := workbench.Trace(context.Background(), state, 0, "Spooler", "v2", domain.DefaultSourcePriorities)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(trace.Codes, string(domain.CodeSnapshotVersion)) {
		t.Fatalf("trace codes = %#v", trace.Codes)
	}
}

func TestSnapshotsIncrementAcrossRuns(t *testing.T) {
	state := t.TempDir()
	for round := 1; round <= 3; round++ {
		outcome, err := workbench.Run(context.Background(), defaultOptions(state))
		if err != nil {
			t.Fatal(err)
		}
		if outcome.Report.Snapshot.ActiveVersion != round {
			t.Fatalf("round %d version = %d", round, outcome.Report.Snapshot.ActiveVersion)
		}
		if len(outcome.Report.Snapshot.ChainVersions) != round {
			t.Fatalf("round %d chain = %#v", round, outcome.Report.Snapshot.ChainVersions)
		}
	}
}

func TestReportIsByteDeterministic(t *testing.T) {
	first, err := workbench.Run(context.Background(), defaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	second, err := workbench.Run(context.Background(), defaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, err := json.Marshal(first.Report)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(second.Report)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstJSON, secondJSON) {
		t.Fatalf("reports differ across fresh identical runs")
	}
}

func TestTargetFormatSwitchIsRejectedWithoutCommit(t *testing.T) {
	state := t.TempDir()
	options := defaultOptions(state)
	options.TargetFormat = "v1"
	first, err := workbench.Run(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Report.Snapshot.Committed || first.Report.Snapshot.ActiveVersion != 1 {
		t.Fatalf("v1 baseline failed: %+v", first.Report.Snapshot)
	}
	options.TargetFormat = "v2"
	second, err := workbench.Run(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(second.Report.BusinessCodes, string(domain.CodeSnapshotVersion)) {
		t.Fatalf("missing snapshot code: %#v", second.Report.BusinessCodes)
	}
	if second.Report.Snapshot.Committed || second.Report.Snapshot.HeadVersion != 1 || second.Report.Snapshot.ActiveVersion != 0 {
		t.Fatalf("incompatible snapshot advanced: %+v", second.Report.Snapshot)
	}
}

func TestPrioritySwitchIsRejectedWithoutCommit(t *testing.T) {
	state := t.TempDir()
	if _, err := workbench.Run(context.Background(), defaultOptions(state)); err != nil {
		t.Fatal(err)
	}
	options := defaultOptions(state)
	options.Priorities = []domain.SourceKind{domain.SourceJSONManifest, domain.SourceBinaryPolicy, domain.SourceLegacyText}
	outcome, err := workbench.Run(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(outcome.Report.BusinessCodes, string(domain.CodeSnapshotVersion)) {
		t.Fatalf("missing snapshot code: %#v", outcome.Report.BusinessCodes)
	}
	if outcome.Report.Snapshot.Committed || outcome.Report.Snapshot.HeadVersion != 1 {
		t.Fatalf("priority-incompatible snapshot advanced: %+v", outcome.Report.Snapshot)
	}
	replay, err := workbench.Replay(context.Background(), state, 1, "v2", options.Priorities)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(replay.Codes, string(domain.CodeSnapshotVersion)) {
		t.Fatalf("replay should reject incompatible config: %+v", replay)
	}
}

func TestFaultInjectionCodesDoNotAdvanceSnapshots(t *testing.T) {
	cases := []struct {
		fault string
		code  domain.Code
	}{
		{samplegen.FaultUnknownVersion, domain.CodeFormatVersion},
		{samplegen.FaultTextPrefixOOB, domain.CodeFormatVersion},
		{samplegen.FaultUnterminatedQuote, domain.CodePathMalformed},
		{samplegen.FaultUnknownAccount, domain.CodeAccountUnknown},
		{samplegen.FaultDeclarationConflict, domain.CodeDeclarationConflict},
	}
	for _, item := range cases {
		t.Run(item.fault, func(t *testing.T) {
			options := defaultOptions(t.TempDir())
			options.Fault = item.fault
			outcome, err := workbench.Run(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}
			report := outcome.Report
			if !contains(report.BusinessCodes, string(item.code)) {
				t.Fatalf("missing %s: %#v", item.code, report.BusinessCodes)
			}
			if report.Snapshot.Committed || report.Snapshot.ActiveVersion != 0 {
				t.Fatalf("snapshot advanced: %+v", report.Snapshot)
			}
			if len(report.Services) != 7 {
				t.Fatalf("unrelated services were lost: %d", len(report.Services))
			}
		})
	}
}

func TestFaultInjectionAgainstBaselineKeepsCommittedVersion(t *testing.T) {
	faults := []string{
		samplegen.FaultUnknownVersion,
		samplegen.FaultTextPrefixOOB,
		samplegen.FaultUnterminatedQuote,
		samplegen.FaultUnknownAccount,
		samplegen.FaultDeclarationConflict,
	}
	for _, fault := range faults {
		t.Run(fault, func(t *testing.T) {
			state := t.TempDir()
			baseline, err := workbench.Run(context.Background(), defaultOptions(state))
			if err != nil {
				t.Fatal(err)
			}
			if baseline.Report.Snapshot.ActiveVersion != 1 {
				t.Fatalf("baseline version = %d", baseline.Report.Snapshot.ActiveVersion)
			}
			options := defaultOptions(state)
			options.Fault = fault
			outcome, err := workbench.Run(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}
			report := outcome.Report
			if report.Snapshot.Committed || report.Snapshot.ActiveVersion != 1 || report.Snapshot.HeadVersion != 1 {
				t.Fatalf("fault advanced committed state: %+v", report.Snapshot)
			}
			replay, err := workbench.Replay(context.Background(), state, 1, "v2", domain.DefaultSourcePriorities)
			if err != nil {
				t.Fatal(err)
			}
			if replay.Snapshot == nil || replay.Snapshot.Version != 1 {
				t.Fatalf("baseline snapshot is no longer replayable: %+v", replay)
			}
		})
	}
}

func TestSnapshotGapExposesFallback(t *testing.T) {
	options := defaultOptions(t.TempDir())
	options.Fault = samplegen.FaultSnapshotGap
	outcome, err := workbench.Run(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	report := outcome.Report
	if !contains(report.BusinessCodes, string(domain.CodeSnapshotVersion)) {
		t.Fatalf("missing snapshot code: %#v", report.BusinessCodes)
	}
	if report.Snapshot.Committed || report.Snapshot.ActiveVersion != 1 || report.Snapshot.FallbackVersion != 1 {
		t.Fatalf("unexpected fallback snapshot: %+v", report.Snapshot)
	}
	if report.Snapshot.HeadVersion != 3 {
		t.Fatalf("head version = %d", report.Snapshot.HeadVersion)
	}
	if len(report.Services) != 7 {
		t.Fatalf("services = %d", len(report.Services))
	}
}

func TestServiceBudgetBlocksSnapshotAndKeepsPartialSources(t *testing.T) {
	options := defaultOptions(t.TempDir())
	options.ServiceBudget = 2
	outcome, err := workbench.Run(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	report := outcome.Report
	if !contains(report.BusinessCodes, string(domain.CodeServiceBudget)) || report.Snapshot.Committed {
		t.Fatalf("unexpected budget result: %#v %+v", report.BusinessCodes, report.Snapshot)
	}
	for _, summary := range report.SourceSummaries {
		if !summary.Partial || summary.RecordsAccepted != 2 {
			t.Fatalf("%s summary: %+v", summary.SourceID, summary)
		}
	}
}

func TestConcurrencyQueuesSourcesAndReportsInFlight(t *testing.T) {
	options := defaultOptions(t.TempDir())
	options.MaxConcurrency = 1
	options.Fault = samplegen.FaultNone
	outcome, err := workbench.Run(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	metrics := outcome.Report.Metrics
	if metrics.MaxConcurrency != 1 || metrics.QueuedSources != 2 || metrics.ObservedMaxInFlight != 1 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
}

func TestCrashDuringCommitRollsBackAndAllowsRetry(t *testing.T) {
	state := t.TempDir()
	if _, err := workbench.Run(context.Background(), defaultOptions(state)); err != nil {
		t.Fatal(err)
	}
	runCrashHelper(t, state, "commit")
	assertCommittedVersionAndProgress(t, state, 1)
	outcome, err := workbench.Run(context.Background(), defaultOptions(state))
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Report.Snapshot.ActiveVersion != 2 {
		t.Fatalf("retry version = %d", outcome.Report.Snapshot.ActiveVersion)
	}
	assertCommittedVersionAndProgress(t, state, 2)
}

func TestCrashDuringMergeLeavesCommittedStateUntouched(t *testing.T) {
	state := t.TempDir()
	if _, err := workbench.Run(context.Background(), defaultOptions(state)); err != nil {
		t.Fatal(err)
	}
	runCrashHelper(t, state, "merge")
	assertCommittedVersionAndProgress(t, state, 1)
	outcome, err := workbench.Run(context.Background(), defaultOptions(state))
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Report.Snapshot.ActiveVersion != 2 {
		t.Fatalf("retry version = %d", outcome.Report.Snapshot.ActiveVersion)
	}
}

func TestDemoIsDeterministic(t *testing.T) {
	first, err := workbench.Demo(context.Background(), "v2")
	if err != nil {
		t.Fatal(err)
	}
	second, err := workbench.Demo(context.Background(), "v2")
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstJSON, secondJSON) {
		t.Fatalf("demo output is not deterministic")
	}
	for _, check := range first.FaultChecks {
		if !check.Passed {
			t.Fatalf("fault check failed: %+v", check)
		}
	}
}

func TestCrashHelper(t *testing.T) {
	if os.Getenv("SVCWORKBENCH_CRASH_HELPER") != "1" {
		return
	}
	state := os.Getenv("SVCWORKBENCH_CRASH_STATE")
	point := os.Getenv("SVCWORKBENCH_CRASH_POINT")
	options := defaultOptions(state)
	options.CrashPoint = point
	options.Crash = func(_ string) { os.Exit(86) }
	if _, err := workbench.Run(context.Background(), options); err != nil {
		os.Exit(87)
	}
	os.Exit(0)
}

func runCrashHelper(t *testing.T, state, point string) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
	command.Env = append(os.Environ(),
		"SVCWORKBENCH_CRASH_HELPER=1",
		"SVCWORKBENCH_CRASH_STATE="+state,
		"SVCWORKBENCH_CRASH_POINT="+point,
	)
	output, err := command.CombinedOutput()
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) {
		t.Fatalf("crash helper did not exit abnormally: err=%v output=%s", err, output)
	}
	if exitError.ExitCode() != 86 {
		t.Fatalf("crash helper exit code = %d output=%s", exitError.ExitCode(), output)
	}
}

func assertCommittedVersionAndProgress(t *testing.T, state string, version int) {
	t.Helper()
	store, err := snapshot.Open(filepath.Join(state, "workbench.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	result, chain, err := store.Load(context.Background(), 0, snapshotConfigForTest())
	if err != nil {
		t.Fatal(err)
	}
	if result.Meta.Version != version || chain.HeadVersion != version {
		t.Fatalf("committed version = %d head=%d want %d", result.Meta.Version, chain.HeadVersion, version)
	}
	progress, err := store.Progress(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range progress {
		if row.SnapshotVersion > version {
			t.Fatalf("uncommitted progress survived: %+v", row)
		}
	}
}

func defaultOptions(state string) workbench.Options {
	return workbench.Options{
		StateDir:       state,
		TargetFormat:   "v2",
		Priorities:     append([]domain.SourceKind(nil), domain.DefaultSourcePriorities...),
		MaxConcurrency: 3,
		ServiceBudget:  128,
		Fault:          samplegen.FaultNone,
	}
}

func snapshotConfigForTest() snapshot.Config {
	return snapshot.Config{
		SchemaVersion: domain.SnapshotSchemaVersion,
		Marker:        domain.ReportMarker,
		TargetFormat:  "v2",
		SourceVersions: map[string]string{
			"binary_policy": "policy-v2",
			"json_manifest": "jsonl-v1",
			"legacy_text":   "legacy-text-v1",
		},
		Priorities: []string{"binary_policy", "json_manifest", "legacy_text"},
	}
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
