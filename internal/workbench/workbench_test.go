package workbench

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"internalpki/workbench/internal/checkpoint"
	"internalpki/workbench/internal/engine"
	"internalpki/workbench/internal/model"
	"internalpki/workbench/internal/samplegen"
	"internalpki/workbench/internal/sampleio"
)

func TestAuditBusinessCodesAndAccounting(t *testing.T) {
	samplesDir := t.TempDir()
	if _, err := samplegen.Generate(samplesDir, samplegen.Options{IncludeAnomalies: true}); err != nil {
		t.Fatalf("generate fixtures: %v", err)
	}
	report := runAudit(t, samplesDir, filepath.Join(t.TempDir(), "checkpoint.json"), 4, 5*time.Millisecond, 500*time.Millisecond, 128*1024*1024)
	if !report.Summary.FullyAccounted {
		t.Fatalf("report not fully accounted: %+v", report.Summary)
	}
	if report.Summary.Completed+report.Summary.Failed+report.Summary.Degraded != report.Summary.Total {
		t.Fatalf("summary count invariant failed: %+v", report.Summary)
	}
	byPath := make(map[string]model.SampleResult)
	for _, sample := range report.Samples {
		byPath[sample.Path] = sample
		if !sample.ExpectationMatched {
			t.Errorf("%s expected %s, got %s", sample.Path, sample.ExpectedResult, sample.ActualResult)
		}
		if !uniqueFindings(sample.Findings) {
			t.Errorf("%s contains duplicate findings: %+v", sample.Path, sample.Findings)
		}
	}
	expected := map[string]string{
		"anomaly/missing-intermediate": model.CodeChainBroken,
		"anomaly/sni-mismatch":         model.CodeHostnameMismatch,
		"anomaly/expired":              model.CodeExpired,
		"anomaly/untrusted-root":       model.CodeChainUntrusted,
		"anomaly/pathlen-exceeded":     model.CodePathLenExceeded,
		"anomaly/self-signed-leaf":     model.CodeUntrustedSelfSigned,
		"anomaly/ext-key-usage":        model.CodeExtKeyUsage,
		"anomaly/tls-version-mismatch": model.CodeTLSVersionMismatch,
	}
	for path, code := range expected {
		if got := byPath[path].BusinessCode; got != code {
			t.Errorf("%s: got %s, want %s", path, got, code)
		}
	}
	for path, sample := range byPath {
		if len(path) >= 7 && path[:7] == "normal/" {
			if sample.Outcome != model.OutcomePassed || sample.BusinessCode != "" {
				t.Errorf("normal sample %s failed: %s", path, sample.BusinessCode)
			}
			if sample.FailureCertificate != nil {
				t.Errorf("passing sample %s has a failure certificate", path)
			}
		}
	}
}

func TestReportIsByteStableAcrossConcurrency(t *testing.T) {
	samplesDir := t.TempDir()
	if _, err := samplegen.Generate(samplesDir, samplegen.Options{IncludeAnomalies: true}); err != nil {
		t.Fatalf("generate fixtures: %v", err)
	}
	reportOne := runAudit(t, samplesDir, filepath.Join(t.TempDir(), "one.json"), 1, 5*time.Millisecond, 500*time.Millisecond, 128*1024*1024)
	reportEight := runAudit(t, samplesDir, filepath.Join(t.TempDir(), "eight.json"), 8, 5*time.Millisecond, 500*time.Millisecond, 128*1024*1024)
	bytesOne, err := MarshalReport(reportOne)
	if err != nil {
		t.Fatal(err)
	}
	bytesEight, err := MarshalReport(reportEight)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytesOne, bytesEight) {
		t.Fatal("report bytes differ across concurrency settings")
	}
}

func TestReportIsByteStableAcrossAbsoluteSampleDirectories(t *testing.T) {
	firstDir := t.TempDir()
	secondDir := t.TempDir()
	if _, err := samplegen.Generate(firstDir, samplegen.Options{IncludeAnomalies: true}); err != nil {
		t.Fatalf("generate first fixtures: %v", err)
	}
	if _, err := samplegen.Generate(secondDir, samplegen.Options{IncludeAnomalies: true}); err != nil {
		t.Fatalf("generate second fixtures: %v", err)
	}
	first := runAudit(t, firstDir, filepath.Join(t.TempDir(), "checkpoint.json"), 3, 5*time.Millisecond, 500*time.Millisecond, 128*1024*1024)
	second := runAudit(t, secondDir, filepath.Join(t.TempDir(), "checkpoint.json"), 3, 5*time.Millisecond, 500*time.Millisecond, 128*1024*1024)
	firstBytes, _ := MarshalReport(first)
	secondBytes, _ := MarshalReport(second)
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatal("report bytes contain absolute sample-directory paths")
	}
}

func TestGlobalBudgetDegradesDeeperSamplesAndKeepsAllRows(t *testing.T) {
	samplesDir := t.TempDir()
	if _, err := samplegen.Generate(samplesDir, samplegen.Options{IncludeAnomalies: true}); err != nil {
		t.Fatalf("generate fixtures: %v", err)
	}
	// Two depth-2 samples are admitted; every remaining sample is at least as
	// deep and must be represented as a degraded row.
	globalCPU := time.Duration(engine.EstimateCost(2).CPUUnits*2) * time.Microsecond
	report := runAudit(t, samplesDir, filepath.Join(t.TempDir(), "checkpoint.json"), 3, 5*time.Millisecond, globalCPU, 128*1024*1024)
	if report.Summary.Degraded == 0 {
		t.Fatalf("expected degraded samples, summary=%+v", report.Summary)
	}
	if report.Summary.Completed+report.Summary.Failed+report.Summary.Degraded != report.Summary.Total {
		t.Fatalf("summary count invariant failed: %+v", report.Summary)
	}
	maxNonDegradedDepth := 0
	minDegradedDepth := int(^uint(0) >> 1)
	for _, sample := range report.Samples {
		if sample.Outcome == model.OutcomeDegraded {
			if sample.BusinessCode != model.CodeGlobalBudgetExhausted {
				t.Errorf("degraded sample %s has code %s", sample.Path, sample.BusinessCode)
			}
			if sample.Metrics.CPUCharged || sample.Metrics.MemoryCharged {
				t.Errorf("degraded sample %s was charged resources", sample.Path)
			}
			if sample.ChainDepth < minDegradedDepth {
				minDegradedDepth = sample.ChainDepth
			}
		} else if sample.ChainDepth > maxNonDegradedDepth {
			maxNonDegradedDepth = sample.ChainDepth
		}
	}
	if minDegradedDepth < maxNonDegradedDepth {
		t.Errorf("degraded sample depth %d is shallower than processed depth %d", minDegradedDepth, maxNonDegradedDepth)
	}
}

func TestSampleBudgetExceededReleasesResources(t *testing.T) {
	samplesDir := t.TempDir()
	if _, err := samplegen.Generate(samplesDir, samplegen.Options{IncludeAnomalies: false}); err != nil {
		t.Fatalf("generate fixtures: %v", err)
	}
	report := runAudit(t, samplesDir, filepath.Join(t.TempDir(), "checkpoint.json"), 2, time.Nanosecond, 500*time.Millisecond, 128*1024*1024)
	if report.Summary.Failed != report.Summary.Total {
		t.Fatalf("expected all samples failed by sample budget: %+v", report.Summary)
	}
	for _, sample := range report.Samples {
		if sample.BusinessCode != model.CodeSampleBudgetExceeded {
			t.Errorf("%s: got %s", sample.Path, sample.BusinessCode)
		}
		if sample.State != model.StateBudgetTerminated {
			t.Errorf("%s: state %s", sample.Path, sample.State)
		}
		if sample.Metrics.CPUCharged || sample.Metrics.MemoryCharged || sample.Metrics.CPUUnits != 0 || sample.Metrics.MemoryBytes != 0 {
			t.Errorf("%s: resources were not released: %+v", sample.Path, sample.Metrics)
		}
	}
}

func TestCheckpointResumeAndSnapshotGap(t *testing.T) {
	samplesDir := t.TempDir()
	if _, err := samplegen.Generate(samplesDir, samplegen.Options{IncludeAnomalies: false}); err != nil {
		t.Fatalf("generate fixtures: %v", err)
	}
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.json")
	first := runAudit(t, samplesDir, checkpointPath, 2, 5*time.Millisecond, 500*time.Millisecond, 128*1024*1024)
	before, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatal(err)
	}
	second := runAudit(t, samplesDir, checkpointPath, 5, 5*time.Millisecond, 500*time.Millisecond, 128*1024*1024)
	firstBytes, _ := MarshalReport(first)
	secondBytes, _ := MarshalReport(second)
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatal("resumed report differs from original report")
	}
	after, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("fully resumed run unexpectedly committed new snapshots")
	}

	var damaged checkpoint.File
	if err := json.Unmarshal(before, &damaged); err != nil {
		t.Fatal(err)
	}
	damaged.Generation = 4
	damaged.CommitLog = []int64{1, 2, 4, 5}
	data, err := json.MarshalIndent(damaged, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(checkpointPath, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	third := runAudit(t, samplesDir, checkpointPath, 2, 5*time.Millisecond, 500*time.Millisecond, 128*1024*1024)
	foundGap := false
	for _, diagnostic := range third.Diagnostics {
		if diagnostic.Code == model.CodeSnapshotGap {
			foundGap = true
		}
	}
	if !foundGap {
		t.Fatalf("expected E_SNAPSHOT_GAP diagnostic, got %+v", third.Diagnostics)
	}
	if !third.Summary.FullyAccounted || third.Summary.Total != first.Summary.Total {
		t.Fatalf("gapped checkpoint recovery did not re-audit all samples: %+v", third.Summary)
	}
}

func TestVerifyingSnapshotIsRedone(t *testing.T) {
	samplesDir := t.TempDir()
	paths, err := samplegen.Generate(samplesDir, samplegen.Options{IncludeAnomalies: false})
	if err != nil {
		t.Fatalf("generate fixtures: %v", err)
	}
	digests := make(map[string]string, len(paths))
	for _, path := range paths {
		sample := sampleio.Load(samplesDir, path)
		digests[path] = sample.InputDigest
	}
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.json")
	store, _, err := checkpoint.Open(checkpointPath, digests)
	if err != nil {
		t.Fatal(err)
	}
	firstPath := paths[0]
	if err := store.MarkVerifying(firstPath, digests[firstPath]); err != nil {
		t.Fatal(err)
	}
	report := runAudit(t, samplesDir, checkpointPath, 2, 5*time.Millisecond, 500*time.Millisecond, 128*1024*1024)
	byPath := make(map[string]model.SampleResult)
	for _, sample := range report.Samples {
		byPath[sample.Path] = sample
	}
	if byPath[firstPath].Outcome != model.OutcomePassed {
		t.Fatalf("verifying snapshot %s was not redone: %+v", firstPath, byPath[firstPath])
	}
}

func runAudit(t *testing.T, samplesDir, checkpointPath string, concurrency int, sampleCPU, globalCPU time.Duration, memory int64) model.Report {
	t.Helper()
	options := DefaultOptions()
	options.SamplesDir = samplesDir
	options.CheckpointPath = checkpointPath
	options.Concurrency = concurrency
	options.SampleCPUBudget = sampleCPU
	options.GlobalCPUBudget = globalCPU
	options.MemoryBudget = memory
	report, err := Run(context.Background(), options)
	if err != nil {
		t.Fatalf("run audit: %v", err)
	}
	return report
}

func uniqueFindings(findings []model.Finding) bool {
	seen := make(map[string]struct{}, len(findings))
	for _, finding := range findings {
		key := finding.Code + "\x00" + finding.Field
		if finding.Certificate != nil {
			key += "\x00" + finding.Certificate.Subject + "\x00" + finding.Certificate.Serial
		}
		if _, exists := seen[key]; exists {
			return false
		}
		seen[key] = struct{}{}
	}
	return true
}
