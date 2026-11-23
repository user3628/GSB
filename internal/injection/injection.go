package injection

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"internalpki/workbench/internal/model"
	"internalpki/workbench/internal/samplegen"
	"internalpki/workbench/internal/workbench"
)

type Check struct {
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Message string `json:"message"`
}

type Result struct {
	Report model.Report `json:"report"`
	Checks []Check      `json:"checks"`
	Passed bool         `json:"passed"`
}

// Verify generates the four required malformed fixture families plus normal
// controls, audits them through the normal scheduler/report pipeline, and
// asserts stable business codes and count invariants.
func Verify(ctx context.Context, outputDir, checkpointPath string) (Result, error) {
	if _, err := samplegen.Generate(outputDir, samplegen.Options{IncludeAnomalies: true}); err != nil {
		return Result{}, err
	}
	options := workbench.DefaultOptions()
	options.SamplesDir = outputDir
	options.CheckpointPath = checkpointPath
	options.Concurrency = 4
	options.SampleCPUBudget = 250 * time.Millisecond
	options.GlobalCPUBudget = 4 * time.Second
	options.MemoryBudget = 512 * 1024 * 1024
	report, err := workbench.Run(ctx, options)
	if err != nil {
		return Result{}, err
	}

	expected := map[string]string{
		"anomaly/missing-intermediate": model.CodeChainBroken,
		"anomaly/sni-mismatch":         model.CodeHostnameMismatch,
		"anomaly/expired":              model.CodeExpired,
		"anomaly/untrusted-root":       model.CodeChainUntrusted,
	}
	paths := make([]string, 0, len(expected))
	for path := range expected {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	byPath := make(map[string]model.SampleResult, len(report.Samples))
	for _, sample := range report.Samples {
		byPath[sample.Path] = sample
	}
	var checks []Check
	for _, path := range paths {
		sample, ok := byPath[path]
		if !ok {
			checks = append(checks, Check{Name: path, Passed: false, Message: "sample missing from report"})
			continue
		}
		passed := sample.BusinessCode == expected[path]
		checks = append(checks, Check{
			Name:    path,
			Passed:  passed,
			Message: fmt.Sprintf("expected=%s actual=%s", expected[path], sample.BusinessCode),
		})
	}

	for _, sample := range report.Samples {
		if !strings.HasPrefix(sample.Path, "normal/") {
			continue
		}
		checks = append(checks, Check{
			Name:    sample.Path,
			Passed:  sample.Outcome == model.OutcomePassed,
			Message: "normal control must remain unaffected: " + sample.BusinessCode,
		})
	}

	for _, sample := range report.Samples {
		checks = append(checks, Check{
			Name:    sample.Path + "#expectation",
			Passed:  sample.ExpectationMatched,
			Message: fmt.Sprintf("expected=%s actual=%s", sample.ExpectedResult, sample.ActualResult),
		})
		checks = append(checks, Check{
			Name:    sample.Path + "#unique_findings",
			Passed:  uniqueFindings(sample.Findings),
			Message: "the same structured deviation must appear only once",
		})
	}

	countsOK := report.Summary.FullyAccounted &&
		report.Summary.Completed+report.Summary.Failed+report.Summary.Degraded == report.Summary.Total
	checks = append(checks, Check{
		Name:    "global_counts",
		Passed:  countsOK,
		Message: fmt.Sprintf("total=%d completed=%d failed=%d degraded=%d", report.Summary.Total, report.Summary.Completed, report.Summary.Failed, report.Summary.Degraded),
	})

	passed := len(checks) > 0
	for _, check := range checks {
		if !check.Passed {
			passed = false
			break
		}
	}
	sort.SliceStable(checks, func(i, j int) bool { return checks[i].Name < checks[j].Name })
	return Result{Report: report, Checks: checks, Passed: passed}, nil
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

// RequirePassed converts injection checks into one aggregate error.
func RequirePassed(result Result) error {
	if result.Passed {
		return nil
	}
	var failures []string
	for _, check := range result.Checks {
		if !check.Passed {
			failures = append(failures, check.Name+": "+check.Message)
		}
	}
	return errors.New("injection verification failed: " + strings.Join(failures, "; "))
}
