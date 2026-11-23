package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"secverify/internal/engine"
	"secverify/internal/model"
	"secverify/internal/report"
	"secverify/internal/sample"
)

func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	opts := commonOptions{}
	addCommonFlags(fs, &opts)
	if err := fs.Parse(args); err != nil {
		return err
	}
	root, cleanup, err := prepareRoot(opts)
	if err != nil {
		return err
	}
	defer cleanup()
	fmt.Fprintf(os.Stderr, "workdir=%s\n", root)

	paths, err := writeSamples(root)
	if err != nil {
		return err
	}
	baseline, bizErr := loadBaseline(paths.Baseline)
	if bizErr != nil {
		return bizErr
	}
	engOpts, _ := engineOpts(root, "demo", opts)
	eng, err := engine.Open(engOpts)
	if err != nil {
		return err
	}
	defer eng.Close()
	if _, bizErr := eng.Initialize(context.Background(), sample.Namespace, baseline.Items); bizErr != nil {
		return bizErr
	}

	type roundInput struct {
		name string
		path string
	}
	inputs := []roundInput{
		{"tighten-account", paths.RoundOne},
		{"tighten-audit", paths.RoundTwo},
		{"add-and-disable", paths.RoundThree},
		{"delete-with-retarget", paths.RoundFour},
	}
	results := make([]engine.RoundResult, 0, len(inputs))
	for _, input := range inputs {
		changeset, readErr := readFile(input.path)
		if readErr != nil {
			return readErr
		}
		result, applyErr := eng.Apply(context.Background(), engine.ApplyRequest{
			Namespace: sample.Namespace,
			RoundName: input.name,
			Changeset: changeset,
		})
		if applyErr != nil && result.Issue == nil {
			result.Issue = &engine.Issue{Code: model.CodeStore, SourceOffset: -1, Message: applyErr.Error(), FallbackVersion: -1}
		}
		results = append(results, result)
	}

	namespace, err := namespaceReport(eng, sample.Namespace, results)
	if err != nil {
		return err
	}
	checks := buildDemoChecks(results, namespace)
	verification := report.New("demo", checks, []report.NamespaceReport{namespace}, nil, nil)
	if _, err := writeReportFile(opts.reportPath, verification); err != nil {
		return err
	}
	summarize(verification)
	if verification.Result != "PASS" {
		return fmt.Errorf("verification report result is %s", verification.Result)
	}
	return nil
}

func buildDemoChecks(results []engine.RoundResult, namespace report.NamespaceReport) []report.CheckAssertion {
	checks := make([]report.CheckAssertion, 0, 16)
	for index, result := range results {
		expected := model.CodeOK
		actual := roundCode(result)
		checks = append(checks, report.CheckAssertion{
			Name:     fmt.Sprintf("round-%02d-business-code", index+1),
			Expected: expected,
			Actual:   actual,
			Passed:   expected == actual,
		})
		allMatched := len(result.Probes) > 0
		for _, probeResult := range result.Probes {
			if !probeResult.Match || probeResult.Code != model.CodeOK {
				allMatched = false
				break
			}
		}
		checks = append(checks, report.CheckAssertion{
			Name:     fmt.Sprintf("round-%02d-probes", index+1),
			Expected: "all-match",
			Actual:   boolText(allMatched),
			Passed:   allMatched,
		})
	}
	if len(results) >= 1 {
		expected := []string{
			"acct.base.min_length", "acct.child.min_length", "audit.logon", "audit.retention",
			"svc.telemetry", "svc.defender", "reg.hklm.software.lab", "reg.hklm.defender",
		}
		checks = append(checks, closureCheck("round-01-closure", expected, results[0].Invalidation.Items))
	}
	if len(results) >= 2 {
		expected := []string{
			"audit.logon", "audit.retention", "svc.telemetry", "svc.defender",
			"reg.hklm.software.lab", "reg.hklm.defender",
		}
		checks = append(checks, closureCheck("round-02-closure", expected, results[1].Invalidation.Items))
	}
	if len(results) >= 4 {
		expected := []string{"svc.defender", "reg.hklm.defender"}
		checks = append(checks, closureCheck("round-04-delete-closure", expected, results[3].Invalidation.Items))
	}
	versions := make([]string, 0, len(namespace.VersionChain))
	for _, ref := range namespace.VersionChain {
		versions = append(versions, fmt.Sprintf("%d", ref.Version))
	}
	checks = append(checks, report.CheckAssertion{
		Name:     "snapshot-version-chain",
		Expected: "0,1,2,3,4",
		Actual:   joinStrings(versions),
		Passed:   joinStrings(versions) == "0,1,2,3,4",
	})
	return checks
}

func closureCheck(name string, expected, actual []string) report.CheckAssertion {
	expectedText := joinItems(expected)
	actualText := joinItems(actual)
	return report.CheckAssertion{Name: name, Expected: expectedText, Actual: actualText, Passed: expectedText == actualText}
}

func boolText(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func joinStrings(values []string) string {
	out := ""
	for i, value := range values {
		if i > 0 {
			out += ","
		}
		out += value
	}
	return out
}
