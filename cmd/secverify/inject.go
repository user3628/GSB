package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"secverify/internal/engine"
	"secverify/internal/model"
	"secverify/internal/report"
	"secverify/internal/sample"
	"secverify/internal/snapshot"
)

type injectionResult struct {
	assertion report.InjectionAssertion
	namespace *report.NamespaceReport
}

func runInject(args []string) error {
	fs := flag.NewFlagSet("inject", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	opts := commonOptions{}
	addCommonFlags(fs, &opts)
	scenarioName := fs.String("scenario", "all", "all, missing-dependency, dependency-cycle, probe-escape, snapshot-version-gap, or concurrent-contention")
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
	ctx := context.Background()
	results := make([]injectionResult, 0, 5)
	runMode := func(name string) error {
		switch name {
		case "missing-dependency":
			result, runErr := runDependencyInjection(ctx, root, opts, paths, name, model.CodeDepMissing, false)
			if runErr == nil {
				results = append(results, result)
			}
			return runErr
		case "dependency-cycle":
			result, runErr := runDependencyInjection(ctx, root, opts, paths, name, model.CodeDepCycle, false)
			if runErr == nil {
				results = append(results, result)
			}
			return runErr
		case "probe-escape":
			result, runErr := runDependencyInjection(ctx, root, opts, paths, name, model.CodeProbeEscape, true)
			if runErr == nil {
				results = append(results, result)
			}
			return runErr
		case "snapshot-version-gap":
			result, runErr := runVersionGapInjection(ctx, root, opts, paths)
			if runErr == nil {
				results = append(results, result)
			}
			return runErr
		case "concurrent-contention":
			result, runErr := runContentionInjection(ctx, root, opts, paths)
			if runErr == nil {
				results = append(results, result)
			}
			return runErr
		default:
			return fmt.Errorf("unknown scenario %q", name)
		}
	}
	if *scenarioName == "all" {
		for _, name := range []string{"missing-dependency", "dependency-cycle", "probe-escape", "snapshot-version-gap", "concurrent-contention"} {
			if err := runMode(name); err != nil {
				return err
			}
		}
	} else if err := runMode(*scenarioName); err != nil {
		return err
	}

	assertions := make([]report.InjectionAssertion, 0, len(results))
	namespaces := make([]report.NamespaceReport, 0, len(results))
	passed := 0
	for _, result := range results {
		assertions = append(assertions, result.assertion)
		if result.namespace != nil {
			namespaces = append(namespaces, *result.namespace)
		}
		if result.assertion.Passed {
			passed++
		}
	}
	checks := []report.CheckAssertion{{
		Name:     "injection-scenarios",
		Expected: fmt.Sprintf("%d/%d-pass", len(results), len(results)),
		Actual:   fmt.Sprintf("%d/%d-pass", passed, len(results)),
		Passed:   passed == len(results),
	}}
	scenarioLabel := "inject"
	if *scenarioName != "all" {
		scenarioLabel = "inject-" + *scenarioName
	}
	verification := report.New(scenarioLabel, checks, namespaces, assertions, nil)
	if _, err := writeReportFile(opts.reportPath, verification); err != nil {
		return err
	}
	summarize(verification)
	if verification.Result != "PASS" {
		return fmt.Errorf("injection verification result is %s", verification.Result)
	}
	return nil
}

func runDependencyInjection(ctx context.Context, root string, opts commonOptions, paths sample.PathSet, name, expectedCode string, probeEscape bool) (injectionResult, error) {
	scenario := "inject-" + name
	engOpts, _ := engineOpts(root, scenario, opts)
	baseline, bizErr := loadBaseline(paths.Baseline)
	if bizErr != nil {
		return injectionResult{}, bizErr
	}
	eng, err := engine.Open(engOpts)
	if err != nil {
		return injectionResult{}, err
	}
	defer eng.Close()
	if _, bizErr := eng.Initialize(ctx, scenario, baseline.Items); bizErr != nil {
		return injectionResult{}, bizErr
	}
	beforeChain, bizErr := eng.RawChain(ctx, scenario)
	if bizErr != nil {
		return injectionResult{}, bizErr
	}
	before := latestFromChain(beforeChain)

	var changeset []byte
	var injectedBy string
	switch name {
	case "missing-dependency":
		changeset, err = readFile(paths.MissingDep)
		injectedBy = "samples/" + filepath.Base(paths.MissingDep)
	case "dependency-cycle":
		changeset, err = readFile(paths.Cycle)
		injectedBy = "samples/" + filepath.Base(paths.Cycle)
	case "probe-escape":
		changeset, err = readFile(paths.ProbeEscape)
		injectedBy = "samples/" + filepath.Base(paths.ProbeEscape)
	default:
		return injectionResult{}, fmt.Errorf("unknown dependency injection %s", name)
	}
	if err != nil {
		return injectionResult{}, err
	}
	canaryPath := filepath.Join(filepath.Dir(eng.NamespaceSandbox(scenario)), "escape-canary")
	canaryValue := []byte("CANARY_MUST_NOT_CHANGE\n")
	canaryOK := true
	if probeEscape {
		if err := os.MkdirAll(filepath.Dir(canaryPath), 0o700); err != nil {
			return injectionResult{}, err
		}
		if err := os.WriteFile(canaryPath, canaryValue, 0o600); err != nil {
			return injectionResult{}, err
		}
	}
	result, applyErr := eng.Apply(ctx, engine.ApplyRequest{Namespace: scenario, RoundName: name, Changeset: changeset})
	if applyErr != nil && result.Issue == nil {
		result.Issue = &engine.Issue{Code: model.CodeStore, SourceOffset: -1, Message: applyErr.Error(), FallbackVersion: -1}
	}
	if probeEscape {
		afterCanary, err := os.ReadFile(canaryPath)
		if err != nil || string(afterCanary) != string(canaryValue) {
			canaryOK = false
		}
	}
	afterChain, chainErr := eng.RawChain(ctx, scenario)
	if chainErr != nil {
		return injectionResult{}, chainErr
	}
	after := latestFromChain(afterChain)
	observedCode := roundCode(result)
	stateUnpolluted := sameSnapshot(before, after) && canaryOK
	notAdvanced := !result.SnapshotAdvanced && sameSnapshot(before, after)
	assertion := report.InjectionAssertion{
		Scenario:            name,
		InjectedBy:          injectedBy,
		ExpectedCode:        expectedCode,
		ObservedCode:        observedCode,
		VersionBefore:       versionOf(before),
		VersionAfter:        versionOf(after),
		FallbackVersion:     issueFallback(result),
		StateUnpolluted:     stateUnpolluted,
		SnapshotNotAdvanced: notAdvanced,
	}
	assertion.Passed = assertion.ExpectedCode == assertion.ObservedCode && stateUnpolluted && notAdvanced
	nsReport, err := namespaceReport(eng, scenario, []engine.RoundResult{result})
	if err != nil {
		return injectionResult{}, err
	}
	return injectionResult{assertion: assertion, namespace: &nsReport}, nil
}

func runVersionGapInjection(ctx context.Context, root string, opts commonOptions, paths sample.PathSet) (injectionResult, error) {
	scenario := "inject-version-gap"
	engOpts, _ := engineOpts(root, scenario, opts)
	baseline, bizErr := loadBaseline(paths.Baseline)
	if bizErr != nil {
		return injectionResult{}, bizErr
	}
	eng, err := engine.Open(engOpts)
	if err != nil {
		return injectionResult{}, err
	}
	defer eng.Close()
	if _, bizErr := eng.Initialize(ctx, scenario, baseline.Items); bizErr != nil {
		return injectionResult{}, bizErr
	}
	originChain, bizErr := eng.RawChain(ctx, scenario)
	if bizErr != nil {
		return injectionResult{}, bizErr
	}
	v0Hash := originChain.Snapshots[0].ContentHash
	rounds := make([]engine.RoundResult, 0, 3)
	for index, path := range []string{paths.RoundOne, paths.RoundTwo} {
		changeset, readErr := readFile(path)
		if readErr != nil {
			return injectionResult{}, readErr
		}
		result, applyErr := eng.Apply(ctx, engine.ApplyRequest{Namespace: scenario, RoundName: fmt.Sprintf("pre-gap-%d", index+1), Changeset: changeset})
		if applyErr != nil {
			return injectionResult{}, applyErr
		}
		rounds = append(rounds, result)
	}
	if err := eng.InjectVersionGap(ctx, scenario, 1); err != nil {
		return injectionResult{}, err
	}
	contiguous, gapErr := eng.RawChain(ctx, scenario)
	if gapErr == nil || gapErr.Code != model.CodeSnapshotVersion {
		return injectionResult{}, fmt.Errorf("version gap was not detected")
	}
	fallback := contiguous.FallbackVersion
	roundThree, readErr := readFile(paths.RoundThree)
	if readErr != nil {
		return injectionResult{}, readErr
	}
	result, applyErr := eng.Apply(ctx, engine.ApplyRequest{Namespace: scenario, RoundName: "after-gap", Changeset: roundThree})
	if applyErr != nil && result.Issue == nil {
		result.Issue = &engine.Issue{Code: model.CodeStore, SourceOffset: -1, Message: applyErr.Error(), FallbackVersion: -1}
	}
	rounds = append(rounds, result)
	afterGap, afterErr := eng.RawChain(ctx, scenario)
	if afterErr == nil || afterErr.Code != model.CodeSnapshotVersion {
		return injectionResult{}, fmt.Errorf("post-injection chain unexpectedly continued")
	}
	stateUnpolluted := len(afterGap.Snapshots) > 0 && afterGap.Snapshots[0].ContentHash == v0Hash
	notAdvanced := chainVersionList(afterGap) == chainVersionList(contiguous) && !result.SnapshotAdvanced
	assertion := report.InjectionAssertion{
		Scenario:            "snapshot-version-gap",
		InjectedBy:          "external inject command: delete committed snapshot version 1",
		ExpectedCode:        model.CodeSnapshotVersion,
		ObservedCode:        roundCode(result),
		VersionBefore:       fallback,
		VersionAfter:        result.VersionAfter,
		FallbackVersion:     fallback,
		StateUnpolluted:     stateUnpolluted,
		SnapshotNotAdvanced: notAdvanced,
	}
	assertion.Passed = assertion.ObservedCode == assertion.ExpectedCode && stateUnpolluted && notAdvanced &&
		assertion.VersionAfter == assertion.VersionBefore
	nsReport := namespaceReportFromChain(scenario, rounds, contiguous)
	return injectionResult{assertion: assertion, namespace: &nsReport}, nil
}

func runContentionInjection(ctx context.Context, root string, opts commonOptions, paths sample.PathSet) (injectionResult, error) {
	scenario := "inject-contention"
	engOpts, _ := engineOpts(root, scenario, opts)
	baseline, bizErr := loadBaseline(paths.Baseline)
	if bizErr != nil {
		return injectionResult{}, bizErr
	}
	eng, err := engine.Open(engOpts)
	if err != nil {
		return injectionResult{}, err
	}
	defer eng.Close()
	if _, bizErr := eng.Initialize(ctx, scenario, baseline.Items); bizErr != nil {
		return injectionResult{}, bizErr
	}
	beforeChain, bizErr := eng.RawChain(ctx, scenario)
	if bizErr != nil {
		return injectionResult{}, bizErr
	}
	before := latestFromChain(beforeChain)
	changeset, err := readFile(paths.Contention)
	if err != nil {
		return injectionResult{}, err
	}
	gate := make(chan struct{})
	ready := make(chan struct{})
	type applyOutcome struct {
		result engine.RoundResult
		err    error
	}
	winnerCh := make(chan applyOutcome, 1)
	go func() {
		result, applyErr := eng.Apply(ctx, engine.ApplyRequest{
			Namespace:  scenario,
			RoundName:  "contention-winner",
			Changeset:  changeset,
			ProbeGate:  gate,
			ProbeReady: func() { close(ready) },
		})
		winnerCh <- applyOutcome{result: result, err: applyErr}
	}()
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		close(gate)
		return injectionResult{}, fmt.Errorf("winner did not reach probe gate")
	}
	loser, loserErr := eng.Apply(ctx, engine.ApplyRequest{
		Namespace:   scenario,
		RoundName:   "contention-loser",
		Changeset:   changeset,
		WaitTimeout: 40 * time.Millisecond,
	})
	close(gate)
	winner := <-winnerCh
	if loserErr != nil && loser.Issue == nil {
		loser.Issue = &engine.Issue{Code: model.CodeStore, SourceOffset: -1, Message: loserErr.Error(), FallbackVersion: -1}
	}
	if winner.err != nil && winner.result.Issue == nil {
		winner.result.Issue = &engine.Issue{Code: model.CodeStore, SourceOffset: -1, Message: winner.err.Error(), FallbackVersion: -1}
	}
	afterChain, bizErr := eng.RawChain(ctx, scenario)
	if bizErr != nil {
		return injectionResult{}, bizErr
	}
	after := latestFromChain(afterChain)
	loserCode := roundCode(loser)
	winnerOK := roundCode(winner.result) == model.CodeOK && winner.result.SnapshotAdvanced
	finalVersionOK := versionOf(after) == versionOf(before)+1
	stateUnpolluted := winnerOK && finalVersionOK && winner.result.Snapshot != nil && after != nil && winner.result.Snapshot.StateHash == after.Payload.StateHash
	loserVersionAfter := loser.VersionAfter
	if loserVersionAfter < 0 {
		loserVersionAfter = versionOf(before)
	}
	notAdvanced := !loser.SnapshotAdvanced && winnerOK && finalVersionOK
	assertion := report.InjectionAssertion{
		Scenario:            "concurrent-change-contention",
		InjectedBy:          "external inject command: blocked probe slot plus 40ms wait threshold",
		ExpectedCode:        model.CodeChangeContention,
		ObservedCode:        loserCode,
		VersionBefore:       versionOf(before),
		VersionAfter:        loserVersionAfter,
		FallbackVersion:     issueFallback(loser),
		StateUnpolluted:     stateUnpolluted,
		SnapshotNotAdvanced: notAdvanced,
	}
	assertion.Passed = loserCode == model.CodeChangeContention && stateUnpolluted && notAdvanced
	nsReport, err := namespaceReport(eng, scenario, []engine.RoundResult{loser, winner.result})
	if err != nil {
		return injectionResult{}, err
	}
	return injectionResult{assertion: assertion, namespace: &nsReport}, nil
}

func sameSnapshot(left, right *snapshot.Snapshot) bool {
	if left == nil || right == nil {
		return false
	}
	return left.Version == right.Version && left.ContentHash == right.ContentHash
}

func versionOf(snap *snapshot.Snapshot) int64 {
	if snap == nil {
		return -1
	}
	return snap.Version
}

func issueFallback(result engine.RoundResult) int64 {
	if result.Issue == nil {
		return -1
	}
	return result.Issue.FallbackVersion
}
