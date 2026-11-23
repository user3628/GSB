package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"secverify/internal/engine"
	"secverify/internal/model"
	"secverify/internal/report"
	"secverify/internal/sample"
	"secverify/internal/snapshot"
)

func runApplyCrash(args []string) error {
	fs := flag.NewFlagSet("__apply-crash", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var dbPath, workDir, namespace, changesetPath, crashPoint, readyPath string
	var maxTx, maxProbes int
	var waitTimeout, probeTimeout time.Duration
	fs.StringVar(&dbPath, "db", "", "database path")
	fs.StringVar(&workDir, "workdir", "", "engine work directory")
	fs.StringVar(&namespace, "namespace", "", "namespace")
	fs.StringVar(&changesetPath, "changeset", "", "changeset file")
	fs.StringVar(&crashPoint, "crash-point", "", "probe or before-commit")
	fs.StringVar(&readyPath, "ready", "", "ready marker path")
	fs.IntVar(&maxTx, "max-tx", 4, "maximum transactions")
	fs.IntVar(&maxProbes, "max-probes", 2, "maximum probes")
	fs.DurationVar(&waitTimeout, "wait-timeout", 2*time.Second, "wait timeout")
	fs.DurationVar(&probeTimeout, "probe-timeout", 10*time.Second, "probe timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if crashPoint != "probe" && crashPoint != "before-commit" {
		return fmt.Errorf("invalid crash point %q", crashPoint)
	}
	changeset, err := os.ReadFile(changesetPath)
	if err != nil {
		return err
	}
	eng, err := engine.Open(engine.Options{
		DBPath: dbPath, WorkDir: workDir, MaxTransactions: maxTx, MaxProbes: maxProbes,
		WaitTimeout: waitTimeout, ProbeTimeout: probeTimeout,
	})
	if err != nil {
		return err
	}
	defer eng.Close()
	_, applyErr := eng.Apply(context.Background(), engine.ApplyRequest{
		Namespace:  namespace,
		RoundName:  "crash-" + crashPoint,
		Changeset:  changeset,
		CrashPoint: crashPoint,
		CrashNotify: func(point string) {
			_ = os.WriteFile(readyPath, []byte(point+"\n"), 0o600)
		},
	})
	if applyErr != nil {
		return applyErr
	}
	return fmt.Errorf("apply completed without hitting crash point %s", crashPoint)
}

func runCrashDemo(args []string) error {
	fs := flag.NewFlagSet("crash-demo", flag.ContinueOnError)
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
	ctx := context.Background()
	checks := make([]report.CrashAssertion, 0, 2)
	namespaces := make([]report.NamespaceReport, 0, 2)
	for _, crashPoint := range []string{"before-commit", "probe"} {
		assertion, nsReport, err := runOneCrashScenario(ctx, root, opts, paths, crashPoint)
		if err != nil {
			return err
		}
		checks = append(checks, assertion)
		namespaces = append(namespaces, nsReport)
	}
	verification := report.New("crash-recovery", nil, namespaces, nil, checks)
	if _, err := writeReportFile(opts.reportPath, verification); err != nil {
		return err
	}
	summarize(verification)
	if verification.Result != "PASS" {
		return fmt.Errorf("crash recovery verification result is %s", verification.Result)
	}
	return nil
}

func runOneCrashScenario(ctx context.Context, root string, opts commonOptions, paths sample.PathSet, crashPoint string) (report.CrashAssertion, report.NamespaceReport, error) {
	scenario := "crash-" + crashPoint
	namespace := scenario
	engOpts, scenarioDir := engineOpts(root, scenario, opts)
	baseline, bizErr := loadBaseline(paths.Baseline)
	if bizErr != nil {
		return report.CrashAssertion{}, report.NamespaceReport{}, bizErr
	}
	roundOne, err := os.ReadFile(paths.RoundOne)
	if err != nil {
		return report.CrashAssertion{}, report.NamespaceReport{}, err
	}
	eng, err := engine.Open(engOpts)
	if err != nil {
		return report.CrashAssertion{}, report.NamespaceReport{}, err
	}
	if _, bizErr := eng.Initialize(ctx, namespace, baseline.Items); bizErr != nil {
		eng.Close()
		return report.CrashAssertion{}, report.NamespaceReport{}, bizErr
	}
	roundOneResult, applyErr := eng.Apply(ctx, engine.ApplyRequest{Namespace: namespace, RoundName: "committed-round-1", Changeset: roundOne})
	if applyErr != nil {
		eng.Close()
		return report.CrashAssertion{}, report.NamespaceReport{}, applyErr
	}
	beforeCrashChain, bizErr := eng.RawChain(ctx, namespace)
	if bizErr != nil {
		eng.Close()
		return report.CrashAssertion{}, report.NamespaceReport{}, bizErr
	}
	committed := beforeCrashChain.Latest
	committedHash := committed.ContentHash
	eng.Close()

	readyPath := filepath.Join(scenarioDir, "crash-ready")
	_ = os.Remove(readyPath)
	exe, err := os.Executable()
	if err != nil {
		return report.CrashAssertion{}, report.NamespaceReport{}, err
	}
	childArgs := []string{
		"__apply-crash",
		"--db", engOpts.DBPath,
		"--workdir", engOpts.WorkDir,
		"--namespace", namespace,
		"--changeset", paths.RoundTwo,
		"--crash-point", crashPoint,
		"--ready", readyPath,
		"--max-tx", fmt.Sprintf("%d", opts.maxTx),
		"--max-probes", fmt.Sprintf("%d", opts.maxProbes),
		"--wait-timeout", opts.waitTimeout.String(),
		"--probe-timeout", opts.probeTimeout.String(),
	}
	cmd := exec.Command(exe, childArgs...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return report.CrashAssertion{}, report.NamespaceReport{}, err
	}
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()
	if err := waitForReady(readyPath, 15*time.Second, waitCh); err != nil {
		select {
		case <-waitCh:
		default:
			_ = cmd.Process.Kill()
			<-waitCh
		}
		return report.CrashAssertion{}, report.NamespaceReport{}, fmt.Errorf("crash child (%s): %w; stderr=%s", crashPoint, err, strings.TrimSpace(stderr.String()))
	}
	if err := cmd.Process.Kill(); err != nil {
		_ = err
	}
	<-waitCh
	time.Sleep(40 * time.Millisecond)

	recoveredEngine, err := engine.Open(engOpts)
	if err != nil {
		return report.CrashAssertion{}, report.NamespaceReport{}, err
	}
	defer recoveredEngine.Close()
	recoveredChain, bizErr := recoveredEngine.RawChain(ctx, namespace)
	if bizErr != nil {
		return report.CrashAssertion{}, report.NamespaceReport{}, bizErr
	}
	recovered := recoveredChain.Latest
	committedPreserved := recovered != nil && recovered.Version == committed.Version && recovered.ContentHash == committedHash
	uncommittedDiscarded := chainVersionList(recoveredChain) == "0,1"
	restartChangeset, err := os.ReadFile(paths.RoundTwo)
	if err != nil {
		return report.CrashAssertion{}, report.NamespaceReport{}, err
	}
	restartResult, restartErr := recoveredEngine.Apply(ctx, engine.ApplyRequest{Namespace: namespace, RoundName: "restarted-round-2", Changeset: restartChangeset})
	if restartErr != nil && restartResult.Issue == nil {
		restartResult.Issue = &engine.Issue{Code: model.CodeStore, SourceOffset: -1, Message: restartErr.Error(), FallbackVersion: -1}
	}
	restartSucceeded := restartErr == nil && restartResult.SnapshotAdvanced && restartResult.VersionAfter == 2
	assertion := report.CrashAssertion{
		CrashPoint:           crashPoint,
		ExpectedVersion:      committed.Version,
		RecoveredVersion:     versionOfSnapshot(recovered),
		CommittedPreserved:   committedPreserved,
		UncommittedDiscarded: uncommittedDiscarded,
		RestartedVersion:     restartResult.VersionAfter,
		RestartSucceeded:     restartSucceeded,
	}
	assertion.Passed = committedPreserved && uncommittedDiscarded && restartSucceeded
	nsReport, err := namespaceReport(recoveredEngine, namespace, []engine.RoundResult{roundOneResult, restartResult})
	if err != nil {
		return report.CrashAssertion{}, report.NamespaceReport{}, err
	}
	return assertion, nsReport, nil
}

func waitForReady(path string, timeout time.Duration, waitCh <-chan error) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(15 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-waitCh:
			return fmt.Errorf("child exited before ready marker: %v", err)
		case <-deadline.C:
			return fmt.Errorf("ready marker timeout")
		case <-ticker.C:
			if _, err := os.Stat(path); err == nil {
				return nil
			}
		}
	}
}

func versionOfSnapshot(snap *snapshot.Snapshot) int64 {
	if snap == nil {
		return -1
	}
	return snap.Version
}
