package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"secverify/internal/engine"
	"secverify/internal/model"
	"secverify/internal/report"
)

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func runExternal(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var workdir, dbPath, baselinePath, namespace, reportPath string
	var changesets stringList
	var maxTx, maxProbes int
	var waitTimeout, probeTimeout time.Duration
	fs.StringVar(&workdir, "workdir", "", "engine working directory (default: fresh temp)")
	fs.StringVar(&dbPath, "db", "", "SQLite path (default: WORKDIR/engine.db)")
	fs.StringVar(&baselinePath, "baseline", "", "baseline JSON file")
	fs.Var(&changesets, "changeset", "length-prefixed changeset file; repeatable")
	fs.StringVar(&namespace, "namespace", "", "snapshot namespace (default: baseline namespace)")
	fs.StringVar(&reportPath, "report", "-", "report path, or - for stdout")
	fs.IntVar(&maxTx, "max-tx", 4, "maximum in-flight transactions")
	fs.IntVar(&maxProbes, "max-probes", 2, "maximum in-flight probes")
	fs.DurationVar(&waitTimeout, "wait-timeout", 2*time.Second, "contention wait threshold")
	fs.DurationVar(&probeTimeout, "probe-timeout", 10*time.Second, "probe subprocess timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if baselinePath == "" {
		return fmt.Errorf("--baseline is required")
	}
	cleanup := func() {}
	if workdir == "" {
		dir, err := os.MkdirTemp("", "secverify-run-")
		if err != nil {
			return err
		}
		workdir = dir
		cleanup = func() { _ = os.RemoveAll(dir) }
	} else if err := os.MkdirAll(workdir, 0o700); err != nil {
		return err
	}
	defer cleanup()
	if dbPath == "" {
		dbPath = filepath.Join(workdir, "engine.db")
	}
	baseline, bizErr := loadBaseline(baselinePath)
	if bizErr != nil {
		return bizErr
	}
	if namespace == "" {
		namespace = baseline.Namespace
	}
	eng, err := engine.Open(engine.Options{
		DBPath: dbPath, WorkDir: workdir, MaxTransactions: maxTx, MaxProbes: maxProbes,
		WaitTimeout: waitTimeout, ProbeTimeout: probeTimeout,
	})
	if err != nil {
		return err
	}
	defer eng.Close()
	initResult, bizErr := eng.Initialize(context.Background(), namespace, baseline.Items)
	if bizErr != nil {
		return bizErr
	}
	rounds := make([]engine.RoundResult, 0, len(changesets))
	checks := make([]report.CheckAssertion, 0, len(changesets))
	for _, path := range changesets {
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		result, applyErr := eng.Apply(context.Background(), engine.ApplyRequest{
			Namespace: namespace, RoundName: filepath.Base(path), Changeset: data,
		})
		if applyErr != nil && result.Issue == nil {
			result.Issue = &engine.Issue{Code: model.CodeStore, SourceOffset: -1, Message: applyErr.Error(), FallbackVersion: -1}
		}
		rounds = append(rounds, result)
		checks = append(checks, report.CheckAssertion{
			Name:     "changeset-" + filepath.Base(path),
			Expected: model.CodeOK,
			Actual:   roundCode(result),
			Passed:   roundCode(result) == model.CodeOK,
		})
		if applyErr != nil {
			break
		}
	}
	nsReport, err := namespaceReport(eng, namespace, rounds)
	if err != nil {
		return err
	}
	initCheck := report.CheckAssertion{
		Name: "namespace-initialized", Expected: "genesis-version-0",
		Actual: fmt.Sprintf("version-%d", initResult.Version), Passed: initResult.Version == 0,
	}
	if initResult.Existed {
		initCheck.Expected = "existing-namespace"
		initCheck.Passed = true
	}
	checks = append(checks, initCheck)
	verification := report.New("run", checks, []report.NamespaceReport{nsReport}, nil, nil)
	if _, err := writeReportFile(reportPath, verification); err != nil {
		return err
	}
	summarize(verification)
	if verification.Result != "PASS" {
		return fmt.Errorf("external verification result is %s", verification.Result)
	}
	return nil
}
