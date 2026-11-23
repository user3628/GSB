package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"secverify/internal/report"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "__apply-crash" {
		if err := runApplyCrash(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "test apply-crash:", err)
			os.Exit(2)
		}
		return
	}
	os.Exit(m.Run())
}

func TestCLIEndToEndDeterministicAndAnomalies(t *testing.T) {
	run := func(args ...string) {
		t.Helper()
		if len(args) == 0 {
			t.Fatal("missing command")
		}
		var err error
		switch args[0] {
		case "demo":
			err = runDemo(args[1:])
		case "inject":
			err = runInject(args[1:])
		case "crash-demo":
			err = runCrashDemo(args[1:])
		case "run":
			err = runExternal(args[1:])
		default:
			t.Fatalf("unknown test command %q", args[0])
		}
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}

	demoRootA := t.TempDir()
	demoReportA := filepath.Join(t.TempDir(), "demo-a.json")
	run("demo", "--workdir", demoRootA, "--report", demoReportA)
	demoRootB := t.TempDir()
	demoReportB := filepath.Join(t.TempDir(), "demo-b.json")
	run("demo", "--workdir", demoRootB, "--report", demoReportB)
	reportA, err := os.ReadFile(demoReportA)
	if err != nil {
		t.Fatal(err)
	}
	reportB, err := os.ReadFile(demoReportB)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reportA, reportB) {
		t.Fatal("demo reports are not byte-identical")
	}
	var demo report.VerificationReport
	if err := json.Unmarshal(reportA, &demo); err != nil {
		t.Fatal(err)
	}
	if demo.Result != "PASS" || len(demo.Checks) != 12 || len(demo.Namespaces) != 1 {
		t.Fatalf("unexpected demo report: result=%s checks=%d namespaces=%d", demo.Result, len(demo.Checks), len(demo.Namespaces))
	}

	externalReport := filepath.Join(t.TempDir(), "run.json")
	run("run",
		"--workdir", t.TempDir(),
		"--baseline", filepath.Join(demoRootA, "samples", "baseline.json"),
		"--changeset", filepath.Join(demoRootA, "samples", "round-01-tighten-account.changes"),
		"--report", externalReport,
	)
	externalData, err := os.ReadFile(externalReport)
	if err != nil {
		t.Fatal(err)
	}
	var external report.VerificationReport
	if err := json.Unmarshal(externalData, &external); err != nil {
		t.Fatal(err)
	}
	if external.Result != "PASS" || len(external.Namespaces) != 1 || external.Namespaces[0].FinalVersion != 1 {
		t.Fatalf("unexpected external run report: result=%s namespaces=%+v", external.Result, external.Namespaces)
	}

	injectReport := filepath.Join(t.TempDir(), "inject.json")
	run("inject", "--workdir", t.TempDir(), "--report", injectReport)
	injectData, err := os.ReadFile(injectReport)
	if err != nil {
		t.Fatal(err)
	}
	var injection report.VerificationReport
	if err := json.Unmarshal(injectData, &injection); err != nil {
		t.Fatal(err)
	}
	if injection.Result != "PASS" || len(injection.Injections) != 5 {
		t.Fatalf("unexpected injection report: result=%s injections=%d", injection.Result, len(injection.Injections))
	}
	for _, assertion := range injection.Injections {
		if !assertion.Passed || assertion.ExpectedCode != assertion.ObservedCode || !assertion.StateUnpolluted || !assertion.SnapshotNotAdvanced {
			t.Fatalf("failed injection assertion: %+v", assertion)
		}
	}

	crashReport := filepath.Join(t.TempDir(), "crash.json")
	run("crash-demo", "--workdir", t.TempDir(), "--report", crashReport)
	crashData, err := os.ReadFile(crashReport)
	if err != nil {
		t.Fatal(err)
	}
	var crash report.VerificationReport
	if err := json.Unmarshal(crashData, &crash); err != nil {
		t.Fatal(err)
	}
	if crash.Result != "PASS" || len(crash.CrashChecks) != 2 {
		t.Fatalf("unexpected crash report: result=%s checks=%d", crash.Result, len(crash.CrashChecks))
	}
	for _, check := range crash.CrashChecks {
		if !check.Passed || !check.CommittedPreserved || !check.UncommittedDiscarded || !check.RestartSucceeded {
			t.Fatalf("failed crash check: %+v", check)
		}
	}
}
