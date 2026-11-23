package configimpact

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
)

const crashHelperEnv = "CONFIGIMPACT_CRASH_HELPER"

func TestForcedTerminationHelper(t *testing.T) {
	if os.Getenv(crashHelperEnv) != "1" {
		t.Skip("helper process only")
	}
	dbPath := os.Getenv("CONFIGIMPACT_CRASH_DB")
	engine, err := Open(context.Background(), dbPath, Options{})
	if err != nil {
		t.Fatalf("helper open: %v", err)
	}
	defer engine.Close()
	engine.testHookAfterIntent = func() {
		// This is a real process termination: no deferred rollback, close, or
		// cleanup is allowed to run.
		os.Exit(91)
	}
	patch := Patch{
		ID:          "crash-patch",
		BaseVersion: 1,
		Changes: []PatchChange{{
			Key:       "db.pool_size",
			Scope:     "component",
			Component: "db",
			Type:      "int",
			Value:     json.RawMessage("16"),
			Order:     20,
		}},
	}
	_, err = engine.SubmitPatch(context.Background(), patch)
	if err == nil {
		t.Fatalf("helper patch unexpectedly completed without crash")
	}
	t.Fatalf("helper patch returned before crash hook: %v", err)
}

func TestRecoveryAfterForcedTermination(t *testing.T) {
	engine, dbPath := newTestEngine(t, Options{})
	loadSample(t, engine)
	if err := engine.Close(); err != nil {
		t.Fatalf("close seed engine: %v", err)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestForcedTerminationHelper$", "-test.count=1")
	command.Env = append(os.Environ(),
		crashHelperEnv+"=1",
		"CONFIGIMPACT_CRASH_DB="+dbPath,
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("helper process did not terminate; output=%s", output)
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("helper launch failed: %v output=%s", err, output)
	}
	if exitErr.ExitCode() != 91 {
		t.Fatalf("helper exit code = %d, want 91; output=%s", exitErr.ExitCode(), output)
	}

	recovered, err := Open(context.Background(), dbPath, Options{})
	if err != nil {
		t.Fatalf("recovery open: %v", err)
	}
	defer recovered.Close()
	report := recovered.RecoveryReport()
	if !report.ScanPerformed || report.PendingIntentsScanned != 1 || len(report.AbortedIntents) != 1 {
		t.Fatalf("unexpected recovery scan: %+v", report)
	}
	if report.LastCompleteVersion != 1 || len(report.CompleteDocumentVersions) != 1 || report.CompleteDocumentVersions[0] != 1 {
		t.Fatalf("recovery promoted an incomplete version: %+v", report)
	}
	aborted := report.AbortedIntents[0]
	if aborted.PatchID != "crash-patch" || aborted.TargetVersion != 2 || aborted.Status != "aborted" {
		t.Fatalf("unexpected aborted intent: %+v", aborted)
	}
	version, err := recovered.CurrentVersion(context.Background())
	if err != nil || version != 1 {
		t.Fatalf("current version after recovery = %d err=%v", version, err)
	}
	var documentRows int
	if err := recovered.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM documents`).Scan(&documentRows); err != nil {
		t.Fatalf("count documents: %v", err)
	}
	if documentRows != 1 {
		t.Fatalf("document rows = %d, want only complete version 1", documentRows)
	}
	var patchState string
	if err := recovered.db.QueryRowContext(context.Background(), `SELECT state FROM patch_log WHERE patch_id='crash-patch'`).Scan(&patchState); err != nil {
		t.Fatalf("read aborted patch log: %v", err)
	}
	if patchState != "aborted" {
		t.Fatalf("patch state = %s, want aborted", patchState)
	}
	if _, err := recovered.Read(context.Background(), ReadRequest{ComponentID: "db", ObservedVersion: 2}); !IsCode(err, ErrStaleRead) {
		t.Fatalf("version 2 must not be readable after crash: %v", err)
	}

	patch := Patch{
		ID:          "crash-patch",
		BaseVersion: 1,
		Changes: []PatchChange{{
			Key:       "db.pool_size",
			Scope:     "component",
			Component: "db",
			Type:      "int",
			Value:     json.RawMessage("16"),
			Order:     20,
		}},
	}
	result, err := recovered.SubmitPatch(context.Background(), patch)
	if err != nil {
		t.Fatalf("retry aborted patch: %v", err)
	}
	if result.DocumentVersion != 2 || result.Duplicate {
		t.Fatalf("retry result: %+v", result)
	}
	read, err := recovered.Read(context.Background(), ReadRequest{ComponentID: "db", ObservedVersion: 2})
	if err != nil {
		t.Fatalf("read retried patch: %v", err)
	}
	if got := jsonString(t, requiredValue(t, read.Config, "db.pool_size").Value); got != "16" {
		t.Fatalf("retried patch value = %s", got)
	}

	// A second clean startup proves the recovery report contains only persisted
	// facts and no lingering pending intent.
	if err := recovered.Close(); err != nil {
		t.Fatalf("close recovered engine: %v", err)
	}
	clean, err := Open(context.Background(), dbPath, Options{})
	if err != nil {
		t.Fatalf("clean reopen: %v", err)
	}
	defer clean.Close()
	cleanReport := clean.RecoveryReport()
	if cleanReport.PendingIntentsScanned != 0 || len(cleanReport.AbortedIntents) != 0 {
		t.Fatalf("clean startup reported fake recovery work: %+v", cleanReport)
	}
	if cleanReport.LastCompleteVersion != 2 {
		t.Fatalf("clean startup last complete version = %d", cleanReport.LastCompleteVersion)
	}
}
