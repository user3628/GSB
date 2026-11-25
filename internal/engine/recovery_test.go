package engine

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"alarmengine/internal/sample"
	"alarmengine/internal/storage"
)

// TestCrashRecoveryAfterIngestIntent spawns a helper process that boots the
// engine, writes the durable ingest intent and then hard-exits (simulating
// SIGKILL) before the domain transaction commits. Reopening the same database
// must roll the dangling work back using only durable facts.
func TestCrashRecoveryAfterIngestIntent(t *testing.T) {
	if os.Getenv("AE_CRASH_HELPER") == "1" {
		crashHelperIngest(t)
		return
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "engine.db")

	cmd := exec.Command(os.Args[0], "-test.run=TestCrashRecoveryAfterIngestIntent", "-test.v")
	cmd.Env = append(os.Environ(),
		"AE_CRASH_HELPER=1",
		"AE_DB_PATH="+dbPath,
		"AE_CRASH_POINT="+string(CrashAfterIntentIngest),
	)
	out, err := cmd.CombinedOutput()
	if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 97 {
		t.Fatalf("helper did not hard-exit with 97: %v\n%s", err, out)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("database not persisted before crash: %v", err)
	}

	// Reopen on a fresh process: recovery must run automatically.
	st, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := New(st, Config{Now: func() int64 { return 2 }})
	if err != nil {
		t.Fatalf("reopen after crash failed: %v", err)
	}
	defer func() { eng.Close(); _ = st.Close() }()

	report := eng.RecoveryReportOf()
	if len(report.RolledBackIntents) == 0 {
		t.Fatalf("recovery found no unfinished intents: %+v", report)
	}
	if report.LastGoodVersion != 1 {
		t.Fatalf("last good version want 1, got %d", report.LastGoodVersion)
	}
	if eng.Version() != 1 {
		t.Fatalf("active version must roll back to 1, got %d", eng.Version())
	}
	// The uncommitted observations must not be visible.
	if n, _ := storage.CountObservations(eng.Store().ReadDB()); n != 0 {
		t.Fatalf("dangling observations survived crash: %d", n)
	}
	// No derived rows may exist for the rolled-back version 2.
	alerts, _ := eng.QueryAlerts(2)
	if len(alerts) != 0 {
		t.Fatalf("derived alerts survived rollback: %d", len(alerts))
	}
	// The old snapshot (v1) remains fully usable.
	if eng.Snapshot().Version != 1 {
		t.Fatal("previous snapshot not usable after rollback")
	}
	// The crashed request id can now be re-submitted cleanly (ledger reconciled).
	data := sample.Generate()
	res, err := eng.IngestObservations(IngestRequest{
		RequestID: "req-crash", BaseVersion: 1, Observations: data.Observations,
	})
	if err != nil {
		t.Fatalf("re-submit after recovery failed: %v", err)
	}
	if res.CommitVersion != 2 {
		t.Fatalf("re-submit should commit version 2, got %d", res.CommitVersion)
	}
}

// crashHelperIngest runs inside the spawned process and expects os.Exit(97).
func crashHelperIngest(t *testing.T) {
	dbPath := os.Getenv("AE_DB_PATH")
	st, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		CrashPoint: CrashPoint(os.Getenv("AE_CRASH_POINT")),
		Now:        func() int64 { return 1 },
	}
	eng, err := New(st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	data := sample.Generate()
	if err := eng.BootstrapServices(data.Services); err != nil {
		t.Fatal(err)
	}
	if err := eng.BootstrapPolicy(data.Policy); err != nil {
		t.Fatal(err)
	}
	_, _ = eng.IngestObservations(IngestRequest{
		RequestID: "req-crash", BaseVersion: 1, Observations: data.Observations,
	})
	t.Fatal("process should have hard-exited at crash point")
}

func TestRecoveryNoPendingIntentsIsClean(t *testing.T) {
	eng, data := bootstrappedEngine(t)
	ingestSeed(t, eng, data)
	report, err := eng.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(report.RolledBackIntents) != 0 {
		t.Fatalf("clean db should have no pending intents: %+v", report)
	}
}
