package service

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"gpusched/internal/domain"
	"gpusched/internal/store"
)

// TestCrashRecoverySubprocess is the end-to-end forced-termination test. The
// child process loads data, admits requests, arms a crash at the round commit
// point and is terminated (exit 137) mid-arbitration. The parent reopens the
// SQLite file and asserts recovery returns the system to the last complete
// version, reporting only durable facts.
func TestCrashRecoverySubprocess(t *testing.T) {
	if os.Getenv("GPU_CRASH_CHILD") == "1" {
		runCrashChild(t)
		return
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "child.db")

	cmd := exec.Command(os.Args[0], "-test.run=TestCrashRecoverySubprocess")
	cmd.Env = append(os.Environ(),
		"GPU_CRASH_CHILD=1",
		"GPU_CRASH_DB="+dbPath,
		"GPU_CRASH_POINT="+os.Getenv("GPU_CRASH_POINT"),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 137 {
		t.Fatalf("child should exit 137 (SIGKILL-like), got %v", err)
	}

	// Give the OS a moment to release file handles, then reopen like a restart.
	time.Sleep(200 * time.Millisecond)
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer st.Close()
	app := New(st)
	report, err := app.Recover(context.Background())
	if err != nil {
		t.Fatalf("recover: %v", err)
	}

	ctx := context.Background()
	for _, id := range []string{"r1", "r2", "r3"} {
		req, gerr := app.GetRequest(ctx, id)
		if gerr != nil {
			t.Fatalf("get %s: %v", id, gerr)
		}
		if req.State != domain.StateAdmitted {
			t.Errorf("request %s state=%s want ADMITTED (last complete version)", id, req.State)
		}
	}
	// No live BATCHED/RUNNING request may survive an incomplete round.
	reqs, _ := app.ListRequests(ctx)
	for _, r := range reqs {
		if r.State == domain.StateBatched || r.State == domain.StateRunning {
			t.Errorf("request %s left in %s after recovery", r.ID, r.State)
		}
	}
	// Quota invariant must still hold after recovery.
	quota, _ := app.QueryQuota(ctx)
	for _, g := range quota.Groups {
		nodeMem, nodeComp := int64(0), int64(0)
		for _, n := range g.Nodes {
			nodeMem += n.MemoryUsed
			nodeComp += n.ComputeUsedM
		}
		if nodeMem != g.MemoryReserved || nodeComp != g.ComputeReservedM {
			t.Errorf("post-recovery invariant broken for %s", g.GroupID)
		}
	}
	// The crash fired before commit, so SQLite atomically discarded the whole
	// round tx: no running round and no batch rows remain. Recovery must still
	// produce a fact-based report without inventing aborted batches.
	if len(report.RunningRounds) != 0 {
		t.Errorf("no running round may remain after recovery: %+v", report.RunningRounds)
	}
	t.Logf("recovery report (pre-commit crash): %+v", report)
}

// runCrashChild executes inside the subprocess and terminates itself.
func runCrashChild(t *testing.T) {
	dbPath := os.Getenv("GPU_CRASH_DB")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	app := New(st)
	set := seededSet(t)
	if err := app.Load(context.Background(), set); err != nil {
		t.Fatalf("load: %v", err)
	}
	ctx := context.Background()
	for _, id := range []string{"r1", "r2", "r3"} {
		if _, err := app.AdmitOne(ctx, id); err != nil {
			t.Fatalf("admit %s: %v", id, err)
		}
	}
	// Real forced termination at the durability boundary, mimicking kill -9.
	crashPoint := os.Getenv("GPU_CRASH_POINT")
	if crashPoint == "" {
		crashPoint = "round:before_commit"
	}
	app.SetCrashHook(func(point string) {
		if point == crashPoint {
			os.Exit(137)
		}
	})
	if _, err := app.RunRound(ctx, 0); err != nil {
		t.Logf("round returned after crash hook (unexpected): %v", err)
	}
}

// TestCrashAfterCommitKeepsCompleteRound verifies the durability side: a kill
// after the round transaction commits leaves the round and batches intact.
func TestCrashAfterCommitKeepsCompleteRound(t *testing.T) {
	if os.Getenv("GPU_CRASH_CHILD") == "1" {
		runCrashChild(t)
		return
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "committed.db")

	cmd := exec.Command(os.Args[0], "-test.run=TestCrashAfterCommitKeepsCompleteRound")
	cmd.Env = append(os.Environ(),
		"GPU_CRASH_CHILD=1",
		"GPU_CRASH_DB="+dbPath,
		"GPU_CRASH_POINT=round:after_commit",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 137 {
			t.Fatalf("child exit: %v", err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st.Close()
	app := New(st)
	report, err := app.Recover(context.Background())
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(report.RunningRounds) != 0 {
		t.Errorf("committed round must not be reported as running: %+v", report.RunningRounds)
	}
	batches, err := app.ListBatches(context.Background())
	if err != nil {
		t.Fatalf("batches: %v", err)
	}
	if len(batches) == 0 {
		t.Errorf("committed batches must survive a post-commit kill")
	}
}
