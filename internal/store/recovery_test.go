package store

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"logagg/internal/agg"
)

func TestSIGKILLIntentRecoveryFromSQLiteFacts(t *testing.T) {
	if os.Getenv("LOGAGG_CRASH_HELPER") == "1" {
		runCrashHelper(t)
		return
	}

	dbPath := filepath.Join(t.TempDir(), "crash-recovery.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestSIGKILLIntentRecoveryFromSQLiteFacts$", "-test.v")
	cmd.Env = append(os.Environ(), "LOGAGG_CRASH_HELPER=1", "LOGAGG_CRASH_DB="+dbPath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	scanner := bufio.NewScanner(stdout)
	ready := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "READY" {
			ready = true
			break
		}
	}
	if !ready {
		t.Fatal("crash helper did not become ready")
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	st, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	report, err := st.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.RolledBack) != 1 {
		t.Fatalf("rolled_back=%d want 1: %+v", len(report.RolledBack), report)
	}
	if len(report.NeedsRecompute) != 1 {
		t.Fatalf("needs_recompute=%d want 1: %+v", len(report.NeedsRecompute), report)
	}
	need := report.NeedsRecompute[0]
	if need.SessionID != "crash-session" || need.BlockNo != 4 || need.SegmentStart != 20 || need.SegmentEnd != 40 {
		t.Fatalf("unexpected recompute ref: %+v", need)
	}
	if len(report.ReusablePartialSums) != 1 {
		t.Fatalf("reusable partials=%d want 1", len(report.ReusablePartialSums))
	}

	recomputed := agg.NewSegmentSum("crash-session", "shard-local-001", 1, 3, 4, 20, 40, "def")
	groups := make([]int64, 20)
	metrics := make([]int64, 20)
	for i := range groups {
		groups[i] = 1
		metrics[i] = int64(100 + i)
	}
	if err := recomputed.AddRows(groups, metrics); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitPartialSum(ctx, recomputed, ""); err != nil {
		t.Fatal(err)
	}
	secondReport, err := st.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondReport.NeedsRecompute) != 0 {
		t.Fatalf("after recompute needs=%+v", secondReport.NeedsRecompute)
	}
	if len(secondReport.ReusablePartialSums) != 2 {
		t.Fatalf("reusable=%d want 2", len(secondReport.ReusablePartialSums))
	}
}

func runCrashHelper(t *testing.T) {
	dbPath := os.Getenv("LOGAGG_CRASH_DB")
	if dbPath == "" {
		t.Fatal("LOGAGG_CRASH_DB is required")
	}
	ctx := context.Background()
	st, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	committed := agg.NewSegmentSum("crash-session", "shard-local-001", 1, 3, 4, 0, 2, "def")
	if err := committed.AddRows([]int64{1, 2}, []int64{10, 20}); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitPartialSum(ctx, committed, ""); err != nil {
		t.Fatal(err)
	}
	intent := Intent{
		ID: "segment-start:crash-pending", Kind: "segment_start", Status: "pending",
		SessionID: "crash-session", ShardID: "shard-local-001", Version: 1,
		BlockNo: 4, SegmentStart: 20, SegmentEnd: 40,
	}
	if err := st.BeginSegment(ctx, intent, 3); err != nil {
		t.Fatal(err)
	}
	fmt.Println("READY")
	os.Stdout.Sync()
	select {}
}

func TestRecoverReconcilesPendingIntentWithCommittedFact(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "reconcile.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.InsertIntent(ctx, Intent{
		ID: "partial-pending-fact", Kind: "partial_commit", Status: "pending",
		SessionID: "s1", ShardID: "shard", Version: 2, BlockNo: 1, SegmentStart: 0, SegmentEnd: 1,
	}); err != nil {
		t.Fatal(err)
	}
	sum := agg.NewSegmentSum("s1", "shard", 2, 0, 1, 0, 1, "def")
	if err := sum.AddRows([]int64{1}, []int64{42}); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitPartialSum(ctx, sum, ""); err != nil {
		t.Fatal(err)
	}
	report, err := st.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Committed) != 1 || report.Committed[0].IntentID != "partial-pending-fact" {
		t.Fatalf("expected committed reconciliation, got %+v", report)
	}
	if len(report.NeedsRecompute) != 0 {
		t.Fatalf("unexpected recompute: %+v", report.NeedsRecompute)
	}
}
