package walgroup

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCleanReopenPreservesLSNAndCatalog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	e, err := Open(dir, Options{SegmentMaxBytes: 360, MaxTxnsPerSegment: 2})
	if err != nil {
		t.Fatal(err)
	}
	var last uint64
	for i := 0; i < 5; i++ {
		res, err := e.Commit(ctx, TransactionInput{ID: fmt.Sprintf("clean-%d", i), Records: []PageUpdate{{PageNo: int64(i), Value: "payload"}}})
		if err != nil {
			t.Fatal(err)
		}
		last = res.EndLSN
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(dir, Options{SegmentMaxBytes: 360, MaxTxnsPerSegment: 2})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e.Close()
	if got := e.RecoveryReport(); got.Status != "CLEAN" || len(got.AbortedTxnIDs) != 0 {
		t.Fatalf("clean report = %+v", got)
	}
	res, err := e.Commit(ctx, TransactionInput{ID: "after-reopen", Records: []PageUpdate{{PageNo: 99, Value: "ok"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.StartLSN != last+1 {
		t.Fatalf("LSN after reopen = %d, want %d", res.StartLSN, last+1)
	}
}

func TestStrongKillGroupCommitRollsBackToIntent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	marker := filepath.Join(dir, "crash-marker")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
	cmd.Env = append(os.Environ(),
		"WALGROUP_CRASH_HELPER=group",
		"WALGROUP_CRASH_DIR="+dir,
		"WALGROUP_CRASH_MARKER="+marker,
	)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitForMarker(t, marker)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	_ = cmd.Wait()

	e, err := Open(dir, Options{SegmentMaxBytes: 4096, MaxTxnsPerSegment: 16})
	if err != nil {
		t.Fatalf("reopen after kill: %v", err)
	}
	defer e.Close()
	report := e.RecoveryReport()
	if report.Status != "RECOVERED" {
		t.Fatalf("report status=%s report=%+v", report.Status, report)
	}
	if !containsAll(report.AbortedTxnIDs, "crash-a", "crash-b") {
		t.Fatalf("aborted ids = %+v", report.AbortedTxnIDs)
	}
	for _, id := range []string{"crash-a", "crash-b"} {
		txn, err := e.Transaction(ctx, id)
		if err != nil || txn.State != TxnAborted {
			t.Fatalf("txn %s = %+v err=%v", id, txn, err)
		}
	}
	base, err := e.Transaction(ctx, "baseline")
	if err != nil || base.State != TxnDurable {
		t.Fatalf("baseline = %+v err=%v", base, err)
	}
	reportBytes, err := os.ReadFile(filepath.Join(dir, "recovery-report.json"))
	if err != nil || !strings.Contains(string(reportBytes), "TRUNCATED_PENDING_GROUP") {
		t.Fatalf("persisted recovery report missing detail: %s err=%v", reportBytes, err)
	}
	segs, err := e.Segments(ctx)
	if err != nil || len(segs) != 1 || segs[0].RecordCount != 1 {
		t.Fatalf("rolled back segments: %+v err=%v", segs, err)
	}
	res, err := e.Commit(ctx, TransactionInput{ID: "after-crash", Records: []PageUpdate{{PageNo: 2, Value: "new"}}})
	if err != nil || res.StartLSN != base.EndLSN+1 {
		t.Fatalf("post-crash commit = %+v err=%v", res, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(dir, Options{SegmentMaxBytes: 4096, MaxTxnsPerSegment: 16})
	if err != nil {
		t.Fatalf("second reopen: %v", err)
	}
	if got := e.RecoveryReport(); got.Status != "CLEAN" || len(got.Intents) != 0 {
		t.Fatalf("terminal intents leaked into a later clean restart: %+v", got)
	}
}

func TestStrongKillCheckpointKeepsOldMetadata(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	e, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Commit(ctx, TransactionInput{ID: "cp-a", Records: []PageUpdate{{PageNo: 1, Value: "one"}}}); err != nil {
		t.Fatal(err)
	}
	cp1, err := e.AdvanceCheckpoint(ctx, CheckpointRequest{Sequence: 1, ReplayStartLSN: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Commit(ctx, TransactionInput{ID: "cp-b", Records: []PageUpdate{{PageNo: 1, Value: "two"}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "checkpoint-marker")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
	cmd.Env = append(os.Environ(),
		"WALGROUP_CRASH_HELPER=checkpoint",
		"WALGROUP_CRASH_DIR="+dir,
		"WALGROUP_CRASH_MARKER="+marker,
	)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitForMarker(t, marker)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	_ = cmd.Wait()
	e, err = Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e.Close()
	cp, err := e.CurrentCheckpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cp == nil || cp.Sequence != cp1.Sequence || cp.Digest != cp1.Digest {
		t.Fatalf("old checkpoint was not preserved: %+v", cp)
	}
	report := e.RecoveryReport()
	if report.Status != "RECOVERED" || len(report.Intents) == 0 || report.Intents[0].Action != "CHECKPOINT_METADATA_UNCHANGED" {
		t.Fatalf("checkpoint recovery report = %+v", report)
	}
}

func TestCrashHelper(t *testing.T) {
	mode := os.Getenv("WALGROUP_CRASH_HELPER")
	if mode == "" {
		t.Skip("helper process only")
	}
	dir := os.Getenv("WALGROUP_CRASH_DIR")
	marker := os.Getenv("WALGROUP_CRASH_MARKER")
	block := func() {
		if err := os.WriteFile(marker, []byte("ready"), 0o644); err != nil {
			os.Exit(2)
		}
		select {}
	}
	opts := Options{SegmentMaxBytes: 4096, MaxTxnsPerSegment: 16}
	if mode == "group" {
		opts.CrashPoint = func(point string, details map[string]any) {
			if point != CrashAfterGroupSync {
				return
			}
			ids, _ := details["txn_ids"].([]string)
			for _, id := range ids {
				if id == "crash-a" {
					block()
				}
			}
		}
	}
	if mode == "checkpoint" {
		opts.CrashPoint = func(point string, details map[string]any) {
			if point == CrashBeforeCheckpointMeta && details["sequence"].(uint64) == 2 {
				block()
			}
		}
	}
	e, err := Open(dir, opts)
	if err != nil {
		os.Exit(3)
	}
	ctx := context.Background()
	switch mode {
	case "group":
		if _, err := e.Commit(ctx, TransactionInput{ID: "baseline", Records: []PageUpdate{{PageNo: 1, Value: "base"}}}); err != nil {
			os.Exit(4)
		}
		_, _ = e.CommitGroup(ctx, []TransactionInput{
			{ID: "crash-a", Records: []PageUpdate{{PageNo: 2, Value: "a"}}},
			{ID: "crash-b", Records: []PageUpdate{{PageNo: 3, Value: "b"}}},
		})
	case "checkpoint":
		_, err := e.AdvanceCheckpoint(ctx, CheckpointRequest{Sequence: 2, ReplayStartLSN: 2})
		if err != nil {
			os.Exit(5)
		}
	}
	os.Exit(0)
}

func waitForMarker(t *testing.T, marker string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(marker); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for child crash marker")
}

func containsAll(hay []string, needles ...string) bool {
	m := make(map[string]bool)
	for _, s := range hay {
		m[s] = true
	}
	for _, n := range needles {
		if !m[n] {
			return false
		}
	}
	return true
}

func TestReclaimMiddleSegmentAllowsValidatedLSNGap(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	e, err := Open(dir, Options{SegmentMaxBytes: 360, MaxTxnsPerSegment: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		if _, err := e.Commit(ctx, TransactionInput{ID: fmt.Sprintf("gap-%d", i), Records: []PageUpdate{{PageNo: 1, Value: "x"}}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.AdvanceCheckpoint(ctx, CheckpointRequest{Sequence: 1, ReplayStartLSN: 7}); err != nil {
		t.Fatal(err)
	}
	segs, _ := e.Segments(ctx)
	if len(segs) < 4 || segs[1].State != SegmentSealed {
		t.Fatalf("setup segments = %+v", segs)
	}
	if _, err := e.ReclaimSegments(ctx, ReclaimRequest{SegmentNos: []uint64{segs[1].SegmentNo}}); err != nil {
		t.Fatalf("middle reclaim: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(dir, Options{SegmentMaxBytes: 360, MaxTxnsPerSegment: 2})
	if err != nil {
		t.Fatalf("reopen with legitimate reclaimed gap: %v", err)
	}
	defer e.Close()
	if got := e.RecoveryReport(); got.Status != "CLEAN" {
		t.Fatalf("unexpected recovery report: %+v", got)
	}
}
