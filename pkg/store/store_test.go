package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"local/quantops/pkg/qerr"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "checkpoint.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func expectedAll(stageConfig string) []ExpectedStage {
	var out []ExpectedStage
	for i, stage := range Stages() {
		out = append(out, ExpectedStage{Stage: stage, StageSeq: i + 1, ConfigVersion: stageConfig})
	}
	return out
}

func publishStages(t *testing.T, st *Store, shard string, ordinal int, count int) []*Published {
	t.Helper()
	var out []*Published
	version := ""
	prevDigest := "input-fp"
	for i := 1; i <= count; i++ {
		stage := StageAtMust(i)
		p, err := st.Publish(context.Background(), PublishRequest{
			ShardID: shard, ShardOrdinal: ordinal, Stage: stage, StageSeq: i,
			ExpectedVersion: version, ConfigVersion: "cfg-" + string(stage),
			InputFingerprint: prevDigest,
			Payload:          []byte("payload-" + shard + "-" + string(stage)),
		})
		if err != nil {
			t.Fatalf("publish stage %d: %v", i, err)
		}
		version = p.Progress.VersionFingerprint
		prevDigest = p.State.StageDigest
		out = append(out, p)
	}
	return out
}

func TestPublishValidationErrors(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	_, err := st.Publish(ctx, PublishRequest{
		ShardID: "s1", ShardOrdinal: 1, Stage: Forward, StageSeq: 2,
		ConfigVersion: "cfg", InputFingerprint: "x", Payload: []byte("x"),
	})
	if !qerr.IsCode(err, qerr.CodeMissingArtifact) {
		t.Fatalf("fresh forward error = %v", err)
	}

	p, err := st.Publish(ctx, PublishRequest{
		ShardID: "s1", ShardOrdinal: 1, Stage: Calibration, StageSeq: 1,
		ConfigVersion: "cfg1", InputFingerprint: "input", Payload: []byte("p1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Duplicate/retry of the completed stage is stale.
	_, err = st.Publish(ctx, PublishRequest{
		ShardID: "s1", ShardOrdinal: 1, Stage: Calibration, StageSeq: 1,
		ConfigVersion: "cfg1", InputFingerprint: "input", Payload: []byte("p1"),
	})
	if !qerr.IsCode(err, qerr.CodeStaleCheckpoint) {
		t.Fatalf("duplicate error = %v", err)
	}
	// Wrong stage at the next sequence is an illegal transition.
	_, err = st.Publish(ctx, PublishRequest{
		ShardID: "s1", ShardOrdinal: 1, Stage: Dequantization, StageSeq: 3,
		ExpectedVersion: p.Progress.VersionFingerprint, ConfigVersion: "cfg3",
		InputFingerprint: "x", Payload: []byte("p3"),
	})
	if !qerr.IsCode(err, qerr.CodeStaleCheckpoint) {
		t.Fatalf("gap error = %v", err)
	}
	// Correct sequence but wrong expected version.
	_, err = st.Publish(ctx, PublishRequest{
		ShardID: "s1", ShardOrdinal: 1, Stage: Forward, StageSeq: 2,
		ExpectedVersion: "bogus", ConfigVersion: "cfg2",
		InputFingerprint: p.State.StageDigest, Payload: []byte("p2"),
	})
	if !qerr.IsCode(err, qerr.CodeStaleCheckpoint) {
		t.Fatalf("expected-version error = %v", err)
	}
	// Cross-shard ordinal mismatch.
	_, err = st.Publish(ctx, PublishRequest{
		ShardID: "s1", ShardOrdinal: 2, Stage: Forward, StageSeq: 2,
		ExpectedVersion: p.Progress.VersionFingerprint, ConfigVersion: "cfg2",
		InputFingerprint: p.State.StageDigest, Payload: []byte("p2"),
	})
	if !qerr.IsCode(err, qerr.CodeStaleCheckpoint) {
		t.Fatalf("cross-shard error = %v", err)
	}
}

func TestReconcileDetectsReplacementAndInvalidatesDownstream(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	publishStages(t, st, "s1", 1, 4)

	// Simulate a state-table replacement with an altered payload but stale
	// digest metadata.
	if _, err := st.db.ExecContext(ctx, `UPDATE stage_states SET payload=? WHERE shard_id=? AND stage_seq=2`,
		[]byte("tampered"), "s1"); err != nil {
		t.Fatal(err)
	}
	res, err := st.Reconcile(ctx, ReconcileRequest{
		ShardID: "s1", ShardOrdinal: 1, BaseInputFingerprint: "input-fp",
		Expected: expectedAll("cfg-calibration"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Classification != ClassReplacement || res.InvalidatedFrom != 2 {
		t.Fatalf("reconcile = %+v", res)
	}
	states, err := st.States(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 || states[0].Stage != Calibration {
		t.Fatalf("states after invalidation = %+v", states)
	}
}

func TestReconcileToleranceOnlyInvalidatesVerification(t *testing.T) {
	st := openTestStore(t)
	published := publishStages(t, st, "s1", 1, 4)
	expected := []ExpectedStage{
		{Stage: Calibration, StageSeq: 1, ConfigVersion: "cfg-calibration"},
		{Stage: Forward, StageSeq: 2, ConfigVersion: "cfg-forward"},
		{Stage: Dequantization, StageSeq: 3, ConfigVersion: "cfg-dequantization"},
		{Stage: Verification, StageSeq: 4, ConfigVersion: "cfg-verification-changed"},
	}
	res, err := st.Reconcile(context.Background(), ReconcileRequest{
		ShardID: "s1", ShardOrdinal: 1, BaseInputFingerprint: "input-fp", Expected: expected,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Classification != ClassReplacement || res.InvalidatedFrom != 4 {
		t.Fatalf("reconcile = %+v", res)
	}
	states, err := st.States(context.Background(), "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 3 {
		t.Fatalf("got %d states, want 3", len(states))
	}
	if published[2].State.StageDigest != states[2].StageDigest {
		t.Fatal("upstream stage digest changed")
	}
}

func TestRollbackRejected(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	publishStages(t, st, "s1", 1, 2)
	if _, err := st.db.ExecContext(ctx, `UPDATE shard_progress SET next_stage_seq=1 WHERE shard_id='s1'`); err != nil {
		t.Fatal(err)
	}
	_, err := st.Reconcile(ctx, ReconcileRequest{
		ShardID: "s1", ShardOrdinal: 1, BaseInputFingerprint: "input-fp",
		Expected: expectedAll("cfg"),
	})
	if !qerr.IsCode(err, qerr.CodeStaleCheckpoint) {
		t.Fatalf("rollback reconcile error = %v", err)
	}
	var be *qerr.Error
	if !errors.As(err, &be) {
		t.Fatal(err)
	}
	if string(be.Details) == "" {
		t.Fatal("rollback lacks classification details")
	}
}

func TestConcurrentPublishSingleWinner(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	const n = 12
	var wg sync.WaitGroup
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := st.Publish(ctx, PublishRequest{
				ShardID: "shared", ShardOrdinal: 1, Stage: Calibration, StageSeq: 1,
				ConfigVersion: "cfg", InputFingerprint: "input", Payload: []byte("payload"),
			})
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	success, stale := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if qerr.IsCode(err, qerr.CodeStaleCheckpoint) {
			stale++
		} else {
			t.Fatalf("unexpected concurrent error: %v", err)
		}
	}
	if success != 1 || stale != n-1 {
		t.Fatalf("success=%d stale=%d", success, stale)
	}
	states, err := st.States(ctx, "shared")
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 {
		t.Fatalf("got %d active states", len(states))
	}
}

func TestSequenceCounterRollbackRejected(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	publishStages(t, st, "s1", 1, 2)
	if _, err := st.db.ExecContext(ctx, `UPDATE sequence_counter SET value=0 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	_, err := st.Reconcile(ctx, ReconcileRequest{
		ShardID: "s1", ShardOrdinal: 1, BaseInputFingerprint: "input-fp",
		Expected: expectedAll("cfg-calibration"),
	})
	if !qerr.IsCode(err, qerr.CodeStaleCheckpoint) {
		t.Fatalf("counter rollback error = %v", err)
	}
}

func TestBudgetRejectionsRoundTrip(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	in := []BudgetRejection{{
		ShardID: "s9", ShardOrdinal: 9, EstimatedElements: 99, WorkingSetBytes: 1024,
		BudgetRemaining: 10, MemoryRemaining: 20, Reason: "budget", Code: qerr.CodeBudgetExceeded,
	}}
	if err := st.SaveBudgetRejections(ctx, in); err != nil {
		t.Fatal(err)
	}
	out, err := st.BudgetRejections(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].ShardID != "s9" || out[0].EstimatedElements != 99 {
		t.Fatalf("round trip = %+v", out)
	}
}

func TestGuardRejectsWholeDatabaseReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	publishStages(t, st, "s1", 1, 1)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".guard"); err != nil {
		t.Fatalf("guard sidecar missing: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	_, err = Open(path)
	if !qerr.IsCode(err, qerr.CodeStaleCheckpoint) {
		t.Fatalf("replacement open error = %v", err)
	}
}

func TestGuardRejectsProgressSnapshotRollback(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	publishStages(t, st, "s1", 1, 2)
	if _, err := st.db.ExecContext(ctx, `UPDATE shard_progress SET global_sequence=1 WHERE shard_id='s1'`); err != nil {
		t.Fatal(err)
	}
	_, err := st.Reconcile(ctx, ReconcileRequest{
		ShardID: "s1", ShardOrdinal: 1, BaseInputFingerprint: "input-fp",
		Expected: expectedAll("cfg"),
	})
	if !qerr.IsCode(err, qerr.CodeStaleCheckpoint) {
		t.Fatalf("guard rollback error = %v", err)
	}
}
