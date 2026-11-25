package store_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"deployverify/internal/demo"
	"deployverify/internal/derive"
	"deployverify/internal/model"
	"deployverify/internal/spec"
	"deployverify/internal/store"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(context.Background(), store.DSN(filepath.Join(t.TempDir(), "test.db")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func commitIncrement(t *testing.T, st *store.Store, current *model.Snapshot, increment *model.Increment) (*derive.Bundle, error) {
	t.Helper()
	candidate, err := spec.ApplyIncrement(current, increment)
	if err != nil {
		t.Fatal(err)
	}
	changes := []derive.Change{}
	for _, change := range increment.Changes {
		var old model.Unit
		for _, env := range current.Environments {
			if env.Name != change.Environment {
				continue
			}
			for _, unit := range env.Units {
				if unit.Service == change.Service {
					old = unit
				}
			}
		}
		changes = append(changes, derive.Change{Ref: model.Ref{Environment: change.Environment, Service: change.Service}, Fields: derive.ChangedFields(old, change.Unit)})
	}
	return st.CommitIncrement(context.Background(), store.IncrementCommit{Increment: increment, Candidate: candidate, Changes: changes})
}

func TestSnapshotCommitLoadsRelationalStateAndDerivedRefs(t *testing.T) {
	st := newTestStore(t)
	snapshot := demo.SnapshotV1()
	bundle, err := st.CommitSnapshot(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Plans) == 0 || len(bundle.Refs) == 0 {
		t.Fatal("derived bundle is empty")
	}
	loaded, err := st.LoadSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version != 1 || len(loaded.Environments) != 3 {
		t.Fatalf("loaded version=%d envs=%d", loaded.Version, len(loaded.Environments))
	}
	derived, err := st.LoadDerived(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(derived.Plans) != len(bundle.Plans) || len(derived.Refs) == 0 {
		t.Fatalf("persisted derived mismatch plans=%d refs=%d", len(derived.Plans), len(derived.Refs))
	}
}

func TestConcurrentIncrementsSameBaseOnlyOneSucceeds(t *testing.T) {
	st := newTestStore(t)
	if _, err := st.CommitSnapshot(context.Background(), demo.SnapshotV1()); err != nil {
		t.Fatal(err)
	}
	current, err := st.LoadSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	success, stale := 0, 0
	for _, increment := range []*model.Increment{demo.ConcurrentIncrement("a"), demo.ConcurrentIncrement("b")} {
		increment := increment
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := commitIncrement(t, st, current, increment)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				success++
				return
			}
			var apiErr *model.APIError
			if errors.As(err, &apiErr) && apiErr.Code == model.CodeStaleBase {
				stale++
			} else {
				t.Errorf("unexpected commit error: %v", err)
			}
		}()
	}
	wg.Wait()
	if success != 1 || stale != 1 {
		t.Fatalf("success=%d stale=%d, want 1/1", success, stale)
	}
	version, err := st.CurrentVersion(context.Background())
	if err != nil || version != 2 {
		t.Fatalf("version=%d err=%v", version, err)
	}
}

func TestRecoveryAbortsPendingIntentAndInterruptedProbeRound(t *testing.T) {
	st := newTestStore(t)
	if _, err := st.CommitSnapshot(context.Background(), demo.SnapshotV1()); err != nil {
		t.Fatal(err)
	}
	pendingID, err := st.InsertIntent(context.Background(), "increment", "base:99", map[string]string{"reason": "kill -9"})
	if err != nil {
		t.Fatal(err)
	}
	run := &store.ProbeRun{ID: "run-crash", Status: "running", GlobalConcurrency: 1, EnvConcurrency: 1, QueueCapacity: 1, BudgetMS: 1000}
	if err := st.CreateProbeRun(context.Background(), run, map[string]string{"phase": "in-flight"}); err != nil {
		t.Fatal(err)
	}
	report, err := st.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.RestoredVersion != 1 || len(report.AbortedIntents) < 2 {
		t.Fatalf("recovery=%+v", report)
	}
	foundPendingIntent := false
	for _, id := range report.AbortedIntents {
		if id == pendingID {
			foundPendingIntent = true
		}
	}
	if !foundPendingIntent || len(report.InterruptedRuns) != 1 {
		t.Fatalf("pendingID found=%v recovery=%+v", foundPendingIntent, report)
	}
	intents, err := st.ListIntents(context.Background(), "pending")
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 0 {
		t.Fatalf("pending intents remain: %+v", intents)
	}
	loadedRun, err := st.GetProbeRun(context.Background(), "run-crash")
	if err != nil {
		t.Fatal(err)
	}
	if loadedRun.Status != "interrupted" {
		t.Fatalf("run status=%s", loadedRun.Status)
	}
}

func TestProbeResultUnitAndPlanCommitAtomically(t *testing.T) {
	st := newTestStore(t)
	if _, err := st.CommitSnapshot(context.Background(), demo.SnapshotV1()); err != nil {
		t.Fatal(err)
	}
	run := &store.ProbeRun{ID: "run-atomic", Status: "running", GlobalConcurrency: 1, EnvConcurrency: 1, BudgetMS: 1000}
	if err := st.CreateProbeRun(context.Background(), run, nil); err != nil {
		t.Fatal(err)
	}
	err := st.CommitProbeResult(context.Background(), store.ProbeResultRecord{
		RunID: "run-atomic", Env: "prod", Service: "identity", Status: "success", Code: "OK", LatencyMS: 7,
	}, 7)
	if err != nil {
		t.Fatal(err)
	}
	derived, err := st.LoadDerived(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, plan := range derived.Plans {
		if plan.Environment == "prod" && plan.Service == "identity" {
			if plan.Status != "success" {
				t.Fatalf("plan status=%s, want success", plan.Status)
			}
			return
		}
	}
	t.Fatal("plan prod/identity not found")
}
