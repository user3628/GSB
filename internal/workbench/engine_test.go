package workbench

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func TestRunBaselineDecisionsAndSnapshots(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	outcome, err := RunWorkbench(context.Background(), dbPath, RunOptions{Concurrency: 4})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Interrupted {
		t.Fatal("unexpected interruption")
	}
	if len(outcome.Report.Tasks) != 9 {
		t.Fatalf("task count = %d, want 9", len(outcome.Report.Tasks))
	}
	if len(outcome.Report.SnapshotChain) != 10 {
		t.Fatalf("snapshot count = %d, want 10", len(outcome.Report.SnapshotChain))
	}
	want := map[string]string{
		"t01-valid":           CodeOK,
		"t02-truncated":       CodeTruncated,
		"t03-length-wrap":     CodeLengthWrap,
		"t04-length-overflow": CodeLengthOverflow,
		"t05-depth":           CodeDepthExceeded,
		"t06-nested-budget":   CodeNestedBudget,
		"t07-task-budget":     CodeTaskMemory,
		"t08-global-budget":   CodeGlobalBudget,
		"t09-valid-after":     CodeOK,
	}
	for _, task := range outcome.Report.Tasks {
		if task.DecisionCode != want[task.TaskID] {
			t.Errorf("%s decision = %s, want %s (state=%s decode=%s)", task.TaskID, task.DecisionCode, want[task.TaskID], task.FinalState, task.DecodeCode)
		}
		if task.SnapshotVersion < 2 || task.SnapshotVersion > 10 {
			t.Errorf("%s snapshot version = %d", task.TaskID, task.SnapshotVersion)
		}
	}
	if len(outcome.Report.RejectedTasks) != 2 {
		t.Fatalf("rejected tasks = %d, want 2", len(outcome.Report.RejectedTasks))
	}
	if outcome.Report.Tasks[0].PeakMemoryBytes <= 8 {
		t.Fatalf("normal task peak did not exceed tiny budget: %d", outcome.Report.Tasks[0].PeakMemoryBytes)
	}
}

func TestReportBytesStableAcrossConcurrency(t *testing.T) {
	var reference []byte
	for _, concurrency := range []int{1, 2, 4, 8} {
		dbPath := filepath.Join(t.TempDir(), "state.db")
		outcome, err := RunWorkbench(context.Background(), dbPath, RunOptions{Concurrency: concurrency})
		if err != nil {
			t.Fatal(err)
		}
		data, err := MarshalReport(outcome.Report)
		if err != nil {
			t.Fatal(err)
		}
		if reference == nil {
			reference = data
			continue
		}
		if !bytes.Equal(reference, data) {
			t.Fatalf("report differs at concurrency %d\nreference digest=%s\nactual digest=%s", concurrency, ReportDigest(reference), ReportDigest(data))
		}
	}
}

func TestResumeDoesNotRedoCommittedTasks(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	first, err := RunWorkbench(context.Background(), dbPath, RunOptions{Concurrency: 3, StopAfter: 3})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Interrupted || first.NewTasks != 3 || len(first.Chain) != 4 {
		t.Fatalf("first run: interrupted=%v new=%d chain=%d", first.Interrupted, first.NewTasks, len(first.Chain))
	}
	committed := map[string]int64{}
	for _, result := range first.State.Results {
		committed[result.TaskID] = result.SnapshotVersion
	}
	second, err := RunWorkbench(context.Background(), dbPath, RunOptions{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if second.Interrupted || second.NewTasks != 6 || len(second.Chain) != 10 {
		t.Fatalf("second run: interrupted=%v new=%d chain=%d", second.Interrupted, second.NewTasks, len(second.Chain))
	}
	for _, result := range second.State.Results {
		if version, ok := committed[result.TaskID]; ok && result.SnapshotVersion != version {
			t.Fatalf("%s was re-committed: %d -> %d", result.TaskID, version, result.SnapshotVersion)
		}
	}
	third, err := RunWorkbench(context.Background(), dbPath, RunOptions{Concurrency: 8})
	if err != nil {
		t.Fatal(err)
	}
	if third.NewTasks != 0 || len(third.Chain) != 10 {
		t.Fatalf("idempotent run: new=%d chain=%d", third.NewTasks, len(third.Chain))
	}
}

func TestSnapshotGapFallbackAndRecovery(t *testing.T) {
	freshReport := runFreshReport(t, 4)
	dbPath := filepath.Join(t.TempDir(), "state.db")
	if _, err := RunWorkbench(context.Background(), dbPath, RunOptions{Concurrency: 2, StopAfter: 3}); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSnapshot(2); err != nil {
		t.Fatal(err)
	}
	load, err := store.Load(false)
	if err != nil {
		t.Fatal(err)
	}
	if load.WarningCode != CodeSnapshotVersion || load.LatestVersion() != 1 {
		t.Fatalf("gap load warning=%s latest=%d", load.WarningCode, load.LatestVersion())
	}
	genesisHash := load.Valid[0].StateSHA256
	store.Close()
	outcome, err := RunWorkbench(context.Background(), dbPath, RunOptions{Concurrency: 3})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.WarningCode != CodeSnapshotVersion {
		t.Fatalf("expected recovery warning, got %q", outcome.WarningCode)
	}
	recovered, err := MarshalReport(outcome.Report)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(freshReport, recovered) {
		t.Fatalf("recovered report differs from fresh report: %s vs %s", ReportDigest(freshReport), ReportDigest(recovered))
	}
	loadAfter, err := LoadWorkspace(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if loadAfter.Valid[0].StateSHA256 != genesisHash {
		t.Fatal("committed genesis snapshot was mutated during recovery")
	}
}

func TestEditCommitsNewPolicySnapshot(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	if _, err := RunWorkbench(context.Background(), dbPath, RunOptions{Concurrency: 2, StopAfter: 2}); err != nil {
		t.Fatal(err)
	}
	load, err := LoadWorkspace(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	next := load.State.Config
	next.NestedBudget = 777
	next.GlobalMemoryBudget = 200000
	updated, snapshot, err := EditWorkspace(dbPath, next)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Kind != "policy-edit" || updated.LatestVersion() != load.LatestVersion()+1 {
		t.Fatalf("edit snapshot kind/version = %s/%d", snapshot.Kind, updated.LatestVersion())
	}
	if len(updated.State.Results) != 0 {
		t.Fatalf("edit retained %d old task results", len(updated.State.Results))
	}
	if updated.State.Config.NestedBudget != 777 {
		t.Fatalf("edited nested budget = %d", updated.State.Config.NestedBudget)
	}
}

func TestSnapshotContentTamperIsRejectedEvenWithSameVersion(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	if _, err := RunWorkbench(context.Background(), dbPath, RunOptions{Concurrency: 2, StopAfter: 2}); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE snapshots SET state_json = ? WHERE version = 2`, []byte(`{"format":"recordlab-state-v1","version":2,"config":{},"tasks":[],"results":[]}`)); err != nil {
		store.Close()
		t.Fatal(err)
	}
	load, err := store.Load(false)
	store.Close()
	if err != nil {
		t.Fatal(err)
	}
	if load.WarningCode != CodeSnapshotVersion || load.LatestVersion() != 1 {
		t.Fatalf("tampered full-state replacement was not rejected: warning=%s latest=%d", load.WarningCode, load.LatestVersion())
	}
}

func TestAllInjectionCasesPass(t *testing.T) {
	report, err := RunInjections(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !report.AllPassed {
		for _, item := range report.Cases {
			t.Logf("%+v", item)
		}
		t.Fatal("one or more injection cases failed")
	}
	if len(report.Cases) != 5 {
		t.Fatalf("injection case count = %d, want 5", len(report.Cases))
	}
}

func runFreshReport(t *testing.T, concurrency int) []byte {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "fresh.db")
	outcome, err := RunWorkbench(context.Background(), dbPath, RunOptions{Concurrency: concurrency})
	if err != nil {
		t.Fatal(err)
	}
	data, err := MarshalReport(outcome.Report)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
