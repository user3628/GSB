package abeval

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "abeval-test.db")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func importTestDataset(t *testing.T, store *Store, dataset Dataset) {
	t.Helper()
	if _, err := store.ImportDataset(context.Background(), dataset.BatchID, dataset); err != nil {
		t.Fatalf("import dataset: %v", err)
	}
}

func TestInterruptRecoveryReportByteIdentical(t *testing.T) {
	ctx := context.Background()
	dataset := DefaultDataset(ScenarioRecovery)
	dataset.BatchID = "batch-recovery-byte"

	continuousPath := filepath.Join(t.TempDir(), "continuous.db")
	continuousStore, err := OpenStore(continuousPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := continuousStore.ImportDataset(ctx, dataset.BatchID, dataset); err != nil {
		t.Fatal(err)
	}
	if _, err := RunBatch(ctx, continuousStore, dataset.BatchID, RunOptions{WriterID: "continuous"}); err != nil {
		t.Fatal(err)
	}
	continuousReport, err := continuousStore.ExportReport(ctx, dataset.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	if err := continuousStore.Close(); err != nil {
		t.Fatal(err)
	}

	restartPath := filepath.Join(t.TempDir(), "restart.db")
	restartStore, err := OpenStore(restartPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restartStore.ImportDataset(ctx, dataset.BatchID, dataset); err != nil {
		t.Fatal(err)
	}
	_, err = RunBatch(ctx, restartStore, dataset.BatchID, RunOptions{
		WriterID:       "graceful-first-process",
		StopAfterStage: dataset.Config.StopAfterStage,
	})
	if !errors.Is(err, ErrGracefulStop) {
		t.Fatalf("first run error = %v, want ErrGracefulStop", err)
	}
	status, err := restartStore.GetStatus(ctx, dataset.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	if status.CurrentStage != 1 || status.Status != StatusGracefulStopped {
		t.Fatalf("status = %+v, want stage 1 graceful_stopped", status)
	}
	if err := restartStore.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedStore, err := OpenStore(restartPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunBatch(ctx, reopenedStore, dataset.BatchID, RunOptions{WriterID: "recovery-process"}); err != nil {
		t.Fatal(err)
	}
	recoveredReport, err := reopenedStore.ExportReport(ctx, dataset.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopenedStore.Close(); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(continuousReport, recoveredReport) {
		t.Fatalf("continuous and recovered reports differ:\ncontinuous:\n%s\nrecovered:\n%s",
			continuousReport, recoveredReport)
	}

	crashPath := filepath.Join(t.TempDir(), "crash.db")
	crashStore, err := OpenStore(crashPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := crashStore.ImportDataset(ctx, dataset.BatchID, dataset); err != nil {
		t.Fatal(err)
	}
	if _, err := crashStore.AdvanceStage(ctx, dataset.BatchID, "before-force-kill"); err != nil {
		t.Fatal(err)
	}
	if err := crashStore.Close(); err != nil {
		t.Fatal(err)
	}
	crashedReopen, err := OpenStore(crashPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunBatch(ctx, crashedReopen, dataset.BatchID, RunOptions{WriterID: "after-force-kill"}); err != nil {
		t.Fatal(err)
	}
	crashReport, err := crashedReopen.ExportReport(ctx, dataset.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	if err := crashedReopen.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(continuousReport, crashReport) {
		t.Fatalf("force-kill recovery report differs from continuous report")
	}
}

func TestVersionReplacementAndRollback(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	batchID := "batch-versions"

	v1 := DefaultDataset(ScenarioRollout)
	v1.BatchID = batchID
	first, err := store.ImportDataset(ctx, batchID, v1)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || first.CurrentVersion != 1 {
		t.Fatalf("first import = %+v", first)
	}
	if _, err := store.AdvanceStage(ctx, batchID, "v1"); err != nil {
		t.Fatal(err)
	}

	v2 := DefaultDataset(ScenarioRollback)
	v2.BatchID = batchID
	v2.Version = 2
	replaced, err := store.ImportDataset(ctx, batchID, v2)
	if err != nil {
		t.Fatal(err)
	}
	if !replaced.Replaced || replaced.PreviousVersion != 1 || replaced.CurrentVersion != 2 {
		t.Fatalf("replacement = %+v", replaced)
	}
	status, err := store.GetStatus(ctx, batchID)
	if err != nil {
		t.Fatal(err)
	}
	if status.CurrentStage != 0 || status.Status != StatusImported {
		t.Fatalf("status after replacement = %+v, want reset", status)
	}

	oldContentHigherVersion := DefaultDataset(ScenarioRollout)
	oldContentHigherVersion.BatchID = batchID
	oldContentHigherVersion.Version = 3
	_, err = store.ImportDataset(ctx, batchID, oldContentHigherVersion)
	if ErrorCode(err) != CodeVersionRollback {
		t.Fatalf("old content import error = %v, want %s", err, CodeVersionRollback)
	}

	olderVersionNewContent := DefaultDataset(ScenarioInsufficient)
	olderVersionNewContent.BatchID = batchID
	olderVersionNewContent.Version = 1
	_, err = store.ImportDataset(ctx, batchID, olderVersionNewContent)
	if ErrorCode(err) != CodeVersionRollback {
		t.Fatalf("older version import error = %v, want %s", err, CodeVersionRollback)
	}

	if _, err := RunBatch(ctx, store, batchID, RunOptions{WriterID: "v2"}); err != nil {
		t.Fatal(err)
	}
	report, err := store.BuildReport(ctx, batchID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Decision != DecisionRollback {
		t.Fatalf("rebuilt decision = %s, want rollback", report.Decision)
	}
}

func TestCommitStageConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	dataset := DefaultDataset(ScenarioRollout)
	dataset.BatchID = "batch-concurrent"
	importTestDataset(t, store, dataset)

	metrics, err := BuildStageMetrics(dataset.Config, dataset.Events, 1)
	if err != nil {
		t.Fatal(err)
	}
	verdict := EvaluateStage(metrics, dataset.Config)
	base := StageCheckpoint{
		BatchID:    dataset.BatchID,
		StageIndex: 1,
		Stats:      metrics,
		Verdict:    verdict,
	}

	const writers = 16
	var wg sync.WaitGroup
	var successes, stale int64
	var unexpected int64
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			checkpoint := base
			checkpoint.WriterID = fmt.Sprintf("writer-%02d", i)
			_, err := store.CommitStage(ctx, checkpoint)
			if err == nil {
				atomic.AddInt64(&successes, 1)
				return
			}
			if ErrorCode(err) == CodeStaleCheckpoint {
				atomic.AddInt64(&stale, 1)
				return
			}
			atomic.AddInt64(&unexpected, 1)
			t.Errorf("unexpected commit error: %v", err)
		}(i)
	}
	wg.Wait()

	if successes != 1 || stale != writers-1 || unexpected != 0 {
		t.Fatalf("successes=%d stale=%d unexpected=%d", successes, stale, unexpected)
	}

	duplicate := base
	duplicate.WriterID = "duplicate"
	_, err = store.CommitStage(ctx, duplicate)
	if ErrorCode(err) != CodeStaleCheckpoint {
		t.Fatalf("duplicate commit error = %v, want %s", err, CodeStaleCheckpoint)
	}

	ahead := base
	ahead.StageIndex = 3
	ahead.WriterID = "ahead"
	_, err = store.CommitStage(ctx, ahead)
	if ErrorCode(err) != CodeStaleCheckpoint {
		t.Fatalf("ahead commit error = %v, want %s", err, CodeStaleCheckpoint)
	}

	var validCount int
	if err := store.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM stage_checkpoints
		WHERE batch_id = ? AND stage_index = 1 AND status = 'valid'`,
		dataset.BatchID).Scan(&validCount); err != nil {
		t.Fatal(err)
	}
	if validCount != 1 {
		t.Fatalf("valid stage 1 records = %d, want 1", validCount)
	}
}

func TestSQLiteCommitStageMultipleProcesses(t *testing.T) {
	if os.Getenv("ABEVAL_STALE_HELPER") == "1" {
		runStaleCheckpointHelper(t)
		return
	}

	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "multi-process.db")
	store, err := OpenStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	dataset := DefaultDataset(ScenarioRollout)
	dataset.BatchID = "batch-multi-process"
	if _, err := store.ImportDataset(ctx, dataset.BatchID, dataset); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	const processes = 8
	var wg sync.WaitGroup
	var exitsOK, exitsStale int64
	for i := 0; i < processes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			command := exec.Command(executable, "-test.run=^TestSQLiteCommitStageMultipleProcesses$")
			command.Env = append(os.Environ(),
				"ABEVAL_STALE_HELPER=1",
				"ABEVAL_DB="+databasePath,
				"ABEVAL_BATCH="+dataset.BatchID,
				fmt.Sprintf("ABEVAL_WRITER=process-%d", i),
			)
			output, err := command.CombinedOutput()
			if err == nil {
				atomic.AddInt64(&exitsOK, 1)
				return
			}
			var exitError *exec.ExitError
			if errors.As(err, &exitError) && exitError.ExitCode() == 20 {
				atomic.AddInt64(&exitsStale, 1)
				return
			}
			t.Errorf("helper process %d failed unexpectedly: %v\n%s", i, err, output)
		}(i)
	}
	wg.Wait()

	if exitsOK != 1 || exitsStale != processes-1 {
		t.Fatalf("process exits ok=%d stale=%d; want 1 and %d", exitsOK, exitsStale, processes-1)
	}

	reopened, err := OpenStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	status, err := reopened.GetStatus(ctx, dataset.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	if status.CurrentStage != 1 {
		t.Fatalf("current stage = %d, want 1", status.CurrentStage)
	}
}

func runStaleCheckpointHelper(t *testing.T) {
	ctx := context.Background()
	store, err := OpenStore(os.Getenv("ABEVAL_DB"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer store.Close()

	batchID := os.Getenv("ABEVAL_BATCH")
	dataset, err := store.LoadDataset(ctx, batchID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	status, err := store.GetStatus(ctx, batchID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	metrics, err := BuildStageMetrics(dataset.Config, dataset.Events, 1)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	verdict := EvaluateStage(metrics, dataset.Config)
	_, err = store.CommitStage(ctx, StageCheckpoint{
		BatchID:     batchID,
		Version:     status.Version,
		ContentHash: status.ContentHash,
		StageIndex:  1,
		Stats:       metrics,
		Verdict:     verdict,
		WriterID:    os.Getenv("ABEVAL_WRITER"),
	})
	if err == nil {
		fmt.Println("HELPER_COMMIT_OK")
		os.Exit(0)
	}
	if ErrorCode(err) == CodeStaleCheckpoint {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(20)
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
