package verify

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"driftengine/internal/engine"
	"driftengine/internal/spec"
	"driftengine/internal/store"
)

func executablePath() (string, error) { return os.Executable() }

func checkForcedKill(ctx context.Context, opts Options) (any, error) {
	crashDir := filepath.Join(opts.WorkDir, "crash")
	if err := os.MkdirAll(crashDir, 0o755); err != nil {
		return nil, err
	}
	writeDB := filepath.Join(crashDir, "write-crash.db")
	if err := runCrashWorker(ctx, opts.ExePath, writeDB, "write"); err != nil {
		return nil, err
	}
	st, err := store.Open(writeDB)
	if err != nil {
		return nil, err
	}
	writeRecovery, recoverErr := engine.Recover(ctx, st)
	_ = st.Close()
	if recoverErr != nil {
		return nil, recoverErr
	}
	if writeRecovery.CurrentCommittedVersion != 1 || len(writeRecovery.RecoveredIntents) != 2 || len(writeRecovery.Objects) != 1 {
		return map[string]any{"write_recovery": writeRecovery}, fmt.Errorf("unexpected write recovery facts")
	}
	object := writeRecovery.Objects[0]
	if object.ObjectID != "crash-target" || object.FinalStatusBasedOnDurableFacts != "pending_write" || len(object.Differences) != 2 {
		return map[string]any{"write_recovery": writeRecovery}, fmt.Errorf("recovery object/differences mismatch")
	}

	snapshotDB := filepath.Join(crashDir, "snapshot-crash.db")
	if err := runCrashWorker(ctx, opts.ExePath, snapshotDB, "snapshot-intent"); err != nil {
		return nil, err
	}
	st2, err := store.Open(snapshotDB)
	if err != nil {
		return nil, err
	}
	snapshotRecovery, err := engine.Recover(ctx, st2)
	_ = st2.Close()
	if err != nil {
		return nil, err
	}
	if snapshotRecovery.CurrentCommittedVersion != 0 || len(snapshotRecovery.RecoveredIntents) != 1 {
		return map[string]any{"snapshot_recovery": snapshotRecovery}, fmt.Errorf("unexpected snapshot recovery facts")
	}

	return map[string]any{
		"platform":                  runtime.GOOS,
		"snapshot_recovery_intents": len(snapshotRecovery.RecoveredIntents),
		"write_recovery":            writeRecovery,
	}, nil
}

func runCrashWorker(parent context.Context, exe, dbPath, scenario string) error {
	if exe == "" {
		resolved, err := os.Executable()
		if err != nil {
			return err
		}
		exe = resolved
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "crash-worker", "-db", dbPath, "-scenario", scenario)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start crash worker %s: %w", exe, err)
	}
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "READY" {
				ready <- line
				return
			}
		}
	}()
	errText := make(chan string, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := stderr.Read(buf)
		errText <- string(buf[:n])
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		return fmt.Errorf("crash worker did not become ready: %s", <-errText)
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := cmd.Process.Kill(); err != nil {
		return fmt.Errorf("force kill crash worker: %w", err)
	}
	_ = cmd.Wait()
	return nil
}

func CrashWorker(dbPath, scenario string) error {
	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	ctx := context.Background()

	switch scenario {
	case "write":
		snapshot := crashSnapshot()
		eng := engine.New(st, "http://127.0.0.1:0")
		if _, err := eng.SubmitSnapshot(ctx, snapshot); err != nil {
			return err
		}
		intents := []store.Intent{
			{ID: "organize-batches:v1", Type: "organize_batch", Version: 1, PayloadJSON: `{"stage":"build_plan"}`, Status: "committed"},
			{ID: "organize-batch:v1:v1-b0001", Type: "organize_batch", Version: 1, BatchID: "v1-b0001",
				PayloadJSON: `{"object_ids":["crash-target"]}`, Status: "running"},
			{ID: "write-object:v1:v1-b0001:crash-target", Type: "write_object", Version: 1, BatchID: "v1-b0001", ObjectID: "crash-target",
				PayloadJSON: `{"desired_digest":"sha256:crash"}`, Status: "running"},
		}
		for _, intent := range intents {
			if err := st.UpsertIntent(ctx, intent); err != nil {
				return err
			}
		}
	case "snapshot-intent":
		if err := st.UpsertIntent(ctx, store.Intent{
			ID: "snapshot-switch:v999", Type: "snapshot_switch", Version: 999,
			PayloadJSON: `{"digest":"prepared-only"}`, Status: "prepared",
		}); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown crash scenario %q", scenario)
	}
	fmt.Println("READY")
	select {}
}

func crashSnapshot() *spec.Snapshot {
	snapshot := &spec.Snapshot{
		Schema:      "drift.snapshot/v1",
		Version:     1,
		GeneratedAt: time.Date(2026, 11, 25, 1, 0, 0, 0, time.UTC),
		Policy: spec.Policy{
			NumericRelativeTolerance: "0.01",
			ToleranceBoundary:        "inclusive",
			MaxBatchSize:             1,
			MaxBatchBudgetUnits:      1,
			RoundTimeBudgetMS:        30000,
			WriteTimeoutMS:           2000,
			MaxRetries:               0,
			MaxParallelBatches:       1,
			WriteQueueSize:           1,
		},
		Objects: []spec.Object{{
			ID: "crash-target", Category: "Crash", Namespace: "default",
			Desired:        spec.Fields{"replicas": 1, "state": "desired"},
			Actual:         spec.Fields{"replicas": 0, "state": "actual"},
			ResourceBudget: spec.Budget{Units: 1},
		}},
	}
	_ = spec.NormalizeSnapshot(snapshot)
	digest, _, _ := spec.DigestOf(snapshot)
	snapshot.Digest = digest
	return snapshot
}
