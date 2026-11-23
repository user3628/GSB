package dnsdetect

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
)

const (
	crashModeEnv    = "DNSDETECT_CRASH_MODE"
	crashDBEnv      = "DNSDETECT_CRASH_DB"
	crashSamplesEnv = "DNSDETECT_CRASH_SAMPLES"
)

func TestCrashRecovery(t *testing.T) {
	dataset := testDataset(t)
	config := testConfig(dataset)
	store := testStore(t, "crash")
	baseline := processGood(t, dataset, config, store)
	if baseline.Snapshot.Version != 1 {
		t.Fatalf("baseline version = %d", baseline.Snapshot.Version)
	}
	dbPath := store.path
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	runCrashHelper(t, "before", dbPath, dataset.Dir)
	reopened, err := OpenSnapshotStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	afterCrash, err := reopened.Load(context.Background())
	if err != nil || afterCrash.Latest == nil || afterCrash.Latest.Version != 1 {
		t.Fatalf("uncommitted transaction survived: %+v, %v", afterCrash, err)
	}
	changedConfig := config
	changedConfig.AssociationWindowTicks -= 1
	engine := NewEngine(changedConfig, reopened)
	recovered, err := engine.Process(context.Background(), dataset.Sources)
	if err != nil {
		t.Fatalf("re-adjudication after crash: %v", err)
	}
	if recovered.Snapshot.Version != 2 || recovered.Snapshot.PrevContentDigest != baseline.Snapshot.ContentDigest {
		t.Fatalf("recovered snapshot = %+v", recovered.Snapshot)
	}
	if len(recovered.Snapshot.WindowProgress) == 0 {
		t.Fatal("recovered snapshot lacks window progress")
	}
	reopened.Close()

	afterStore := testStore(t, "crash-after-commit")
	afterBaseline := processGood(t, dataset, testConfig(dataset), afterStore)
	afterPath := afterStore.path
	if err := afterStore.Close(); err != nil {
		t.Fatal(err)
	}
	runCrashHelper(t, "after", afterPath, dataset.Dir)
	committed, err := OpenSnapshotStore(afterPath)
	if err != nil {
		t.Fatal(err)
	}
	defer committed.Close()
	loaded, err := committed.Load(context.Background())
	if err != nil || loaded.Latest == nil || loaded.Latest.Version != 2 || loaded.Latest.PrevContentDigest != afterBaseline.Snapshot.ContentDigest {
		t.Fatalf("committed snapshot not recovered: %+v, %v", loaded, err)
	}
	repeatEngine := NewEngine(changedConfig, committed)
	repeated, err := repeatEngine.Process(context.Background(), dataset.Sources)
	if err != nil {
		t.Fatal(err)
	}
	if repeated.Snapshot.Version != 2 {
		t.Fatalf("committed recovery advanced to version %d", repeated.Snapshot.Version)
	}
}

func runCrashHelper(t *testing.T, mode, dbPath, samplesDir string) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
	command.Env = append(os.Environ(),
		crashModeEnv+"="+mode,
		crashDBEnv+"="+dbPath,
		crashSamplesEnv+"="+samplesDir,
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("crash helper unexpectedly exited successfully: %s", output)
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != 17 {
		t.Fatalf("crash helper exit = %v, output=%s", err, output)
	}
}

func TestCrashHelper(t *testing.T) {
	mode := os.Getenv(crashModeEnv)
	if mode == "" {
		t.Skip("crash helper only runs as a subprocess")
	}
	dbPath := os.Getenv(crashDBEnv)
	samplesDir := os.Getenv(crashSamplesEnv)
	if dbPath == "" || samplesDir == "" {
		fmt.Fprintln(os.Stderr, "missing crash helper environment")
		os.Exit(2)
	}
	store, err := OpenSnapshotStore(dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	switch mode {
	case "before":
		latest, err := store.Latest(context.Background())
		if err != nil || latest == nil {
			fmt.Fprintln(os.Stderr, "latest:", err)
			os.Exit(2)
		}
		tx, err := store.db.BeginTx(context.Background(), nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		content := []byte("uncommitted-window")
		_, err = tx.ExecContext(context.Background(), `
INSERT INTO snapshots(version, created_ticks, format_versions, source_priorities, ledger_digest, content_digest, prev_content_digest, window_progress, content)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`, latest.Version+1, latest.CreatedTicks+1, "[]", "[]", "ledger", contentDigest(content), latest.ContentDigest, "[]", content)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		// Simulate SIGKILL during snapshot submission: no commit and no defers.
		os.Exit(17)
	case "after":
		dataset, err := GenerateDataset(samplesDir)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		config := DefaultConfig()
		config.WindowAnchorTicks = dataset.ObservationStartTicks
		config.AssociationWindowTicks--
		engine := NewEngine(config, store)
		if _, err := engine.Process(context.Background(), dataset.Sources); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		os.Exit(17)
	default:
		fmt.Fprintln(os.Stderr, "unknown crash mode", strconv.Quote(mode))
		os.Exit(2)
	}
}
