package cache

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const crashHelperEnv = "CIARTIFACTS_CRASH_HELPER"

func TestCrashRecoveryHelper(t *testing.T) {
	if os.Getenv(crashHelperEnv) != "1" {
		return
	}
	dir := os.Getenv("CIARTIFACTS_CRASH_DIR")
	marker := os.Getenv("CIARTIFACTS_CRASH_MARKER")
	scenario := os.Getenv("CIARTIFACTS_CRASH_SCENARIO")
	samplesPath := os.Getenv("CIARTIFACTS_SAMPLES")
	crashPoint := CrashAfterWriting
	capacity := int64(1024)
	if scenario == "evict" {
		crashPoint = CrashAfterEvictIntent
		capacity = 1
	}
	hook := func(point string) {
		_ = os.WriteFile(marker, []byte(point), 0o644)
		for {
			time.Sleep(time.Hour)
		}
	}
	store, err := Open(context.Background(), Config{
		Dir:           dir,
		CapacityBytes: capacity,
		CrashPoint:    crashPoint,
		CrashHook:     hook,
	})
	if err != nil {
		t.Fatalf("helper open: %v", err)
	}
	defer store.Close()
	inputs, err := LoadSampleFile(samplesPath)
	if err != nil {
		t.Fatalf("helper samples: %v", err)
	}
	input := sample(t, inputs, "recovery")
	if scenario == "evict" {
		input = sample(t, inputs, "build")
	}
	if _, err := store.Put(context.Background(), input); err != nil {
		t.Fatalf("helper put: %v", err)
	}
	if scenario == "evict" {
		if _, err := store.Evict(context.Background()); err != nil {
			t.Fatalf("helper evict: %v", err)
		}
	}
	os.Exit(0)
}

func TestForcedTerminationRecoversWriteAndEviction(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		marker := filepath.Join(dir, "crash-marker")
		runCrashHelper(t, dir, marker, "write")
		store, err := Open(ctx, Config{Dir: dir, CapacityBytes: 1024})
		if err != nil {
			t.Fatalf("reopen store: %v", err)
		}
		defer store.Close()

		inputs := testSamples(t)
		input := sample(t, inputs, "recovery")
		entry, ok := store.EntryByKey(input.Key())
		if !ok || entry.State != StatePending {
			t.Fatalf("interrupted write was not rolled back to PENDING: %+v", entry)
		}
		report, err := store.LatestRecoveryReport(ctx)
		if err != nil {
			t.Fatalf("recovery report: %v", err)
		}
		if report.IntentsExamined == 0 || report.IntentsAborted == 0 || len(report.EntriesRolledBack) != 1 {
			t.Fatalf("unexpected write recovery report: %+v", report)
		}
		if report.EntriesRolledBack[0].From != StateWriting || report.EntriesRolledBack[0].To != StatePending {
			t.Fatalf("unexpected write rollback: %+v", report.EntriesRolledBack[0])
		}
		if report.Facts.EntryCounts[string(StatePending)] != 1 {
			t.Fatalf("recovery facts do not show PENDING: %+v", report.Facts)
		}
		artifactPath, _ := store.artifactPath(input.ArtifactDigest)
		if _, err := os.Stat(artifactPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("uncommitted artifact was not removed: %v", err)
		}
		if _, err := store.Hit(ctx, input.Key(), ""); !IsCode(err, CodeCacheMiss) {
			t.Fatalf("PENDING entry should not hit, got %v", err)
		}
	})

	t.Run("eviction", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		marker := filepath.Join(dir, "crash-marker")
		runCrashHelper(t, dir, marker, "evict")
		store, err := Open(ctx, Config{Dir: dir, CapacityBytes: 1})
		if err != nil {
			t.Fatalf("reopen store: %v", err)
		}
		defer store.Close()

		input := sample(t, testSamples(t), "build")
		entry, ok := store.EntryByKey(input.Key())
		if !ok || entry.State != StateAvailable {
			t.Fatalf("interrupted eviction was not rolled back to AVAILABLE: %+v", entry)
		}
		report, err := store.LatestRecoveryReport(ctx)
		if err != nil {
			t.Fatalf("recovery report: %v", err)
		}
		if len(report.EvictionsRolledBack) != 1 || report.EvictionsRolledBack[0].To != StateAvailable {
			t.Fatalf("unexpected eviction recovery report: %+v", report)
		}
		hit, err := store.Hit(ctx, input.Key(), "")
		if err != nil {
			t.Fatalf("recovered entry is not reusable: %v", err)
		}
		if !bytes.Equal(hit.Content, input.ArtifactContent) {
			t.Fatalf("recovered artifact content mismatch: %q", hit.Content)
		}
	})
}

func runCrashHelper(t *testing.T, dir, marker, scenario string) {
	t.Helper()
	samplesPath, err := filepath.Abs(filepath.Join("..", "..", "samples", "entries.json"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashRecoveryHelper$")
	cmd.Env = append(os.Environ(),
		crashHelperEnv+"=1",
		"CIARTIFACTS_CRASH_DIR="+dir,
		"CIARTIFACTS_CRASH_MARKER="+marker,
		"CIARTIFACTS_CRASH_SCENARIO="+scenario,
		"CIARTIFACTS_SAMPLES="+samplesPath,
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start crash helper: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("crash marker not observed; helper output:\n%s", output.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill crash helper: %v", err)
	}
	_ = cmd.Wait()
}
