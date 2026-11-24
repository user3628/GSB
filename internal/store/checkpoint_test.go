package store_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"motionlab/internal/engine"
	"motionlab/internal/model"
	"motionlab/internal/motion"
	"motionlab/internal/store"
)

func setupRun(t *testing.T) (string, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	if err := motion.Initialize(dir, true); err != nil {
		t.Fatal(err)
	}
	cat, err := motion.LoadCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	eng := engine.New(cat, st, engine.Options{})
	_, err = eng.EnsureRun(context.Background(), "orchid_drift", cat.Policy, model.RunConfig{SequenceID: "orchid_drift", RequestedSteps: 1, DisablePrune: true}, true)
	if err != nil {
		t.Fatal(err)
	}
	return dir, st
}

func TestCheckpointConditionAndConcurrentPublishers(t *testing.T) {
	dir, st := setupRun(t)
	cat, err := motion.LoadCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng := engine.New(cat, st, engine.Options{})
	run, err := eng.EnsureRun(context.Background(), "orchid_drift", cat.Policy, model.RunConfig{SequenceID: "orchid_drift", RequestedSteps: 1, DisablePrune: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- st.PublishCheckpoint(context.Background(), run.RunID, 1, fmt.Sprintf("payload-%d", i))
		}()
	}
	wg.Wait()
	close(results)
	success, stale := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if model.AsAppError(err).Code == model.CodeStaleCheckpoint {
			stale++
		} else {
			t.Fatalf("unexpected publisher error: %v", err)
		}
	}
	if success != 1 || stale != 1 {
		t.Fatalf("expected one winner and one stale publisher, got success=%d stale=%d", success, stale)
	}
	if err := st.PublishCheckpoint(context.Background(), run.RunID, 3, "gap"); err == nil || model.AsAppError(err).Code != model.CodeStaleCheckpoint {
		t.Fatalf("gap publication must be rejected, got %v", err)
	}
	if err := st.PublishCheckpoint(context.Background(), run.RunID, 2, "next"); err != nil {
		t.Fatalf("sequential publication failed: %v", err)
	}
}

func TestMultiProcessCheckpointPublisher(t *testing.T) {
	dir, st := setupRun(t)
	cat, err := motion.LoadCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng := engine.New(cat, st, engine.Options{})
	run, err := eng.EnsureRun(context.Background(), "orchid_drift", cat.Policy, model.RunConfig{SequenceID: "orchid_drift", RequestedSteps: 1, DisablePrune: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	type child struct {
		cmd *exec.Cmd
		out *strings.Builder
	}
	children := make([]child, 2)
	for i := range children {
		var out strings.Builder
		cmd := exec.Command(exe, "-test.run=TestCheckpointPublisherHelper", "-test.v")
		cmd.Env = append(os.Environ(), "MOTIONLAB_PUBLISH_HELPER=1", "MOTIONLAB_PUBLISH_DIR="+dir, "MOTIONLAB_PUBLISH_RUN="+run.RunID)
		cmd.Stdout = &out
		cmd.Stderr = &out
		children[i] = child{cmd: cmd, out: &out}
	}
	for i := range children {
		if err := children[i].cmd.Start(); err != nil {
			t.Fatal(err)
		}
	}
	for i := range children {
		_ = children[i].cmd.Wait()
	}
	all := children[0].out.String() + children[1].out.String()
	if strings.Count(all, "PUBLISH_OK") != 1 || strings.Count(all, "E_STALE_CHECKPOINT") != 1 {
		t.Fatalf("multi-process publisher outcome unexpected:\n%s", all)
	}
}

func TestCheckpointPublisherHelper(t *testing.T) {
	if os.Getenv("MOTIONLAB_PUBLISH_HELPER") != "1" {
		t.Skip("helper process only")
	}
	dir := os.Getenv("MOTIONLAB_PUBLISH_DIR")
	runID := os.Getenv("MOTIONLAB_PUBLISH_RUN")
	st, err := store.Open(dir)
	if err != nil {
		fmt.Println("HELPER_OPEN_ERROR", err)
		os.Exit(2)
	}
	defer st.Close()
	err = st.PublishCheckpoint(context.Background(), runID, 1, "multi-process")
	if err == nil {
		fmt.Println("PUBLISH_OK", filepath.Base(dir))
		return
	}
	appErr := model.AsAppError(err)
	fmt.Println("PUBLISH_ERROR", appErr.Code, appErr.Message)
}
