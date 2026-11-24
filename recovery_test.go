package cockpitaudio

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func buildGoldenResult(t *testing.T, dir string) (*Dataset, *SessionResult) {
	t.Helper()
	ds, err := GenerateDataset(GenerateOptions{Root: filepath.Join(dir, "samples")})
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	report, rerr := NewEngine(ds, DefaultConfig(), store, "continuous").Run(context.Background())
	_ = store.Close()
	if rerr != nil {
		t.Fatal(rerr)
	}
	return ds, report.Result
}

func TestGracefulStopAndReopenRecovery(t *testing.T) {
	goldenDir := t.TempDir()
	_, golden := buildGoldenResult(t, goldenDir)

	recoveryDir := t.TempDir()
	ds, err := GenerateDataset(GenerateOptions{Root: filepath.Join(recoveryDir, "samples")})
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(filepath.Join(recoveryDir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	eng := NewEngine(ds, DefaultConfig(), store, "stopping")
	eng.OnUnitCommitted = func(committed int, _, _ string, _ int) {
		if committed == 3 {
			cancel()
		}
	}
	stopped, rerr := eng.Run(ctx)
	if ae, ok := AsError(rerr); !ok || ae.Code != CodeStopped || !stopped.Stopped {
		t.Fatalf("expected stopped report, got report=%+v err=%v", stopped, rerr)
	}
	if len(stopped.Committed) != 3 {
		t.Fatalf("committed = %d, want 3", len(stopped.Committed))
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(filepath.Join(recoveryDir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report, rerr := NewEngine(ds, DefaultConfig(), reopened, "recovery").Run(context.Background())
	if rerr != nil {
		t.Fatal(rerr)
	}
	if len(report.Skipped) < 3 {
		t.Fatalf("recovery skipped units = %v", report.Skipped)
	}
	if diff := CompareSessionResults(golden, report.Result); diff != nil {
		t.Fatalf("recovery mismatch: %v", diff)
	}
	gb, _ := CanonicalJSON(golden)
	rb, _ := CanonicalJSON(report.Result)
	if string(gb) != string(rb) {
		t.Fatal("recovered report bytes differ from continuous report")
	}
}

func TestHardExitRecoverySubprocess(t *testing.T) {
	if os.Getenv("COCKPIT_HARD_EXIT_HELPER") == "1" {
		runHardExitHelper(t)
		return
	}
	if testing.Short() {
		t.Skip("skipping subprocess recovery in short mode")
	}

	dir := t.TempDir()
	exe, exeErr := os.Executable()
	if exeErr != nil {
		t.Fatal(exeErr)
	}
	cmd := exec.Command(exe, "-test.run=^TestHardExitRecoverySubprocess$", "-test.v")
	cmd.Env = append(os.Environ(), "COCKPIT_HARD_EXIT_HELPER=1", "COCKPIT_HELPER_DIR="+dir)
	output, runErr := cmd.CombinedOutput()
	if runErr == nil {
		t.Fatalf("hard-exit helper unexpectedly returned: %s", output)
	}

	goldenDir := t.TempDir()
	_, golden := buildGoldenResult(t, goldenDir)

	ds, err := GenerateDataset(GenerateOptions{Root: filepath.Join(dir, "samples")})
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatalf("open after hard exit: %v\n%s", err, output)
	}
	defer store.Close()
	report, rerr := NewEngine(ds, DefaultConfig(), store, "after-hard-exit").Run(context.Background())
	if rerr != nil {
		t.Fatalf("recover after hard exit: %v", rerr)
	}
	if diff := CompareSessionResults(golden, report.Result); diff != nil {
		t.Fatal(diff)
	}
}

func runHardExitHelper(t *testing.T) {
	dir := os.Getenv("COCKPIT_HELPER_DIR")
	if dir == "" {
		os.Exit(2)
	}
	ds, err := GenerateDataset(GenerateOptions{Root: filepath.Join(dir, "samples")})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	store, err := OpenStore(filepath.Join(dir, "state.db"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	eng := NewEngine(ds, DefaultConfig(), store, "hard-exit")
	eng.OnUnitCommitted = func(committed int, _, _ string, _ int) {
		if committed == 4 {
			os.Exit(137)
		}
	}
	if _, err := eng.Run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
