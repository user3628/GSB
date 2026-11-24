package main

import (
	"os"
	"os/exec"
	"testing"
)

// TestCrashRecoveryCLI kills a real subprocess between snapshot write and
// commit, then verifies restart consistency from committed transactions.
func TestCrashRecoveryCLI(t *testing.T) {
	if os.Getenv("BIRE_CRASH_HELPER") == "1" {
		os.Args = []string{"bire", "inspect",
			"--repo", "r",
			"--samples", os.Getenv("BIRE_SAMPLES"),
			"--state", os.Getenv("BIRE_STATE"),
			"--crash-before-commit"}
		main()
		return
	}
	samples := genSamples(t, &Plan{Repos: []PlanRepo{{Name: "r", Generations: 3}}})
	state := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashRecoveryCLI")
	cmd.Env = append(os.Environ(),
		"BIRE_CRASH_HELPER=1",
		"BIRE_SAMPLES="+samples,
		"BIRE_STATE="+state,
	)
	err := cmd.Run()
	exitErr, ok := err.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != 2 {
		t.Fatalf("expected simulated kill exit code 2, got %v", err)
	}
	// Restart: committed transactions define state; nothing was committed.
	store, err := OpenStore(state)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	snaps, _, err := store.LoadSnapshots("r")
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 0 {
		t.Fatalf("uncommitted transaction must be discarded, found %d snapshots", len(snaps))
	}
	// The repo can be re-inspected; committed conclusions never roll back.
	res, err := InspectRepo(samples, "r", 0)
	if err != nil {
		t.Fatal(err)
	}
	v, err := store.CommitSnapshot("r", res, false)
	if err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Fatalf("version must be 1 after crash recovery, got %d", v)
	}
	latest, version, code, err := store.LatestResult("r")
	if err != nil {
		t.Fatal(err)
	}
	if code != "" || version != 1 || latest.RestoreGen != 3 {
		t.Fatalf("bad post-recovery state: version=%d code=%q restore=%d", version, code, latest.RestoreGen)
	}
}
