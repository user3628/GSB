package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"sandboxpolicy/cli"
)

type currentView struct {
	Version int    `json:"version"`
	Digest  string `json:"digest"`
	Warning *struct {
		Code            string `json:"code"`
		FallbackVersion int    `json:"fallback_version"`
	} `json:"chain_warning"`
}

func TestExternalInvalidInputsNeverAdvanceOrPolluteSnapshots(t *testing.T) {
	tests := []struct {
		name       string
		changeset  string
		exitCode   int
		business   string
		baseBefore int
	}{
		{name: "reference-missing", changeset: "missing-reference.json", exitCode: 2, business: "E_POLICY_MALFORMED", baseBefore: 1},
		{name: "dependency-cycle", changeset: "dependency-cycle.json", exitCode: 2, business: "E_POLICY_MALFORMED", baseBefore: 1},
		{name: "namespace-scope", changeset: "cross-namespace-reference.json", exitCode: 7, business: "E_NAMESPACE_SCOPE", baseBefore: 1},
		{name: "delete-then-add", changeset: "delete-then-add-conflict.json", exitCode: 6, business: "E_CHANGE_CONFLICT", baseBefore: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "state.db")
			initCLI(t, dbPath)
			before := readCurrent(t, dbPath)
			if before.Version != test.baseBefore {
				t.Fatalf("baseline version = %d", before.Version)
			}
			code, _, stderr := runCLI(t, "apply", "--db", dbPath, "--changeset", samplePath("invalid", test.changeset))
			if code != test.exitCode {
				t.Fatalf("exit code = %d, want %d; stderr=%s", code, test.exitCode, stderr)
			}
			var failure struct {
				Code string `json:"code"`
			}
			if err := json.Unmarshal(stderr, &failure); err != nil {
				t.Fatalf("decode error output: %v: %s", err, stderr)
			}
			if failure.Code != test.business {
				t.Fatalf("business code = %s, want %s", failure.Code, test.business)
			}
			after := readCurrent(t, dbPath)
			if after.Version != before.Version || after.Digest != before.Digest {
				t.Fatalf("failed apply polluted snapshot: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestVersionGapFallsBackAndTerminalVersionDoesNotAdvance(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	initCLI(t, dbPath)
	code, _, stderr := runCLI(t, "apply", "--db", dbPath, "--changeset", samplePath("changes", "01-add-cache.json"))
	if code != 0 {
		t.Fatalf("initial apply failed: %s", stderr)
	}
	before := readCurrent(t, dbPath)
	if before.Version != 2 || before.Digest == "" {
		t.Fatalf("unexpected version 2 state: %+v", before)
	}
	code, _, stderr = runCLI(t, "inject", "version-gap", "--db", dbPath, "--namespace", "prod/win-sandbox", "--version", "4")
	if code != 0 {
		t.Fatalf("inject gap: %s", stderr)
	}
	fallback := readCurrent(t, dbPath)
	if fallback.Version != 2 || fallback.Digest != before.Digest || fallback.Warning == nil || fallback.Warning.Code != "E_SNAPSHOT_VERSION" || fallback.Warning.FallbackVersion != 2 {
		t.Fatalf("gap did not fall back cleanly: %+v", fallback)
	}
	code, _, stderr = runCLI(t, "apply", "--db", dbPath, "--changeset", samplePath("changes", "02-delete-directory.json"))
	if code != 3 {
		t.Fatalf("apply after gap exit = %d: %s", code, stderr)
	}
	var failure struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(stderr, &failure); err != nil || failure.Code != "E_SNAPSHOT_VERSION" {
		t.Fatalf("gap apply error = %s (%v)", stderr, err)
	}
	after := readCurrent(t, dbPath)
	if after.Version != 2 || after.Digest != before.Digest {
		t.Fatalf("gap apply changed committed prefix: before=%+v after=%+v", before, after)
	}
}

func TestSameVersionDigestReplacementIsRejectedByCLI(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	initCLI(t, dbPath)
	code, _, stderr := runCLI(t, "apply", "--db", dbPath, "--changeset", samplePath("changes", "01-add-cache.json"))
	if code != 0 {
		t.Fatalf("apply: %s", stderr)
	}
	code, _, stderr = runCLI(t, "inject", "digest-mismatch", "--db", dbPath, "--namespace", "prod/win-sandbox", "--version", "2")
	if code != 0 {
		t.Fatalf("inject digest mismatch: %s", stderr)
	}
	current := readCurrent(t, dbPath)
	if current.Version != 1 || current.Warning == nil || current.Warning.Code != "E_SNAPSHOT_VERSION" {
		t.Fatalf("same-version replacement was mixed into current: %+v", current)
	}
	code, _, stderr = runCLI(t, "snapshot", "--db", dbPath, "--version", "2")
	if code != 3 {
		t.Fatalf("snapshot v2 exit = %d: %s", code, stderr)
	}
}

func TestCLIHelpAndMissingDatabaseDoNotProceed(t *testing.T) {
	code, stdout, stderr := runCLI(t, "init", "--help")
	if code != 0 || len(stdout) == 0 || len(stderr) != 0 {
		t.Fatalf("help exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	code, _, stderr = runCLI(t, "current")
	if code != 2 {
		t.Fatalf("missing --db exit=%d stderr=%s", code, stderr)
	}
}

func TestMalformedPolicySampleIsRejectedWithoutSnapshot(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	code, _, stderr := runCLI(t, "init", "--db", dbPath, "--policy", samplePath("invalid", "policy-duplicate-id.json"))
	if code != 2 {
		t.Fatalf("malformed policy exit=%d stderr=%s", code, stderr)
	}
	var failure struct {
		Code   string `json:"code"`
		Entry  string `json:"entry"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(stderr, &failure); err != nil {
		t.Fatal(err)
	}
	if failure.Code != "E_POLICY_MALFORMED" || failure.Entry == "" || failure.Reason != "duplicate_identifier" {
		t.Fatalf("malformed policy error = %+v", failure)
	}
	code, _, _ = runCLI(t, "current", "--db", dbPath, "--namespace", "prod/malformed-sandbox")
	if code != 8 {
		t.Fatalf("malformed policy left a snapshot; current exit=%d", code)
	}
}

func initCLI(t *testing.T, dbPath string) {
	t.Helper()
	code, _, stderr := runCLI(t, "init", "--db", dbPath, "--policy", samplePath("policies", "baseline.json"))
	if code != 0 {
		t.Fatalf("init failed (%d): %s", code, stderr)
	}
}

func readCurrent(t *testing.T, dbPath string) currentView {
	t.Helper()
	code, stdout, stderr := runCLI(t, "current", "--db", dbPath)
	if code != 0 {
		t.Fatalf("current failed (%d): %s", code, stderr)
	}
	var view currentView
	if err := json.Unmarshal(stdout, &view); err != nil {
		t.Fatalf("decode current: %v: %s", err, stdout)
	}
	return view
}

func runCLI(t *testing.T, args ...string) (int, []byte, []byte) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := cli.Run(context.Background(), args, &stdout, &stderr)
	return code, stdout.Bytes(), stderr.Bytes()
}

func samplePath(dir, name string) string {
	return filepath.Join("..", "samples", dir, name)
}
