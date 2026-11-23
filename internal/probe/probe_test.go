package probe_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"secverify/internal/config"
	"secverify/internal/deps"
	"secverify/internal/model"
	"secverify/internal/probe"
	"secverify/internal/sample"
)

func resolvedSampleItems(t *testing.T) []model.Item {
	t.Helper()
	baseline, bizErr := config.ParseBaseline(sample.BaselineJSON())
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	graph := deps.Build(baseline.Items)
	if bizErr := graph.Validate(); bizErr != nil {
		t.Fatal(bizErr)
	}
	items, bizErr := config.ResolveAccounts(baseline.Items)
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	return items
}

func TestWorkerProcessObservesRealFilesystem(t *testing.T) {
	root := filepath.Join(t.TempDir(), "sandbox")
	items := resolvedSampleItems(t)
	prepared, bizErr := probe.Materialize(root, items)
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	runner := &probe.Runner{Timeout: 5 * time.Second}
	observations, bizErr := runner.Run(context.Background(), root, prepared.Specs)
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if len(observations) != len(items) {
		t.Fatalf("observations = %d, want %d", len(observations), len(items))
	}
	byID := make(map[string]probe.Observation)
	for _, observation := range observations {
		byID[observation.ItemID] = observation
		if observation.SyncMarker != probe.SyncMarker {
			t.Fatalf("missing sync marker for %s", observation.ItemID)
		}
		if observation.ErrorCode != "" || !observation.Exists {
			t.Fatalf("observation failed for %s: %+v", observation.ItemID, observation)
		}
	}
	if got := byID["audit.logon"].Mode; got != "0750" {
		t.Fatalf("audit mode = %s, want 0750", got)
	}

	// Change a real directory permission after materialization; the child must
	// report the changed kernel-visible mode on the next probe.
	if err := os.Chmod(filepath.Join(root, "audit", "logon"), 0o700); err != nil {
		t.Fatal(err)
	}
	observations, bizErr = runner.Run(context.Background(), root, prepared.Specs)
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	for _, observation := range observations {
		if observation.ItemID == "audit.logon" && observation.Mode != "0700" {
			t.Fatalf("tampered audit mode = %s, want 0700", observation.Mode)
		}
	}
}

func TestChildRejectsSymlinkEscape(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink setup may require elevated Windows privileges")
	}
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "canary.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	runner := &probe.Runner{Timeout: 5 * time.Second}
	observations, bizErr := runner.Run(context.Background(), root, []probe.Spec{{
		ItemID: "escape.symlink", Kind: model.KindAudit, Path: "link/canary.txt",
	}})
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if len(observations) != 1 || observations[0].ErrorCode != model.CodeProbeEscape {
		t.Fatalf("expected child escape refusal, got %+v", observations)
	}
}

func TestMaterializeEscapeDoesNotTouchCanary(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "sandbox")
	canaryPath := filepath.Join(base, "escape-canary")
	canary := []byte("CANARY_MUST_NOT_CHANGE\n")
	if err := os.WriteFile(canaryPath, canary, 0o600); err != nil {
		t.Fatal(err)
	}
	item := model.Item{
		ID: "audit.escape", Kind: model.KindAudit,
		Audit: &model.AuditPolicy{Name: "escape", Category: "logon", Path: "../escape-canary", Mode: "0700", Enabled: true},
	}
	_, bizErr := probe.Materialize(root, []model.Item{item})
	if bizErr == nil || bizErr.Code != model.CodeProbeEscape {
		t.Fatalf("expected E_PROBE_ESCAPE, got %+v", bizErr)
	}
	after, err := os.ReadFile(canaryPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(canary) {
		t.Fatal("outside canary was modified")
	}
}

func TestSecureJoinRejectsParentTraversal(t *testing.T) {
	root := t.TempDir()
	if _, err := probe.SecureJoin(root, "../outside"); err == nil {
		t.Fatal("parent traversal was accepted")
	}
	if _, err := probe.SecureJoin(root, filepath.Join("safe", "file")); err != nil {
		t.Fatalf("safe path rejected: %v", err)
	}
}
