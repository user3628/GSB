package feedbackstream

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestCLISIGKILLRecovery(t *testing.T) {
	binPath := filepath.Join(t.TempDir(), "feedbackstream")
	build := exec.Command("go", "build", "-o", binPath, "./cmd/feedbackstream")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}

	modes := []struct {
		name  string
		mode  string
		check func(*testing.T, RecoveryReport)
	}{
		{"batch intent", "batch", func(t *testing.T, r RecoveryReport) {
			if len(r.RolledBackBatches) != 1 || r.RolledBackBatches[0] != "batch-p0-w0" {
				t.Fatalf("unexpected batch recovery: %#v", r)
			}
		}},
		{"window transaction", "window", func(t *testing.T, r RecoveryReport) {
			if len(r.DiscardedWindowIntents) != 1 {
				t.Fatalf("unexpected window recovery: %#v", r)
			}
		}},
		{"cascade transaction", "cascade", func(t *testing.T, r RecoveryReport) {
			if len(r.AbortedPolicySwitches) != 1 || r.ActivePolicyVersion != 1 {
				t.Fatalf("unexpected cascade recovery: %#v", r)
			}
		}},
		{"policy intent", "policy", func(t *testing.T, r RecoveryReport) {
			if len(r.AbortedPolicySwitches) != 1 || r.ActivePolicyVersion != 1 {
				t.Fatalf("unexpected policy recovery: %#v", r)
			}
		}},
	}
	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := filepath.Join(t.TempDir(), "data")
			crash := exec.Command(binPath, "crash", "--mode", tc.mode, "--data-dir", dataDir)
			if err := crash.Run(); err == nil {
				t.Fatalf("expected %s crash command to be killed", tc.mode)
			}
			output, err := exec.Command(binPath, "recover", "--data-dir", dataDir).CombinedOutput()
			if err != nil {
				t.Fatalf("recover: %v\n%s", err, output)
			}
			var report RecoveryReport
			if err := json.Unmarshal(output, &report); err != nil {
				t.Fatalf("decode recovery report: %v\n%s", err, output)
			}
			if report.ScannedPendingIntents != 1 {
				t.Fatalf("expected one pending intent: %#v", report)
			}
			tc.check(t, report)
		})
	}
}
