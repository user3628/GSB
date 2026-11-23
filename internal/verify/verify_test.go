package verify_test

import (
	"context"
	"path/filepath"
	"testing"

	"dns-cache-workbench/internal/sample"
	"dns-cache-workbench/internal/verify"
	"dns-cache-workbench/internal/workbench"
)

func TestFiveInjectionAssertions(t *testing.T) {
	root := t.TempDir()
	if err := sample.Generate(root); err != nil {
		t.Fatalf("generate: %v", err)
	}
	result, err := verify.Run(context.Background(), verify.Options{
		InputRoot: root,
		WorkDir:   filepath.Join(root, "verification"),
		Target:    "v2",
		Window:    10,
		Flip:      3,
	})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !result.AllPassed {
		t.Fatalf("verification failed: %+v", result)
	}
	wantCodes := map[string]string{
		sample.InjectionTruncatedBinary: workbench.CodeTruncated,
		sample.InjectionUnknownVersion:  workbench.CodeVersionUnsupported,
		sample.InjectionTextOverflow:    workbench.CodeLengthOverflow,
		sample.InjectionNameConflict:    workbench.CodeNameConflict,
		sample.InjectionSnapshotJump:    workbench.CodeSnapshotVersion,
	}
	if len(result.Cases) != len(wantCodes) {
		t.Fatalf("cases = %d, want %d", len(result.Cases), len(wantCodes))
	}
	for _, caseResult := range result.Cases {
		if caseResult.ObservedCode != wantCodes[caseResult.Case] {
			t.Errorf("%s observed %s, want %s", caseResult.Case, caseResult.ObservedCode, wantCodes[caseResult.Case])
		}
		if !caseResult.Passed {
			t.Errorf("%s failed checks: %+v", caseResult.Case, caseResult.Checks)
		}
	}
}
