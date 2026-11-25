package convergence

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDeterministicSampleFilesParseAndVerify(t *testing.T) {
	snapshotData, err := os.ReadFile(filepath.Join("samples", "snapshot-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := ParseSnapshot(snapshotData)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.VerifyChecksum(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Policy.VerifyChecksum(); err != nil {
		t.Fatal(err)
	}

	policyData, err := os.ReadFile(filepath.Join("samples", "policy-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParsePolicy(policyData); err != nil {
		t.Fatal(err)
	}

	deltaData, err := os.ReadFile(filepath.Join("samples", "delta-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseDelta(deltaData); err != nil {
		t.Fatal(err)
	}

	cycleData, err := os.ReadFile(filepath.Join("samples", "cycle-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	cycleSnapshot, err := ParseSnapshot(cycleData)
	if err != nil {
		t.Fatal(err)
	}
	if _, planErr := BuildPlan(cycleSnapshot.Objects, cycleSnapshot.Policy); !HasCode(planErr, CodeDependencyCycle) {
		t.Fatalf("sample cycle plan error=%v", planErr)
	}

	invalidData, err := os.ReadFile(filepath.Join("samples", "invalid-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseSnapshot(invalidData); !HasCode(err, CodeSpecInvalid) {
		t.Fatalf("invalid sample err=%v, want E_SPEC_INVALID", err)
	}
}
