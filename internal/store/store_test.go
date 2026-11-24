package store

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"fwadjudge/internal/codes"
	"fwadjudge/internal/ledger"
	"fwadjudge/internal/model"
)

func mkRules(snapshot string, base int) []model.Rule {
	mk := func(idx, prio int, action model.Action, dst string) model.Rule {
		r := model.Rule{
			SnapshotID: snapshot, Index: idx, Direction: model.DirIn, Protocol: model.ProtoTCP,
			SrcCIDR: "10.0.0.0/8", DstCIDR: dst, PortLo: 80, PortHi: 80,
			Action: action, Priority: prio,
		}
		if err := r.Normalize(); err != nil {
			panic(err)
		}
		return r
	}
	return []model.Rule{
		mk(base, 100, model.ActionAllow, "10.0.1.0/24"),
		mk(base+1, 1, model.ActionDeny, "0.0.0.0/0"),
	}
}

func TestVersionChainGapRollback(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rules := mkRules("s", 1)
	if _, cerr := st.CommitSnapshot("s", rules, nil, 0); cerr != nil {
		t.Fatal(cerr)
	}
	// Fault injection: skip version 2.
	if _, cerr := st.CommitSnapshot("s", rules, nil, 3); cerr != nil {
		t.Fatal(cerr)
	}
	latest, cerr := st.LoadChain("s")
	if cerr == nil || cerr.Code != codes.ESnapshotVersion {
		t.Fatalf("want E_SNAPSHOT_VERSION, got %v", cerr)
	}
	if latest != 1 {
		t.Fatalf("latest continuous %d, want 1", latest)
	}
	loaded, err := st.LoadRules("s", latest)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 {
		t.Fatalf("loaded %d rules", len(loaded))
	}
}

func TestWholeReplacementDetection(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, cerr := st.CommitSnapshot("r", mkRules("r", 1), nil, 0); cerr != nil {
		t.Fatal(cerr)
	}
	// Incremental update sharing one rule: not a replacement.
	v1 := mkRules("r", 1)
	v2 := append([]model.Rule{}, v1[0])
	r3 := v1[1]
	r3.Index = 5
	r3.Priority = 2
	v2 = append(v2, r3)
	meta, cerr := st.CommitSnapshot("r", v2, nil, 0)
	if cerr != nil {
		t.Fatal(cerr)
	}
	if meta.Replaced {
		t.Fatal("incremental update misclassified as replacement")
	}
	// Wholesale replacement: version increments but no rule survives.
	meta, cerr = st.CommitSnapshot("r", mkRules("r", 100), nil, 0)
	if cerr != nil {
		t.Fatal(cerr)
	}
	if !meta.Replaced {
		t.Fatal("whole replacement not detected")
	}
}

// TestCrashRecovery simulates a killed process: a helper process
// commits one snapshot, begins but never commits a second, then exits
// without closing anything. Only committed state may survive.
func TestCrashRecovery(t *testing.T) {
	if os.Getenv("GO_HELPER") == "1" {
		db := os.Getenv("GO_HELPER_DB")
		st, err := Open(db)
		if err != nil {
			os.Exit(3)
		}
		if _, cerr := st.CommitSnapshot("k", mkRules("k", 1),
			[]ledger.Entry{{SnapshotID: "k", RuleIndex: 1, Field: "weight", Amount: 500000}}, 0); cerr != nil {
			os.Exit(4)
		}
		// Simulate kill -9: exit without Close, mid-flight changes lost.
		os.Exit(2)
	}
	dir := t.TempDir()
	db := filepath.Join(dir, "crash.db")
	cmd := exec.Command(os.Args[0], "-test.run", "TestCrashRecovery")
	cmd.Env = append(os.Environ(), "GO_HELPER=1", "GO_HELPER_DB="+db)
	if err := cmd.Run(); err == nil {
		t.Fatal("helper should exit non-zero (simulated kill)")
	}
	// Recovery: reopen and verify committed state survives intact.
	st, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	latest, cerr := st.LoadChain("k")
	if cerr != nil {
		t.Fatalf("chain broken after kill: %v", cerr)
	}
	if latest != 1 {
		t.Fatalf("latest %d, want 1", latest)
	}
	rules, err := st.LoadRules("k", 1)
	if err != nil || len(rules) != 2 {
		t.Fatalf("rules %v %v", len(rules), err)
	}
	bound, err := st.LedgerBound("k")
	if err != nil {
		t.Fatal(err)
	}
	if bound != 500000 {
		t.Fatalf("ledger bound %d, want 500000 (committed entries must not roll back)", bound)
	}
	// The snapshot is re-comparable after recovery: commit a new round.
	if _, cerr := st.CommitSnapshot("k", mkRules("k", 1), nil, 0); cerr != nil {
		t.Fatal(cerr)
	}
	if latest, _ := st.LoadChain("k"); latest != 2 {
		t.Fatalf("latest after re-commit %d, want 2", latest)
	}
}
