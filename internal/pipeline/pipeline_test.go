package pipeline

import (
	"os"
	"path/filepath"
	"testing"

	"flowtrace/internal/codes"
	"flowtrace/internal/report"
	"flowtrace/internal/samplegen"
)

func setup(t *testing.T) (dir, db string) {
	t.Helper()
	dir = t.TempDir()
	if err := samplegen.WriteDir(dir); err != nil {
		t.Fatal(err)
	}
	return dir, filepath.Join(t.TempDir(), "trace.db")
}

func runOnce(t *testing.T, dir, db string, opts Options) *Outcome {
	t.Helper()
	opts.Dir, opts.DBPath = dir, db
	out, err := Run(opts)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func windowByID(rep *report.Report, id string) *report.WindowReport {
	for i := range rep.Windows {
		if rep.Windows[i].WindowID == id {
			return &rep.Windows[i]
		}
	}
	return nil
}

func TestEndToEndEscapesAndConsistency(t *testing.T) {
	dir, db := setup(t)
	out := runOnce(t, dir, db, Options{MaxConcurrent: 4})
	rep := out.Report
	if !rep.OverallConsistent {
		t.Fatalf("must be consistent: %+v", rep.Windows)
	}
	escapes := 0
	for _, w := range rep.Windows {
		for _, j := range w.Judgments {
			if j.Code == codes.EFlowEscaped {
				escapes++
				if len(j.Path) == 0 {
					t.Fatalf("escape judgment must keep minimal path: %+v", j)
				}
			}
		}
	}
	if escapes == 0 {
		t.Fatal("expected E_FLOW_ESCAPED judgments in base sample")
	}
	if len(rep.Latency) == 0 {
		t.Fatal("latency stats missing")
	}
	if len(rep.Chains) != 3 {
		t.Fatalf("want 3 committed windows, got %+v", rep.Chains)
	}
}

func TestReportByteIdenticalAcrossRuns(t *testing.T) {
	dir, _ := setup(t)
	r1 := runOnce(t, dir, filepath.Join(t.TempDir(), "a.db"), Options{MaxConcurrent: 4})
	r2 := runOnce(t, dir, filepath.Join(t.TempDir(), "b.db"), Options{MaxConcurrent: 4})
	b1, b2 := report.Marshal(r1.Report), report.Marshal(r2.Report)
	if string(b1) != string(b2) {
		t.Fatalf("reports diverge:\n%s\n---\n%s", b1, b2)
	}
}

func TestInjectReorderCorrectionsButConsistent(t *testing.T) {
	dir, db := setup(t)
	out := runOnce(t, dir, db, Options{MaxConcurrent: 4, Inject: InjectFlags{Reorder: true}})
	rep := out.Report
	if !rep.OverallConsistent {
		t.Fatal("reordered run must still settle consistently")
	}
	corrections := 0
	for _, w := range rep.Windows {
		corrections += len(w.Corrections)
	}
	if corrections == 0 {
		t.Fatal("reordered arrival must produce forward corrections")
	}
}

func TestInjectCycleTerminates(t *testing.T) {
	dir, db := setup(t)
	out := runOnce(t, dir, db, Options{MaxConcurrent: 4, Inject: InjectFlags{Cycle: true}})
	if !out.Report.OverallConsistent {
		t.Fatal("cycle run must be consistent")
	}
	w1 := windowByID(out.Report, "W1")
	if w1 == nil || len(w1.Events) != 9 {
		t.Fatalf("W1 should have 4 base + 5 cycle events: %+v", w1)
	}
}

func TestInjectEscapeAddsJudgment(t *testing.T) {
	dir, db := setup(t)
	out := runOnce(t, dir, db, Options{MaxConcurrent: 4, Inject: InjectFlags{Escape: true}})
	w2 := windowByID(out.Report, "W2")
	found := false
	for _, j := range w2.Judgments {
		if j.EventSeq == 201 && j.Code == codes.EFlowEscaped {
			found = true
		}
	}
	if !found {
		t.Fatal("injected escape event must yield E_FLOW_ESCAPED")
	}
}

func TestInjectSnapshotGap(t *testing.T) {
	dir, db := setup(t)
	out := runOnce(t, dir, db, Options{MaxConcurrent: 4, Inject: InjectFlags{SnapshotGap: true}})
	var broken *report.ChainReport
	for i := range out.Report.Chains {
		if out.Report.Chains[i].Error != "" {
			broken = &out.Report.Chains[i]
		}
	}
	if broken == nil {
		t.Fatal("expected a broken chain")
	}
	if broken.Error[:len(codes.ESnapshotVersion)] != codes.ESnapshotVersion {
		t.Fatalf("chain error: %s", broken.Error)
	}
	// Other windows' chains must be unaffected.
	for _, c := range out.Report.Chains {
		if c.WindowID != broken.WindowID && c.Error != "" {
			t.Fatalf("unrelated window polluted: %+v", c)
		}
	}
}

func TestInjectContentionAllWindowsComplete(t *testing.T) {
	dir, db := setup(t)
	out := runOnce(t, dir, db, Options{MaxConcurrent: 4, Inject: InjectFlags{Contention: true}})
	if len(out.Report.Windows) != 9 { // 3 base + 6 contention
		t.Fatalf("got %d windows", len(out.Report.Windows))
	}
	if !out.Report.OverallConsistent {
		t.Fatal("contention run must be consistent")
	}
}

func TestEventBudgetExcludesIncompleteWindow(t *testing.T) {
	dir, db := setup(t)
	out := runOnce(t, dir, db, Options{MaxConcurrent: 4, MaxEventsPerWindow: 2})
	w1 := windowByID(out.Report, "W1")
	if w1 == nil || w1.Code != codes.EEventBudget || !w1.Incomplete {
		t.Fatalf("W1 must be budget-terminated: %+v", w1)
	}
	// Incomplete windows are excluded from the consistency comparison.
	if !out.Report.OverallConsistent {
		t.Fatal("incomplete window must not break overall consistency")
	}
	for _, c := range out.Report.Chains {
		if c.WindowID == "W1" {
			t.Fatal("incomplete window must not commit snapshots")
		}
	}
}

func TestCrashAndResume(t *testing.T) {
	dir, db := setup(t)
	// First process: commits 2 windows then is "killed".
	out, err := Run(Options{Dir: dir, DBPath: db, MaxConcurrent: 4, CrashAfter: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !out.Crashed {
		t.Fatal("expected simulated kill")
	}
	// Second process: resumes; committed windows kept, the rest re-traced.
	out2, err := Run(Options{Dir: dir, DBPath: db, MaxConcurrent: 4, Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if !out2.Report.OverallConsistent {
		t.Fatal("resumed run must be consistent")
	}
	// The recovered final report must equal a crash-free run byte-for-byte.
	clean := runOnce(t, dir, filepath.Join(t.TempDir(), "clean.db"), Options{MaxConcurrent: 4})
	if string(report.Marshal(out2.Report)) != string(report.Marshal(clean.Report)) {
		t.Fatal("recovered report diverges from crash-free report")
	}
	if _, err := os.Stat(db); err != nil {
		t.Fatal(err)
	}
}
