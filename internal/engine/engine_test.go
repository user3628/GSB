package engine

import (
	"bytes"
	"database/sql"
	"path/filepath"
	"testing"
)

func testBudget() BudgetParams {
	return BudgetParams{GlobalMemory: 1600, GlobalCPU: 600, WindowCPUMax: 100}
}

func testOptions(db string) Options {
	return Options{
		Seed:        37,
		NumWindows:  12,
		Concurrency: 4,
		Budget:      testBudget(),
		DBPath:      db,
		CrashAfter:  -1,
	}
}

func freshDB(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.db")
}

func runOK(t *testing.T, opts Options) *Outcome {
	t.Helper()
	outcome, err := Run(opts)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	return outcome
}

func windowByID(r Report, id string) WindowReport {
	for _, w := range r.Windows {
		if w.ID == id {
			return w
		}
	}
	return WindowReport{}
}

func hasCode(w WindowReport, code string) bool {
	for _, c := range w.Codes {
		if c == code {
			return true
		}
	}
	return false
}

func TestDeterministicAcrossConcurrency(t *testing.T) {
	var reference []byte
	for _, concurrency := range []int{1, 2, 4, 8} {
		opts := testOptions(freshDB(t))
		opts.Concurrency = concurrency
		outcome := runOK(t, opts)
		if reference == nil {
			reference = outcome.ReportJSON
			continue
		}
		if !bytes.Equal(reference, outcome.ReportJSON) {
			t.Fatalf("report differs at concurrency %d", concurrency)
		}
	}
}

func TestRepeatRunByteIdentical(t *testing.T) {
	first := runOK(t, testOptions(freshDB(t)))
	second := runOK(t, testOptions(freshDB(t)))
	if !bytes.Equal(first.ReportJSON, second.ReportJSON) {
		t.Fatal("identical inputs produced different reports")
	}
}

func TestCorrelateDeterministicStructure(t *testing.T) {
	sample := GenerateSample(37, 12, nil)
	valid, malformed := ParseWindows(sample)
	if len(malformed) != 0 {
		t.Fatalf("unexpected malformed windows: %v", malformed)
	}
	for _, w := range valid {
		a := Correlate(w)
		b := Correlate(w)
		if a.Complexity != b.Complexity || len(a.Chains) != len(b.Chains) {
			t.Fatalf("correlation not deterministic for %s", w.ID)
		}
		for i := range a.Chains {
			if a.Chains[i].Number != b.Chains[i].Number {
				t.Fatalf("chain numbering unstable for %s", w.ID)
			}
			if len(a.Chains[i].Nodes) != len(b.Chains[i].Nodes) {
				t.Fatalf("chain nodes unstable for %s", w.ID)
			}
			for j := range a.Chains[i].Nodes {
				if a.Chains[i].Nodes[j] != b.Chains[i].Nodes[j] {
					t.Fatalf("chain node order unstable for %s", w.ID)
				}
			}
		}
	}
}

func TestInjectDupEventID(t *testing.T) {
	baseline := runOK(t, testOptions(freshDB(t)))
	opts := testOptions(freshDB(t))
	opts.Inject = []string{InjectDupEventID}
	outcome := runOK(t, opts)

	dup := windowByID(outcome.Report, "W-INJ-DUP")
	if dup.Verdict != VerdictRejected || !hasCode(dup, CodeWindowMalformed) {
		t.Fatalf("expected E_WINDOW_MALFORMED for W-INJ-DUP, got %+v", dup)
	}
	for _, w := range baseline.Report.Windows {
		got := windowByID(outcome.Report, w.ID)
		if got.Verdict != w.Verdict {
			t.Fatalf("window %s verdict changed by injection: %s -> %s", w.ID, w.Verdict, got.Verdict)
		}
	}
}

func TestInjectOrphanRef(t *testing.T) {
	opts := testOptions(freshDB(t))
	opts.Budget = BudgetParams{GlobalMemory: 5000, GlobalCPU: 5000, WindowCPUMax: 100}
	opts.Inject = []string{InjectOrphanRef}
	outcome := runOK(t, opts)

	orphan := windowByID(outcome.Report, "W-INJ-ORPHAN")
	if !hasCode(orphan, CodeOrphanRef) {
		t.Fatalf("expected E_ORPHAN_REF for W-INJ-ORPHAN, got %+v", orphan)
	}
	if len(orphan.Orphans) != 2 {
		t.Fatalf("expected 2 orphan events, got %v", orphan.Orphans)
	}
}

func TestOrphanAttachVirtualRoot(t *testing.T) {
	w := Window{
		ID: "W-T",
		Events: []Event{
			{ID: "E1", Subject: "s", Object: "o", Op: OpCreate, TimeOffset: 0, Weight: 5},
			{ID: "E2", Subject: "s", Object: "ghost", Op: OpRead, TimeOffset: 1, Weight: 5},
		},
		Claim: ResourceClaim{Depth: 2, Memory: 10},
	}
	c := Correlate(w)
	if len(c.Chains) == 0 || !c.Chains[0].Virtual || c.Chains[0].Number != 0 {
		t.Fatalf("expected virtual root chain first, got %+v", c.Chains)
	}
	if len(c.Chains[0].Nodes) != 1 || c.Chains[0].Nodes[0] != "E2" {
		t.Fatalf("orphan not attached to virtual root: %+v", c.Chains[0])
	}
}

func TestInjectTinyWindowBudget(t *testing.T) {
	opts := testOptions(freshDB(t))
	opts.Inject = []string{InjectTinyWindowBudget}
	outcome := runOK(t, opts)

	deep := windowByID(outcome.Report, "W-INJ-DEEP")
	if !hasCode(deep, CodeWindowBudget) {
		t.Fatalf("expected E_WINDOW_BUDGET for W-INJ-DEEP, got %+v", deep)
	}
	if deep.Verdict != VerdictDegraded && deep.Verdict != VerdictDeferred {
		t.Fatalf("expected degraded or deferred verdict, got %s", deep.Verdict)
	}
}

func TestInjectGlobalBudgetShortage(t *testing.T) {
	opts := testOptions(freshDB(t))
	opts.Inject = []string{InjectGlobalBudgetShortage}
	outcome := runOK(t, opts)

	found := false
	for _, id := range []string{"W-INJ-HOG-0", "W-INJ-HOG-1", "W-INJ-HOG-2"} {
		w := windowByID(outcome.Report, id)
		if hasCode(w, CodeBudgetExhausted) {
			found = true
		}
	}
	if !found {
		t.Fatal("expected at least one hog window with E_BUDGET_EXHAUSTED")
	}
}

func TestInjectConflictMerge(t *testing.T) {
	opts := testOptions(freshDB(t))
	opts.Budget = BudgetParams{GlobalMemory: 5000, GlobalCPU: 5000, WindowCPUMax: 100}
	opts.Inject = []string{InjectConflictMerge}
	outcome := runOK(t, opts)

	byEntity := map[string]ConflictItem{}
	for _, c := range outcome.Report.Conflicts {
		byEntity[c.Entity] = c
	}
	shared1, ok := byEntity["SHARED-1"]
	if !ok {
		t.Fatalf("expected conflict on SHARED-1, got %+v", outcome.Report.Conflicts)
	}
	if shared1.FinalState != OpRevoke {
		t.Fatalf("expected final state revoke for SHARED-1, got %s", shared1.FinalState)
	}
	if len(shared1.Windows) != 2 || shared1.Windows[0] != "W-INJ-CONF-A" || shared1.Windows[1] != "W-INJ-CONF-B" {
		t.Fatalf("unexpected participating windows: %v", shared1.Windows)
	}
	shared2, ok := byEntity["SHARED-2"]
	if !ok || shared2.FinalState != OpDelete {
		t.Fatalf("expected create/delete conflict on SHARED-2 with final delete, got %+v", shared2)
	}
}

func TestConflictMergeIndependentOfWindowOrder(t *testing.T) {
	opts := testOptions(freshDB(t))
	opts.Budget = BudgetParams{GlobalMemory: 5000, GlobalCPU: 5000, WindowCPUMax: 100}
	opts.Inject = []string{InjectConflictMerge}
	first := runOK(t, opts)

	opts2 := testOptions(freshDB(t))
	opts2.Budget = BudgetParams{GlobalMemory: 5000, GlobalCPU: 5000, WindowCPUMax: 100}
	opts2.Inject = []string{InjectConflictMerge}
	opts2.Concurrency = 1
	second := runOK(t, opts2)

	if len(first.Report.Conflicts) != len(second.Report.Conflicts) {
		t.Fatal("conflict sets differ")
	}
	for i := range first.Report.Conflicts {
		a, b := first.Report.Conflicts[i], second.Report.Conflicts[i]
		if a.Entity != b.Entity || a.FinalState != b.FinalState {
			t.Fatalf("conflict merge differs at %d", i)
		}
	}
}

func TestCrashRecovery(t *testing.T) {
	db := freshDB(t)

	crashOpts := testOptions(db)
	crashOpts.CrashAfter = 3
	if _, err := Run(crashOpts); err != ErrSimulatedCrash {
		t.Fatalf("expected simulated crash, got %v", err)
	}

	store, err := OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	committedAfterCrash, err := store.LoadVerdicts()
	if err != nil {
		t.Fatal(err)
	}
	snapsAfterCrash, brokenAfterCrash, err := store.LoadSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	if len(committedAfterCrash) != 3 {
		t.Fatalf("expected 3 committed verdicts after crash, got %d", len(committedAfterCrash))
	}
	if len(snapsAfterCrash) != 0 || brokenAfterCrash {
		t.Fatal("no snapshot may be committed before the crash point")
	}

	recovered := runOK(t, testOptions(db))

	store2, err := OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	committedAfterRecovery, err := store2.LoadVerdicts()
	if err != nil {
		t.Fatal(err)
	}
	store2.Close()
	for id, before := range committedAfterCrash {
		after, ok := committedAfterRecovery[id]
		if !ok {
			t.Fatalf("committed verdict %s lost after recovery", id)
		}
		if after.Verdict != before.Verdict || len(after.Codes) != len(before.Codes) {
			t.Fatalf("committed verdict %s rolled back or changed", id)
		}
	}
	for _, code := range recovered.Report.Snapshot.Codes {
		if code == CodeSnapshotVersion {
			t.Fatal("recovery must not produce a version chain gap")
		}
	}

	clean := runOK(t, testOptions(freshDB(t)))
	if len(clean.Report.Windows) != len(recovered.Report.Windows) {
		t.Fatal("window count differs after recovery")
	}
	for i := range clean.Report.Windows {
		a, b := clean.Report.Windows[i], recovered.Report.Windows[i]
		if a.ID != b.ID || a.Verdict != b.Verdict {
			t.Fatalf("window %s verdict differs after recovery: %s vs %s", a.ID, a.Verdict, b.Verdict)
		}
		if len(a.Codes) != len(b.Codes) {
			t.Fatalf("window %s codes differ after recovery", a.ID)
		}
		for j := range a.Codes {
			if a.Codes[j] != b.Codes[j] {
				t.Fatalf("window %s code %d differs after recovery", a.ID, j)
			}
		}
	}
	if len(clean.Report.Conflicts) != len(recovered.Report.Conflicts) {
		t.Fatal("conflict set differs after recovery")
	}
}

func TestSnapshotVersionSkipInjection(t *testing.T) {
	opts := testOptions(freshDB(t))
	opts.InjectSnapshotGap = true
	outcome := runOK(t, opts)

	found := false
	for _, c := range outcome.Report.Snapshot.Codes {
		if c == CodeSnapshotVersion {
			found = true
		}
	}
	if !found {
		t.Fatal("expected E_SNAPSHOT_VERSION after gap injection")
	}
	if outcome.Report.Snapshot.Version != 1 {
		t.Fatalf("expected fallback to version 1, got %d", outcome.Report.Snapshot.Version)
	}

	store, err := OpenStore(opts.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	chain, broken, err := store.LoadSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if !broken {
		t.Fatal("chain must be reported broken after gap injection")
	}
	if len(chain) != 1 {
		t.Fatalf("expected 1 valid snapshot, got %d", len(chain))
	}
	if chain[0].Hash != contentHash(chain[0].Content) {
		t.Fatal("committed snapshot content polluted")
	}
}

func TestSnapshotWholesaleReplacementDetected(t *testing.T) {
	db := freshDB(t)
	store, err := OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	v1, err := store.CommitSnapshot("fp", `{"round":1}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitSnapshot("fp", `{"round":2}`); err != nil {
		t.Fatal(err)
	}
	store.Close()

	// Wholesale replacement: rewrite version 2 content in place, keeping the
	// version number. The hash chain must reject it.
	raw, err := sql.Open("sqlite", db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`UPDATE snapshots SET content='{"replaced":true}' WHERE version=2`); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	store2, err := OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	chain, broken, err := store2.LoadSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if !broken {
		t.Fatal("wholesale replacement must break the chain")
	}
	if len(chain) != 1 || chain[0].Version != v1.Version {
		t.Fatalf("expected fallback to version 1, got %+v", chain)
	}
}

func TestQueueStatsObservable(t *testing.T) {
	outcome := runOK(t, testOptions(freshDB(t)))
	if outcome.Report.Queue.Enqueued != 12 || outcome.Report.Queue.Dequeued != 12 {
		t.Fatalf("unexpected queue stats: %+v", outcome.Report.Queue)
	}
}
