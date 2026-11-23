package engine_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"example.com/logicplan/editor/internal/bizerr"
	"example.com/logicplan/editor/internal/codec"
	"example.com/logicplan/editor/internal/engine"
	"example.com/logicplan/editor/internal/fault"
	"example.com/logicplan/editor/internal/fixtures"
	"example.com/logicplan/editor/internal/model"
	"example.com/logicplan/editor/internal/planstate"
	_ "github.com/mattn/go-sqlite3"
)

func TestFormatsVersionDecisionsAndSnapshotReplacement(t *testing.T) {
	ctx := context.Background()
	editor := openEditor(t)
	defer editor.Close()

	currentJSONL := mustSample(t, "plan_current.jsonl")
	result, err := editor.Import(ctx, currentJSONL, "import-current-jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != 200 || result.Format != "jsonl" || result.Decision != "accept" {
		t.Fatalf("unexpected import result: %+v", result)
	}
	state := editor.Current()
	if state.Version != 200 || state.Root != "sort_amount" {
		t.Fatalf("unexpected state: %+v", state.Summary())
	}
	currentBinary := mustSample(t, "plan_current.bin")
	replacement, err := editor.Import(ctx, currentBinary, "import-current-bin")
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Version != 201 || replacement.Format != "binary" {
		t.Fatalf("unexpected replacement: %+v", replacement)
	}
	if editor.Current().Version != 201 {
		t.Fatal("binary replacement did not become current")
	}

	_, err = editor.Import(ctx, mustSample(t, "plan_v1.bin"), "stale-import")
	assertCode(t, err, bizerr.CodeSnapshotStale)
	if editor.Current().Version != 201 || editor.Current().Derived.RootHash != replacement.TreeHash {
		t.Fatal("stale import changed the committed snapshot")
	}

	_, err = editor.Import(ctx, mustSample(t, "plan_v3_required.jsonl"), "future-import")
	assertCode(t, err, bizerr.CodeFormatUnsupported)
	if editor.Current().Version != 201 || editor.Current().Derived.RootHash != replacement.TreeHash {
		t.Fatal("unsupported import changed the committed snapshot")
	}
	_, err = editor.Import(ctx, mustSample(t, "plan_v3_required.bin"), "future-binary-import")
	assertCode(t, err, bizerr.CodeFormatUnsupported)
	if editor.Current().Version != 201 || editor.Current().Derived.RootHash != replacement.TreeHash {
		t.Fatal("unsupported binary import changed the committed snapshot")
	}

	replayed, err := editor.Import(ctx, currentJSONL, "import-current-jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || replayed.Version != 200 {
		t.Fatalf("unexpected replay: %+v", replayed)
	}
	if editor.Current().Version != 201 {
		t.Fatal("idempotent replay must not switch the current snapshot backwards")
	}
	_, err = editor.Import(ctx, currentBinary, "import-current-jsonl")
	assertCode(t, err, bizerr.CodeConflictingRequest)
}

func TestLegacyImportDegradesInBothFormats(t *testing.T) {
	for _, name := range []string{"plan_v1.bin", "plan_v1.jsonl"} {
		t.Run(name, func(t *testing.T) {
			editor := openEditor(t)
			defer editor.Close()
			result, err := editor.Import(context.Background(), mustSample(t, name), "")
			if err != nil {
				t.Fatal(err)
			}
			if result.Decision != "degrade" || result.Degradations != 8 {
				t.Fatalf("unexpected degradation result: %+v", result)
			}
			state := editor.Current()
			if state.Decision != "degrade" || len(state.Degradations) != 8 {
				t.Fatalf("state decision = %s, degradations = %d", state.Decision, len(state.Degradations))
			}
			for _, node := range state.Nodes {
				if node.Stats == nil || node.Stats.RowCount != 0 || node.Columns == nil || len(node.Columns) != 0 {
					t.Fatalf("legacy defaults missing for %s: %+v", node.ID, node)
				}
			}
		})
	}
}

func TestInvalidAndCorruptImportsDoNotChangeSnapshot(t *testing.T) {
	ctx := context.Background()
	editor := openEditor(t)
	defer editor.Close()
	if _, err := editor.Import(ctx, mustSample(t, "plan_current.jsonl"), "base-invalid-tests"); err != nil {
		t.Fatal(err)
	}
	before := editor.Current()

	invalid := fixtures.Current(2, 400)
	for _, node := range invalid.Nodes {
		if node.ID == "filter_high_value" {
			node.Columns = []model.ColumnRef{{Qualifier: "unknown", Name: "column"}}
		}
	}
	var invalidBuffer bytes.Buffer
	if err := codec.EncodeBinary(&invalidBuffer, invalid, codec.BinaryOptions{}); err != nil {
		t.Fatal(err)
	}
	_, err := editor.Import(ctx, invalidBuffer.Bytes(), "invalid-plan")
	assertCode(t, err, bizerr.CodePlanInvalid)
	if editor.Current().Version != before.Version || editor.Current().Derived.RootHash != before.Derived.RootHash {
		t.Fatal("invalid plan changed current snapshot")
	}

	corrupt := append([]byte(nil), mustSample(t, "plan_next.bin")...)
	corrupt[len(corrupt)-8] ^= 0xff
	_, err = editor.Import(ctx, corrupt, "corrupt-format")
	assertCode(t, err, bizerr.CodeFormatCorrupt)
	be, _ := bizerr.As(err)
	if be.Context["record_seq"] == nil || be.Context["crc32"] == nil {
		t.Fatalf("corrupt error is missing record/checksum context: %#v", be.Context)
	}
	if editor.Current().Version != before.Version || editor.Current().Derived.RootHash != before.Derived.RootHash {
		t.Fatal("corrupt plan changed current snapshot")
	}
}

func TestPatchSequenceCascadeAndIdempotency(t *testing.T) {
	ctx := context.Background()
	editor := openEditor(t)
	defer editor.Close()
	if _, err := editor.Import(ctx, mustSample(t, "plan_current.jsonl"), "base-200"); err != nil {
		t.Fatal(err)
	}

	patch1 := readPatch(t, "patch_001_set_predicate.json")
	r1, err := editor.ApplyPatch(ctx, patch1)
	if err != nil {
		t.Fatal(err)
	}
	if r1.Version != 201 || !r1.Applied {
		t.Fatalf("patch1 result = %+v", r1)
	}
	if contains(r1.RebuiltStructures, "column_index") || !contains(r1.RebuiltStructures, "predicate_index") || !contains(r1.RebuiltStructures, "cost_estimate") {
		t.Fatalf("predicate/stat edit rebuilt the wrong structures: %#v", r1.RebuiltStructures)
	}
	state := editor.Current()
	if got := state.Nodes["filter_high_value"].Predicate.Expr; got != "orders.amount > 500" {
		t.Fatalf("predicate = %q", got)
	}
	if got := state.Derived.CostEstimate.ByNode["filter_high_value"].Rows; got != 1800 {
		t.Fatalf("filter rows = %v", got)
	}
	if !contains(state.Derived.PredicateIndex.ByOperator["filter_high_value"], "orders.amount") {
		t.Fatal("predicate index missing edited predicate")
	}
	if !contains(state.Derived.ColumnIndex.References["orders.amount"], "filter_high_value") {
		t.Fatal("column index missing edited operator")
	}
	assertVersionInvariants(t, state)

	r2, err := editor.ApplyPatch(ctx, readPatch(t, "patch_002_reorder_join.json"))
	if err != nil || r2.Version != 202 {
		t.Fatalf("patch2 = %+v, err = %v", r2, err)
	}
	if len(r2.RebuiltStructures) != 4 {
		t.Fatalf("structural edit should cascade all derived structures: %#v", r2.RebuiltStructures)
	}
	if got := editor.Current().Nodes["join_orders_customers"].Children; len(got) != 2 || got[0] != "scan_customers" {
		t.Fatalf("join order = %#v", got)
	}

	r3, err := editor.ApplyPatch(ctx, readPatch(t, "patch_003_add_filter.json"))
	if err != nil || r3.Version != 203 {
		t.Fatalf("patch3 = %+v, err = %v", r3, err)
	}
	if editor.Current().Nodes["filter_region"] == nil {
		t.Fatal("added filter is missing")
	}
	if !contains(editor.Current().Derived.PredicateIndex.ByColumn["customers.region"], "filter_region") {
		t.Fatal("added filter missing from predicate index")
	}
	assertVersionInvariants(t, editor.Current())

	r4, err := editor.ApplyPatch(ctx, readPatch(t, "patch_004_delete_filter.json"))
	if err != nil || r4.Version != 204 {
		t.Fatalf("patch4 = %+v, err = %v", r4, err)
	}
	state = editor.Current()
	if state.Nodes["filter_region"] != nil {
		t.Fatal("deleted filter remains in tree")
	}
	if contains(state.Derived.PredicateIndex.ByColumn["customers.region"], "filter_region") {
		t.Fatal("deleted filter remains in predicate index")
	}
	assertVersionInvariants(t, state)

	_, err = editor.ApplyPatch(ctx, readPatch(t, "patch_stale_base.json"))
	assertCode(t, err, bizerr.CodeStaleBase)
	if editor.Current().Version != 204 {
		t.Fatal("stale patch changed committed state")
	}

	replayed, err := editor.ApplyPatch(ctx, patch1)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || replayed.Version != 201 {
		t.Fatalf("expected cached patch result, got %+v", replayed)
	}
	if editor.Current().Version != 204 {
		t.Fatal("replayed patch moved state backwards")
	}
	_, err = editor.ApplyPatch(ctx, readPatch(t, "patch_conflicting_commit.json"))
	assertCode(t, err, bizerr.CodeConflictingRequest)
}

func TestConcurrentSameBaseHasSingleWinnerAndReadersSeeWholeStates(t *testing.T) {
	ctx := context.Background()
	editor := openEditor(t)
	defer editor.Close()
	if _, err := editor.Import(ctx, mustSample(t, "plan_current.jsonl"), "base"); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	readerErrors := make(chan error, 32)
	var readers sync.WaitGroup
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					state := editor.Current()
					if state == nil {
						readerErrors <- errors.New("reader saw nil state")
						return
					}
					if err := planstate.DerivedInvariantError(state); err != nil {
						readerErrors <- err
						return
					}
					if state.Derived.TreeSummary[state.Root] != state.Derived.RootHash {
						readerErrors <- errors.New("reader saw mismatched root summary")
						return
					}
				}
			}
		}()
	}

	patches := []engine.PatchRequest{
		{CommitID: "concurrent-a", BaseVersion: 200, Operations: []planstate.Operation{{
			Op: "set_predicate", NodeID: "filter_high_value",
			Predicate: predicate("orders.amount > 500"),
		}}},
		{CommitID: "concurrent-b", BaseVersion: 200, Operations: []planstate.Operation{{
			Op: "set_predicate", NodeID: "filter_high_value",
			Predicate: predicate("orders.amount > 900"),
		}}},
	}
	type outcome struct {
		result engine.PatchResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	start := make(chan struct{})
	for _, patch := range patches {
		patch := patch
		go func() {
			<-start
			result, err := editor.ApplyPatch(ctx, patch)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(start)
	first, second := <-outcomes, <-outcomes
	successes, stale := 0, 0
	for _, out := range []outcome{first, second} {
		if out.err == nil {
			successes++
		} else if bizerr.Code(out.err) == bizerr.CodeStaleBase {
			stale++
		} else {
			t.Fatalf("unexpected concurrent error: %v", out.err)
		}
	}
	if successes != 1 || stale != 1 {
		t.Fatalf("successes=%d stale=%d, want 1/1", successes, stale)
	}
	close(stop)
	readers.Wait()
	close(readerErrors)
	for err := range readerErrors {
		t.Fatal(err)
	}
	if editor.Current().Version != 201 {
		t.Fatalf("version = %d, want 201", editor.Current().Version)
	}
}

func TestImportInvalidatesInFlightBaseVersion(t *testing.T) {
	ctx := context.Background()
	editor := openEditor(t)
	defer editor.Close()
	if _, err := editor.Import(ctx, mustSample(t, "plan_current.jsonl"), "base-before-import"); err != nil {
		t.Fatal(err)
	}
	if _, err := editor.ApplyPatch(ctx, readPatch(t, "patch_001_set_predicate.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := editor.Import(ctx, mustSample(t, "plan_next.jsonl"), "replacement-300"); err != nil {
		t.Fatal(err)
	}
	stateAfterImport := editor.Current()
	if stateAfterImport.Version != 300 {
		t.Fatalf("version after import = %d", stateAfterImport.Version)
	}
	stalePatch := engine.PatchRequest{
		CommitID: "in-flight-before-import", BaseVersion: 201,
		Operations: []planstate.Operation{{
			Op: "set_predicate", NodeID: "filter_high_value",
			Predicate: predicate("orders.amount > 777"),
		}},
	}
	_, err := editor.ApplyPatch(ctx, stalePatch)
	assertCode(t, err, bizerr.CodeStaleBase)
	if editor.Current().Version != 300 || editor.Current().Derived.RootHash != stateAfterImport.Derived.RootHash {
		t.Fatal("stale in-flight patch changed snapshot after import")
	}
}

func TestConcurrentSameCommitWaitsAndReusesPersistedResult(t *testing.T) {
	ctx := context.Background()
	editor := openEditor(t)
	defer editor.Close()
	if _, err := editor.Import(ctx, mustSample(t, "plan_current.jsonl"), "base-idempotent-concurrency"); err != nil {
		t.Fatal(err)
	}
	patch := readPatch(t, "patch_001_set_predicate.json")
	type outcome struct {
		result engine.PatchResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			result, err := editor.ApplyPatch(ctx, patch)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(start)
	a, b := <-outcomes, <-outcomes
	for _, out := range []outcome{a, b} {
		if out.err != nil || out.result.Version != 201 {
			t.Fatalf("idempotent concurrent outcome: result=%+v err=%v", out.result, out.err)
		}
	}
	if a.result.Replayed == b.result.Replayed {
		t.Fatalf("exactly one response should be a replay: a=%v b=%v", a.result.Replayed, b.result.Replayed)
	}
	if editor.Current().Version != 201 {
		t.Fatalf("version = %d, want 201", editor.Current().Version)
	}
}

func TestPersistenceReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db := filepath.Join(dir, "editor.db")
	editor, err := engine.Open(ctx, db, fault.New())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := editor.Import(ctx, mustSample(t, "plan_current.jsonl"), "base"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"patch_001_set_predicate.json", "patch_002_reorder_join.json", "patch_003_add_filter.json", "patch_004_delete_filter.json"} {
		if _, err := editor.ApplyPatch(ctx, readPatch(t, name)); err != nil {
			t.Fatal(err)
		}
	}
	before := editor.Current()
	if err := editor.Close(); err != nil {
		t.Fatal(err)
	}
	editor, err = engine.Open(ctx, db, fault.New())
	if err != nil {
		t.Fatal(err)
	}
	defer editor.Close()
	after := editor.Current()
	if after.Version != 204 || after.Derived.RootHash != before.Derived.RootHash {
		t.Fatalf("reopened state = %+v, want version 204 hash %s", after.Summary(), before.Derived.RootHash)
	}
	assertVersionInvariants(t, after)
	if editor.Recovery().CurrentVersion != 204 || !editor.Recovery().Consistent {
		t.Fatalf("unexpected recovery report: %+v", editor.Recovery())
	}
	replayed, err := editor.ApplyPatch(ctx, readPatch(t, "patch_001_set_predicate.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || replayed.Version != 201 || editor.Current().Version != 204 {
		t.Fatalf("persisted idempotency was not reused after restart: %+v", replayed)
	}
}

func TestSQLiteContainsVersionsOperatorsDerivedIndexesIntentsAndCommits(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "editor.db")
	editor, err := engine.Open(ctx, dbPath, fault.New())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := editor.Import(ctx, mustSample(t, "plan_current.jsonl"), "sql-import"); err != nil {
		t.Fatal(err)
	}
	if _, err := editor.ApplyPatch(ctx, readPatch(t, "patch_001_set_predicate.json")); err != nil {
		t.Fatal(err)
	}
	if err := editor.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", "file:"+dbPath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	checks := []struct {
		query string
		want  int
	}{
		{`SELECT COUNT(*) FROM snapshot_versions`, 2},
		{`SELECT COUNT(*) FROM derived_indexes`, 8},
		{`SELECT COUNT(*) FROM intents WHERE status='completed'`, 2},
		{`SELECT COUNT(*) FROM commits`, 2},
		{`SELECT COUNT(*) FROM operators WHERE version=201`, 6},
	}
	for _, check := range checks {
		var got int
		if err := db.QueryRow(check.query).Scan(&got); err != nil {
			t.Fatalf("%s: %v", check.query, err)
		}
		if got != check.want {
			t.Fatalf("%s = %d, want %d", check.query, got, check.want)
		}
	}
	var current string
	if err := db.QueryRow(`SELECT value FROM meta WHERE key='current_version'`).Scan(&current); err != nil {
		t.Fatal(err)
	}
	if current != "201" {
		t.Fatalf("current_version = %q, want 201", current)
	}
}

func TestRecoveryAfterPatchAndImportIntentCrashes(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		stage string
		crash func(*engine.Engine)
	}{
		{
			name: "patch_after_intent", stage: fault.PatchAfterIntent,
			crash: func(editor *engine.Engine) {
				_, _ = editor.ApplyPatch(ctx, readPatch(t, "patch_001_set_predicate.json"))
			},
		},
		{
			name: "import_after_intent", stage: fault.ImportAfterIntent,
			crash: func(editor *engine.Engine) {
				_, _ = editor.Import(ctx, mustSample(t, "plan_next.jsonl"), "crashing-import")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db := filepath.Join(dir, "editor.db")
			controller := fault.New()
			editor, err := engine.Open(ctx, db, controller)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := editor.Import(ctx, mustSample(t, "plan_current.jsonl"), "base"); err != nil {
				t.Fatal(err)
			}
			controller.SetHook(func(stage string) {
				if stage == tc.stage {
					panic("simulated hard termination")
				}
			})
			if err := controller.Arm(tc.stage, "crash", 0, 0); err != nil {
				t.Fatal(err)
			}
			triggerPanic(t, func() { tc.crash(editor) })
			if err := editor.Close(); err != nil {
				t.Fatal(err)
			}

			restarted, err := engine.Open(ctx, db, fault.New())
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			report := restarted.Recovery()
			if report.PendingFound != 1 || len(report.Aborted) != 1 || report.CurrentVersion != 200 || !report.Consistent {
				t.Fatalf("recovery report = %+v", report)
			}
			state := restarted.Current()
			if state.Version != 200 {
				t.Fatalf("restored version = %d, want 200", state.Version)
			}
			if state.Nodes["filter_high_value"].Predicate.Expr != "orders.amount > 100" {
				t.Fatal("interrupted patch leaked into restored snapshot")
			}
			assertVersionInvariants(t, state)
		})
	}
}

func TestRecoveryAfterCommitPublishesCommittedVersion(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "editor.db")
	controller := fault.New()
	editor, err := engine.Open(ctx, db, controller)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := editor.Import(ctx, mustSample(t, "plan_current.jsonl"), "base-after-commit"); err != nil {
		t.Fatal(err)
	}
	controller.SetHook(func(stage string) {
		if stage == fault.PatchAfterCommit {
			panic("simulated termination after sqlite commit")
		}
	})
	if err := controller.Arm(fault.PatchAfterCommit, "crash", 0, 0); err != nil {
		t.Fatal(err)
	}
	triggerPanic(t, func() {
		_, _ = editor.ApplyPatch(ctx, readPatch(t, "patch_001_set_predicate.json"))
	})
	// The fault fires before the atomic in-memory pointer swap. The old in-memory
	// state is no longer authoritative after restart; the committed SQLite state is.
	if editor.Current().Version != 200 {
		t.Fatal("test expected the old unswapped state in the dying process")
	}
	if err := editor.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := engine.Open(ctx, db, fault.New())
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	state := restarted.Current()
	if state.Version != 201 || state.Nodes["filter_high_value"].Predicate.Expr != "orders.amount > 500" {
		t.Fatalf("restart did not load committed version 201: %+v", state.Summary())
	}
	report := restarted.Recovery()
	if report.PendingFound != 0 || report.CurrentVersion != 201 || !report.Consistent {
		t.Fatalf("unexpected post-commit recovery report: %+v", report)
	}
	assertVersionInvariants(t, state)
}

func openEditor(t *testing.T) *engine.Engine {
	t.Helper()
	editor, err := engine.Open(context.Background(), filepath.Join(t.TempDir(), "editor.db"), fault.New())
	if err != nil {
		t.Fatal(err)
	}
	return editor
}

func mustSample(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "samples", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func readPatch(t *testing.T, name string) engine.PatchRequest {
	t.Helper()
	var patch engine.PatchRequest
	if err := json.Unmarshal(mustSample(t, name), &patch); err != nil {
		t.Fatal(err)
	}
	return patch
}

func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s", code)
	}
	if got := bizerr.Code(err); got != code {
		t.Fatalf("code = %s (%v), want %s", got, err, code)
	}
}

func assertVersionInvariants(t *testing.T, state *planstate.State) {
	t.Helper()
	if err := planstate.DerivedInvariantError(state); err != nil {
		t.Fatal(err)
	}
	for id := range state.Nodes {
		if _, ok := state.Derived.CostEstimate.ByNode[id]; !ok {
			t.Fatalf("operator %s has no cost estimate", id)
		}
		if _, ok := state.Derived.TreeSummary[id]; !ok {
			t.Fatalf("operator %s has no tree summary", id)
		}
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func predicate(expr string) *model.Predicate {
	return &model.Predicate{Expr: expr, Columns: []model.ColumnRef{{Qualifier: "orders", Name: "amount"}}}
}

func triggerPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected simulated termination panic")
		}
	}()
	fn()
}
