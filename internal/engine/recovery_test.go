package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"queryrewrite/internal/gen"
	"queryrewrite/internal/spec"
	"queryrewrite/internal/store"
)

func openTestEngine(t *testing.T, dir string) (*Engine, *store.Store, spec.Policy) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	p := gen.DemoPolicy()
	st, err := store.Open(filepath.Join(dir, "t.db"), filepath.Join(dir, "t.db.lock"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SavePolicy(p, "baseline"); err != nil {
		t.Fatal(err)
	}
	e, err := New(st, p)
	if err != nil {
		t.Fatal(err)
	}
	return e, st, p
}

func TestTurn1DeterminismAcrossEngines(t *testing.T) {
	turns := gen.DemoSessions()
	qt := turns[0]

	e1, st1, p1 := openTestEngine(t, t.TempDir())
	o1, err := e1.RunTurn(qt)
	if err != nil {
		t.Fatal(err)
	}
	st1.Close()

	// Second engine: same empty start, should be byte identical.
	e2, st2, _ := openTestEngine(t, t.TempDir())
	o2, err := e2.RunTurn(qt)
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := json.Marshal(o1.Adj)
	b2, _ := json.Marshal(o2.Adj)
	if string(b1) != string(b2) {
		t.Fatalf("adjudication differs:\n%s\nvs\n%s", b1, b2)
	}
	_ = p1
	st2.Close()
}

func TestCrashAfterStage2Recovery(t *testing.T) {
	turns := gen.DemoSessions()
	qt := turns[0]
	dir := t.TempDir()

	// Reference full run.
	eRef, stRef, p := openTestEngine(t, filepath.Join(dir, "ref"))
	oRef, err := eRef.RunTurn(qt)
	if err != nil {
		t.Fatal(err)
	}
	stRef.Close()

	// Crash run: commit stages 1-2 only, then open a fresh engine (simulates
	// process restart) and resume the same turn.
	cdir := filepath.Join(dir, "crash")
	if err := os.MkdirAll(cdir, 0o755); err != nil {
		t.Fatal(err)
	}
	stC, err := store.Open(filepath.Join(cdir, "c.db"), filepath.Join(cdir, "c.db.lock"))
	if err != nil {
		t.Fatal(err)
	}
	_ = stC.SavePolicy(p, "baseline")
	eC, err := New(stC, p)
	if err != nil {
		t.Fatal(err)
	}
	_, cands, berr := Stage1(qt, p)
	if berr != nil {
		t.Fatal(berr)
	}
	hits := Stage2(cands, p, eC.cache, qt.Turn)
	payload, _ := json.Marshal(struct {
		Hits []spec.HitResult `json:"hits"`
	}{hits})
	_, err = stC.CommitCheckpoint(spec.Checkpoint{
		Stage: spec.StagePrefix, SessionID: qt.SessionID, Turn: qt.Turn,
		InputDigest: "x", PolicyVersion: p.Version, ProductDigest: "d", Payload: string(payload),
	})
	if err != nil {
		t.Fatal(err)
	}
	stC.Close()

	// Restart: cache must be empty because no stage4 committed.
	stR, err := store.Open(filepath.Join(cdir, "c.db"), filepath.Join(cdir, "c.db.lock"))
	if err != nil {
		t.Fatal(err)
	}
	eR, err := New(stR, p)
	if err != nil {
		t.Fatal(err)
	}
	if got := eR.cache.durableSize(); got != 0 {
		t.Fatalf("expected empty cache after restart, got %d slots", got)
	}
	oR, err := eR.RunTurn(qt)
	if err != nil {
		t.Fatal(err)
	}
	bRef, _ := json.Marshal(oRef.Adj)
	bR, _ := json.Marshal(oR.Adj)
	if string(bRef) != string(bR) {
		t.Fatalf("post-crash adjudication differs:\nREF=%s\nREC=%s", bRef, bR)
	}
	stR.Close()
}

func TestFullSequenceCrashRecovery(t *testing.T) {
	turns := gen.DemoSessions()
	dir := t.TempDir()

	// Reference: run every turn in one engine.
	refDir := filepath.Join(dir, "ref")
	eRef, stRef, p := openTestEngine(t, refDir)
	refOuts := map[string]spec.TurnOutput{}
	for _, q := range turns {
		o, err := eRef.RunTurn(q)
		if berr, ok := err.(*spec.BizError); ok && berr.Code == spec.CodeEmptyQuery {
			continue
		} else if err != nil {
			t.Fatal(err)
		}
		refOuts[q.SessionID+"#"+itoa(q.Turn)] = o
	}
	stRef.Close()

	// Crash engine: simulate kill after stage2 of the very first turn.
	cDir := filepath.Join(dir, "crash")
	if err := os.MkdirAll(cDir, 0o755); err != nil {
		t.Fatal(err)
	}
	stC, err := store.Open(filepath.Join(cDir, "c.db"), filepath.Join(cDir, "c.db.lock"))
	if err != nil {
		t.Fatal(err)
	}
	_ = stC.SavePolicy(p, "baseline")
	eC, err := New(stC, p)
	if err != nil {
		t.Fatal(err)
	}
	q0 := turns[0]
	if err := stC.AdvanceTurn(q0.SessionID, q0.Turn); err != nil {
		t.Fatal(err)
	}
	_, cands, berr := Stage1(q0, p)
	if berr != nil {
		t.Fatal(berr)
	}
	hits := Stage2(cands, p, eC.cache, q0.Turn)
	payload, _ := json.Marshal(struct {
		Hits []spec.HitResult `json:"hits"`
	}{hits})
	if _, err := stC.CommitCheckpoint(spec.Checkpoint{
		Stage: spec.StagePrefix, SessionID: q0.SessionID, Turn: q0.Turn,
		InputDigest: "x", PolicyVersion: p.Version, ProductDigest: "d", Payload: string(payload),
	}); err != nil {
		t.Fatal(err)
	}
	stC.Close()

	// Restart and run all turns.
	stR, err := store.Open(filepath.Join(cDir, "c.db"), filepath.Join(cDir, "c.db.lock"))
	if err != nil {
		t.Fatal(err)
	}
	eR, err := New(stR, p)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range turns {
		o, err := eR.RunTurn(q)
		if berr, ok := err.(*spec.BizError); ok && berr.Code == spec.CodeEmptyQuery {
			continue
		} else if err != nil {
			t.Fatal(err)
		}
		key := q.SessionID + "#" + itoa(q.Turn)
		ref, ok := refOuts[key]
		if !ok {
			continue
		}
		b1, _ := json.Marshal(ref.Hits)
		b2, _ := json.Marshal(o.Hits)
		if string(b1) != string(b2) {
			t.Fatalf("HITS diverge at %s\nREF=%s\nREC=%s", key, b1, b2)
		}
		a1, _ := json.Marshal(ref.Ledger)
		a2, _ := json.Marshal(o.Ledger)
		if string(a1) != string(a2) {
			t.Fatalf("LEDGER diverge at %s\nREF=%s\nREC=%s", key, a1, a2)
		}
	}
	stR.Close()
}

func TestStaleReadRejectsOldObservedVersion(t *testing.T) {
	dir := t.TempDir()
	e, st, p := openTestEngine(t, filepath.Join(dir, "e"))
	qt := gen.DemoSessions()[0]
	if _, err := e.RunTurn(qt); err != nil {
		t.Fatal(err)
	}
	// Bump active policy version, then read with an older observed version.
	next := p
	next.Version = p.Version + 1
	next.ParentVersion = p.Version
	if err := e.SetPolicy(next, "incremental"); err != nil {
		t.Fatal(err)
	}
	_, berr := e.ReadAdjudication(qt.SessionID, qt.Turn, p.Version)
	if berr == nil || berr.Code != spec.CodeStaleRead {
		t.Fatalf("want E_STALE_READ, got %+v", berr)
	}
	st.Close()
}
