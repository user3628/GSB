package audit

import (
	"bytes"
	"context"
	"testing"

	"example.com/ntfsacl/internal/acl"
	"example.com/ntfsacl/internal/fixture"
	"example.com/ntfsacl/internal/store"
)

func TestDeterministicSampleConflictsAndReports(t *testing.T) {
	root1 := t.TempDir()
	root2 := t.TempDir()
	if _, err := fixture.Generate(root1, fixture.Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Generate(root2, fixture.Options{}); err != nil {
		t.Fatal(err)
	}
	r1, err := Run(context.Background(), root1, nil, Config{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := Run(context.Background(), root2, nil, Config{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][3]any{
		acl.CodeDenyOverride:     {"finance/budget.txt", 1, fixture.SIDFinance},
		acl.CodeInheritBroken:    {"docs/plan.txt", 2, fixture.SIDDocs},
		acl.CodeFlagIncompatible: {"legacy/orphan.txt", 0, fixture.SIDIntern},
		acl.CodeOrphanSID:        {"legacy/orphan.txt", 1, fixture.SIDOrphan},
	}
	if len(r1.Conflicts) != 4 {
		t.Fatalf("got %d conflicts, want 4: %+v", len(r1.Conflicts), r1.Conflicts)
	}
	for _, c := range r1.Conflicts {
		w, ok := want[c.Code]
		if !ok {
			t.Fatalf("unexpected code %s", c.Code)
		}
		if c.NodePath != w[0] || c.ACEIndex != w[1] || c.SubjectSID != w[2] {
			t.Fatalf("wrong first deviation for %s: %+v", c.Code, c)
		}
	}
	p1, err := r1.Report(1, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	p2, err := r2.Report(1, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := p1.Marshal()
	b2, _ := p2.Marshal()
	if !bytes.Equal(b1, b2) {
		t.Fatal("same fixture produced different report bytes")
	}
	// Commit both to independent SQLite databases and compare exact payloads.
	db1, _ := store.Open(p1FixtureDB(t, root1))
	defer db1.Close()
	db2, _ := store.Open(p1FixtureDB(t, root2))
	defer db2.Close()
	s1, err := db1.CommitSnapshot(context.Background(), store.KindAudit, func(v, pv int64, ph string) ([]byte, error) {
		r, _ := r1.Report(v, pv, ph)
		return r.Marshal()
	})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := db2.CommitSnapshot(context.Background(), store.KindAudit, func(v, pv int64, ph string) ([]byte, error) {
		r, _ := r2.Report(v, pv, ph)
		return r.Marshal()
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(s1.Payload, s2.Payload) {
		t.Fatal("committed snapshot payloads differ")
	}
}

func TestNodeBudgetAndConcurrentSubtreeQueue(t *testing.T) {
	root := t.TempDir()
	if _, err := fixture.Generate(root, fixture.Options{}); err != nil {
		t.Fatal(err)
	}
	partial, err := Run(context.Background(), root, nil, Config{Concurrency: 2, NodeBudget: 3})
	if err != nil {
		t.Fatal(err)
	}
	if partial.Code != CodeNodeBudget || len(partial.Sessions) != 1 || !partial.Sessions[0].Partial {
		t.Fatalf("budget not surfaced: %+v", partial)
	}
	if len(partial.Nodes) != 3 {
		t.Fatalf("partial nodes=%d want 3", len(partial.Nodes))
	}
	reqs := []Request{{ID: "s1", Subtree: "finance"}, {ID: "s2", Subtree: "docs"}, {ID: "s3", Subtree: "legacy"}}
	q1, err := Run(context.Background(), root, reqs, Config{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	q3, err := Run(context.Background(), root, reqs, Config{Concurrency: 3})
	if err != nil {
		t.Fatal(err)
	}
	r1, _ := q1.Report(1, 0, "")
	r2, _ := q3.Report(1, 0, "")
	b1, _ := r1.Marshal()
	b2, _ := r2.Marshal()
	if !bytes.Equal(b1, b2) {
		t.Fatal("concurrency limit changed deterministic session report")
	}
	if len(q1.Sessions) != 3 {
		t.Fatalf("queued sessions lost: %+v", q1.Sessions)
	}
}

func TestMalformedStopsBeforeInheritance(t *testing.T) {
	root := t.TempDir()
	if _, err := fixture.Generate(root, fixture.Options{Malformed: "flags"}); err != nil {
		t.Fatal(err)
	}
	_, err := Run(context.Background(), root, nil, Config{})
	me, ok := IsSessionError(err)
	if !ok || me.Code != acl.CodeACLMalformed || me.NodePath != "finance/budget.txt" || me.ACEIndex != 0 {
		t.Fatalf("unexpected malformed error: %v", err)
	}
}

func p1FixtureDB(t *testing.T, root string) string {
	t.Helper()
	fs, err := fixture.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	return fs.DBPath()
}
