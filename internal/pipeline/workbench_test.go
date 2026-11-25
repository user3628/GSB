package pipeline

import (
	"path/filepath"
	"testing"

	"headprune/internal/generate"
	"headprune/internal/model"
	"headprune/internal/store"
)

func openWorkbench(t *testing.T) (*Workbench, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ds := generate.Build().Dataset
	wb, err := NewWorkbench(st, ds)
	if err != nil {
		t.Fatal(err)
	}
	return wb, st
}

func TestCacheReuseAcrossRuns(t *testing.T) {
	wb, st := openWorkbench(t)
	res1, err := wb.Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(res1.ReusedRows) != 0 {
		t.Fatalf("first run must not reuse, got %d", len(res1.ReusedRows))
	}
	if _, err := wb.Commit(res1); err != nil {
		t.Fatal(err)
	}
	res2, err := wb.Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.ReusedRows) != len(wb.Dataset.Heads) {
		t.Fatalf("second run should reuse all %d rows, reused=%d", len(wb.Dataset.Heads), len(res2.ReusedRows))
	}
	for _, row := range res2.Probe.HeadErrors {
		if row.ReusedFromVer != 1 {
			t.Fatalf("row %s missing reuse source version, got %d", row.Head, row.ReusedFromVer)
		}
	}
	// Reused and fresh tables must stay byte-identical.
	b1, _ := model.CanonicalJSON(res1.Probe.HeadErrors)
	stripped := make([]HeadErrorRow, len(res2.Probe.HeadErrors))
	for i, r := range res2.Probe.HeadErrors {
		r.ReusedFromVer = 0
		stripped[i] = r
	}
	b2, _ := model.CanonicalJSON(stripped)
	if string(b1) != string(b2) {
		t.Fatal("reused rows must match freshly computed rows")
	}
	_ = st
}

func TestHeadAppendInvalidatesOnlyNewHead(t *testing.T) {
	wb, _ := openWorkbench(t)
	res, err := wb.Run()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wb.Commit(res); err != nil {
		t.Fatal(err)
	}
	// Append a brand new head: existing heads stay reusable, the new head is
	// computed fresh.
	ds := wb.Dataset
	clone := ds.Heads[0]
	clone.Layer = 7
	clone.Head = 7
	ds.Heads = append(ds.Heads, clone)
	wb.Dataset = ds
	if err := ValidateDataset(wb.Dataset); err != nil {
		t.Fatal(err)
	}
	verdict, err := ClassifyHeads(generate.Build().Dataset, wb.Dataset, nil)
	if err != nil {
		t.Fatal(err)
	}
	if verdict.Relation != RelationAppend {
		t.Fatalf("relation = %s, want append", verdict.Relation)
	}
	res2, err := wb.Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.ReusedRows) != len(generate.Build().Dataset.Heads) {
		t.Fatalf("expected %d reused original heads, got %d",
			len(generate.Build().Dataset.Heads), len(res2.ReusedRows))
	}
	for _, id := range res2.ReusedRows {
		if id.Layer == 7 && id.Head == 7 {
			t.Fatal("newly appended head must not be served from cache")
		}
	}
}
