package appcli

import (
	"path/filepath"
	"testing"

	"headprune/internal/generate"
	"headprune/internal/model"
)

func TestHeadAppendLineageAndReuseAnnotation(t *testing.T) {
	svc := newTestService(t)
	if _, err := svc.Generate(); err != nil {
		t.Fatal(err)
	}
	_, err := svc.RunAll()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Commit(); err != nil {
		t.Fatal(err)
	}

	// Build an appended head file: original 12 heads + one brand-new head.
	in := generate.Build()
	extra := in.Dataset.Heads[0]
	extra.Layer, extra.Head = 7, 7
	heads := append(in.Dataset.Heads, extra)
	path := filepath.Join(t.TempDir(), "heads.json")
	if err := model.WriteJSON(path, heads); err != nil {
		t.Fatal(err)
	}
	if err := svc.ImportHeads(path); err != nil {
		t.Fatal(err)
	}
	lineage, err := svc.Lineage()
	if err != nil {
		t.Fatal(err)
	}
	if len(lineage["heads"]) == 0 {
		t.Fatal("head lineage must be recorded on import")
	}
	res2, err := svc.RunAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.ReusedRows) != len(in.Dataset.Heads) {
		t.Fatalf("expected %d reused rows, got %d", len(in.Dataset.Heads), len(res2.ReusedRows))
	}
	reused := map[model.HeadID]bool{}
	for _, id := range res2.ReusedRows {
		reused[id] = true
	}
	if reused[extra.ID()] {
		t.Fatal("appended head must be computed fresh, not reused")
	}
	for _, row := range res2.Probe.HeadErrors {
		if reused[row.Head] && row.ReusedFromVer != 1 {
			t.Fatalf("reused head %s must cite source version 1", row.Head)
		}
	}
}
