package report_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"keylife/internal/report"
	"keylife/internal/sample"
)

func TestReportIsByteDeterministicAndSorted(t *testing.T) {
	ctx := context.Background()
	build := func(dir string) []byte {
		ws, err := sample.Build(ctx, sample.Options{Dir: dir, Threshold: 4, NowTick: 420, Scale: 2})
		if err != nil {
			t.Fatal(err)
		}
		defer ws.Close()
		rep, err := report.Build(ctx, ws.Store, report.Options{NowTick: 420, Threshold: 4})
		if err != nil {
			t.Fatal(err)
		}
		data, err := report.Bytes(rep)
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i < len(rep.Keys); i++ {
			a, b := rep.Keys[i-1].Key, rep.Keys[i].Key
			if a.Subject > b.Subject || (a.Subject == b.Subject && a.ID > b.ID) {
				t.Fatalf("keys not sorted: %s/%s before %s/%s", a.Subject, a.ID, b.Subject, b.ID)
			}
		}
		if len(rep.SnapshotChain.Entries) == 0 || !rep.SnapshotChain.Valid {
			t.Fatalf("invalid snapshot chain: %+v", rep.SnapshotChain)
		}
		if rep.Adjudication.Digest == "" {
			t.Fatal("adjudication digest is empty")
		}
		return data
	}
	a := build(filepath.Join(t.TempDir(), "a"))
	b := build(filepath.Join(t.TempDir(), "b"))
	if !bytes.Equal(a, b) {
		t.Fatal("reports differ between identical runs")
	}
	if bytes.Contains(a, []byte("2026-")) || bytes.Contains(a, []byte("2025-")) {
		t.Fatal("report contains a wall-clock timestamp")
	}
}
