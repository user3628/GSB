package demo

import (
	"context"
	"testing"

	"walgroup"
	walsample "walgroup/sample"
)

func TestFixedSeedWorkload(t *testing.T) {
	ctx := context.Background()
	w := walsample.Generate(42)
	dir := t.TempDir()
	e, err := walgroup.Open(dir, walgroup.Options{
		SegmentMaxBytes: w.SegmentMaxBytes, MaxTxnsPerSegment: w.MaxTxnsPerSegment,
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := Run(ctx, e, w)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !report.AllOK {
		for _, a := range report.Actions {
			if !a.OK {
				t.Errorf("action %d %s expected %s got %s: %s", a.Index, a.Type, a.Expect, a.ObservedCode, a.Message)
			}
		}
		t.Fatalf("workload failed: %+v", report)
	}
	var reclaimed, open int
	for _, a := range report.Actions {
		if a.Reclaim != nil {
			reclaimed += len(a.Reclaim.Segments)
		}
	}
	for _, seg := range report.Segments {
		if seg.State == walgroup.SegmentOpen {
			open++
		}
	}
	if reclaimed < 4 || open != 1 || len(report.Segments) != 1 {
		t.Fatalf("unexpected final segment lifecycle reclaimed=%d open=%d segments=%+v", reclaimed, open, report.Segments)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = walgroup.Open(dir, walgroup.Options{
		SegmentMaxBytes: w.SegmentMaxBytes, MaxTxnsPerSegment: w.MaxTxnsPerSegment,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e.Close()
	rr := e.RecoveryReport()
	if rr.Status != "CLEAN" {
		t.Fatalf("reopen after clean demo reported %+v", rr)
	}
}
