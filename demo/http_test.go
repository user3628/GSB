package demo

import (
	"context"
	"net/http/httptest"
	"testing"

	"walgroup"
	walsample "walgroup/sample"
)

func TestFixedSeedWorkloadOverHTTP(t *testing.T) {
	ctx := context.Background()
	w := walsample.Generate(42)
	e, err := walgroup.Open(t.TempDir(), walgroup.Options{
		SegmentMaxBytes: w.SegmentMaxBytes, MaxTxnsPerSegment: w.MaxTxnsPerSegment,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	srv := httptest.NewServer(walgroup.NewHTTPHandler(e))
	defer srv.Close()
	report, err := RunHTTP(ctx, srv.URL, w)
	if err != nil {
		t.Fatalf("RunHTTP: %v", err)
	}
	if !report.AllOK {
		for _, a := range report.Actions {
			if !a.OK {
				t.Errorf("action %d %s expected %s got %s: %s", a.Index, a.Type, a.Expect, a.ObservedCode, a.Message)
			}
		}
		t.Fatal("HTTP workload failed")
	}
}
