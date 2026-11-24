package sample

import (
	"bytes"
	"testing"
)

func TestGenerateDeterministic(t *testing.T) {
	a, err := Generate(42).JSON()
	if err != nil {
		t.Fatal(err)
	}
	b, err := Generate(42).JSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("same seed produced different workloads")
	}
	c, _ := Generate(43).JSON()
	if bytes.Equal(a, c) {
		t.Fatal("different seeds produced identical workloads")
	}
}

func TestWorkloadShape(t *testing.T) {
	w := Generate(2024)
	if w.FormatVersion != FormatVersion || w.SegmentMaxBytes <= 0 || w.MaxTxnsPerSegment <= 0 {
		t.Fatalf("bad header: %+v", w)
	}
	counts := map[string]int{}
	for _, a := range w.Actions {
		counts[a.Type]++
	}
	for _, typ := range []string{"commit_group", "checkpoint", "reclaim", "read", "reclaim_current"} {
		if counts[typ] == 0 {
			t.Fatalf("workload missing %s action: %+v", typ, counts)
		}
	}
}
