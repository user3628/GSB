package smuggle

import (
	"path/filepath"
	"testing"
)

func TestInjectionChain(t *testing.T) {
	st, err := OpenStore(filepath.Join(t.TempDir(), "inject.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	eng := NewEngine(DefaultConfig(), st)
	results, err := RunInjection(eng, st, AllAnomalies())
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 5 {
		t.Fatalf("got %d results", len(results))
	}
	for _, r := range results {
		if !r.Pass {
			t.Fatalf("injection %s failed: %+v", r.Anomaly, r)
		}
	}
}
