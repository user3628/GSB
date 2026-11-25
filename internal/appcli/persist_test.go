package appcli

import (
	"path/filepath"
	"testing"
)

// Regression: a freshly opened service must load inputs persisted by an
// earlier process instead of silently starting with an empty dataset.
func TestFreshServiceLoadsPersistedDataset(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ws")
	first, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Generate(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if len(second.dataset.Heads) == 0 {
		t.Fatal("reopened service must load persisted head shards")
	}
	if len(second.dataset.Sentences) == 0 || second.dataset.Policy.Version == 0 {
		t.Fatal("reopened service must load sentences and policy")
	}
	if _, err := second.RunAll(); err != nil {
		t.Fatalf("pipeline must run on reopened workspace: %v", err)
	}
}
