package sample

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestGeneratedSamplesAreDeterministic(t *testing.T) {
	first := All()
	second := All()
	if !reflect.DeepEqual(first, second) {
		t.Fatal("sample generator is not deterministic")
	}
	if len(first.SnapshotV1.LogicalTables) != 6 || len(first.SnapshotV1.PhysicalTables) != 16 {
		t.Fatalf("unexpected sample cardinality: %#v", first.SnapshotV1)
	}
	if first.IncrementV2.BaseVersion != 1 || first.LedgerCrash.BaseVersion != 2 {
		t.Fatalf("unexpected increment base versions: %#v %#v", first.IncrementV2, first.LedgerCrash)
	}
}

func TestWriteFilesProducesParseableJSON(t *testing.T) {
	dir := t.TempDir()
	if err := WriteFiles(dir); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 6 {
		t.Fatalf("expected six sample files, got %d", len(entries))
	}
	for _, entry := range entries {
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatalf("%s is not valid JSON: %v", entry.Name(), err)
		}
	}
}
