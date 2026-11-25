package gen

import (
	"encoding/json"
	"os"
	"testing"

	"gpusched/internal/domain"
)

func readFile(path string) ([]byte, error) { return os.ReadFile(path) }

func TestGenerateDeterministic(t *testing.T) {
	a := Generate(DefaultSeed)
	b := Generate(DefaultSeed)
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if string(ja) != string(jb) {
		t.Fatal("generator output is not deterministic for a fixed seed")
	}
	c := Generate(DefaultSeed + 1)
	jc, _ := json.Marshal(c)
	if string(ja) == string(jc) {
		t.Fatal("different seeds must produce different output")
	}
}

func TestGeneratedSampleNormalizes(t *testing.T) {
	spec := Generate(DefaultSeed)
	set, err := domain.Normalize(spec)
	if err != nil {
		t.Fatalf("generated sample must normalize cleanly: %v", err)
	}
	if len(set.Requests) == 0 || len(set.Snapshots) != 2 || len(set.Fragments) != 1 {
		t.Fatalf("unexpected generated set sizes: %+v", set)
	}
	// Snapshot digest chain must be intact.
	if set.Snapshots[1].PrevDigest != set.Snapshots[0].Digest {
		t.Fatal("snapshot digest chain broken")
	}
}

func TestWriteFileRoundTrip(t *testing.T) {
	path, err := WriteFile(t.TempDir(), DefaultSeed)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	var spec domain.LoadSpec
	data, err := readFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(spec.Requests) == 0 {
		t.Fatal("round-tripped sample has no requests")
	}
}
