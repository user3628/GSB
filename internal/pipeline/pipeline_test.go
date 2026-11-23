package pipeline

import (
	"path/filepath"
	"testing"
)

func TestCommittedSamplesAndDeterministicPipeline(t *testing.T) {
	doc, err := Load(filepath.Join("..", "..", "samples", "samples.json"))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Seed != 424242 || len(doc.Vectors) != 64 {
		t.Fatalf("unexpected sample document: seed=%d vectors=%d", doc.Seed, len(doc.Vectors))
	}
	config := map[string]float64{
		WeightX: 0.75, WeightY: 0.25, Bias: 0.05,
		NormalizationRatio: 0.8, DecisionThreshold: 0.3,
	}
	first, err := Compute("sample", 1, config, doc)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Compute("sample", 1, config, doc)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("pipeline is not deterministic: %+v != %+v", first, second)
	}
	if first.WeightedSum != 0.6911695 || first.Normalized != 0.5529356 || !first.Decision {
		t.Fatalf("unexpected committed sample result: %+v", first)
	}
	if first.ResultDigest != "sha256:4b43f8c85a16e24c874ebbbf6369f7eb795e9dbb8419e8d833f5628862163893" {
		t.Fatalf("unexpected result digest: %s", first.ResultDigest)
	}
}

func TestGeneratedVectorsAreStable(t *testing.T) {
	a := GenerateVectors(424242, 4)
	b := GenerateVectors(424242, 4)
	if len(a) != len(b) {
		t.Fatal("vector lengths differ")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("vector %d differs: %+v != %+v", i, a[i], b[i])
		}
	}
	if a[0] == b[1] {
		t.Fatal("generator produced a suspiciously constant tensor")
	}
}
