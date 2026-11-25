package demo

import (
	"encoding/json"
	"testing"
)

func TestGenerateIsDeterministicAndComplete(t *testing.T) {
	first, err := json.Marshal(Generate())
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(Generate())
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("fixed-seed generator is not deterministic")
	}
	bundle := Generate()
	if GenerationSeed != 20261126 || len(bundle.Documents) != 4 || bundle.Strategy.Version != 3 {
		t.Fatalf("unexpected demo metadata: seed=%d docs=%d strategy=%d", GenerationSeed, len(bundle.Documents), bundle.Strategy.Version)
	}
	seen := map[string]bool{}
	for _, doc := range bundle.Documents {
		seen[doc.DocID] = true
		if len(doc.Boxes) < 5 {
			t.Fatalf("%s has too few boxes", doc.DocID)
		}
	}
	for _, expected := range []string{"doc-single-zh", "doc-double-en", "doc-span-title-zh-en", "doc-dual-band-mixed"} {
		if !seen[expected] {
			t.Fatalf("missing demo document %s", expected)
		}
	}
}
