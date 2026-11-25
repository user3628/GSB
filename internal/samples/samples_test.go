package samples_test

import (
	"testing"

	"ropechunk/internal/model"
	"ropechunk/internal/samples"
)

func TestGeneratorIsDeterministicAndCoversRequiredCases(t *testing.T) {
	first, err := samples.Generate()
	if err != nil {
		t.Fatal(err)
	}
	second, err := samples.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Shards) < 4 {
		t.Fatalf("only %d shards", len(first.Shards))
	}
	categories := make(map[string]bool)
	hasBatch, hasOdd, hasLong := false, false, false
	for i := range first.Shards {
		a, b := first.Shards[i], second.Shards[i]
		if model.TensorDigest(a.Q) != model.TensorDigest(b.Q) || model.TensorDigest(*a.ReferenceQ) != model.TensorDigest(*b.ReferenceQ) {
			t.Fatalf("shard %s generator is not deterministic", a.ID)
		}
		meta, _ := a.SampleMetadata.(samples.SampleMetadata)
		categories[meta.Category] = true
		hasBatch = hasBatch || a.Shape.Batch() > 1
		hasOdd = hasOdd || a.Shape.HeadDim()%2 == 1
		hasLong = hasLong || a.Shape.Seq() >= 64
	}
	for _, required := range []string{"short", "long", "odd_head_dim", "extremes"} {
		if !categories[required] {
			t.Fatalf("missing category %s: %v", required, categories)
		}
	}
	if !hasBatch || !hasOdd || !hasLong {
		t.Fatalf("coverage batch=%v odd=%v long=%v", hasBatch, hasOdd, hasLong)
	}
}
