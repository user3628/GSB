package generator_test

import (
	"path/filepath"
	"testing"

	"qaudit/internal/generator"
)

func TestGenerateDeterministicAndCoversPolicyMatrix(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	ds, err := generator.Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds.Network.Layers) != 4 {
		t.Fatalf("demo must cover 4 layers, got %d", len(ds.Network.Layers))
	}
	if len(ds.Shards) != generator.NumShards {
		t.Fatalf("shard count %d", len(ds.Shards))
	}
	bits := map[int]bool{}
	symmetric := map[bool]int{}
	perChannel := map[bool]int{}
	for _, l := range ds.Network.Layers {
		pol := ds.Policy.LayerPolicy(l.Name)
		bits[pol.BitWidth] = true
		symmetric[pol.Symmetric]++
		perChannel[pol.PerChannel]++
	}
	if !bits[4] || !bits[8] {
		t.Fatalf("must cover both 4-bit and 8-bit, got %v", bits)
	}
	if symmetric[true] == 0 || symmetric[false] == 0 {
		t.Fatal("must cover symmetric and asymmetric modes")
	}
	if perChannel[true] == 0 {
		t.Fatal("must include at least one per-channel layer")
	}
	// Outlier channel: fc2 channel 3 must contain a magnitude clearly above peers.
	fc2 := ds.Network.Layers[1]
	var maxOther, outlier float64
	for o, row := range fc2.Weights {
		for _, v := range row {
			if o == 3 {
				if v > outlier {
					outlier = v
				}
			} else if v > maxOther {
				maxOther = v
			}
		}
	}
	if outlier <= maxOther {
		t.Fatalf("expected outlier channel, outlier=%g otherMax=%g", outlier, maxOther)
	}
	// Determinism: regenerate into a second directory and compare references.
	ds2, err := generator.Generate(filepath.Join(t.TempDir(), "d2"))
	if err != nil {
		t.Fatal(err)
	}
	if len(ds2.References) != len(ds.References) {
		t.Fatal("reference count differs across regenerations")
	}
	for i, r := range ds.References {
		r2 := ds2.References[i]
		if r.Position != r2.Position || len(r.Logits) != len(r2.Logits) {
			t.Fatal("reference shape differs across regenerations")
		}
		for j := range r.Logits {
			if r.Logits[j] != r2.Logits[j] {
				t.Fatal("reference outputs are not deterministic")
			}
		}
	}
}
