package dataset

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateDeterministic(t *testing.T) {
	d1 := t.TempDir()
	d2 := t.TempDir()
	a, err := Generate(d1, 12345)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Generate(d2, 12345)
	if err != nil {
		t.Fatal(err)
	}
	if a.Fingerprint != b.Fingerprint {
		t.Fatalf("fingerprints differ: %s != %s", a.Fingerprint, b.Fingerprint)
	}
	for _, name := range []string{"manifest.json", "tensors/s04-asym4-pertensor-nan.activation.json"} {
		ab, err := os.ReadFile(filepath.Join(d1, filepath.FromSlash(name)))
		if err != nil {
			t.Fatal(err)
		}
		bb, err := os.ReadFile(filepath.Join(d2, filepath.FromSlash(name)))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ab, bb) {
			t.Fatalf("%s is not deterministic", name)
		}
	}
}

func TestGeneratedCoverage(t *testing.T) {
	dir := t.TempDir()
	ds, err := Generate(dir, DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	nanSeen := false
	zeroSeen := false
	chainSeen := false
	for _, sh := range ds.Manifest.Shards {
		for _, layer := range sh.Chain {
			seen[fmt.Sprintf("%s/%d/%s", layer.Activation.Mode, layer.Activation.Bits, layer.Activation.Granularity)] = true
		}
		if len(sh.Chain) > 1 {
			chainSeen = true
		}
		act := ds.Tensors[sh.Activation.Name]
		for _, ref := range sh.Weights {
			w := ds.Tensors[ref.Name]
			for _, v := range w.Values {
				if v == 0 {
					zeroSeen = true
				}
			}
		}
		for _, v := range act.Values {
			if v != v {
				nanSeen = true
			}
		}
	}
	for _, want := range []string{
		"symmetric/8/per_channel", "asymmetric/8/per_tensor",
		"symmetric/4/per_channel", "asymmetric/4/per_tensor",
		"asymmetric/4/per_channel",
	} {
		if !seen[want] {
			t.Errorf("missing coverage %s (have %v)", want, seen)
		}
	}
	if !nanSeen {
		t.Error("generated data has no NaN anomaly")
	}
	if !zeroSeen {
		t.Error("generated data has no explicit zero")
	}
	if !chainSeen {
		t.Error("generated data has no multi-layer chain")
	}
}
