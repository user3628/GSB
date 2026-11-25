package model

import (
	"math"
	"testing"
)

func TestDigestStableAcrossKeyOrder(t *testing.T) {
	a := map[string]any{"x": 1, "y": "z", "n": []int{3, 1, 2}}
	b := map[string]any{"n": []int{3, 1, 2}, "y": "z", "x": 1}
	da, err := Digest(a)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Digest(b)
	if err != nil {
		t.Fatal(err)
	}
	if da != db {
		t.Fatal("digest must be independent of map key order")
	}
}

func TestDigestHandlesNonFinite(t *testing.T) {
	type holder struct {
		V float64 `json:"v"`
	}
	d, err := Digest(holder{V: math.NaN()})
	if err != nil {
		t.Fatalf("NaN digest must not error: %v", err)
	}
	if len(d) != 64 {
		t.Fatalf("unexpected digest length %d", len(d))
	}
}

func TestFeatureBlobDeterministic(t *testing.T) {
	feats := [][]float64{{1.5, -2.25}, {0, 3.25}}
	b1 := FeatureBlob(feats)
	b2 := FeatureBlob(feats)
	if len(b1) == 0 || DigestBytes(b1) != DigestBytes(b2) {
		t.Fatal("feature blob must be deterministic")
	}
}
