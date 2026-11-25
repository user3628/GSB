package ropechunk_test

import (
	"testing"

	"ropechunk"
)

func TestPublicLibraryAPI(t *testing.T) {
	cfg := ropechunk.DefaultConfig()
	shape := ropechunk.Shape{1, 1, 2, 4}
	src := ropechunk.QuantTensor{Shape: shape, Scale: 1_000_000, Data: []int64{1_000_000, 0, -1_000_000, 0, 0, 1_000_000, 0, -1_000_000}}
	rotated, blocks, err := ropechunk.RotateTensor(src, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rotated.Data) != len(src.Data) || len(blocks) != 1 {
		t.Fatalf("unexpected rotation result: %d values, %d blocks", len(rotated.Data), len(blocks))
	}
	reference, err := ropechunk.ReferenceRotate(src, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := range reference.Data {
		if rotated.Data[i] != reference.Data[i] {
			t.Fatalf("public API mismatch at %d", i)
		}
	}
	fp := ropechunk.ComputeFingerprints(ropechunk.Shard{ID: "x", Shape: shape, Q: src, K: src}, cfg)
	if fp.Rotation == "" || fp.Score == "" {
		t.Fatal("missing public fingerprints")
	}
}
