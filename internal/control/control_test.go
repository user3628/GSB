package control

import (
	"math"
	"testing"

	"matbench/internal/errorsx"
)

func TestVectorUnitAndDeterministic(t *testing.T) {
	a, err := Generate("MAT-1", "Brushed Ａｌｕｍｉｎｕｍ 拉丝铝")
	if err != nil {
		t.Fatal(err)
	}
	b, err := Generate("MAT-1", "Brushed Ａｌｕｍｉｎｕｍ 拉丝铝")
	if err != nil {
		t.Fatal(err)
	}
	if a.Fingerprint != b.Fingerprint {
		t.Fatal("fingerprints differ for identical input")
	}
	n := 0.0
	for _, x := range a.Vector {
		n += x * x
	}
	if math.Abs(n-1) > 1e-12 {
		t.Fatalf("vector not unit: norm=%.15f", n)
	}
	if len(a.Tokens) == 0 || a.HashSeed != HashSeed {
		t.Fatal("missing tokens or hash seed recording")
	}
}

func TestEmptyPromptError(t *testing.T) {
	_, err := Generate("MAT-X", " ／，。 ")
	if err == nil || errorsx.CodeOf(err) != errorsx.EEmptyPrompt {
		t.Fatalf("want E_EMPTY_PROMPT, got %v", err)
	}
}

func TestHashDegenerateCarriesMaterialID(t *testing.T) {
	err := &errorsx.Error{Code: errorsx.EHashDegenerate, MaterialID: "MAT-Z",
		Detail: "synthetic degenerate hash"}
	if err.MaterialID != "MAT-Z" || errorsx.CodeOf(err) != errorsx.EHashDegenerate {
		t.Fatalf("bad error shape: %+v", err)
	}
}
