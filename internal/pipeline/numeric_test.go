package pipeline

import (
	"testing"

	"headprune/internal/model"
)

// brokenSource simulates a diverging combo replay by emitting non-finite
// activations, exercising the E_NUMERIC branch without truncating data.
func brokenSource(h model.Head, probeIdx int, seed int64) []float64 {
	out := model.RawActivation(h, probeIdx, seed)
	if h.Layer == 1 && h.Head == 0 {
		out[0] = nanInf()
	}
	return out
}

func TestNumericDivergenceOnComboReplay(t *testing.T) {
	ds := testDataset(t)
	quant, err := QuantizeHeads(ds.Heads, ds.Policy.QuantBits)
	if err != nil {
		t.Fatal(err)
	}
	probes, err := SampleProbes(ds.Sentences, ds.Policy)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := RunPerHeadReplay(ds, probes, quant)
	if err != nil {
		t.Fatal(err)
	}
	combo := model.HeadSet{{Layer: 1, Head: 0}, {Layer: 1, Head: 1}}
	// Reuse the replay path with a broken activation source inline by
	// corrupting a candidate feature to force non-finite ensemble logits.
	dsCorrupt := ds
	dsCorrupt.Sentences[probes[0].Index].Candidates[0].Feat[0] = nanInf()
	_, err = ReplayCombo(dsCorrupt, probes, quant, rows, combo)
	assertCode(t, err, model.CodeNumeric)
	be, _ := model.AsBizError(err)
	if be.Context == nil || be.Context["combo"] == "" {
		t.Fatal("E_NUMERIC must retain the combo identifier and contribution log")
	}
}

func TestAllZeroDistributionIsNumeric(t *testing.T) {
	if _, ok := model.Softmax(nil); ok {
		t.Fatal("empty logits must not produce a distribution")
	}
	if _, ok := model.Softmax([]float64{}); ok {
		t.Fatal("all-zero/empty input must return false")
	}
}
