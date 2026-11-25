package pipeline

import (
	"testing"

	"headprune/internal/model"
)

func TestStableErrorCodesCarryIdentifiers(t *testing.T) {
	ds := testDataset(t)

	// Sentence with no truth candidate must name the probe identifier.
	bad := ds
	bad.Sentences = append([]model.Sentence(nil), ds.Sentences...)
	for i := range bad.Sentences[0].Candidates {
		bad.Sentences[0].Candidates[i].IsTrue = false
	}
	err := ValidateDataset(bad)
	assertCode(t, err, model.CodeSentenceInvalid)
	be, _ := model.AsBizError(err)
	if be.ProbeID != bad.Sentences[0].ID {
		t.Fatalf("expected probe id %s, got %s", bad.Sentences[0].ID, be.ProbeID)
	}

	// Bad candidate feature dimension names the probe.
	bad2 := ds
	bad2.Sentences[0].Candidates[0].Feat = bad2.Sentences[0].Candidates[0].Feat[:1]
	err = ValidateDataset(bad2)
	assertCode(t, err, model.CodeSentenceInvalid)
	be, _ = model.AsBizError(err)
	if be.ProbeID == "" {
		t.Fatal("feature dimension error must carry the probe id")
	}

	// Bad quant bits.
	bad3 := ds
	bad3.Policy.QuantBits = 1
	assertCode(t, ValidatePolicy(bad3.Policy), model.CodePolicyInvalid)

	// Empty candidate list.
	bad4 := ds
	bad4.Sentences[0].Candidates = nil
	assertCode(t, ValidateDataset(bad4), model.CodeSentenceInvalid)
}
