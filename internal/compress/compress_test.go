package compress

import (
	"encoding/json"
	"testing"

	"searchlab/internal/apperr"
	"searchlab/internal/config"
	"searchlab/internal/index"
)

func TestBuildDecodeRoundTripAndSkips(t *testing.T) {
	for _, deltaEncoding := range []string{"docid_delta", "docid_delta_first"} {
		t.Run(deltaEncoding, func(t *testing.T) {
			cfg := config.MustDefault()
			cfg.Index.BlockSize = 3
			cfg.Index.SkipStep = 2
			cfg.Index.DeltaEncoding = deltaEncoding
			postings := make([]index.QuantPosting, 10)
			for i := range postings {
				postings[i] = index.QuantPosting{DocID: int64(100 + i*3), Quantized: int64(1000 + i*17)}
			}
			stats := index.StatsArtifact{
				DocCount: 10,
				Terms: []index.TermStat{{
					TermID: 1, Term: "go", DF: len(postings), Postings: postings,
					Scale: 1, QuantStep: 1, QuantErrorBound: 0.5,
				}},
			}
			payload, artifact, err := Build(cfg, stats)
			if err != nil {
				t.Fatalf("build compressed postings: %v", err)
			}
			if err := VerifyArtifact(*artifact); err != nil {
				t.Fatalf("verify artifact: %v", err)
			}
			term := artifact.Terms[0]
			var all []Posting
			for _, block := range term.Blocks {
				decoded, err := DecodeBlock(block)
				if err != nil {
					t.Fatalf("decode block %d independently: %v", block.Ordinal, err)
				}
				all = append(all, decoded...)
			}
			for i, posting := range postings {
				if all[i].DocID != posting.DocID || all[i].Quant != posting.Quantized {
					t.Fatalf("decoded posting %d = %+v, want doc=%d q=%d", i, all[i], posting.DocID, posting.Quantized)
				}
			}
			if len(term.Skips) == 0 {
				t.Fatal("expected sparse skip entries")
			}
			for _, skip := range term.Skips {
				blockDecoded, _ := DecodeBlock(term.Blocks[skip.Block])
				if blockDecoded[skip.Index].DocID != skip.DocID {
					t.Fatalf("skip doc=%d does not match block %d index %d", skip.DocID, skip.Block, skip.Index)
				}
			}
			var decodedArtifact Artifact
			if err := json.Unmarshal(payload, &decodedArtifact); err != nil {
				t.Fatal(err)
			}
			again, _ := json.Marshal(decodedArtifact)
			if string(again) != string(payload) {
				t.Fatal("compressed artifact JSON is not stable")
			}
		})
	}
}

func TestCorruptBlockReportsFirstDifference(t *testing.T) {
	cfg := config.MustDefault()
	cfg.Index.BlockSize = 2
	stats := index.StatsArtifact{Terms: []index.TermStat{{
		TermID: 1, Term: "go", DF: 4,
		Postings: []index.QuantPosting{{DocID: 1, Quantized: 10}, {DocID: 3, Quantized: 20}, {DocID: 8, Quantized: 30}, {DocID: 9, Quantized: 40}},
	}}}
	_, artifact, err := Build(cfg, stats)
	if err != nil {
		t.Fatal(err)
	}
	tampered := *artifact
	tampered.Terms = append([]CompressedTerm(nil), artifact.Terms...)
	tampered.Terms[0].Blocks = append([]Block(nil), artifact.Terms[0].Blocks...)
	tampered.Terms[0].Blocks[1].Checksum = "deadbeef"
	err = VerifyArtifact(tampered)
	if err == nil {
		t.Fatal("expected checksum mismatch")
	}
	business := apperr.As(err)
	if business.Code != apperr.CodeDecodeMismatch || business.FirstDiffBlock != 2 {
		t.Fatalf("unexpected verify error: %+v", business)
	}
	_, decodeErr := DecodeBlock(tampered.Terms[0].Blocks[1])
	decodeBusiness := apperr.As(decodeErr)
	if decodeBusiness.Code != apperr.CodeDecodeMismatch || decodeBusiness.FirstDiffBlock != 2 {
		t.Fatalf("direct DecodeBlock error = %+v, want E_DECODE_MISMATCH block 2", decodeBusiness)
	}
}
