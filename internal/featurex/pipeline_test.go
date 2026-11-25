package featurex_test

import (
	"strings"
	"testing"

	"featureplatform/internal/configx"
	"featureplatform/internal/featurex"
	"featureplatform/internal/model"
	"featureplatform/internal/samplegen"
)

func TestNormalizeTokenizeAndDeterministicRecord(t *testing.T) {
	cfg := samplegen.DefaultConfig()
	sample := model.Sample{
		SampleID: "T-1", Task: samplegen.TaskClassification,
		RawText: "ＡＩ   技术\n 很强大", Label: "科技", TaskVersion: 2,
		Coverage: &model.CoverageDeclaration{Denominator: 8},
	}
	rec, coded := featurex.BuildRecord(sample, cfg, 1)
	if coded != nil {
		t.Fatal(coded)
	}
	if rec.NormalizedText != "AI 技术 很强大" {
		t.Fatalf("normalized text = %q", rec.NormalizedText)
	}
	kinds := map[string]bool{}
	var hasLatin bool
	for _, tok := range rec.Tokens {
		kinds[tok.Kind] = true
		if tok.Kind == "latin" && tok.Text == "ai" {
			hasLatin = true
		}
	}
	if !kinds["han_unigram"] || !kinds["han_bigram"] || !hasLatin {
		t.Fatalf("missing token classes: %+v", rec.Tokens)
	}
	again, _ := featurex.BuildRecord(sample, cfg, 1)
	a := model.MustCanonicalJSON(rec)
	b := model.MustCanonicalJSON(again)
	if string(a) != string(b) {
		t.Fatal("same input/config did not produce byte-identical record")
	}
	for _, f := range rec.Features {
		if f.SourceTask != sample.Task || f.MappingVersion == "" || f.MappedTerm == "" {
			t.Fatalf("feature provenance incomplete: %+v", f)
		}
	}
}

func TestRequiredBusinessCodesCarrySampleAndTask(t *testing.T) {
	cfg := samplegen.DefaultConfig()
	sample := model.Sample{SampleID: "SID", Task: samplegen.TaskIntent, RawText: "查询快递", Label: "查询物流", TaskVersion: 1}

	zero := cfg
	zero.HashDim = 0
	_, err := featurex.BuildRecord(sample, zero, 1)
	assertCoded(t, err, model.CodeHashDimZero, "SID", sample.Task)

	missing := cfg
	missing.TaskMappings = map[string]configx.TaskMapping{}
	_, err = featurex.BuildRecord(sample, missing, 1)
	assertCoded(t, err, model.CodeTaskMappingMissing, "SID", sample.Task)

	empty := sample
	empty.RawText = " \u3000\n\t "
	_, err = featurex.BuildRecord(empty, cfg, 1)
	assertCoded(t, err, model.CodeEmptyText, "SID", sample.Task)

	badLabel := sample
	badLabel.SampleID = "LID"
	badLabel.Label = "不存在"
	rec, err := featurex.BuildRecord(badLabel, cfg, 1)
	assertCoded(t, err, model.CodeLabelUnmapped, "LID", sample.Task)
	if rec.Status != "rejected" || !strings.Contains(rec.Rejections[0].Message, "不存在") {
		t.Fatalf("bad label was not recorded: %+v", rec.Rejections)
	}
}

func assertCoded(t *testing.T, err *model.CodedError, code, sampleID, task string) {
	t.Helper()
	if err == nil || err.Code != code {
		t.Fatalf("error=%v, want %s", err, code)
	}
	if err.SampleID != sampleID || err.Task != task {
		t.Fatalf("error metadata = sample=%q task=%q, want %q/%q", err.SampleID, err.Task, sampleID, task)
	}
}
