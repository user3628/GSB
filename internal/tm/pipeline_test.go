package tm

import (
	"math"
	"math/rand"
	"testing"
)

func testRecords() []SegmentRecord {
	return seededRecords(rand.New(rand.NewSource(GeneratorSeed)))
}

func TestNormalizeSegmentOffsetsAndEmpty(t *testing.T) {
	normalized := NormalizeText("Ｈｅｌｌｏ   世界\r\n第二　段")
	if normalized != "Hello 世界\n第二 段" {
		t.Fatalf("unexpected normalization: %q", normalized)
	}
	seg := SegmentText("Ｈｅｌｌｏ   世界。第二　段!")
	if len(seg.Sentences) < 2 {
		t.Fatalf("expected multiple sentences, got %d", len(seg.Sentences))
	}
	runes := []rune(seg.Text)
	for _, sentence := range seg.Sentences {
		if sentence.Empty {
			continue
		}
		if got := string(runes[sentence.Start:sentence.End]); got != sentence.Text {
			t.Fatalf("offset mismatch: %q != %q", got, sentence.Text)
		}
	}

	empty := SegmentText("。。。！！")
	if len(ValidSentences(empty.Sentences)) != 0 {
		t.Fatalf("punctuation-only text must have no valid sentences")
	}
	if _, err := ProcessSegment(SegmentRecord{SegmentID: "seg-empty", SourceText: "正常来源。", TargetText: ""}, DefaultAlignConfig()); err == nil || err.Code != "E_SEGMENT_EMPTY" {
		t.Fatalf("expected E_SEGMENT_EMPTY, got %v", err)
	}
}

func TestAlignmentInvalidNonFinite(t *testing.T) {
	cfg := DefaultAlignConfig()
	cfg.NGramWeight = math.NaN()
	_, err := AlignSegment("seg-bad-score", "zh-CN", "en-US",
		SegmentText("数据同步。"), SegmentText("Data sync."), cfg)
	if err == nil {
		t.Fatal("expected E_ALIGN_INVALID")
	}
	biz, ok := AsBizError(err)
	if !ok || biz.Code != "E_ALIGN_INVALID" || biz.SegmentID != "seg-bad-score" {
		t.Fatalf("expected E_ALIGN_INVALID with segment id, got %#v", err)
	}
	if biz.Details["source_offset"] == nil || biz.Details["target_offset"] == nil {
		t.Fatalf("expected offsets in details: %#v", biz.Details)
	}
}

func TestSharedSourceMergesOccurrences(t *testing.T) {
	result := BuildCorpus(testRecords(), 1, DefaultAlignConfig())
	if len(result.Errors) != 2 {
		t.Fatalf("expected two deterministic empty-segment errors, got %d: %#v", len(result.Errors), result.Errors)
	}
	bySource := map[string]Unit{}
	for _, u := range result.Units {
		bySource[u.SourceText] = u
	}
	submit, ok := bySource["请立即提交。"]
	if !ok {
		t.Fatalf("missing repeated submit unit; units=%d", len(result.Units))
	}
	if submit.OccurrenceCount != 3 || submit.DistinctSegmentCount != 3 {
		t.Fatalf("submit count=%d distinct=%d", submit.OccurrenceCount, submit.DistinctSegmentCount)
	}
	save, ok := bySource["请及时保存。"]
	if !ok || save.OccurrenceCount != 2 {
		t.Fatalf("save unit missing or wrong count: %#v", ok)
	}
	if submit.Digest == "" || len(result.Suggestions) != len(result.Units) {
		t.Fatalf("units/suggestions digests incomplete: %d/%d", len(result.Units), len(result.Suggestions))
	}
}
