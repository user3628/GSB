package ttsflow

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestNormalizeAndChunkOffsets(t *testing.T) {
	cfg, _, _ := testDataset(t)
	sample := TextSample{UTTID: "normalize-1", Text: "  ＡＢＣ　你好，\n世界！ ", Language: "en", SpeakerID: 1}
	frontend, err := BuildFrontend(sample, cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := "ABC 你好, 世界!"
	if frontend.NormalizedText != want {
		t.Fatalf("normalized text = %q, want %q", frontend.NormalizedText, want)
	}
	var joined strings.Builder
	for _, chunk := range frontend.Chunks {
		joined.WriteString(chunk.Text)
		if got := frontend.NormalizedText[chunk.StartByte:chunk.EndByte]; got != chunk.Text {
			t.Fatalf("chunk %d byte slice = %q, text = %q", chunk.Index, got, chunk.Text)
		}
		runeCount := utf8.RuneCountInString(frontend.NormalizedText[:chunk.StartByte])
		if runeCount != chunk.StartRune {
			t.Fatalf("chunk %d StartRune = %d, actual %d", chunk.Index, chunk.StartRune, runeCount)
		}
	}
	if joined.String() != frontend.NormalizedText {
		t.Fatalf("chunks do not partition normalized text: %q", joined.String())
	}
	if len(frontend.Units) == 0 {
		t.Fatal("expected pronunciation units")
	}
}

func TestFrontendStableErrors(t *testing.T) {
	cfg, _, _ := testDataset(t)
	_, err := BuildFrontend(TextSample{UTTID: "empty-utt", Text: "   \n", Language: "zh"}, cfg)
	be, ok := AsBizError(err)
	if !ok || be.Code != CodeEmptyText || be.UTTID != "empty-utt" || be.BlockSeq != 0 {
		t.Fatalf("empty text error = %#v", err)
	}

	_, err = BuildFrontend(TextSample{UTTID: "mapping-utt", Text: "你好龘", Language: "zh"}, cfg)
	be, ok = AsBizError(err)
	if !ok || be.Code != CodePhonemeMapping || be.UTTID != "mapping-utt" || be.RuneOffset != 2 {
		t.Fatalf("mapping error = %#v", err)
	}
	if !strings.Contains(be.Context, "龘") {
		t.Fatalf("diagnostic context missing grapheme: %q", be.Context)
	}
}

func TestPunctuationForceAndCharacterTargets(t *testing.T) {
	cfg, _, _ := testDataset(t)
	cfg.Splitting.ForceAtPunctuation = true
	cfg.Splitting.FirstMinChars = 100
	cfg.Splitting.FollowingChars = 100
	frontend, err := BuildFrontend(TextSample{UTTID: "punct", Text: "你好，世界！", Language: "zh"}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(frontend.Chunks) != 2 {
		t.Fatalf("forced punctuation produced %d chunks, want 2", len(frontend.Chunks))
	}
	if frontend.Chunks[0].Text != "你好," || frontend.Chunks[1].Text != "世界!" {
		t.Fatalf("unexpected forced chunks: %#v", frontend.Chunks)
	}

	tokens := tokenize("一二三四五六七八九十")
	chunks, err := splitTokens(tokens, SplitConfig{FirstMinChars: 4, FollowingChars: 3, ForceAtPunctuation: false})
	if err != nil {
		t.Fatal(err)
	}
	lengths := make([]int, 0, len(chunks))
	for _, chunk := range chunks {
		lengths = append(lengths, meaningfulRunes(chunk))
	}
	if got := fmt.Sprint(lengths); got != "[4 3 3]" {
		t.Fatalf("chunk rune lengths = %v, want [4 3 3]", lengths)
	}
}
