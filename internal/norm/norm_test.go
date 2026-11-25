package norm

import "testing"

func TestNormalizeAndTokenize(t *testing.T) {
	// NFKC + full-width folding + lowercase + whitespace collapse.
	got := Normalize("　Ｗｉｒｅｌｅｓｓ　　Earphone　Price\r\n")
	want := "wireless earphone price"
	if got != want {
		t.Fatalf("normalize: got %q want %q", got, want)
	}
	lex := NewLexicon([]string{"耳机", "价格"})
	toks := Tokenize("无线耳机 价格 abc123", lex)
	texts := []string{}
	for _, tk := range toks {
		texts = append(texts, tk.Text)
		if tk.End <= tk.Start {
			t.Fatalf("bad offsets: %+v", tk)
		}
	}
	joined := ""
	for _, x := range texts {
		joined += x + "|"
	}
	if joined != "无|线|耳机|价格|abc123|" {
		t.Fatalf("tokens = %v", texts)
	}
	// Longest-match prefers the multi-char word for lexicon entries; unknown
	// Han characters ("无","线") fall back to single-character tokens.
	if texts[2] != "耳机" || texts[3] != "价格" || texts[4] != "abc123" {
		t.Fatalf("lexicon longest match failed: %v", texts)
	}
}

func TestOverlap(t *testing.T) {
	if got := Overlap([]string{"a", "b"}, []string{"a", "c"}); got != 1.0/3.0 {
		t.Fatalf("jaccard = %v", got)
	}
	if Overlap(nil, []string{"a"}) != 0 {
		t.Fatal("empty overlap must be 0")
	}
}
