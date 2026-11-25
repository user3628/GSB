package normalize

import (
	"testing"

	"ticketpipeline/internal/determ"
	"ticketpipeline/internal/model"
)

func TestNormalizeFullwidthAndWhitespace(t *testing.T) {
	got := NormalizeText("ＡＢＣ１２３\u3000　 hello\tWORLD")
	want := "abc123 hello world"
	if got != want {
		t.Fatalf("NormalizeText = %q want %q", got, want)
	}
}

func TestTokenizeHanAndLatinOffsets(t *testing.T) {
	tokens, err := Tokenize("账号 account 锁", "web", 64)
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}
	if len(tokens) < 3 {
		t.Fatalf("expected han uni/bi plus latin tokens, got %d", len(tokens))
	}
	// Every token must carry the channel and an in-range feature bucket.
	for _, tk := range tokens {
		if tk.Channel != "web" {
			t.Fatalf("missing channel marker on %+v", tk)
		}
		if tk.Index < 0 || tk.Index >= 64 {
			t.Fatalf("feature index out of range: %+v", tk)
		}
		if tk.ByteStart > tk.ByteEnd || tk.RuneStart >= tk.RuneEnd {
			t.Fatalf("invalid offsets: %+v", tk)
		}
	}
	// A bigram must be present for adjacent Han characters.
	foundBi := false
	for _, tk := range tokens {
		if tk.Kind == KindHanBi {
			foundBi = true
		}
	}
	if !foundBi {
		t.Fatal("expected at least one han_bi token")
	}
}

func TestEmptyAndPunctuationOnly(t *testing.T) {
	cases := []string{"", "   ", "。。。！？", "\u3000\t"}
	for _, text := range cases {
		_, err := Process(model.RawTicket{TicketID: "T1", Text: text, Channel: "im"}, 32)
		be, ok := model.AsBizError(err)
		if !ok || be.Code != model.CodeEmptyText || be.TicketID != "T1" || be.Stage != model.StageNormalize {
			t.Fatalf("text %q: want E_EMPTY_TEXT for T1, got %v", text, err)
		}
	}
}

func TestDeterministicByteDigest(t *testing.T) {
	tk := model.RawTicket{TicketID: "T9", Text: "账单 invoice 错误 error", Channel: "app"}
	r1, err := Process(tk, 128)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := Process(tk, 128)
	if err != nil {
		t.Fatal(err)
	}
	if r1.Digest != r2.Digest {
		t.Fatal("digest drift across identical runs")
	}
	b1, _ := determ.MarshalJSON(r1)
	b2, _ := determ.MarshalJSON(r2)
	if string(b1) != string(b2) {
		t.Fatal("canonical artifact bytes differ across identical runs")
	}
	// Channel sensitivity: same text, different channel -> different digest.
	tk.Channel = "web"
	r3, err := Process(tk, 128)
	if err != nil {
		t.Fatal(err)
	}
	if r3.Digest == r1.Digest {
		t.Fatal("channel marker did not affect the token digest")
	}
}

func TestZeroHashDimIsConfigError(t *testing.T) {
	if err := ValidateConfig(model.Policy{HashDim: 0, SampleRatio: 0.7}); err == nil || err.Code != model.CodeConfig {
		t.Fatalf("want E_CONFIG for zero hash dim, got %v", err)
	}
}
