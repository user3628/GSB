package nlp

import (
	"testing"

	"csrouter/internal/domain"
)

func TestNormalizeNFKCOffsetsAndDeterminism(t *testing.T) {
	ticket := domain.Ticket{
		TicketID: "T1",
		Text:     "Ｈｅｌｌｏ　世界！Hello世界",
		Channel:  "app",
		Language: "mixed",
	}
	first, err := NormalizeTicket(ticket, 1)
	if err != nil {
		t.Fatalf("normalize returned unexpected error: %v", err)
	}
	if first.Normalized != "hello 世界!hello世界" {
		t.Fatalf("unexpected normalized text: %q", first.Normalized)
	}
	runes := []rune(first.Normalized)
	for _, token := range first.Tokens {
		if token.Channel != "app" {
			t.Fatalf("channel marker missing: %+v", token)
		}
		if token.Start < 0 || token.End > len(runes) || token.Start >= token.End {
			t.Fatalf("bad rune offsets: %+v", token)
		}
		if got := string(runes[token.Start:token.End]); got != token.Text {
			t.Fatalf("offset mismatch: token=%q slice=%q offsets=%d:%d", token.Text, got, token.Start, token.End)
		}
		if first.Normalized[token.ByteStart:token.ByteEnd] != token.Text {
			t.Fatalf("byte offset mismatch for %q", token.Text)
		}
	}

	second, _ := NormalizeTicket(ticket, 1)
	if domain.FingerprintHex("test", first) != domain.FingerprintHex("test", second) {
		t.Fatal("normalization is not byte deterministic")
	}
}

func TestEmptyAndPunctuationReturnStructuredError(t *testing.T) {
	for _, text := range []string{"", "   \n\t ", "。，！？"} {
		artifact, bizErr := NormalizeTicket(domain.Ticket{TicketID: "EMPTY", Text: text}, 7)
		if bizErr == nil || bizErr.Code != domain.ErrEmptyText {
			t.Fatalf("text=%q expected E_EMPTY_TEXT, got artifact=%+v err=%v", text, artifact, bizErr)
		}
		if bizErr.TicketID != "EMPTY" || bizErr.ConfigVersion != 7 {
			t.Fatalf("error missing ticket/version: %+v", bizErr)
		}
	}
}
