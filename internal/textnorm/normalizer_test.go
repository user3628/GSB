package textnorm

import "testing"

func TestNormalizeFullWidthAndWhitespace(t *testing.T) {
	in := "Brushed  Ａｌｕｍｉｎｕｍ　拉丝铝／fine"
	got := Normalize(in)
	want := "brushed aluminum 拉丝铝/fine"
	if got != want {
		t.Fatalf("Normalize()=%q want %q", got, want)
	}
}

func TestNormalizeHalfwidthKatakana(t *testing.T) {
	got := Normalize("ｶﾞﾗｽ ﾊﾟｲﾌﾟ")
	want := "ガラス パイプ"
	if got != want {
		t.Fatalf("Normalize()=%q want %q", got, want)
	}
}

func TestTokenizeMixed(t *testing.T) {
	tokens := Tokenize("Rust铁锈 rusted／锈蚀")
	want := []string{"rust", "铁", "锈", "rusted", "锈", "蚀"}
	if len(tokens) != len(want) {
		t.Fatalf("tokens=%v want %v", tokens, want)
	}
	for i := range want {
		if tokens[i] != want[i] {
			t.Fatalf("token[%d]=%q want %q (full=%v)", i, tokens[i], want[i], tokens)
		}
	}
}

func TestTokenizePunctOnly(t *testing.T) {
	if got := Tokenize(" ／，。!!  "); len(got) != 0 {
		t.Fatalf("expected no tokens, got %v", got)
	}
}
