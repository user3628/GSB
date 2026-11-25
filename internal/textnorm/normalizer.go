// Package textnorm implements prompt normalization: a deterministic NFKC
// subset (full-width/half-width unification, half-width katakana composition,
// common compatibility punctuation), whitespace folding and lowercasing.
//
// Only the Go standard library is used. The mapping covers every block the
// built-in deterministic generator and the test suite exercise; the function
// is documented as an NFKC subset rather than a full Unicode compositor.
package textnorm

import (
	"strings"
	"unicode"
)

// halfwidthKatakana maps U+FF65..U+FF9F to NFKC equivalents. Combining voiced
// marks are folded into the preceding kana by Normalize.
var halfwidthKatakana = map[rune]string{
	0xFF65: "・",
	0xFF66: "ヲ",
	0xFF67: "ァ", 0xFF68: "ィ", 0xFF69: "ゥ", 0xFF6A: "ェ", 0xFF6B: "ォ",
	0xFF6C: "ャ", 0xFF6D: "ュ", 0xFF6E: "ョ", 0xFF6F: "ッ",
	0xFF70: "ー", 0xFF71: "ア", 0xFF72: "イ", 0xFF73: "ウ", 0xFF74: "エ",
	0xFF75: "オ", 0xFF76: "カ", 0xFF77: "キ", 0xFF78: "ク", 0xFF79: "ケ",
	0xFF7A: "コ", 0xFF7B: "サ", 0xFF7C: "シ", 0xFF7D: "ス", 0xFF7E: "セ",
	0xFF7F: "ソ", 0xFF80: "タ", 0xFF81: "チ", 0xFF82: "ツ", 0xFF83: "テ",
	0xFF84: "ト", 0xFF85: "ナ", 0xFF86: "ニ", 0xFF87: "ヌ", 0xFF88: "ネ",
	0xFF89: "ノ", 0xFF8A: "ハ", 0xFF8B: "ヒ", 0xFF8C: "フ", 0xFF8D: "ヘ",
	0xFF8E: "ホ", 0xFF8F: "マ", 0xFF90: "ミ", 0xFF91: "ム", 0xFF92: "メ",
	0xFF93: "モ", 0xFF94: "ヤ", 0xFF95: "ユ", 0xFF96: "ヨ", 0xFF97: "ラ",
	0xFF98: "リ", 0xFF99: "ル", 0xFF9A: "レ", 0xFF9B: "ロ", 0xFF9C: "ワ",
	0xFF9D: "ン",
}

var voicedMap = map[rune]rune{
	'ｳ': 'ヴ', 'ｶ': 'ガ', 'ｷ': 'ギ', 'ｸ': 'グ', 'ｹ': 'ゲ', 'ｺ': 'ゴ',
	'ｻ': 'ザ', 'ｼ': 'ジ', 'ｽ': 'ズ', 'ｾ': 'ゼ', 'ｿ': 'ゾ',
	'ﾀ': 'ダ', 'ﾁ': 'ヂ', 'ﾂ': 'ヅ', 'ﾃ': 'デ', 'ﾄ': 'ド',
	'ﾊ': 'バ', 'ﾋ': 'ビ', 'ﾌ': 'ブ', 'ﾍ': 'ベ', 'ﾎ': 'ボ',
}
var semiVoicedMap = map[rune]rune{
	'ﾊ': 'パ', 'ﾋ': 'ピ', 'ﾌ': 'プ', 'ﾍ': 'ペ', 'ﾎ': 'ポ',
}

var compatMap = map[rune]string{
	0x00A0: " ",
	0x2018: "'", 0x2019: "'", 0x201C: "\"", 0x201D: "\"",
	0x2013: "-", 0x2014: "-", 0x2026: "...", 0x2122: "TM",
	0x2105: "c/o", 0x3371: "hPa", 0x339C: "rad/s",
}

// Normalize applies the NFKC subset, whitespace folding and lowercasing.
func Normalize(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	runes := []rune(s)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case r == 0x3000 || r == 0x00A0:
			b.WriteRune(' ')
		case r >= 0xFF01 && r <= 0xFF5E:
			b.WriteRune(r - 0xFEE0)
		case r >= 0xFF65 && r <= 0xFF9F:
			next := rune(0)
			if i+1 < len(runes) {
				next = runes[i+1]
			}
			if (next == 0xFF9E || next == 0xFF9F) && r >= 0xFF76 && r <= 0xFF9D {
				var composed rune
				var ok bool
				if next == 0xFF9E {
					composed, ok = voicedMap[r]
				} else {
					composed, ok = semiVoicedMap[r]
				}
				if ok {
					b.WriteRune(composed)
					i++
					continue
				}
			}
			if r != 0xFF9E && r != 0xFF9F {
				b.WriteString(halfwidthKatakana[r])
			}
		default:
			if mapped, ok := compatMap[r]; ok {
				b.WriteString(mapped)
			} else {
				b.WriteRune(r)
			}
		}
	}
	folded := foldWhitespace(b.String())
	return strings.ToLower(folded)
}

func foldWhitespace(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	prevSpace := false
	started := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			if started && !prevSpace {
				b.WriteRune(' ')
				prevSpace = true
			}
			continue
		}
		b.WriteRune(r)
		started = true
		prevSpace = false
	}
	out := b.String()
	return strings.TrimRight(out, " ")
}

func isCJK(r rune) bool {
	switch {
	case r >= 0x3040 && r <= 0x30FF:
		return true
	case r >= 0x3400 && r <= 0x4DBF:
		return true
	case r >= 0x4E00 && r <= 0x9FFF:
		return true
	case r >= 0xF900 && r <= 0xFAFF:
		return true
	case r >= 0x20000 && r <= 0x2A6DF:
		return true
	}
	return false
}

// Tokenize normalizes first, then splits into tokens: maximal Latin/digit
// runs and one-rune CJK ideograph/kana tokens. Punctuation and whitespace are
// separators.
func Tokenize(s string) []string {
	norm := Normalize(s)
	var tokens []string
	var run strings.Builder
	flush := func() {
		if run.Len() > 0 {
			tokens = append(tokens, run.String())
			run.Reset()
		}
	}
	for _, r := range norm {
		if isCJK(r) {
			flush()
			tokens = append(tokens, string(r))
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			run.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return tokens
}
