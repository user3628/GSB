package textx

import (
	"strings"
	"unicode"

	"featureplatform/internal/model"

	"golang.org/x/text/unicode/norm"
)

const NormalizerVersion = "nfkc-whitespace-cjk-latin-v1"

// Normalize applies NFKC (which also performs full/half-width compatibility
// folding), then folds every Unicode whitespace run to one ASCII space.
func Normalize(raw string) string {
	compatibility := norm.NFKC.String(raw)
	return strings.Join(strings.Fields(compatibility), " ")
}

func isHan(r rune) bool {
	return unicode.Is(unicode.Han, r)
}

func isLatinLetter(r rune) bool {
	return unicode.IsLetter(r) && unicode.Is(unicode.Latin, r)
}

// Tokenize returns Chinese unigrams, adjacent Chinese bigrams and maximal
// contiguous Latin letter strings in deterministic source order.
func Tokenize(normalized string) []model.Token {
	runes := []rune(normalized)
	tokens := make([]model.Token, 0, len(runes)*2)
	position := 0

	for i := 0; i < len(runes); {
		r := runes[i]
		switch {
		case isHan(r):
			tokens = append(tokens, model.Token{Position: position, Text: string(r), Kind: "han_unigram"})
			position++
			if i+1 < len(runes) && isHan(runes[i+1]) {
				tokens = append(tokens, model.Token{
					Position: position,
					Text:     string([]rune{r, runes[i+1]}),
					Kind:     "han_bigram",
				})
				position++
			}
			i++
		case isLatinLetter(r):
			start := i
			for i < len(runes) && isLatinLetter(runes[i]) {
				i++
			}
			word := strings.ToLower(string(runes[start:i]))
			tokens = append(tokens, model.Token{Position: position, Text: word, Kind: "latin"})
			position++
		default:
			i++
		}
	}
	return tokens
}

func NormalizeAndTokenize(raw string) (string, []model.Token) {
	n := Normalize(raw)
	return n, Tokenize(n)
}
