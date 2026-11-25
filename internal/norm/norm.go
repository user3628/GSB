package norm

import (
	"sort"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"queryrewrite/internal/spec"
)

// Normalize applies Unicode NFKC (which unifies full/half-width forms),
// lowercases and folds every run of whitespace into a single ASCII space,
// trimming the edges. The mapping is pure and deterministic.
func Normalize(s string) string {
	nfkc := norm.NFKC.String(s)
	var b strings.Builder
	b.Grow(len(nfkc))
	prevSpace := true
	for _, r := range nfkc {
		if unicode.IsSpace(r) {
			if !prevSpace {
				b.WriteByte(' ')
				prevSpace = true
			}
			continue
		}
		b.WriteRune(unicode.ToLower(r))
		prevSpace = false
	}
	return strings.TrimRight(b.String(), " ")
}

// isHan reports a CJK unified ideograph.
func isHan(r rune) bool { return unicode.Is(unicode.Han, r) }

// isLatinRune reports a rune that belongs to a continuous latin/digit run.
func isLatinRune(r rune) bool {
	if r >= 'a' && r <= 'z' {
		return true
	}
	if r >= '0' && r <= '9' {
		return true
	}
	return false
}

// Lexicon is the optional multi-character Han word set used for longest-match
// segmentation. It is supplied by the fixed local synonym table.
type Lexicon struct {
	words map[string]bool
	max   int
}

// NewLexicon builds a lexicon from word strings.
func NewLexicon(words []string) *Lexicon {
	l := &Lexicon{words: map[string]bool{}}
	for _, w := range words {
		l.words[w] = true
		if n := len([]rune(w)); n > l.max {
			l.max = n
		}
	}
	return l
}

// Has reports whether the rune substring is a known multi-character word.
func (l *Lexicon) Has(s string) bool { return l.words[s] }

// MaxLen returns the longest word length in runes.
func (l *Lexicon) MaxLen() int { return l.max }

// SortedWords returns lexicon words in deterministic sorted order.
func (l *Lexicon) SortedWords() []string {
	out := make([]string, 0, len(l.words))
	for w := range l.words {
		out = append(out, w)
	}
	sort.Strings(out)
	return out
}

// Tokenize segments normalized text into Han words (longest lexicon match with
// single-character fallback) and continuous latin/digit strings, recording
// rune [start,end) offsets. Deterministic for identical input.
func Tokenize(normalized string, lex *Lexicon) []spec.Token {
	runes := []rune(normalized)
	tokens := make([]spec.Token, 0, len(runes))
	i := 0
	for i < len(runes) {
		r := runes[i]
		switch {
		case isHan(r):
			// Greedy longest match over the local lexicon.
			span := 1
			if lex != nil {
				limit := i + lex.MaxLen()
				if limit > len(runes) {
					limit = len(runes)
				}
				for end := limit; end > i+1; end-- {
					if lex.Has(string(runes[i:end])) {
						span = end - i
						break
					}
				}
			}
			tokens = append(tokens, spec.Token{
				Text: string(runes[i : i+span]), Start: i, End: i + span, Kind: "han",
			})
			i += span
		case isLatinRune(r):
			start := i
			for i < len(runes) && isLatinRune(runes[i]) {
				i++
			}
			tokens = append(tokens, spec.Token{
				Text: string(runes[start:i]), Start: start, End: i, Kind: "latin",
			})
		default:
			// spaces and punctuation are separators only
			i++
		}
	}
	return tokens
}

// TermSet returns the ordered-unique token texts (stable order = first seen).
func TermSet(tokens []spec.Token) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if !seen[t.Text] {
			seen[t.Text] = true
			out = append(out, t.Text)
		}
	}
	return out
}

// Overlap computes the Jaccard overlap of two ordered term sets.
func Overlap(a, b []string) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	setA := map[string]bool{}
	for _, t := range a {
		setA[t] = true
	}
	setB := map[string]bool{}
	for _, t := range b {
		setB[t] = true
	}
	inter := 0
	for t := range setA {
		if setB[t] {
			inter++
		}
	}
	union := len(setA) + len(setB) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}
