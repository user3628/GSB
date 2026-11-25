package textnorm

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

type Token struct {
	Text string `json:"text"`
	Kind string `json:"kind"`
}

type Result struct {
	Original         string   `json:"original"`
	Normalized       string   `json:"normalized"`
	Tokens           []Token  `json:"tokens"`
	Terms            []string `json:"terms"`
	UniqueTerms      []string `json:"unique_terms"`
	ChineseCharCount int      `json:"chinese_char_count"`
	LatinRunCount    int      `json:"latin_run_count"`
}

func Normalize(s string) string {
	s = norm.NFKC.String(s)
	s = strings.ToLower(s)
	var b strings.Builder
	b.Grow(len(s))
	lastSpace := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			if !lastSpace {
				b.WriteByte(' ')
			}
			lastSpace = true
			continue
		}
		lastSpace = false
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}

func isHan(r rune) bool {
	return unicode.Is(unicode.Han, r)
}

func isLatinTokenRune(r rune) bool {
	return unicode.Is(unicode.Latin, r) || (r >= '0' && r <= '9')
}

func Tokenize(raw string) Result {
	normalized := Normalize(raw)
	res := Result{Original: raw, Normalized: normalized}
	seen := map[string]struct{}{}
	var han []rune
	flushHan := func() {
		if len(han) == 0 {
			return
		}
		for _, r := range han {
			term := string(r)
			res.Tokens = append(res.Tokens, Token{Text: term, Kind: "han_unigram"})
			res.Terms = append(res.Terms, term)
			res.ChineseCharCount++
		}
		for i := 0; i+1 < len(han); i++ {
			term := string(han[i : i+2])
			res.Tokens = append(res.Tokens, Token{Text: term, Kind: "han_bigram"})
			res.Terms = append(res.Terms, term)
		}
		han = nil
	}

	var latin []rune
	flushLatin := func() {
		if len(latin) == 0 {
			return
		}
		term := string(latin)
		res.Tokens = append(res.Tokens, Token{Text: term, Kind: "latin_run"})
		res.Terms = append(res.Terms, term)
		res.LatinRunCount++
		latin = nil
	}

	for _, r := range normalized {
		switch {
		case isHan(r):
			flushLatin()
			han = append(han, r)
		case isLatinTokenRune(r):
			flushHan()
			latin = append(latin, r)
		default:
			flushHan()
			flushLatin()
		}
	}
	flushHan()
	flushLatin()

	for _, term := range res.Terms {
		if _, ok := seen[term]; !ok {
			seen[term] = struct{}{}
			res.UniqueTerms = append(res.UniqueTerms, term)
		}
	}
	return res
}
