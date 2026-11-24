package rewardpack

import (
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// NormalizePair applies NFKC, whitespace collapse, lowercasing and deterministic
// CJK/Latin tokenization. Empty or punctuation-only replies are hard errors.
func NormalizePair(a Annotation, cfg Config) (NormalizedPair, error) {
	if strings.TrimSpace(a.PairID) == "" {
		return NormalizedPair{}, &BusinessError{Code: CodeInvalidInput, Field: "pair_id", Message: "pair_id must not be empty"}
	}
	if strings.ContainsRune(a.PairID, '\x00') {
		return NormalizedPair{}, &BusinessError{Code: CodeInvalidInput, PairID: a.PairID, Field: "pair_id", Message: "pair_id contains NUL"}
	}
	rawDigest := digestJSON(a)

	prompt, err := normalizeText(a.Prompt, cfg.Normalization)
	if err != nil {
		return NormalizedPair{}, err
	}
	chosen, err := normalizeText(a.Chosen, cfg.Normalization)
	if err != nil {
		return NormalizedPair{}, err
	}
	if chosen.TokenCount == 0 {
		return NormalizedPair{}, &BusinessError{Code: CodeEmptyText, PairID: a.PairID, Field: "chosen", Message: "chosen is empty or contains punctuation only after normalization"}
	}
	rejected, err := normalizeText(a.Rejected, cfg.Normalization)
	if err != nil {
		return NormalizedPair{}, err
	}
	if rejected.TokenCount == 0 {
		return NormalizedPair{}, &BusinessError{Code: CodeEmptyText, PairID: a.PairID, Field: "rejected", Message: "rejected is empty or contains punctuation only after normalization"}
	}

	result := NormalizedPair{
		PairID:    a.PairID,
		Source:    a.Source,
		Lang:      normalizeLanguage(a.Lang),
		Prompt:    prompt,
		Chosen:    chosen,
		Rejected:  rejected,
		TokenCost: chosen.TokenCount + rejected.TokenCount,
		RawDigest: rawDigest,
	}
	if result.Lang == "" {
		result.Lang = combinedLanguage(prompt, chosen, rejected)
	}
	result.InputDigest = hashBytes([]byte("normalize\x00"), []byte(rawDigest), []byte(cfg.NormalizationDigest()))
	result.Digest = digestJSON(result)
	return result, nil
}

func normalizeText(raw string, cfg NormalizationConfig) (TextFeatures, error) {
	if !utf8.ValidString(raw) {
		return TextFeatures{}, &BusinessError{Code: CodeInvalidInput, Field: "text", Message: "text is not valid UTF-8"}
	}
	text := raw
	if cfg.NFKC {
		text = norm.NFKC.String(text)
	}
	if cfg.CollapseWhitespace {
		text = collapseWhitespace(text)
	}
	if cfg.Lowercase {
		text = strings.ToLower(text)
	}
	runes := []rune(text)
	byteOffsets := make([]int, len(runes)+1)
	for i, r := range runes {
		byteOffsets[i+1] = byteOffsets[i] + utf8.RuneLen(r)
	}
	tokens := make([]Token, 0)
	counts := map[string]int{}
	for i := 0; i < len(runes); {
		r := runes[i]
		switch {
		case unicode.Is(unicode.Han, r):
			startByte := byteOffsets[i]
			endByte := byteOffsets[i+1]
			token := Token{Text: string(r), Start: i, End: i + 1, ByteStart: startByte, ByteEnd: endByte}
			tokens = append(tokens, token)
			counts[token.Text]++
			i++
		case isLatinTokenRune(r):
			start := i
			for i < len(runes) && isLatinTokenRune(runes[i]) {
				i++
			}
			startByte := byteOffsets[start]
			endByte := byteOffsets[i]
			token := Token{Text: string(runes[start:i]), Start: start, End: i, ByteStart: startByte, ByteEnd: endByte}
			tokens = append(tokens, token)
			counts[token.Text]++
		default:
			i++
		}
	}
	terms := make([]TermFrequency, 0, len(counts))
	for term, count := range counts {
		terms = append(terms, TermFrequency{Term: term, Count: count})
	}
	sort.Slice(terms, func(i, j int) bool { return terms[i].Term < terms[j].Term })
	return TextFeatures{
		Normalized: text,
		Tokens:     tokens,
		Terms:      terms,
		TokenCount: len(tokens),
		CharCount:  utf8.RuneCountInString(text),
		Language:   detectLanguage(tokens),
	}, nil
}

func collapseWhitespace(in string) string {
	var b strings.Builder
	b.Grow(len(in))
	pendingSpace := false
	wrote := false
	for _, r := range in {
		if unicode.IsSpace(r) || r == '\u200b' {
			if wrote {
				pendingSpace = true
			}
			continue
		}
		if pendingSpace {
			b.WriteByte(' ')
			pendingSpace = false
		}
		b.WriteRune(r)
		wrote = true
	}
	return b.String()
}

func isLatinTokenRune(r rune) bool {
	if r >= '0' && r <= '9' {
		return true
	}
	return unicode.IsLetter(r) && unicode.Is(unicode.Latin, r)
}

func detectLanguage(tokens []Token) string {
	hasHan, hasLatin := false, false
	for _, token := range tokens {
		for _, r := range token.Text {
			if unicode.Is(unicode.Han, r) {
				hasHan = true
			}
			if isLatinTokenRune(r) {
				hasLatin = true
			}
		}
	}
	switch {
	case hasHan && hasLatin:
		return "mixed"
	case hasHan:
		return "zh"
	case hasLatin:
		return "en"
	default:
		return "und"
	}
}

func combinedLanguage(fields ...TextFeatures) string {
	hasHan, hasLatin := false, false
	for _, field := range fields {
		for _, token := range field.Tokens {
			for _, r := range token.Text {
				if unicode.Is(unicode.Han, r) {
					hasHan = true
				}
				if isLatinTokenRune(r) {
					hasLatin = true
				}
			}
		}
	}
	switch {
	case hasHan && hasLatin:
		return "mixed"
	case hasHan:
		return "zh"
	case hasLatin:
		return "en"
	default:
		return "und"
	}
}

func normalizeLanguage(lang string) string {
	lang = strings.ToLower(strings.TrimSpace(lang))
	switch lang {
	case "zh", "zh-cn", "zh-hans", "cn", "chinese":
		return "zh"
	case "en", "en-us", "en-gb", "english":
		return "en"
	case "mixed", "zh-en", "en-zh":
		return "mixed"
	case "":
		return ""
	default:
		return lang
	}
}
