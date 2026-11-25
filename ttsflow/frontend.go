package ttsflow

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
	"golang.org/x/text/width"
)

type tokenKind uint8

const (
	tokenHan tokenKind = iota
	tokenLatin
	tokenPunctuation
	tokenSymbol
	tokenSpace
)

type textToken struct {
	text      string
	startRune int
	endRune   int
	startByte int
	endByte   int
	kind      tokenKind
}

func normalizeText(input string) (string, error) {
	if !utf8.ValidString(input) {
		return "", biz(CodeSplit, "text is not valid UTF-8")
	}
	// width.Fold performs explicit full-width/half-width folding before NFKC.
	folded := width.Fold.String(input)
	normalized := norm.NFKC.String(folded)

	var b strings.Builder
	b.Grow(len(normalized))
	spacePending := false
	for _, r := range normalized {
		if unicode.IsSpace(r) {
			if b.Len() > 0 {
				spacePending = true
			}
			continue
		}
		if spacePending {
			b.WriteByte(' ')
			spacePending = false
		}
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String()), nil
}

func tokenize(text string) []textToken {
	runes := []rune(text)
	byteOffsets := make([]int, len(runes)+1)
	pos := 0
	for i, r := range runes {
		byteOffsets[i] = pos
		pos += utf8.RuneLen(r)
	}
	byteOffsets[len(runes)] = pos

	tokens := make([]textToken, 0, len(runes))
	for i := 0; i < len(runes); {
		start := i
		r := runes[i]
		var kind tokenKind
		switch {
		case unicode.Is(unicode.Han, r):
			kind = tokenHan
			i++
		case unicode.IsSpace(r):
			kind = tokenSpace
			i++
		case unicode.IsPunct(r):
			kind = tokenPunctuation
			i++
		case unicode.IsSymbol(r):
			kind = tokenSymbol
			i++
		case unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsMark(r):
			kind = tokenLatin
			i++
			for i < len(runes) {
				n := runes[i]
				if unicode.IsSpace(n) || unicode.IsPunct(n) || unicode.IsSymbol(n) || unicode.Is(unicode.Han, n) {
					break
				}
				if !(unicode.IsLetter(n) || unicode.IsDigit(n) || unicode.IsMark(n)) {
					break
				}
				i++
			}
		default:
			kind = tokenSymbol
			i++
		}
		tokens = append(tokens, textToken{
			text:      string(runes[start:i]),
			startRune: start,
			endRune:   i,
			startByte: byteOffsets[start],
			endByte:   byteOffsets[i],
			kind:      kind,
		})
	}
	return tokens
}

func meaningfulRunes(tokens []textToken) int {
	n := 0
	for _, t := range tokens {
		if t.kind != tokenSpace {
			n += t.endRune - t.startRune
		}
	}
	return n
}

func splitTokens(tokens []textToken, cfg SplitConfig) ([][]textToken, error) {
	if len(tokens) == 0 {
		return nil, nil
	}
	chunks := make([][]textToken, 0, 8)
	start := 0
	for start < len(tokens) {
		target := cfg.FollowingChars
		if len(chunks) == 0 {
			target = cfg.FirstMinChars
		}
		end := start
		meaningful := 0
		lastRelevant := start
		for end < len(tokens) {
			tok := tokens[end]
			end++
			if tok.kind != tokenSpace {
				meaningful += tok.endRune - tok.startRune
				lastRelevant = end
			}
			if cfg.ForceAtPunctuation && (tok.kind == tokenPunctuation || tok.kind == tokenSymbol) {
				break
			}
			if meaningful >= target {
				break
			}
		}
		if lastRelevant > start {
			end = lastRelevant
		}
		if end <= start {
			// A whole token can exceed the target, but progress must still be
			// made. This guard turns a malformed tokenizer state into a stable
			// business error instead of an infinite loop.
			return nil, biz(CodeSplit, "tokenizer made no progress while forming a block")
		}
		chunks = append(chunks, tokens[start:end])
		start = end
	}
	return chunks, nil
}

func makeChunk(index int, tokens []textToken) Chunk {
	first := tokens[0]
	last := tokens[len(tokens)-1]
	var builder strings.Builder
	for _, token := range tokens {
		builder.WriteString(token.text)
	}
	text := builder.String()
	return Chunk{
		Index:     index,
		Text:      text,
		StartRune: first.startRune,
		EndRune:   last.endRune,
		StartByte: first.startByte,
		EndByte:   last.endByte,
	}
}

func lookupGrapheme(cfg Config, grapheme string, kind tokenKind, sampleLanguage string) ([]string, string, bool) {
	order := []string{sampleLanguage, "common", "zh", "en"}
	switch kind {
	case tokenHan:
		order = []string{"zh", sampleLanguage, "common", "en"}
	case tokenLatin:
		order = []string{"en", sampleLanguage, "common", "zh"}
	case tokenPunctuation, tokenSymbol:
		order = []string{"common", sampleLanguage, "zh", "en"}
	}
	seen := map[string]bool{}
	for _, language := range order {
		if language == "" || seen[language] {
			continue
		}
		seen[language] = true
		if phonemes, ok := cfg.PhonemesFor(grapheme, language); ok {
			return phonemes, language, true
		}
	}
	return nil, "", false
}

// BuildFrontend normalizes and splits text, then maps graphemes to phonemes and
// phonemes to indivisible pronunciation units.
func BuildFrontend(sample TextSample, cfg Config) (FrontendResult, error) {
	result := FrontendResult{OriginalText: sample.Text}
	normalized, err := normalizeText(sample.Text)
	if err != nil {
		be, _ := AsBizError(err)
		be = withUTT(be, sample.UTTID)
		be.BlockSeq = 0
		return result, be
	}
	if normalized == "" {
		return result, &BizError{
			Code: CodeEmptyText, Message: "text is empty after normalization",
			UTTID: sample.UTTID, BlockSeq: 0,
		}
	}
	result.NormalizedText = normalized
	result.NormalizedDigest = digestString(normalized)
	result.TextDigest = sample.ContentDigest()
	result.SplitDigest = cfg.SplitDigest()

	tokens := tokenize(normalized)
	if len(tokens) == 0 {
		return result, &BizError{Code: CodeSplit, Message: "normalized text produced no tokens", UTTID: sample.UTTID}
	}
	tokenChunks, splitErr := splitTokens(tokens, cfg.Splitting)
	if splitErr != nil {
		be, _ := AsBizError(splitErr)
		be.UTTID = sample.UTTID
		be.BlockSeq = 0
		return result, be
	}
	if len(tokenChunks) == 0 {
		return result, &BizError{Code: CodeSplit, Message: "text could not be split into blocks", UTTID: sample.UTTID}
	}

	var units []Unit
	for blockSeq, group := range tokenChunks {
		chunk := makeChunk(blockSeq, group)
		chunk.UnitStart = len(units)
		for _, tok := range group {
			if tok.kind == tokenSpace {
				continue
			}
			graphemes := []string{tok.text}
			if tok.kind == tokenLatin {
				graphemes = graphemes[:0]
				for _, r := range tok.text {
					graphemes = append(graphemes, string(r))
				}
			}
			for _, grapheme := range graphemes {
				phonemes, language, ok := lookupGrapheme(cfg, grapheme, tok.kind, sample.Language)
				if !ok {
					return result, &BizError{
						Code: CodePhonemeMapping, Message: "grapheme has no phoneme mapping",
						UTTID: sample.UTTID, BlockSeq: blockSeq, RuneOffset: tok.startRune,
						Context: fmt.Sprintf("language=%s grapheme=%q token=%q", sample.Language, grapheme, tok.text),
					}
				}
				for _, phoneme := range phonemes {
					symbol, ok := cfg.UnitFor(phoneme)
					if !ok {
						return result, &BizError{
							Code: CodePhonemeMapping, Message: "phoneme has no pronunciation-unit mapping",
							UTTID: sample.UTTID, BlockSeq: blockSeq, RuneOffset: tok.startRune,
							Context: fmt.Sprintf("phoneme=%q grapheme=%q", phoneme, grapheme),
						}
					}
					unit := Unit{
						Index:        len(units),
						ID:           fmt.Sprintf("U%06d", len(units)),
						ChunkIndex:   blockSeq,
						Grapheme:     grapheme,
						Language:     language,
						Phoneme:      phoneme,
						Symbol:       symbol,
						PhonemeIndex: len(chunk.Phonemes),
					}
					units = append(units, unit)
					chunk.Phonemes = append(chunk.Phonemes, phoneme)
				}
			}
		}
		chunk.UnitEnd = len(units)
		if chunk.UnitEnd <= chunk.UnitStart {
			return result, &BizError{
				Code: CodeSplit, Message: "block contains no pronunciation units",
				UTTID: sample.UTTID, BlockSeq: blockSeq, RuneOffset: chunk.StartRune,
				Context: fmt.Sprintf("block_text=%q", chunk.Text),
			}
		}
		result.Chunks = append(result.Chunks, chunk)
	}
	if len(units) == 0 {
		return result, &BizError{Code: CodeSplit, Message: "text produced no pronunciation units", UTTID: sample.UTTID}
	}
	result.Units = units
	result.Digest = digestParts(result.TextDigest, result.SplitDigest, result.NormalizedDigest, result.Chunks, result.Units)
	return result, nil
}
