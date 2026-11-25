// Package normalize implements stage 1: Unicode normalization and deterministic
// token extraction. The same input and policy always produce byte-identical
// token summaries; map ordering never affects emitted artifacts.
package normalize

import (
	"hash/fnv"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"ticketpipeline/internal/determ"
	"ticketpipeline/internal/model"
)

// Token kinds. They are persisted as short stable strings.
const (
	KindHanUni = "han_uni" // single Chinese character
	KindHanBi  = "han_bi"  // adjacent Chinese bigram
	KindLatin  = "latin"   // maximal latin-letter/digit run, lowercased
)

// Token is one emitted term with both rune and byte offsets plus the channel
// marker requested by the specification.
type Token struct {
	Term      string `json:"term"`
	Kind      string `json:"kind"`
	Channel   string `json:"channel"`
	RuneStart int    `json:"rune_start"`
	RuneEnd   int    `json:"rune_end"`
	ByteStart int    `json:"byte_start"`
	ByteEnd   int    `json:"byte_end"`
	Index     int    `json:"index"` // feature bucket in [0,hashDim)
}

// Result is the persisted stage-1 artifact for one ticket.
type Result struct {
	TicketID   string  `json:"ticket_id"`
	Normalized string  `json:"normalized"`
	Tokens     []Token `json:"tokens"`
	Digest     string  `json:"digest"` // fingerprint of this exact artifact
}

// ValidateConfig rejects illegal stage-1 configuration with E_CONFIG before
// any ticket is processed, so a zero hash dimension never silently degrades
// into bucket zero.
func ValidateConfig(p model.Policy) *model.BizError {
	if p.HashDim <= 0 {
		return model.NewBizError(model.CodeConfig, model.StageNormalize, "",
			"hash dimension must be positive", map[string]string{"hash_dim": itoa(p.HashDim)})
	}
	if p.SampleRatio <= 0 || p.SampleRatio > 1 {
		return model.NewBizError(model.CodeConfig, model.StageNormalize, "",
			"sample ratio must be within (0,1]", map[string]string{"sample_ratio": floatStr(p.SampleRatio)})
	}
	return nil
}

// NormalizeText applies NFKC folding (which also unifies full/half width
// forms), lowercases the result and collapses every whitespace run to one
// ASCII space with trimmed edges.
func NormalizeText(raw string) string {
	nfkc := norm.NFKC.String(raw)
	lowered := strings.ToLower(nfkc)
	return strings.Join(strings.Fields(lowered), " ")
}

// Tokenize extracts Han unigrams, adjacent Han bigrams and maximal
// latin/digit runs from the normalized text.
func Tokenize(normalized, channel string, hashDim int) ([]Token, *model.BizError) {
	if hashDim <= 0 {
		return nil, model.NewBizError(model.CodeConfig, model.StageNormalize, "",
			"hash dimension must be positive", map[string]string{"hash_dim": itoa(hashDim)})
	}
	runes := []rune(normalized)
	tokens := make([]Token, 0, len(runes))
	bytePos := 0
	runeIdx := 0
	for runeIdx < len(runes) {
		r := runes[runeIdx]
		switch {
		case unicode.Is(unicode.Han, r):
			start := runeIdx
			byteStart := bytePos
			for runeIdx < len(runes) && unicode.Is(unicode.Han, runes[runeIdx]) {
				bytePos += len(string(runes[runeIdx]))
				runeIdx++
			}
			run := runes[start:runeIdx]
			emitHanRun(run, channel, start, byteStart, hashDim, &tokens)
		case isLatinDigit(r):
			start := runeIdx
			byteStart := bytePos
			for runeIdx < len(runes) && isLatinDigit(runes[runeIdx]) {
				bytePos += len(string(runes[runeIdx]))
				runeIdx++
			}
			term := string(runes[start:runeIdx])
			tokens = append(tokens, Token{
				Term: term, Kind: KindLatin, Channel: channel,
				RuneStart: start, RuneEnd: runeIdx,
				ByteStart: byteStart, ByteEnd: bytePos,
				Index: featureIndex(term, channel, hashDim),
			})
		default:
			bytePos += len(string(r))
			runeIdx++
		}
	}
	return tokens, nil
}

func emitHanRun(run []rune, channel string, runeStart, byteStart, hashDim int, out *[]Token) {
	offset := 0
	for i, r := range run {
		term := string(r)
		bs := byteStart + offset
		offset += len(term)
		*out = append(*out, Token{
			Term: term, Kind: KindHanUni, Channel: channel,
			RuneStart: runeStart + i, RuneEnd: runeStart + i + 1,
			ByteStart: bs, ByteEnd: bs + len(term),
			Index: featureIndex(term, channel, hashDim),
		})
		if i+1 < len(run) {
			bi := string(run[i : i+2])
			*out = append(*out, Token{
				Term: bi, Kind: KindHanBi, Channel: channel,
				RuneStart: runeStart + i, RuneEnd: runeStart + i + 2,
				ByteStart: bs, ByteEnd: bs + len(bi),
				Index: featureIndex(bi, channel, hashDim),
			})
		}
	}
}

// featureIndex hashes term and channel together so the same text on different
// channels lands in different buckets deterministically.
func featureIndex(term, channel string, hashDim int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(channel))
	_, _ = h.Write([]byte{0x1f})
	_, _ = h.Write([]byte(term))
	return int(h.Sum32() % uint32(hashDim))
}

func isLatinDigit(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
}

// Process normalizes and tokenizes one ticket. Empty or punctuation-only text
// yields E_EMPTY_TEXT carrying the ticket id; the caller persists that fact.
func Process(t model.RawTicket, hashDim int) (*Result, *model.BizError) {
	normalized := NormalizeText(t.Text)
	if normalized == "" {
		return nil, model.NewBizError(model.CodeEmptyText, model.StageNormalize, t.TicketID,
			"ticket text is empty or punctuation-only", map[string]string{"channel": t.Channel})
	}
	tokens, bizErr := Tokenize(normalized, t.Channel, hashDim)
	if bizErr != nil {
		return nil, bizErr
	}
	// Punctuation-only text normalizes to a non-empty string but yields zero
	// terms; the contract still classifies it as E_EMPTY_TEXT.
	if len(tokens) == 0 {
		return nil, model.NewBizError(model.CodeEmptyText, model.StageNormalize, t.TicketID,
			"ticket text contains no tokenizable characters", map[string]string{"channel": t.Channel})
	}
	res := &Result{TicketID: t.TicketID, Normalized: normalized, Tokens: tokens}
	digest, err := determ.Fingerprint(res)
	if err != nil {
		return nil, model.NewBizError(model.CodeNumeric, model.StageNormalize, t.TicketID, err.Error(), nil)
	}
	res.Digest = digest
	return res, nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func floatStr(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}
