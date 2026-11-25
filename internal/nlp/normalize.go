package nlp

import (
	"strings"
	"unicode"

	"csrouter/internal/domain"

	"golang.org/x/text/unicode/norm"
)

const NormalizationVersion = "nfkc-width-ws-cjk-unigram-bigram-latin-alnum/v1"

func NormalizeTicket(ticket domain.Ticket, version int64) (domain.NormalizeArtifact, *domain.BizError) {
	normalized := FoldWhitespace(strings.ToLower(norm.NFKC.String(ticket.Text)))
	tokens := Tokenize(normalized, ticket.Channel)

	artifact := domain.NormalizeArtifact{
		TicketID:   ticket.TicketID,
		Channel:    ticket.Channel,
		Language:   ticket.Language,
		Normalized: normalized,
		Tokens:     tokens,
		TokenCount: len(tokens),
	}
	if len(tokens) == 0 {
		artifact.ErrorCode = domain.ErrEmptyText
		bizErr := domain.NewBizError(domain.ErrEmptyText, ticket.TicketID, "", version, "text is empty or contains no lexical tokens")
		artifact.Error = bizErr
		artifact.Digest = domain.DigestArtifact(domain.StageNormalize, artifact)
		return artifact, bizErr
	}
	artifact.Digest = domain.DigestArtifact(domain.StageNormalize, artifact)
	return artifact, nil
}

func FoldWhitespace(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	pendingSpace := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			pendingSpace = b.Len() > 0
			continue
		}
		if pendingSpace {
			b.WriteByte(' ')
			pendingSpace = false
		}
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}

func isCJKHan(r rune) bool {
	return unicode.Is(unicode.Han, r)
}

func isLatinAlphaNumeric(r rune) bool {
	return (r < 128 && (unicode.IsLetter(r) || unicode.IsDigit(r))) ||
		(unicode.Is(unicode.Latin, r) && (unicode.IsLetter(r) || unicode.IsDigit(r)))
}

func Tokenize(s, channel string) []domain.Token {
	runes := []rune(s)
	byteStarts := make([]int, len(runes)+1)
	offset := 0
	for i, r := range runes {
		byteStarts[i] = offset
		offset += len(string(r))
	}
	byteStarts[len(runes)] = len(s)

	tokens := make([]domain.Token, 0, len(runes)*2)
	add := func(text string, kind string, startChar, endChar int) {
		tokens = append(tokens, domain.Token{
			Text:      text,
			Kind:      kind,
			Start:     startChar,
			End:       endChar,
			ByteStart: byteStarts[startChar],
			ByteEnd:   byteStarts[endChar],
			Channel:   channel,
		})
	}

	for i := 0; i < len(runes); {
		r := runes[i]
		switch {
		case isCJKHan(r):
			add(string(r), "cjk_char", i, i+1)
			if i+1 < len(runes) && isCJKHan(runes[i+1]) {
				add(string(runes[i:i+2]), "cjk_bigram", i, i+2)
			}
			i++
		case isLatinAlphaNumeric(r):
			j := i + 1
			for j < len(runes) && isLatinAlphaNumeric(runes[j]) {
				j++
			}
			add(string(runes[i:j]), "latin_alnum", i, j)
			i = j
		default:
			i++
		}
	}
	return tokens
}

func Terms(tokens []domain.Token) []string {
	terms := make([]string, len(tokens))
	for i, token := range tokens {
		terms[i] = token.Text
	}
	return terms
}
