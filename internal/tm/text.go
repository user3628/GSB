package tm

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

type Sentence struct {
	Index     int    `json:"index"`
	Paragraph int    `json:"paragraph"`
	Text      string `json:"text"`
	Start     int    `json:"start"`
	End       int    `json:"end"`
	Core      string `json:"core"`
	Empty     bool   `json:"empty"`
}

type SegmentedText struct {
	Text        string     `json:"text"`
	Sentences   []Sentence `json:"sentences"`
	ContentFP   string     `json:"content_fp"`
	ArtifactSum string     `json:"artifact_digest"`
}

func NormalizeText(input string) string {
	nfkc := norm.NFKC.String(input)
	paragraphs := splitParagraphs(nfkc)
	for i, p := range paragraphs {
		paragraphs[i] = collapseWhitespace(p)
	}
	return strings.Join(paragraphs, "\n")
}

func splitParagraphs(s string) []string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return strings.Split(s, "\n")
}

func collapseWhitespace(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inSpace := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			inSpace = true
			continue
		}
		if inSpace && b.Len() > 0 {
			b.WriteByte(' ')
		}
		inSpace = false
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}

func isPunctuationRune(r rune) bool {
	return unicode.IsPunct(r) || r == '…' || r == '～' || r == '~'
}

func CoreText(s string) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

func SegmentText(input string) SegmentedText {
	normalized := NormalizeText(input)
	paragraphs := splitParagraphs(normalized)
	out := SegmentedText{Text: normalized}
	globalOffset := 0
	sentenceIndex := 0

	for paragraphIndex, paragraph := range paragraphs {
		runes := []rune(paragraph)
		start := -1
		flush := func(end int) {
			if start < 0 || end <= start {
				return
			}
			textRunes := runes[start:end]
			text := string(textRunes)
			core := CoreText(text)
			out.Sentences = append(out.Sentences, Sentence{
				Index:     sentenceIndex,
				Paragraph: paragraphIndex,
				Text:      text,
				Start:     globalOffset + start,
				End:       globalOffset + end,
				Core:      core,
				Empty:     core == "",
			})
			sentenceIndex++
			start = -1
		}

		for i := 0; i < len(runes); i++ {
			r := runes[i]
			if start < 0 && !unicode.IsSpace(r) && !isPunctuationRune(r) {
				start = i
			}
			if isPunctuationRune(r) {
				if start < 0 {
					start = i
				}
				end := i + 1
				for end < len(runes) && (isPunctuationRune(runes[end]) || unicode.IsSpace(runes[end])) {
					if isPunctuationRune(runes[end]) {
						end++
						i = end - 1
						continue
					}
					break
				}
				for end > start && unicode.IsSpace(runes[end-1]) {
					end--
				}
				flush(end)
				i = end - 1
			}
		}
		if start >= 0 {
			end := len(runes)
			for end > start && unicode.IsSpace(runes[end-1]) {
				end--
			}
			flush(end)
		}
		globalOffset += len([]rune(paragraph)) + 1
	}

	fp := SHA256Hex([]byte(normalized))
	out.ContentFP = fp
	digest, _, _ := DigestCanonical(out)
	out.ArtifactSum = digest
	return out
}

func ValidSentences(in []Sentence) []Sentence {
	out := make([]Sentence, 0, len(in))
	for _, s := range in {
		if !s.Empty {
			out = append(out, s)
		}
	}
	return out
}
