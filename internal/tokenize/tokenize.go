package tokenize

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"searchlab/internal/apperr"
	"searchlab/internal/config"
	"searchlab/internal/model"
)

type Token struct {
	Field  string `json:"field"`
	Start  int    `json:"start"`
	End    int    `json:"end"`
	TermID int32  `json:"term_id"`
}

type Term struct {
	ID   int32  `json:"id"`
	Term string `json:"term"`
}

type DocTokens struct {
	DocID           int64   `json:"doc_id"`
	Lang            string  `json:"lang"`
	Source          string  `json:"source"`
	NormalizedTitle string  `json:"normalized_title"`
	NormalizedBody  string  `json:"normalized_body"`
	TitleTokens     []Token `json:"title_tokens"`
	BodyTokens      []Token `json:"body_tokens"`
}

type Artifact struct {
	ConfigVersion   string      `json:"config_version"`
	TokenizerDigest string      `json:"tokenizer_digest"`
	Terms           []Term      `json:"terms"`
	Docs            []DocTokens `json:"docs"`
}

type Builder struct {
	cfg      config.Config
	stopword map[string]struct{}
}

func New(cfg config.Config) *Builder {
	sw := make(map[string]struct{}, len(cfg.Tokenizer.Stopwords))
	for _, word := range cfg.Tokenizer.Stopwords {
		sw[NormalizeText(word)] = struct{}{}
	}
	return &Builder{cfg: cfg, stopword: sw}
}

func (b *Builder) Build(corpus model.Corpus) ([]byte, *Artifact, error) {
	terms := make(map[string]struct{})
	docs := make([]DocTokens, 0, len(corpus.Docs))
	for _, doc := range corpus.Docs {
		titleNorm := NormalizeText(doc.Title)
		bodyNorm := NormalizeText(doc.Body)
		titleRaw := b.scan(titleNorm, "title")
		bodyRaw := b.scan(bodyNorm, "body")
		titleTokens := b.filterStopwords(titleRaw)
		bodyTokens := b.filterStopwords(bodyRaw)
		for _, t := range titleRaw {
			terms[t.term] = struct{}{}
		}
		for _, t := range bodyRaw {
			terms[t.term] = struct{}{}
		}
		docs = append(docs, DocTokens{
			DocID:           doc.DocID,
			Lang:            doc.Lang,
			Source:          doc.Source,
			NormalizedTitle: titleNorm,
			NormalizedBody:  bodyNorm,
			TitleTokens:     make([]Token, 0, len(titleTokens)),
			BodyTokens:      make([]Token, 0, len(bodyTokens)),
		})
		for _, t := range titleTokens {
			docs[len(docs)-1].TitleTokens = append(docs[len(docs)-1].TitleTokens, Token{Field: "title", Start: t.start, End: t.end})
		}
		for _, t := range bodyTokens {
			docs[len(docs)-1].BodyTokens = append(docs[len(docs)-1].BodyTokens, Token{Field: "body", Start: t.start, End: t.end})
		}
	}
	allTerms := make([]string, 0, len(terms))
	for term := range terms {
		if _, ok := b.stopword[term]; ok {
			continue
		}
		allTerms = append(allTerms, term)
	}
	sort.Strings(allTerms)
	termTable := make([]Term, len(allTerms))
	ids := make(map[string]int32, len(allTerms))
	for i, term := range allTerms {
		id := int32(i + 1)
		termTable[i] = Term{ID: id, Term: term}
		ids[term] = id
	}
	for i := range docs {
		for j := range docs[i].TitleTokens {
			docs[i].TitleTokens[j].TermID = ids[termAt(b, docs[i].NormalizedTitle, docs[i].TitleTokens[j].Start, docs[i].TitleTokens[j].End)]
		}
		for j := range docs[i].BodyTokens {
			docs[i].BodyTokens[j].TermID = ids[termAt(b, docs[i].NormalizedBody, docs[i].BodyTokens[j].Start, docs[i].BodyTokens[j].End)]
		}
	}
	artifact := Artifact{ConfigVersion: b.cfg.Version, TokenizerDigest: b.cfg.TokenizerDigest(), Terms: termTable, Docs: docs}
	payload, err := json.Marshal(artifact)
	if err != nil {
		return nil, nil, apperr.E(apperr.CodeInput, "marshal token artifact", err)
	}
	return payload, &artifact, nil
}

type rawToken struct {
	term  string
	start int
	end   int
}

func (b *Builder) scan(text, field string) []rawToken {
	runes := []rune(text)
	out := make([]rawToken, 0)
	for i := 0; i < len(runes); {
		r := runes[i]
		if unicode.Is(unicode.Han, r) {
			j := i + 1
			for j < len(runes) && unicode.Is(unicode.Han, runes[j]) {
				j++
			}
			for k := i; k < j; k++ {
				out = append(out, rawToken{term: string(runes[k]), start: k, end: k + 1})
				if k+1 < j {
					out = append(out, rawToken{term: string(runes[k : k+2]), start: k, end: k + 2})
				}
			}
			i = j
			continue
		}
		if isWordRune(r) {
			j := i + 1
			for j < len(runes) && isWordRune(runes[j]) {
				j++
			}
			out = append(out, rawToken{term: string(runes[i:j]), start: i, end: j})
			i = j
			continue
		}
		i++
	}
	return out
}

func isWordRune(r rune) bool {
	return unicode.Is(unicode.Latin, r) || unicode.IsDigit(r)
}

func (b *Builder) filterStopwords(in []rawToken) []rawToken {
	out := make([]rawToken, 0, len(in))
	for _, t := range in {
		if _, stop := b.stopword[t.term]; stop {
			continue
		}
		out = append(out, t)
	}
	return out
}

func termAt(b *Builder, text string, start, end int) string {
	return string([]rune(text)[start:end])
}

func NormalizeText(input string) string {
	nfkc := norm.NFKC.String(input)
	var b strings.Builder
	space := false
	for _, r := range nfkc {
		if unicode.IsSpace(r) {
			if !space && b.Len() > 0 {
				b.WriteRune(' ')
			}
			space = true
			continue
		}
		space = false
		b.WriteString(strings.ToLower(string(r)))
	}
	return strings.TrimSpace(b.String())
}

func (b *Builder) QueryTerms(raw []string) []string {
	seen := make(map[string]struct{})
	terms := make([]string, 0)
	for _, item := range raw {
		norm := NormalizeText(item)
		for _, t := range b.scan(norm, "query") {
			if _, stop := b.stopword[t.term]; stop {
				continue
			}
			if _, ok := seen[t.term]; ok {
				continue
			}
			seen[t.term] = struct{}{}
			terms = append(terms, t.term)
		}
	}
	sort.Strings(terms)
	return terms
}

func DigestPayload(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
