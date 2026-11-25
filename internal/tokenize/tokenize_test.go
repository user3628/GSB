package tokenize

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"searchlab/internal/config"
	"searchlab/internal/model"
)

func TestNormalizeAndTokenOffsets(t *testing.T) {
	cfg := config.MustDefault()
	builder := New(cfg)
	corpus := model.Corpus{Docs: []model.Document{{DocID: 1, Title: "  Ｇｏ　语言\tSEARCH ", Body: "the 的 Go，语言 123"}}}
	payload, artifact, err := builder.Build(corpus)
	if err != nil {
		t.Fatalf("build tokens: %v", err)
	}
	if artifact.Docs[0].NormalizedTitle != "go 语言 search" {
		t.Fatalf("unexpected normalized title %q", artifact.Docs[0].NormalizedTitle)
	}
	if artifact.Docs[0].NormalizedBody != "the 的 go,语言 123" {
		t.Fatalf("unexpected normalized body %q", artifact.Docs[0].NormalizedBody)
	}
	foundGoTitle := false
	foundLanguage := false
	for _, token := range artifact.Docs[0].TitleTokens {
		term := termForID(artifact, token.TermID)
		if term == "go" {
			foundGoTitle = true
			if token.Start != 0 || token.End != 2 {
				t.Fatalf("go offset = %d:%d", token.Start, token.End)
			}
		}
		if term == "语言" {
			foundLanguage = true
			if token.Start != 3 || token.End != 5 {
				t.Fatalf("语言 offset = %d:%d", token.Start, token.End)
			}
		}
	}
	if !foundGoTitle || !foundLanguage {
		t.Fatalf("expected go and 语言 tokens: %+v", artifact.Docs[0].TitleTokens)
	}
	for i, term := range artifact.Terms {
		if term.ID != int32(i+1) {
			t.Fatalf("term ids are not monotonic: %+v", artifact.Terms)
		}
	}
	if !bytes.Equal(payload, mustMarshal(t, artifact)) {
		t.Fatal("artifact payload is not canonical JSON")
	}
}

func TestEmptyBodyRetainedAndDeterministic(t *testing.T) {
	cfg := config.MustDefault()
	corpus := model.Corpus{Docs: []model.Document{
		{DocID: 7, Title: "空正文占位", Body: "", Lang: "zh", Source: "test"},
		{DocID: 8, Title: "normal", Body: "content", Lang: "en", Source: "test"},
	}}
	first, artifact, err := New(cfg).Build(corpus)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := New(cfg).Build(corpus)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("token artifact is not byte deterministic")
	}
	if artifact.Docs[0].BodyTokens == nil || len(artifact.Docs[0].BodyTokens) != 0 {
		t.Fatalf("empty body document was skipped or encoded as null: %+v", artifact.Docs[0])
	}
	if !strings.Contains(string(first), `"body_tokens":[]`) {
		t.Fatalf("empty body is not retained as an empty token array: %s", first)
	}
}

func TestQueryTermsNormalizeAndDeduplicate(t *testing.T) {
	builder := New(config.MustDefault())
	terms := builder.QueryTerms([]string{"ＧＯ", "语言", "go", "the"})
	expected := []string{"go", "言", "语", "语言"}
	if strings.Join(terms, ",") != strings.Join(expected, ",") {
		t.Fatalf("query terms = %#v, want %#v", terms, expected)
	}
}

func termForID(artifact *Artifact, id int32) string {
	for _, term := range artifact.Terms {
		if term.ID == id {
			return term.Term
		}
	}
	return ""
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
