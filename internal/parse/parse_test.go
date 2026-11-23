package parse

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"injectctl/internal/model"
)

func block(id, surface, raw string) []byte {
	body := append([]byte("id="+id+"\n"), []byte("surface="+surface+"\n")...)
	body = append(body, []byte(raw)...)
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], uint32(len(body)))
	return append(p[:], body...)
}

func TestRoundTripAndFaultTolerance(t *testing.T) {
	doc := model.ContextFile{
		ContextVersion: "ctx-test",
		Declarations: []model.ContextDecl{
			{ID: "ok", Surface: "sql", Landing: model.LandingStringLiteral,
				Encodings: []string{model.EncodingSingleQuoteEscape}},
			{ID: "bad-landing", Surface: "sql", Landing: "nonsense",
				Encodings: []string{model.EncodingSingleQuoteEscape}},
			{ID: "no-encoding", Surface: "sql", Landing: model.LandingStringLiteral},
			{ID: "ok", Surface: "sql", Landing: model.LandingStringLiteral,
				Encodings: []string{model.EncodingSingleQuoteEscape}},
		},
	}
	var stream bytes.Buffer
	stream.Write(block("ok", "sql", "abc"))
	stream.Write(block("orphan", "sql", "xyz"))

	blocks, err := ReadPayloadBlocks(&stream)
	if err != nil {
		t.Fatal(err)
	}
	out := BuildSamples(doc, blocks)

	if len(out.Samples) != 1 || out.Samples[0].ID != "ok" {
		t.Fatalf("valid samples: %+v", out.Samples)
	}
	if len(out.Errors) != 4 {
		t.Fatalf("expected 3 non-fatal errors, got %d: %+v", len(out.Errors), out.Errors)
	}
	for _, e := range out.Errors {
		if e.Code != model.CodeContextInvalid {
			t.Fatalf("unexpected code %s", e.Code)
		}
	}
}

func TestTruncatedStream(t *testing.T) {
	var stream bytes.Buffer
	stream.Write([]byte{0, 0, 0, 10, 'i'})
	if _, err := ReadPayloadBlocks(&stream); err == nil ||
		!strings.Contains(err.Error(), model.CodeContextInvalid) {
		t.Fatalf("expected E_CONTEXT_INVALID truncation, got %v", err)
	}
}

func TestOutOfBoundsSurface(t *testing.T) {
	doc := model.ContextFile{Declarations: []model.ContextDecl{{
		ID: "s", Surface: "sql", Landing: model.LandingStringLiteral,
		Encodings: []string{model.EncodingSingleQuoteEscape},
	}}}
	blocks := []model.PayloadBlock{{ID: "s", Surface: "command", Raw: "x"}}
	out := BuildSamples(doc, blocks)
	if len(out.Errors) != 1 || out.Errors[0].Code != model.CodeContextOutOfBounds {
		t.Fatalf("expected E_CONTEXT_OOB, got %+v", out.Errors)
	}
}

func TestSourceOffsetsRecorded(t *testing.T) {
	docJSON := `{"context_version":"v","declarations":[
		{"id":"a","surface":"sql","landing":"string_literal","encodings":["single_quote_escape"]},
		{"id":"b","surface":"sql","landing":"string_literal","encodings":["single_quote_escape"]}]}`
	doc, err := ParseContextFile(strings.NewReader(docJSON))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Declarations[0].SourceOffset <= 0 || doc.Declarations[1].SourceOffset <= doc.Declarations[0].SourceOffset {
		t.Fatalf("offsets not recorded: %+v", doc.Declarations)
	}
}
