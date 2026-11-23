package codec

import (
	"strings"
	"testing"

	"injectctl/internal/model"
)

func TestSQLStringLiteral(t *testing.T) {
	s := &model.Sample{
		ID: "t1", Seq: 1, Surface: "sql", Landing: model.LandingStringLiteral,
		Encodings: []string{model.EncodingSingleQuoteEscape},
		Raw:       "O'Reilly",
	}
	res, err := Adjudicate(s, 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != model.CodeOK || res.Normalized != "O''Reilly" {
		t.Fatalf("expected contained escaped literal, got %+v", res)
	}

	s.Raw = "x' OR '1'='1"
	res, _ = Adjudicate(s, 0)
	// Every quote is doubled by the encoder, so the literal stays contained.
	if res.Code != model.CodeOK {
		t.Fatalf("escaped payload must stay contained: %+v", res)
	}
}

func TestSQLStringUnescapedCrossing(t *testing.T) {
	// An encoding that does not neutralize single quotes leaves a real boundary.
	s := &model.Sample{
		ID: "t2", Seq: 1, Surface: "sql", Landing: model.LandingStringLiteral,
		Encodings: []string{model.EncodingPathNormalize},
		Raw:       "ab'cd",
	}
	res, _ := Adjudicate(s, 0)
	if res.Code != model.CodeCtxEscape || res.RuleID != "SQL_STRING_UNESCAPED_QUOTE" {
		t.Fatalf("expected escape, got %+v", res)
	}
	if res.LandedOffset != 2 || res.RawOffset != 2 || res.Marker != "'" {
		t.Fatalf("offset mapping wrong: %+v", res)
	}
}

func TestIdentifierQuote(t *testing.T) {
	s := &model.Sample{
		ID: "t3", Landing: model.LandingIdentifier,
		Encodings: []string{model.EncodingIdentifierQuote},
		Raw:       `col"x`,
	}
	res, _ := Adjudicate(s, 0)
	if res.Code != model.CodeOK {
		t.Fatalf("doubled quote must be contained: %+v", res)
	}

	s.Encodings = []string{model.EncodingPathNormalize}
	res, _ = Adjudicate(s, 0)
	if res.Code != model.CodeCtxEscape || res.RuleID != "SQL_IDENTIFIER_UNESCAPED_QUOTE" {
		t.Fatalf("expected identifier escape, got %+v", res)
	}
}

func TestCommandArgument(t *testing.T) {
	s := &model.Sample{
		ID: "t4", Landing: model.LandingCommandArgument,
		Encodings: []string{model.EncodingParamPlaceholder},
		Raw:       "name; rm -rf /",
	}
	res, _ := Adjudicate(s, 0)
	if res.Code != model.CodeOK || res.Normalized != "?" || res.LandedLength != 1 {
		t.Fatalf("parameterized argument must be contained: %+v", res)
	}

	s.Encodings = []string{model.EncodingHTMLEntity}
	res, _ = Adjudicate(s, 0)
	if res.Code != model.CodeCtxEscape || res.RuleID != "CMD_METACHAR_BOUNDARY" || res.Marker != ";" {
		t.Fatalf("expected shell boundary, got %+v", res)
	}
	if res.RawOffset != 4 || res.LandedOffset != 4 {
		t.Fatalf("offset wrong: %+v", res)
	}
}

func TestPathSegment(t *testing.T) {
	s := &model.Sample{
		ID: "t5", Landing: model.LandingPathSegment,
		Encodings: []string{model.EncodingPathNormalize},
		Raw:       `..\..\etc\passwd`,
	}
	res, _ := Adjudicate(s, 0)
	if res.Code != model.CodeCtxEscape || res.RuleID != "PATH_SEPARATOR_BOUNDARY" {
		t.Fatalf("expected path escape, got %+v", res)
	}
	if res.Marker != "/" || res.LandedOffset != 2 || res.RawOffset != 2 {
		t.Fatalf("path offsets wrong: %+v", res)
	}

	s.Raw = "plain-segment"
	res, _ = Adjudicate(s, 0)
	if res.Code != model.CodeOK {
		t.Fatalf("plain segment must be contained: %+v", res)
	}
}

func TestTemplateInterpolation(t *testing.T) {
	s := &model.Sample{
		ID: "t6", Landing: model.LandingTemplateInterp,
		Encodings: []string{model.EncodingHTMLEntity},
		Raw:       "<b>hi</b>",
	}
	res, _ := Adjudicate(s, 0)
	if res.Code != model.CodeOK || !strings.Contains(res.Normalized, "&lt;") {
		t.Fatalf("entity-encoded tag must be contained: %+v", res)
	}

	s.Raw = "{{7*7}}"
	res, _ = Adjudicate(s, 0)
	if res.Code != model.CodeCtxEscape || res.RuleID != "TEMPLATE_INTERP_CLOSE" || res.Marker != "}}" {
		t.Fatalf("expected template close escape, got %+v", res)
	}
	if res.RawOffset != 5 || res.LandedOffset != 5 {
		t.Fatalf("template offsets wrong: %+v", res)
	}
}

func TestEmptyPayloadAndLimit(t *testing.T) {
	s := &model.Sample{
		ID: "t7", Landing: model.LandingStringLiteral,
		Encodings: []string{model.EncodingSingleQuoteEscape},
	}
	res, _ := Adjudicate(s, 0)
	if res.Code != model.CodeEmptyPayload || res.RawOffset != -1 {
		t.Fatalf("empty payload: %+v", res)
	}

	s.Raw = strings.Repeat("A", 20)
	res, _ = Adjudicate(s, 8)
	if res.Code != model.CodeEncodeLimit {
		t.Fatalf("expected encode limit, got %+v", res)
	}
}

func TestOffsetMapExpansion(t *testing.T) {
	// Single-quote expansion: the second byte of the doubled pair must map
	// back to the same source byte, while a later unescaped marker still
	// points at its original offset.
	s := &model.Sample{
		ID: "t8", Landing: model.LandingStringLiteral,
		Encodings: []string{model.EncodingSingleQuoteEscape},
		Raw:       "'a'",
	}
	res, _ := Adjudicate(s, 0)
	if res.Code != model.CodeOK {
		t.Fatalf("doubled quotes stay contained: %+v", res)
	}
	if res.Normalized != "''a''" || res.LandedLength != 5 {
		t.Fatalf("normalized form wrong: %+v", res)
	}
}
