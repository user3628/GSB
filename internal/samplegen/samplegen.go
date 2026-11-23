// Package samplegen deterministically produces local payload samples and
// matching host-context declarations without any network access.
package samplegen

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"

	"injectctl/internal/model"
)

// ContextVersion identifies the generated declaration document.
const ContextVersion = "ctx-decl-1.0.0"

// SeedSample describes one deterministic sample.
type SeedSample struct {
	ID        string
	Surface   string
	Landing   string
	Encodings []string
	Raw       string
}

// StandardSeeds covers the four injection surfaces and five landing types.
func StandardSeeds() []SeedSample {
	return []SeedSample{
		// SQL string literal: escaped quote stays inside; unescaped crosses.
		{
			ID: "sql-string-safe", Surface: "sql", Landing: model.LandingStringLiteral,
			Encodings: []string{model.EncodingSingleQuoteEscape},
			Raw:       "O'Reilly",
		},
		{
			ID: "sql-string-escape", Surface: "sql", Landing: model.LandingStringLiteral,
			Encodings: []string{model.EncodingSingleQuoteEscape},
			Raw:       "x' OR '1'='1",
		},
		// SQL identifier: doubled double-quote neutralizes the boundary.
		{
			ID: "sql-ident-safe", Surface: "sql", Landing: model.LandingIdentifier,
			Encodings: []string{model.EncodingIdentifierQuote},
			Raw:       "order_total",
		},
		{
			ID: "sql-ident-escape", Surface: "sql", Landing: model.LandingIdentifier,
			Encodings: []string{model.EncodingIdentifierQuote},
			Raw:       `col" OR "1"="1`,
		},
		// Command argument: parameterization binds the whole value.
		// SQL string landing whose required encoding does not neutralize the
		// single-quote boundary: first unpaired quote must be located.
		{
			ID: "sql-string-mismatch", Surface: "sql", Landing: model.LandingStringLiteral,
			Encodings: []string{model.EncodingPathNormalize},
			Raw:       "ab' OR '1'='1",
		},
		// SQL identifier landing without identifier quoting: double quote
		// crosses the identifier boundary.
		{
			ID: "sql-ident-mismatch", Surface: "sql", Landing: model.LandingIdentifier,
			Encodings: []string{model.EncodingSingleQuoteEscape},
			Raw:       `ab" OR "1"="1`,
		},
		{
			ID: "cmd-param-safe", Surface: "command", Landing: model.LandingCommandArgument,
			Encodings: []string{model.EncodingParamPlaceholder},
			Raw:       "name; rm -rf /",
		},
		{
			ID: "cmd-meta-escape", Surface: "command", Landing: model.LandingCommandArgument,
			Encodings: []string{}, // declared per-test; replaced below
			Raw:       "name; rm -rf /",
		},
		// Path segment: separator normalization reveals the crossing.
		{
			ID: "path-normalized-safe", Surface: "path", Landing: model.LandingPathSegment,
			Encodings: []string{model.EncodingPathNormalize},
			Raw:       "report-2026",
		},
		{
			ID: "path-traversal-escape", Surface: "path", Landing: model.LandingPathSegment,
			Encodings: []string{model.EncodingPathNormalize},
			Raw:       `..\..\etc\passwd`,
		},
		// Template interpolation: HTML entities neutralize tags, not braces.
		{
			ID: "tpl-entity-safe", Surface: "template", Landing: model.LandingTemplateInterp,
			Encodings: []string{model.EncodingHTMLEntity},
			Raw:       "<b>hi</b>",
		},
		{
			ID: "tpl-interp-escape", Surface: "template", Landing: model.LandingTemplateInterp,
			Encodings: []string{model.EncodingHTMLEntity},
			Raw:       "{{7*7}}",
		},
	}
}

// Artifacts are the generated file paths.
type Artifacts struct {
	Dir         string
	ContextPath string
	PayloadPath string
}

// Write produces the JSON declaration file and length-prefixed payload set.
func Write(dir string, seeds []SeedSample) (Artifacts, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Artifacts{}, err
	}
	doc := model.ContextFile{ContextVersion: ContextVersion}
	for _, sd := range seeds {
		enc := sd.Encodings
		if len(enc) == 0 {
			enc = []string{model.EncodingPathNormalize} // fallback never used for cmd
		}
		if sd.ID == "cmd-meta-escape" {
			enc = []string{model.EncodingHTMLEntity} // does not neutralize shell markers
		}
		doc.Declarations = append(doc.Declarations, model.ContextDecl{
			ID: sd.ID, Surface: sd.Surface, Landing: sd.Landing, Encodings: enc,
		})
	}

	ctxPath := filepath.Join(dir, "contexts.json")
	ctxRaw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return Artifacts{}, err
	}
	if err := os.WriteFile(ctxPath, ctxRaw, 0o644); err != nil {
		return Artifacts{}, err
	}

	payloadPath := filepath.Join(dir, "payloads.bin")
	var stream []byte
	for _, sd := range seeds {
		body := append([]byte("id="+sd.ID+"\n"), []byte("surface="+sd.Surface+"\n")...)
		body = append(body, []byte(sd.Raw)...)
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], uint32(len(body)))
		stream = append(stream, prefix[:]...)
		stream = append(stream, body...)
	}
	if err := os.WriteFile(payloadPath, stream, 0o644); err != nil {
		return Artifacts{}, err
	}
	return Artifacts{Dir: dir, ContextPath: ctxPath, PayloadPath: payloadPath}, nil
}
