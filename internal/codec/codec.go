// Package codec performs deterministic context encoding simulation and
// boundary-escape adjudication based solely on encoded byte positions and
// context boundary markers (no keyword blacklists).
package codec

import (
	"fmt"

	"injectctl/internal/model"
)

// offsetMap maps every normalized byte position back to its source byte
// position inside the original payload.
type offsetMap map[int]int

type encoder func(in []byte, m offsetMap) ([]byte, offsetMap)

// encoders is the deterministic registry of encoding simulations.
var encoders = map[string]encoder{
	model.EncodingSingleQuoteEscape: encodeSingleQuote,
	model.EncodingIdentifierQuote:   encodeIdentifierQuote,
	model.EncodingParamPlaceholder:  encodeParamPlaceholder,
	model.EncodingPathNormalize:     encodePathNormalize,
	model.EncodingHTMLEntity:        encodeHTMLEntity,
}

// placeholderToken stands in for a fully parameter-bound value. It is never
// scanned for boundary markers because the value is handed to the driver as
// data rather than concatenated into the command string.
const placeholderToken = "?"

// Result carries the normalized form and the first boundary crossing, if any.
type Result struct {
	Normalized   string
	Code         string
	RuleID       string
	Marker       string
	RawOffset    int // -1 when no crossing
	LandedOffset int // -1 when no crossing
	RawLength    int
	LandedLength int
}

// Adjudicate encodes the sample according to its declared encoding chain and
// locates the first boundary crossing inside the normalized form.
// maxEncodedBytes <= 0 disables the per-sample encoded byte budget.
func Adjudicate(s *model.Sample, maxEncodedBytes int) (Result, error) {
	if len(s.Raw) == 0 {
		return Result{
			Code:         model.CodeEmptyPayload,
			RawOffset:    -1,
			LandedOffset: -1,
			RawLength:    0,
			LandedLength: 0,
		}, nil
	}

	cur := []byte(s.Raw)
	m := offsetMap{}
	for i := range cur {
		m[i] = i
	}

	// A parameterized placeholder binds the whole value: every boundary
	// marker is neutralized by construction and no scanning is needed.
	parameterized := false
	for _, enc := range s.Encodings {
		fn, ok := encoders[enc]
		if !ok {
			return Result{}, fmt.Errorf("%s: unsupported encoding %q", model.CodeContextInvalid, enc)
		}
		cur, m = fn(cur, m)
		if enc == model.EncodingParamPlaceholder {
			parameterized = true
		}
	}

	res := Result{
		Normalized:   string(cur),
		RawLength:    len(s.Raw),
		LandedLength: len(cur),
		RawOffset:    -1,
		LandedOffset: -1,
		Code:         model.CodeOK,
	}

	if maxEncodedBytes > 0 && len(cur) > maxEncodedBytes {
		res.Code = model.CodeEncodeLimit
		res.Normalized = ""
		res.RawOffset = -1
		res.LandedOffset = -1
		return res, nil
	}

	if parameterized {
		return res, nil
	}

	if rule, marker, landed := scanBoundary(s.Landing, cur); landed >= 0 {
		res.Code = model.CodeCtxEscape
		res.RuleID = rule
		res.Marker = marker
		res.LandedOffset = landed
		res.RawOffset = m[landed]
	}
	return res, nil
}

func encodeSingleQuote(in []byte, m offsetMap) ([]byte, offsetMap) {
	out := make([]byte, 0, len(in)*2)
	nm := offsetMap{}
	for i, b := range in {
		if b == '\'' {
			pos := len(out)
			out = append(out, '\'', '\'')
			nm[pos] = m[i]
			nm[pos+1] = m[i]
		} else {
			nm[len(out)] = m[i]
			out = append(out, b)
		}
	}
	return out, nm
}

func encodeIdentifierQuote(in []byte, m offsetMap) ([]byte, offsetMap) {
	out := make([]byte, 0, len(in)*2)
	nm := offsetMap{}
	for i, b := range in {
		if b == '"' {
			pos := len(out)
			out = append(out, '"', '"')
			nm[pos] = m[i]
			nm[pos+1] = m[i]
		} else {
			nm[len(out)] = m[i]
			out = append(out, b)
		}
	}
	return out, nm
}

func encodeParamPlaceholder(_ []byte, _ offsetMap) ([]byte, offsetMap) {
	return []byte(placeholderToken), offsetMap{0: 0}
}

func encodePathNormalize(in []byte, m offsetMap) ([]byte, offsetMap) {
	out := make([]byte, 0, len(in))
	nm := offsetMap{}
	for i, b := range in {
		if b == '\\' {
			nm[len(out)] = m[i]
			out = append(out, '/')
		} else {
			nm[len(out)] = m[i]
			out = append(out, b)
		}
	}
	return out, nm
}

var htmlEntities = map[byte][]byte{
	'&':  []byte("&amp;"),
	'<':  []byte("&lt;"),
	'>':  []byte("&gt;"),
	'"':  []byte("&quot;"),
	'\'': []byte("&#39;"),
}

func encodeHTMLEntity(in []byte, m offsetMap) ([]byte, offsetMap) {
	out := make([]byte, 0, len(in)*6)
	nm := offsetMap{}
	for i, b := range in {
		if ent, ok := htmlEntities[b]; ok {
			start := len(out)
			out = append(out, ent...)
			for k := range ent {
				nm[start+k] = m[i]
			}
		} else {
			nm[len(out)] = m[i]
			out = append(out, b)
		}
	}
	return out, nm
}

// shellMetacharacters are actual shell grammar boundary characters. This is a
// structural marker table, not a keyword blacklist: detection is based on the
// encoded byte occupying a boundary-delimiter position.
var shellMetacharacters = map[byte]bool{
	';': true, '|': true, '&': true, '$': true, '(': true, ')': true,
	'<': true, '>': true, '`': true, ' ': true, '\t': true, '\n': true,
	'\r': true, '*': true, '[': true, ']': true, '{': true, '}': true,
	'~': true, '#': true, '=': true, '!': true, '\'': true, '"': true,
	'\\': true,
}

// scanBoundary returns the rule id, the boundary marker text and its first
// byte position inside the normalized payload. Position -1 means contained.
func scanBoundary(landing string, norm []byte) (rule, marker string, pos int) {
	switch landing {
	case model.LandingStringLiteral:
		for i := 0; i < len(norm); i++ {
			if norm[i] == '\'' {
				if i+1 < len(norm) && norm[i+1] == '\'' {
					i++ // escaped pair, stay inside the literal
					continue
				}
				return "SQL_STRING_UNESCAPED_QUOTE", "'", i
			}
		}
	case model.LandingIdentifier:
		for i := 0; i < len(norm); i++ {
			if norm[i] == '"' {
				if i+1 < len(norm) && norm[i+1] == '"' {
					i++
					continue
				}
				return "SQL_IDENTIFIER_UNESCAPED_QUOTE", "\"", i
			}
		}
	case model.LandingCommandArgument:
		for i, b := range norm {
			if shellMetacharacters[b] {
				return "CMD_METACHAR_BOUNDARY", string(b), i
			}
		}
	case model.LandingPathSegment:
		for i, b := range norm {
			if b == '/' {
				return "PATH_SEPARATOR_BOUNDARY", "/", i
			}
		}
	case model.LandingTemplateInterp:
		closePos, tagPos := -1, -1
		for i := 0; i < len(norm)-1; i++ {
			if norm[i] == '}' && norm[i+1] == '}' && closePos == -1 {
				closePos = i
			}
		}
		for i, b := range norm {
			if b == '<' {
				tagPos = i
				break
			}
		}
		switch {
		case closePos >= 0 && (tagPos < 0 || closePos <= tagPos):
			return "TEMPLATE_INTERP_CLOSE", "}}", closePos
		case tagPos >= 0:
			return "TEMPLATE_TAG_OPEN", "<", tagPos
		}
	}
	return "", "", -1
}
